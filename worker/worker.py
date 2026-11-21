import os,json,time,hashlib,traceback,psycopg2,redis,numpy as np
from datetime import datetime,timezone
from psycopg2.extras import RealDictCursor,Json
from sklearn.preprocessing import StandardScaler
from sklearn.svm import OneClassSVM
DB=os.getenv('DATABASE_URL','postgresql://bamboo:bamboo_dev_password@db:5432/bamboo'); R=redis.Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379/0'),decode_responses=True)
Q='bamboo:moisture-analysis'; MIN_POINTS=4
def db():
 for _ in range(30):
  try:return psycopg2.connect(DB)
  except Exception:time.sleep(1)
 raise RuntimeError('db unavailable')
def vec(v):
 a=np.array(v,float); return [float(a.mean()),float(a.std()),float(a.min()),float(a.max())]
def detect(refs,vals):
 scaler=StandardScaler(); rs=scaler.fit_transform(np.array(refs,float)); x=scaler.transform(np.array(vec(vals)).reshape(1,-1)); m=OneClassSVM(kernel='rbf',gamma='scale',nu=.2);m.fit(rs);pred=int(m.predict(x)[0]);score=float(m.decision_function(x)[0]);dist=float(np.linalg.norm(x[0]));out={'seed':1075,'reference_count':len(refs),'vector':vec(vals),'prediction':pred,'decision_score':round(score,6),'standardized_distance':round(dist,6),'is_anomaly':pred==-1 or dist>=2.25};out['checksum']=hashlib.sha256(json.dumps(out,sort_keys=True).encode()).hexdigest()[:16];return out
def heartbeat():
 p=os.getenv('HEARTBEAT_PATH','/shared/worker.heartbeat')
 try:open(p,'w').write(datetime.now(timezone.utc).isoformat())
 except OSError:pass
# 排除任何 ACTIVE 校准事件覆盖的读数：这些读数不能用于放行判定。
TRUSTED="NOT EXISTS(SELECT 1 FROM instrument_calibrations c WHERE c.status='ACTIVE' AND c.instrument_id=s.instrument_id AND s.sampled_at>=c.window_start AND s.sampled_at<=c.window_end)"
def process(jid):
 c=db(); c.autocommit=True; cur=c.cursor(cursor_factory=RealDictCursor); cur.execute('SELECT * FROM analysis_jobs WHERE id=%s',(jid,)); job=cur.fetchone()
 if not job or job['status'] in ('SUCCEEDED','BLOCKED'):cur.close();c.close();return
 if job['blocked_by_calibration_id']:
  # 登记生效时仍在跑的初检：不得再用这段读数给批次放行，交由校准重算处理。
  cur.execute("UPDATE analysis_jobs SET status='BLOCKED',error=%s,finished_at=now() WHERE id=%s",('校准事件登记时该初检仍在运行，读数不可用于放行',jid));cur.close();c.close();return
 cur.execute("UPDATE analysis_jobs SET status='RUNNING' WHERE id=%s",(jid,))
 recompute=job['kind']=='RECOMPUTE'
 cur.execute(f'SELECT s.id,s.moisture_pct FROM moisture_samples s WHERE s.lot_id=%s AND {TRUSTED} ORDER BY s.point_code',(job['lot_id'],))
 own=cur.fetchall(); vals=[float(x['moisture_pct']) for x in own]; own_ids=[x['id'] for x in own]
 cur.execute(f"SELECT l.id,array_agg(s.moisture_pct ORDER BY s.point_code) vals FROM bamboo_lots l JOIN moisture_samples s ON s.lot_id=l.id LEFT JOIN anomaly_lots a ON a.lot_id=l.id WHERE l.id<>%s AND a.id IS NULL AND {TRUSTED} GROUP BY l.id HAVING count(s.id)>=%s",(job['lot_id'],MIN_POINTS))
 refs_rows=cur.fetchall(); refs=[vec([float(v) for v in x['vals']]) for x in refs_rows]; ref_ids=[x['id'] for x in refs_rows]
 cur.close();c.close()

 out=None
 if len(vals)>=MIN_POINTS:
  try: out=detect(refs,vals)
  except Exception as e:
   c=db();c.autocommit=True;c.cursor().execute("UPDATE analysis_jobs SET status='FAILED',error=%s,finished_at=now() WHERE id=%s",(str(e),jid));c.close();traceback.print_exc();return

 # 结果、来源固化与追查闭环在同一事务内完成，并与校准登记按批次行锁串行化。
 c=db(); c.autocommit=False
 try:
  cur=c.cursor(cursor_factory=RealDictCursor)
  cur.execute('SELECT * FROM bamboo_lots WHERE id=%s FOR UPDATE',(job['lot_id'],)); lot=cur.fetchone()
  cur.execute('SELECT blocked_by_calibration_id FROM analysis_jobs WHERE id=%s',(jid,)); blocked=cur.fetchone()['blocked_by_calibration_id']
  if not blocked and own_ids:
   cur.execute("SELECT 1 FROM moisture_samples s WHERE s.id=ANY(%s::int[]) AND EXISTS(SELECT 1 FROM instrument_calibrations c WHERE c.status='ACTIVE' AND c.instrument_id=s.instrument_id AND s.sampled_at>=c.window_start AND s.sampled_at<=c.window_end) LIMIT 1",(own_ids,))
   if cur.fetchone():blocked=jid
  if blocked:
   cur.execute("UPDATE analysis_jobs SET status='BLOCKED',error=%s,finished_at=now() WHERE id=%s",('初检执行期间校准事件生效，读数不可用于放行',jid));c.commit();cur.close();c.close();return
  if out is None:
   msg=f'可信测点不足，至少需要 {MIN_POINTS} 个测点，当前 {len(vals)} 个'
   cur.execute("UPDATE analysis_jobs SET status='FAILED',error=%s,finished_at=now() WHERE id=%s",(msg,jid))
   if recompute and job['calibration_id']:cur.execute("UPDATE calibration_traces SET disposition='HELD',reason=reason||'；'||%s,updated_at=now() WHERE calibration_id=%s AND lot_id=%s AND disposition IN('RECOMPUTING','HELD')",(msg,job['calibration_id'],job['lot_id']))
   c.commit();cur.close();c.close();return
  cur.execute("UPDATE analysis_jobs SET status='SUCCEEDED',result=%s,finished_at=now(),provenance_captured=true WHERE id=%s",(Json(out),jid))
  cur.execute('DELETE FROM analysis_job_samples WHERE job_id=%s',(jid,))
  for sid in own_ids:cur.execute('INSERT INTO analysis_job_samples(job_id,sample_id) VALUES(%s,%s) ON CONFLICT DO NOTHING',(jid,sid))
  cur.execute('DELETE FROM analysis_job_references WHERE job_id=%s',(jid,))
  for rid in ref_ids:cur.execute('INSERT INTO analysis_job_references(job_id,reference_lot_id) VALUES(%s,%s) ON CONFLICT DO NOTHING',(jid,rid))
  cur.execute('SELECT id FROM anomaly_lots WHERE lot_id=%s',(job['lot_id'],));a=cur.fetchone()
  done=None
  if a:cur.execute("SELECT result FROM kiln_rechecks WHERE anomaly_id=%s AND status='COMPLETED' ORDER BY id DESC LIMIT 1",(a['id'],));done=cur.fetchone()
  if recompute and done:
   # 窑干复测已闭环：校准重算只补可信判定证据，不改写已完成的复测结论；批次状态回到复测结论。
   cur.execute("UPDATE anomaly_lots SET decision_score=%s,standardized_distance=%s,model_checksum=%s WHERE id=%s",(out['decision_score'],out['standardized_distance'],out['checksum'],a['id']))
   cur.execute("UPDATE bamboo_lots SET status=%s WHERE id=%s AND status<>'RELEASED'",('CLEARED' if done['result']=='PASS' else 'QUARANTINED',job['lot_id']))
  elif out['is_anomaly']:
   if a:cur.execute("UPDATE anomaly_lots SET status='OPEN',reason='OneClassSVM识别为高湿异常，需窑干复测',decision_score=%s,standardized_distance=%s,model_checksum=%s,resolved_at=NULL WHERE id=%s",(out['decision_score'],out['standardized_distance'],out['checksum'],a['id']))
   else:cur.execute("INSERT INTO anomaly_lots(lot_id,status,reason,decision_score,standardized_distance,model_checksum) VALUES(%s,'OPEN','OneClassSVM识别为高湿异常，需窑干复测',%s,%s,%s)",(job['lot_id'],out['decision_score'],out['standardized_distance'],out['checksum']))
   cur.execute("UPDATE bamboo_lots SET status='QUARANTINED' WHERE id=%s AND status<>'RELEASED'",(job['lot_id'],))
  else:
   if a:cur.execute("UPDATE anomaly_lots SET status='CLEARED',resolved_at=now(),decision_score=%s,standardized_distance=%s,model_checksum=%s WHERE id=%s",(out['decision_score'],out['standardized_distance'],out['checksum'],a['id']))
   cur.execute("UPDATE bamboo_lots SET status='CLEARED' WHERE id=%s AND status<>'RELEASED'",(job['lot_id'],))
  # 重算实际排除掉的受影响读数，对应追查记录闭环；仍含受影响读数的保持待重算。
  cur.execute("SELECT id,affected_sample_ids,reason FROM calibration_traces WHERE lot_id=%s AND disposition IN('RECOMPUTING','HELD')",(job['lot_id'],))
  extra='可信读数重算结果：'+('识别为高湿异常，转窑干复测' if out['is_anomaly'] else '判定正常')
  if done:extra+='（窑干复测已闭环，保留原结论）'
  for t in cur.fetchall():
   if not set(t['affected_sample_ids'] or []).intersection(own_ids):
    cur.execute("UPDATE calibration_traces SET disposition='RESOLVED',reason=%s,resolved_at=now(),updated_at=now(),job_id=%s WHERE id=%s",(t['reason'].split('；')[0]+'；'+extra,jid,t['id']))
  c.commit();cur.close();c.close()
 except Exception as e:
  try:c.rollback();c.close()
  except Exception:pass
  c=db();c.autocommit=True;c.cursor().execute("UPDATE analysis_jobs SET status='FAILED',error=%s,finished_at=now() WHERE id=%s",(str(e),jid));c.close();traceback.print_exc()
heartbeat()
def main():
 while True:
  heartbeat(); item=R.blpop(Q,timeout=5)
  if item:
   try:process(json.loads(item[1])['job_id'])
   except Exception:traceback.print_exc()
if __name__=='__main__':main()
