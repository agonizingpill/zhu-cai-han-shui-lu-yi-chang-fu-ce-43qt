import json, os
from datetime import datetime, timezone
from pathlib import Path
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from psycopg2.extras import RealDictCursor
from .utils.db import conn, one, all_rows
from .utils.security import hp, token, user, roles
from .config.settings import MIN_SAMPLE_POINTS
from .services.queue import put
from .services import calibration as calib
from .middlewares.audit import AuditMiddleware
from .middlewares.rate_limit import RateLimitMiddleware

app=FastAPI(title='Bamboo Moisture Recheck',version='1.0.0')
app.add_middleware(CORSMiddleware,allow_origins=['*'],allow_methods=['*'],allow_headers=['*'])
app.add_middleware(RateLimitMiddleware); app.add_middleware(AuditMiddleware)
SCHEMA=(Path(__file__).parent/'models/schema.sql').read_text()

class Login(BaseModel): username:str; password:str
class LotIn(BaseModel): lot_code:str; origin:str; harvest_date:str; species:str='毛竹'; weight_kg:float=Field(gt=0); target_moisture:float=Field(gt=0,lt=100)
class SampleIn(BaseModel): point_code:str; moisture_pct:float=Field(ge=0,le=100); instrument_id:str
class ScheduleIn(BaseModel): kiln_code:str; scheduled_start:str; scheduled_end:str
class ResultIn(BaseModel): post_moisture:float=Field(ge=0,le=100); result:str; notes:str=''
class ReleaseIn(BaseModel): decision:str; reason:str
class CalibrationIn(BaseModel): instrument_id:str; window_start:str; window_end:str; reason:str

def detail(lot_id):
    l=one('SELECT * FROM bamboo_lots WHERE id=%s',(lot_id,))
    if not l: raise HTTPException(404,'竹材批次不存在')
    ss=all_rows('SELECT * FROM moisture_samples WHERE lot_id=%s ORDER BY point_code',(lot_id,)); a=one('SELECT * FROM anomaly_lots WHERE lot_id=%s',(lot_id,))
    traces=all_rows("SELECT t.*,c.instrument_id,c.window_start,c.window_end,c.status calibration_status FROM calibration_traces t JOIN instrument_calibrations c ON c.id=t.calibration_id WHERE t.lot_id=%s ORDER BY t.updated_at DESC,t.id DESC",(lot_id,))
    rels=all_rows('SELECT decision,reason,decided_at FROM release_decisions WHERE lot_id=%s ORDER BY id DESC',(lot_id,))
    return {'lot':dict(l),'samples':[dict(x) for x in ss],'anomaly':dict(a) if a else None,'calibration_traces':[dict(x) for x in traces],'releases':[dict(x) for x in rels]}

def reference_vectors(exclude):
    rows=all_rows("SELECT l.id,array_agg(s.moisture_pct ORDER BY s.point_code) vals FROM bamboo_lots l JOIN moisture_samples s ON s.lot_id=l.id LEFT JOIN anomaly_lots a ON a.lot_id=l.id WHERE l.id<>%s AND a.id IS NULL GROUP BY l.id HAVING count(s.id)>=%s",(exclude,MIN_SAMPLE_POINTS))
    import numpy as np
    return [[float(np.mean(v)), float(np.std(v)), float(min(v)), float(max(v))] for v in [[float(x) for x in r['vals']] for r in rows]]

@app.on_event('startup')
def startup():
    with conn() as c:
        with c.cursor() as cur: cur.execute(SCHEMA)

@app.get('/health')
def health():
    with conn() as c:
        with c.cursor() as cur: cur.execute('SELECT 1'); cur.fetchone()
    return {'status':'ok','service':'backend'}

@app.post('/auth/login')
def login(d:Login):
    u=one('SELECT * FROM users WHERE username=%s',(d.username,))
    if not u or u['password_hash']!=hp(d.password): raise HTTPException(401,'账号或密码错误')
    return {'access_token':token(u),'token_type':'bearer','user':{'username':u['username'],'display_name':u['display_name'],'role':u['role']}}

@app.get('/dashboard')
def dashboard(u=Depends(roles('operator','supervisor','viewer'))):
    return {'lots':one('SELECT count(*) n FROM bamboo_lots')['n'],'sampled':one("SELECT count(*) n FROM bamboo_lots WHERE status<>'RECEIVED'")['n'],'open_anomalies':one("SELECT count(*) n FROM anomaly_lots WHERE status IN ('OPEN','SCHEDULED','RECHECKING')")['n'],'pending_release':one("SELECT count(*) n FROM bamboo_lots WHERE status='CLEARED'")['n']}

@app.get('/lots')
def lots(u=Depends(roles('operator','supervisor','viewer'))):
    return [dict(x) for x in all_rows("SELECT l.*,count(s.id) sample_count,a.status anomaly_status,a.standardized_distance FROM bamboo_lots l LEFT JOIN moisture_samples s ON s.lot_id=l.id LEFT JOIN anomaly_lots a ON a.lot_id=l.id GROUP BY l.id,a.status,a.standardized_distance ORDER BY l.received_at DESC")]

@app.post('/lots')
def create_lot(d:LotIn,u=Depends(roles('operator','supervisor'))):
    try:
        with conn() as c:
            with c.cursor() as cur:
                cur.execute('INSERT INTO bamboo_lots(lot_code,origin,harvest_date,species,weight_kg,target_moisture,operator_id) VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id',(d.lot_code,d.origin,d.harvest_date,d.species,d.weight_kg,d.target_moisture,u['id']))
                lid=cur.fetchone()[0]
        return detail(lid)
    except Exception as e:
        if 'duplicate key' in str(e).lower(): raise HTTPException(409,'批次号已存在，请使用已有批次')
        raise

@app.get('/lots/{lot_id}')
def get_lot(lot_id:int,u=Depends(roles('operator','supervisor','viewer'))): return detail(lot_id)

@app.post('/lots/{lot_id}/samples')
def sample(lot_id:int,d:SampleIn,u=Depends(roles('operator','supervisor'))):
    l=one('SELECT status FROM bamboo_lots WHERE id=%s',(lot_id,))
    if not l: raise HTTPException(404,'批次不存在')
    if l['status']=='RELEASED': raise HTTPException(409,'已放行批次不能补录采样')
    try:
        with conn() as c:
            with c.cursor() as cur:
                cur.execute('INSERT INTO moisture_samples(lot_id,point_code,moisture_pct,instrument_id) VALUES(%s,%s,%s,%s)',(lot_id,d.point_code,d.moisture_pct,d.instrument_id))
                cur.execute("UPDATE bamboo_lots SET status='SAMPLED' WHERE id=%s AND status='RECEIVED'",(lot_id,))
    except Exception as e:
        if 'duplicate key' in str(e).lower(): raise HTTPException(409,'同一测点已采样')
        raise
    return detail(lot_id)

@app.post('/lots/{lot_id}/analyze')
def analyze(lot_id:int,u=Depends(roles('operator','supervisor'))):
    d=detail(lot_id); n=len(d['samples'])
    if n<MIN_SAMPLE_POINTS: raise HTTPException(409,f'采样点不足，至少需要 {MIN_SAMPLE_POINTS} 个测点，当前 {n} 个')
    refs=all_rows("SELECT l.id FROM bamboo_lots l JOIN moisture_samples s ON s.lot_id=l.id LEFT JOIN anomaly_lots a ON a.lot_id=l.id WHERE l.id<>%s AND a.id IS NULL GROUP BY l.id HAVING count(s.id)>=%s",(lot_id,MIN_SAMPLE_POINTS))
    with conn() as c:
        with c.cursor(cursor_factory=RealDictCursor) as cur:
            # 若该批次仍有未闭环的校准扣留，重新送检即为按可信读数重算，直接闭环原追查记录。
            cur.execute("SELECT calibration_id FROM calibration_traces WHERE lot_id=%s AND disposition IN('RECOMPUTING','HELD') ORDER BY id DESC LIMIT 1",(lot_id,))
            pending=cur.fetchone()
            cid=pending['calibration_id'] if pending else None
            if cid:
                cur.execute("INSERT INTO analysis_jobs(lot_id,kind,calibration_id,input_summary,provenance_captured) VALUES(%s,'RECOMPUTE',%s,%s,true) RETURNING id",(lot_id,cid,json.dumps({'sample_count':n,'points':[s['point_code'] for s in d['samples']],'reason':'reanalyze'})))
            else:
                cur.execute('INSERT INTO analysis_jobs(lot_id,input_summary,provenance_captured) VALUES(%s,%s,true) RETURNING id',(lot_id,json.dumps({'sample_count':n,'points':[s['point_code'] for s in d['samples']]})))
            jid=cur.fetchone()['id']
            # 固化初检实际读到的测点与训练参考批次，供校准事件追溯。
            for s in d['samples']: cur.execute('INSERT INTO analysis_job_samples(job_id,sample_id) VALUES(%s,%s) ON CONFLICT DO NOTHING',(jid,s['id']))
            for r in refs: cur.execute('INSERT INTO analysis_job_references(job_id,reference_lot_id) VALUES(%s,%s) ON CONFLICT DO NOTHING',(jid,r['id']))
            if cid:
                cur.execute("UPDATE analysis_jobs SET calibration_id=%s WHERE id=%s",(cid,jid))
                cur.execute("UPDATE calibration_traces SET job_id=%s,updated_at=now() WHERE lot_id=%s AND calibration_id=%s AND disposition IN('RECOMPUTING','HELD')",(jid,lot_id,cid))
    put(jid); return {'job_id':jid,'status':'QUEUED'}

@app.get('/anomalies')
def anomalies(u=Depends(roles('operator','supervisor','viewer'))):
    rows=all_rows("SELECT a.*,l.lot_code,l.origin,l.target_moisture,l.weight_kg,k.id recheck_id,k.kiln_code,k.scheduled_start,k.scheduled_end,k.status recheck_status,k.post_moisture,k.result FROM anomaly_lots a JOIN bamboo_lots l ON l.id=a.lot_id LEFT JOIN LATERAL(SELECT * FROM kiln_rechecks z WHERE z.anomaly_id=a.id ORDER BY z.id DESC LIMIT 1) k ON true ORDER BY a.detected_at DESC")
    return [dict(x) for x in rows]

@app.post('/anomalies/{aid}/schedule')
def schedule(aid:int,d:ScheduleIn,u=Depends(roles('operator','supervisor'))):
    a=one('SELECT * FROM anomaly_lots WHERE id=%s',(aid,))
    if not a: raise HTTPException(404,'异常批次不存在')
    conflict=one("SELECT id,anomaly_id,kiln_code,scheduled_start,scheduled_end FROM kiln_rechecks WHERE kiln_code=%s AND status IN('SCHEDULED','RESCHEDULED','IN_PROGRESS') AND scheduled_start<%s::timestamptz AND scheduled_end>%s::timestamptz",(d.kiln_code,d.scheduled_end,d.scheduled_start))
    status='RESCHEDULED' if conflict else 'SCHEDULED'
    with conn() as c:
        with c.cursor() as cur:
            cur.execute('SELECT id FROM kiln_rechecks WHERE anomaly_id=%s ORDER BY id DESC LIMIT 1',(aid,)); prev=cur.fetchone()
            cur.execute('INSERT INTO kiln_rechecks(anomaly_id,kiln_code,scheduled_start,scheduled_end,status,previous_recheck_id,created_by,notes) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id',(aid,d.kiln_code,d.scheduled_start,d.scheduled_end,status,prev[0] if prev else None,u['id'],'窑位冲突，保留原排程记录并创建重排版本' if conflict else '首次排程'))
            rid=cur.fetchone()[0]; cur.execute("UPDATE anomaly_lots SET status='SCHEDULED' WHERE id=%s",(aid,))
    return {'id':rid,'status':status,'conflict_with':dict(conflict) if conflict else None}

@app.post('/anomalies/{aid}/recheck-result')
def recheck_result(aid:int,d:ResultIn,u=Depends(roles('operator','supervisor'))):
    a=one('SELECT * FROM anomaly_lots WHERE id=%s',(aid,)); r=one("SELECT * FROM kiln_rechecks WHERE anomaly_id=%s AND status IN('SCHEDULED','RESCHEDULED','IN_PROGRESS') ORDER BY id DESC LIMIT 1",(aid,))
    if not a: raise HTTPException(404,'异常批次不存在')
    if not r: raise HTTPException(409,'没有可完成的窑干复测排程')
    passed=d.result=='PASS'; status='CLEARED' if passed else 'REJECTED'
    with conn() as c:
        with c.cursor() as cur:
            cur.execute("UPDATE kiln_rechecks SET status='COMPLETED',post_moisture=%s,result=%s,notes=%s WHERE id=%s",(d.post_moisture,d.result,d.notes,r['id']))
            cur.execute("UPDATE anomaly_lots SET status=%s,resolved_at=CASE WHEN %s THEN now() ELSE NULL END WHERE id=%s",(status,passed,aid))
            cur.execute("UPDATE bamboo_lots SET status=%s WHERE id=%s AND status<>'RELEASED'",('CLEARED' if passed else 'QUARANTINED',a['lot_id']))
    return {'status':status,'recheck_id':r['id']}

@app.get('/releases')
def releases(u=Depends(roles('operator','supervisor','viewer'))):
    return [dict(x) for x in all_rows("SELECT r.*,l.lot_code,l.origin,a.status anomaly_status FROM release_decisions r JOIN bamboo_lots l ON l.id=r.lot_id LEFT JOIN anomaly_lots a ON a.id=r.anomaly_id ORDER BY r.decided_at DESC")]

@app.post('/releases/lots/{lot_id}')
def release(lot_id:int,d:ReleaseIn,u=Depends(roles('supervisor'))):
    with conn() as c:
        with c.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('SELECT * FROM bamboo_lots WHERE id=%s FOR UPDATE',(lot_id,)); l=cur.fetchone()
            if not l: raise HTTPException(404,'批次不存在')
            cur.execute('SELECT * FROM anomaly_lots WHERE lot_id=%s',(lot_id,)); a=cur.fetchone()
            if l['status']=='RELEASED': raise HTTPException(409,'批次已出库，保留原放行决定')
            if a and a['status'] not in ('CLEARED','REJECTED'): raise HTTPException(409,'异常批次尚未完成窑干复测，不能出库')
            if d.decision=='RELEASE' and a and a['status']=='REJECTED': raise HTTPException(409,'复测未通过，不能放行')
            if d.decision=='RELEASE':
                block=calib.release_block(cur,lot_id)
                if block: raise HTTPException(409,f'校准追查未闭环，不能出库：{block["reason"]}')
            cur.execute('INSERT INTO release_decisions(lot_id,anomaly_id,decision,reason,decided_by) VALUES(%s,%s,%s,%s,%s)',(lot_id,a['id'] if a else None,d.decision,d.reason,u['id']))
            cur.execute("UPDATE bamboo_lots SET status=%s WHERE id=%s",('RELEASED' if d.decision=='RELEASE' else 'QUARANTINED',lot_id))
    return {'lot_id':lot_id,'decision':d.decision}

@app.post('/calibrations')
def register_calibration(d:CalibrationIn,u=Depends(roles('supervisor'))):
    try:
        start=datetime.fromisoformat(d.window_start.replace('Z','+00:00'))
        end=datetime.fromisoformat(d.window_end.replace('Z','+00:00'))
    except ValueError: raise HTTPException(422,'时间格式不正确')
    if end<=start: raise HTTPException(422,'校准结束时间必须晚于开始时间')
    if start.tzinfo is None: start=start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None: end=end.replace(tzinfo=timezone.utc)
    with conn() as c:
        with c.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("INSERT INTO instrument_calibrations(instrument_id,window_start,window_end,reason,registered_by) VALUES(%s,%s,%s,%s,%s) ON CONFLICT(instrument_id,window_start,window_end) DO NOTHING RETURNING *",(d.instrument_id,start,end,d.reason,u['id']))
            cal=cur.fetchone()
            if not cal:
                cur.execute('SELECT * FROM instrument_calibrations WHERE instrument_id=%s AND window_start=%s AND window_end=%s',(d.instrument_id,start,end))
                cal=cur.fetchone(); duplicate=True
            else: duplicate=False
            summary=calib.apply_calibration(cur,cal)
    for jid in summary['queued_job_ids']: put(jid)
    return {'calibration':dict(cal) if cal else None,'duplicate':duplicate,**summary}

@app.post('/calibrations/{cid}/retry')
def retry_calibration(cid:int,u=Depends(roles('supervisor'))):
    with conn() as c:
        with c.cursor(cursor_factory=RealDictCursor) as cur:
            cal=calib.load_calibration(cur,cid)
            if not cal: raise HTTPException(404,'校准登记不存在')
            summary=calib.apply_calibration(cur,cal)
            pending=calib.requeue_pending(cur,cid)
    for jid in sorted(set(summary['queued_job_ids'])|set(pending)): put(jid)
    return {'calibration_id':cid,**summary,'requeued_job_ids':pending}

@app.get('/calibrations')
def list_calibrations(u=Depends(roles('operator','supervisor','viewer'))):
    rows=all_rows("SELECT c.*,count(t.id) trace_count,count(t.id) FILTER(WHERE t.disposition IN('RECOMPUTING','HELD')) open_traces FROM instrument_calibrations c LEFT JOIN calibration_traces t ON t.calibration_id=c.id GROUP BY c.id ORDER BY c.registered_at DESC")
    return [dict(x) for x in rows]

@app.get('/calibrations/{cid}/traces')
def calibration_traces(cid:int,u=Depends(roles('operator','supervisor','viewer'))):
    if not one('SELECT id FROM instrument_calibrations WHERE id=%s',(cid,)): raise HTTPException(404,'校准登记不存在')
    rows=all_rows("SELECT t.*,l.lot_code,l.origin,l.status lot_status,j.kind job_kind,j.status job_status,j.result job_result FROM calibration_traces t JOIN bamboo_lots l ON l.id=t.lot_id LEFT JOIN analysis_jobs j ON j.id=t.job_id WHERE t.calibration_id=%s ORDER BY t.id",(cid,))
    return [dict(x) for x in rows]
