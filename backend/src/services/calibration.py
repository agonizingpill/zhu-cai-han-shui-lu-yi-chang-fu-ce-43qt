"""校准事件登记与初检来源追溯。

一次校准登记后要做三件事：
1. 找出这批仪器/时段读数实际进入过的初检（含作为训练参考的样本）。
2. 受影响或来源不可追溯的批次暂停出库；已出库批次保留原决定。
3. 判定失效的批次用可信读数重算；可信测点不足时继续扣留。

追查记录以 (calibration_id, lot_id) 唯一，重试只更新不新增。
"""
import json

from ..config.settings import MIN_SAMPLE_POINTS

# 还需要卡住出库的处置状态
OPEN_DISPOSITIONS = ('RECOMPUTING', 'HELD')
# 已经定案、重试不再打回的处置状态
FINAL_DISPOSITIONS = ('RELEASED_KEPT', 'RESOLVED')


def load_calibration(cur, cid):
    cur.execute('SELECT * FROM instrument_calibrations WHERE id=%s', (cid,))
    return cur.fetchone()


def affected_samples(cur, cid):
    """该校准事件（仪器 + 时段）覆盖到的采样点。"""
    cur.execute(
        """SELECT s.id,s.lot_id,s.instrument_id,s.sampled_at
           FROM moisture_samples s
           JOIN instrument_calibrations c ON c.id=%s
           WHERE s.instrument_id=c.instrument_id
             AND s.sampled_at>=c.window_start AND s.sampled_at<=c.window_end""",
        (cid,),
    )
    return cur.fetchall()


def trusted_samples(cur, lot_id):
    """排除所有 ACTIVE 校准事件覆盖读数后，批次仍可信的测点。"""
    cur.execute(
        """SELECT s.id,s.point_code FROM moisture_samples s
           WHERE s.lot_id=%s AND NOT EXISTS(
             SELECT 1 FROM instrument_calibrations c
             WHERE c.status='ACTIVE' AND c.instrument_id=s.instrument_id
               AND s.sampled_at>=c.window_start AND s.sampled_at<=c.window_end)
           ORDER BY s.point_code""",
        (lot_id,),
    )
    return cur.fetchall()


def _ensure_recompute_job(cur, lot_id, cid, samples):
    """同一批次同一校准事件只保留一个在跑的重算任务；返回 (job_id, created)。"""
    cur.execute(
        """SELECT id FROM analysis_jobs
           WHERE lot_id=%s AND kind='RECOMPUTE' AND calibration_id=%s
             AND status IN ('QUEUED','RUNNING')
           ORDER BY id DESC LIMIT 1""",
        (lot_id, cid),
    )
    row = cur.fetchone()
    if row:
        return row['id'], False
    summary = {'sample_count': len(samples), 'points': [s['point_code'] for s in samples], 'reason': 'calibration'}
    cur.execute(
        """INSERT INTO analysis_jobs(lot_id,kind,calibration_id,input_summary,provenance_captured)
           VALUES(%s,'RECOMPUTE',%s,%s,true) RETURNING id""",
        (lot_id, cid, json.dumps(summary)),
    )
    jid = cur.fetchone()['id']
    for s in samples:
        cur.execute('INSERT INTO analysis_job_samples(job_id,sample_id) VALUES(%s,%s) ON CONFLICT DO NOTHING', (jid, s['id']))
    return jid, True


def _upsert_trace(cur, cid, lot_id, job_id, impact, disposition, reason, own_ids, ref_ids):
    """按 (calibration_id, lot_id) 幂等写入；已定案的处置不被重试打回。"""
    finals = list(FINAL_DISPOSITIONS)
    cur.execute(
        """INSERT INTO calibration_traces
             (calibration_id,lot_id,job_id,impact,disposition,reason,affected_sample_ids,affected_reference_lot_ids,resolved_at,updated_at)
           VALUES(%s,%s,%s,%s,%s,%s,%s::int[],%s::int[],CASE WHEN %s THEN now() ELSE NULL END,now())
           ON CONFLICT(calibration_id,lot_id) DO UPDATE SET
             job_id=COALESCE(EXCLUDED.job_id,calibration_traces.job_id),
             affected_sample_ids=EXCLUDED.affected_sample_ids,
             affected_reference_lot_ids=EXCLUDED.affected_reference_lot_ids,
             disposition=CASE WHEN calibration_traces.disposition=ANY(%s::text[])
               THEN calibration_traces.disposition ELSE EXCLUDED.disposition END,
             reason=CASE WHEN calibration_traces.disposition=ANY(%s::text[])
               THEN calibration_traces.reason ELSE EXCLUDED.reason END,
             impact=CASE WHEN calibration_traces.disposition=ANY(%s::text[])
               THEN calibration_traces.impact ELSE EXCLUDED.impact END,
             resolved_at=CASE WHEN calibration_traces.disposition=ANY(%s::text[])
               THEN calibration_traces.resolved_at ELSE EXCLUDED.resolved_at END,
             updated_at=now()""",
        (cid, lot_id, job_id, impact, disposition, reason, own_ids, ref_ids,
         disposition in FINAL_DISPOSITIONS, finals, finals, finals, finals),
    )


def _reason_text(impact, own_ids, ref_ids, extra):
    if impact == 'UNTRACEABLE':
        base = '无法追溯初检实际使用的读数来源'
    elif own_ids:
        base = '初检使用了校准仪器在该时段的读数'
        if ref_ids:
            base += '，且训练参考样本也含受影响读数'
    else:
        base = '初检训练所用参考批次含受影响读数'
    return base + ('；' + extra if extra else '')


def _untraceable_jobs(cur, instrument_id):
    """用了这台仪器、但初检既没留下读数也没留下参考批次记录，来源无法确认。"""
    cur.execute(
        """SELECT DISTINCT ON (j.lot_id) j.id,j.lot_id,j.status
           FROM analysis_jobs j
           WHERE j.kind='INITIAL' AND j.status IN ('QUEUED','RUNNING','SUCCEEDED')
             AND NOT j.provenance_captured
             AND NOT EXISTS(SELECT 1 FROM analysis_job_samples js WHERE js.job_id=j.id)
             AND NOT EXISTS(SELECT 1 FROM analysis_job_references r WHERE r.job_id=j.id)
             AND EXISTS(SELECT 1 FROM moisture_samples s WHERE s.lot_id=j.lot_id AND s.instrument_id=%s)
           ORDER BY j.lot_id,j.id DESC""",
        (instrument_id,),
    )
    return cur.fetchall()


def apply_calibration(cur, cal):
    """对一条校准事件执行幂等追溯。返回摘要（含待投递的重算任务 id）。"""
    cid = cal['id']
    affected = affected_samples(cur, cid)
    affected_sample_ids = sorted({r['id'] for r in affected})
    own_by_lot = {}
    for r in affected:
        own_by_lot.setdefault(r['lot_id'], set()).add(r['id'])

    impacted = {}  # lot_id -> {impact, own, refs, job_id}

    def touch(lot_id, impact, own=None, refs=None, job_id=None):
        d = impacted.setdefault(lot_id, {'impact': 'AFFECTED', 'own': set(), 'refs': set(), 'job_id': None})
        if impact == 'UNTRACEABLE':
            d['impact'] = 'UNTRACEABLE'
        if own:
            d['own'] |= set(own)
        if refs:
            d['refs'] |= set(refs)
        if job_id and not d['job_id']:
            d['job_id'] = job_id

    # 1) 自身读数实际进入过的初检（含已排队/在跑）。
    if affected_sample_ids:
        cur.execute(
            """SELECT js.job_id,j.lot_id,j.kind,j.status,array_agg(js.sample_id) used
               FROM analysis_job_samples js JOIN analysis_jobs j ON j.id=js.job_id
               WHERE js.sample_id=ANY(%s::int[])
               GROUP BY js.job_id,j.lot_id,j.kind,j.status""",
            (affected_sample_ids,),
        )
        for row in cur.fetchall():
            touch(row['lot_id'], 'AFFECTED', own=row['used'], job_id=row['job_id'])
            if row['kind'] == 'INITIAL' and row['status'] in ('QUEUED', 'RUNNING'):
                # 登记生效时还在跑的初检不得再用这段读数给批次放行。
                cur.execute('UPDATE analysis_jobs SET blocked_by_calibration_id=%s WHERE id=%s AND blocked_by_calibration_id IS NULL', (cid, row['job_id']))

    # 2) 训练参考样本受影响的初检/重算。
    if own_by_lot:
        ref_lot_ids = sorted(own_by_lot)
        cur.execute(
            """SELECT r.job_id,j.lot_id,r.reference_lot_id
               FROM analysis_job_references r JOIN analysis_jobs j ON j.id=r.job_id
               WHERE r.reference_lot_id=ANY(%s::int[])""",
            (ref_lot_ids,),
        )
        for row in cur.fetchall():
            touch(row['lot_id'], 'AFFECTED', refs={row['reference_lot_id']}, job_id=row['job_id'])
        cur.execute('UPDATE analysis_job_references SET references_affected=true WHERE reference_lot_id=ANY(%s::int[])', (ref_lot_ids,))

    # 3) 有受影响的自身读数、但从未建过任务的批次。
    for lot_id in own_by_lot:
        touch(lot_id, 'AFFECTED', own=own_by_lot[lot_id])

    # 4) 用了这台仪器却查不到初检来源的批次：不能确认没受影响。
    for row in _untraceable_jobs(cur, cal['instrument_id']):
        touch(row['lot_id'], 'UNTRACEABLE', job_id=row['id'])
        if row['status'] in ('QUEUED', 'RUNNING'):
            cur.execute('UPDATE analysis_jobs SET blocked_by_calibration_id=%s WHERE id=%s AND blocked_by_calibration_id IS NULL', (cid, row['id']))

    summary = {'calibration_id': cid, 'affected_samples': len(affected_sample_ids),
               'affected_lots': len(impacted), 'recomputing': 0, 'held': 0,
               'released_kept': 0, 'queued_job_ids': []}

    for lot_id in sorted(impacted):
        d = impacted[lot_id]
        cur.execute('SELECT * FROM bamboo_lots WHERE id=%s FOR UPDATE', (lot_id,))
        lot = cur.fetchone()
        if not lot:
            continue
        own_ids = sorted(d['own'])
        ref_ids = sorted(d['refs'])

        if lot['status'] == 'RELEASED':
            # 已出库批次保留原决定，只登记追查原因。
            _upsert_trace(cur, cid, lot_id, d['job_id'], d['impact'], 'RELEASED_KEPT',
                          _reason_text(d['impact'], own_ids, ref_ids, '批次已出库，保留原放行决定'),
                          own_ids, ref_ids)
            summary['released_kept'] += 1
            continue

        cur.execute('SELECT disposition FROM calibration_traces WHERE calibration_id=%s AND lot_id=%s', (cid, lot_id))
        existing = cur.fetchone()
        if existing and existing['disposition'] in FINAL_DISPOSITIONS:
            # 重试不重复生成、也不打回已闭环的判定。
            continue

        trusted = trusted_samples(cur, lot_id)
        cur.execute("UPDATE bamboo_lots SET status='QUARANTINED' WHERE id=%s", (lot_id,))
        if len(trusted) >= MIN_SAMPLE_POINTS:
            jid, created = _ensure_recompute_job(cur, lot_id, cid, trusted)
            if created:
                summary['queued_job_ids'].append(jid)
            extra = '判定失效，已按可信读数重新判定，重算完成前暂停出库'
            _upsert_trace(cur, cid, lot_id, jid, d['impact'], 'RECOMPUTING',
                          _reason_text(d['impact'], own_ids, ref_ids, extra), own_ids, ref_ids)
            summary['recomputing'] += 1
        else:
            extra = f'可信测点不足（{len(trusted)}/{MIN_SAMPLE_POINTS}），继续扣留'
            _upsert_trace(cur, cid, lot_id, d['job_id'], d['impact'], 'HELD',
                          _reason_text(d['impact'], own_ids, ref_ids, extra), own_ids, ref_ids)
            summary['held'] += 1

    return summary


def requeue_pending(cur, cid):
    """重试入口：重新投递该事件尚未开始的重算任务。"""
    cur.execute(
        """SELECT id FROM analysis_jobs
           WHERE calibration_id=%s AND kind='RECOMPUTE' AND status='QUEUED' ORDER BY id""",
        (cid,),
    )
    return [r['id'] for r in cur.fetchall()]


def release_block(cur, lot_id):
    """出库前检查：校准追查未闭环，或批次读数落在有效校准时段且尚未按可信读数重算。"""
    cur.execute(
        """SELECT t.reason,t.disposition,c.instrument_id,c.window_start,c.window_end
           FROM calibration_traces t JOIN instrument_calibrations c ON c.id=t.calibration_id
           WHERE t.lot_id=%s AND t.disposition=ANY(%s::text[])
           ORDER BY t.updated_at DESC,t.id DESC LIMIT 1""",
        (lot_id, list(OPEN_DISPOSITIONS)),
    )
    t = cur.fetchone()
    if t:
        return dict(t)

    cur.execute(
        """SELECT c.* FROM instrument_calibrations c
           WHERE c.status='ACTIVE' AND EXISTS(
             SELECT 1 FROM moisture_samples s
             WHERE s.lot_id=%s AND s.instrument_id=c.instrument_id
               AND s.sampled_at>=c.window_start AND s.sampled_at<=c.window_end)
           ORDER BY c.id DESC LIMIT 1""",
        (lot_id,),
    )
    cal = cur.fetchone()
    if not cal:
        return None
    cur.execute(
        """SELECT id FROM analysis_jobs
           WHERE lot_id=%s AND status='SUCCEEDED' AND provenance_captured
             AND created_at>=%s ORDER BY id DESC LIMIT 1""",
        (lot_id, cal['registered_at']),
    )
    if cur.fetchone():
        return None
    return {'reason': '批次含校准仪器在该时段的读数，尚未按可信读数重算',
            'disposition': 'RECOMPUTING', 'instrument_id': cal['instrument_id'],
            'window_start': cal['window_start'], 'window_end': cal['window_end']}
