CREATE TABLE IF NOT EXISTS users(id SERIAL PRIMARY KEY,username TEXT UNIQUE NOT NULL,display_name TEXT NOT NULL,role TEXT NOT NULL,password_hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bamboo_lots(id SERIAL PRIMARY KEY,lot_code TEXT UNIQUE NOT NULL,origin TEXT NOT NULL,harvest_date DATE NOT NULL,species TEXT NOT NULL,weight_kg NUMERIC(10,2) NOT NULL CHECK(weight_kg>0),target_moisture NUMERIC(5,2) NOT NULL,status TEXT NOT NULL DEFAULT 'RECEIVED',received_at TIMESTAMPTZ NOT NULL DEFAULT now(),operator_id INT REFERENCES users(id));
CREATE TABLE IF NOT EXISTS moisture_samples(id SERIAL PRIMARY KEY,lot_id INT REFERENCES bamboo_lots(id) ON DELETE CASCADE,point_code TEXT NOT NULL,moisture_pct NUMERIC(6,2) NOT NULL CHECK(moisture_pct BETWEEN 0 AND 100),instrument_id TEXT NOT NULL,sampled_at TIMESTAMPTZ NOT NULL DEFAULT now(),UNIQUE(lot_id,point_code));
CREATE TABLE IF NOT EXISTS anomaly_lots(id SERIAL PRIMARY KEY,lot_id INT UNIQUE REFERENCES bamboo_lots(id) ON DELETE CASCADE,status TEXT NOT NULL DEFAULT 'OPEN',reason TEXT NOT NULL,decision_score NUMERIC(10,6),standardized_distance NUMERIC(10,6),model_checksum TEXT,detected_at TIMESTAMPTZ DEFAULT now(),resolved_at TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS kiln_rechecks(id SERIAL PRIMARY KEY,anomaly_id INT REFERENCES anomaly_lots(id) ON DELETE CASCADE,kiln_code TEXT NOT NULL,scheduled_start TIMESTAMPTZ NOT NULL,scheduled_end TIMESTAMPTZ NOT NULL,status TEXT NOT NULL DEFAULT 'SCHEDULED',pre_moisture NUMERIC(6,2),post_moisture NUMERIC(6,2),result TEXT,notes TEXT,previous_recheck_id INT REFERENCES kiln_rechecks(id),created_by INT REFERENCES users(id));
CREATE TABLE IF NOT EXISTS release_decisions(id SERIAL PRIMARY KEY,lot_id INT REFERENCES bamboo_lots(id) ON DELETE CASCADE,anomaly_id INT REFERENCES anomaly_lots(id),decision TEXT NOT NULL,reason TEXT NOT NULL,decided_by INT REFERENCES users(id),decided_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS analysis_jobs(id SERIAL PRIMARY KEY,lot_id INT REFERENCES bamboo_lots(id) ON DELETE CASCADE,status TEXT NOT NULL DEFAULT 'QUEUED',input_summary JSONB NOT NULL,result JSONB,error TEXT,created_at TIMESTAMPTZ DEFAULT now(),finished_at TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS audit_logs(id SERIAL PRIMARY KEY,action TEXT NOT NULL,path TEXT,method TEXT,status_code INT,created_at TIMESTAMPTZ DEFAULT now());

-- 校准事件：主管登记哪台仪器在哪个时段读偏。
CREATE TABLE IF NOT EXISTS instrument_calibrations(
 id SERIAL PRIMARY KEY,
 instrument_id TEXT NOT NULL,
 window_start TIMESTAMPTZ NOT NULL,
 window_end TIMESTAMPTZ NOT NULL,
 reason TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'ACTIVE',
 registered_by INT REFERENCES users(id),
 registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 recomputed_at TIMESTAMPTZ,
 CHECK(window_end>window_start)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_instrument_calibrations_window ON instrument_calibrations(instrument_id,window_start,window_end);

-- 初检来源：登记生效时用于判断哪些在跑/已跑的初检真的读过这批读数。
ALTER TABLE analysis_jobs ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'INITIAL';
ALTER TABLE analysis_jobs ADD COLUMN IF NOT EXISTS provenance_captured BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE analysis_jobs ADD COLUMN IF NOT EXISTS calibration_id INT REFERENCES instrument_calibrations(id);
ALTER TABLE analysis_jobs ADD COLUMN IF NOT EXISTS blocked_by_calibration_id INT REFERENCES instrument_calibrations(id);
ALTER TABLE analysis_jobs ADD COLUMN IF NOT EXISTS invalidated_by_calibration_id INT REFERENCES instrument_calibrations(id);

CREATE TABLE IF NOT EXISTS analysis_job_samples(
 job_id INT REFERENCES analysis_jobs(id) ON DELETE CASCADE,
 sample_id INT REFERENCES moisture_samples(id) ON DELETE CASCADE,
 PRIMARY KEY(job_id,sample_id)
);
CREATE TABLE IF NOT EXISTS analysis_job_references(
 job_id INT REFERENCES analysis_jobs(id) ON DELETE CASCADE,
 reference_lot_id INT REFERENCES bamboo_lots(id) ON DELETE CASCADE,
 references_affected BOOLEAN NOT NULL DEFAULT FALSE,
 PRIMARY KEY(job_id,reference_lot_id)
);

-- 追查记录：每个校准事件 × 批次只留一条，重试只更新不新增。
CREATE TABLE IF NOT EXISTS calibration_traces(
 id SERIAL PRIMARY KEY,
 calibration_id INT NOT NULL REFERENCES instrument_calibrations(id) ON DELETE CASCADE,
 lot_id INT NOT NULL REFERENCES bamboo_lots(id) ON DELETE CASCADE,
 job_id INT REFERENCES analysis_jobs(id) ON DELETE SET NULL,
 impact TEXT NOT NULL,
 disposition TEXT NOT NULL,
 reason TEXT NOT NULL,
 affected_sample_ids INT[] NOT NULL DEFAULT '{}',
 affected_reference_lot_ids INT[] NOT NULL DEFAULT '{}',
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 resolved_at TIMESTAMPTZ,
 UNIQUE(calibration_id,lot_id)
);

CREATE INDEX IF NOT EXISTS idx_moisture_samples_instrument_time ON moisture_samples(instrument_id,sampled_at);
CREATE INDEX IF NOT EXISTS idx_calibration_traces_lot ON calibration_traces(lot_id,disposition);
CREATE INDEX IF NOT EXISTS idx_analysis_jobs_lot ON analysis_jobs(lot_id,id);
