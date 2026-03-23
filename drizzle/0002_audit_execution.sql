CREATE TABLE IF NOT EXISTS audit_executions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  execution_id TEXT NOT NULL UNIQUE,
  scope TEXT NOT NULL,
  cnpj TEXT,
  user_name TEXT,
  status TEXT NOT NULL DEFAULT 'created',
  code_version TEXT,
  parameters_json TEXT,
  host_name TEXT,
  started_at INTEGER,
  finished_at INTEGER,
  created_at INTEGER DEFAULT (unixepoch())
);

CREATE TABLE IF NOT EXISTS audit_execution_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  execution_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  status TEXT NOT NULL,
  message TEXT,
  context_json TEXT,
  started_at INTEGER,
  finished_at INTEGER,
  duration_ms INTEGER,
  created_at INTEGER DEFAULT (unixepoch()),
  FOREIGN KEY (execution_id) REFERENCES audit_executions(execution_id)
);

CREATE TABLE IF NOT EXISTS audit_artifacts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  execution_id TEXT NOT NULL,
  artifact_type TEXT NOT NULL,
  artifact_path TEXT NOT NULL,
  artifact_hash TEXT,
  metadata_json TEXT,
  created_at INTEGER DEFAULT (unixepoch()),
  FOREIGN KEY (execution_id) REFERENCES audit_executions(execution_id)
);

CREATE INDEX IF NOT EXISTS idx_audit_executions_cnpj ON audit_executions(cnpj);
CREATE INDEX IF NOT EXISTS idx_audit_executions_scope ON audit_executions(scope);
CREATE INDEX IF NOT EXISTS idx_audit_events_execution_id ON audit_execution_events(execution_id);
CREATE INDEX IF NOT EXISTS idx_audit_artifacts_execution_id ON audit_artifacts(execution_id);
