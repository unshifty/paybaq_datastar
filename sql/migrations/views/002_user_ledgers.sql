CREATE TABLE IF NOT EXISTS user_ledgers (
  user_cookie_id TEXT NOT NULL,
  ledger_id      TEXT NOT NULL,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (user_cookie_id, ledger_id)
);