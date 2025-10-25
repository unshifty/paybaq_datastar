CREATE TABLE IF NOT EXISTS ledgers (
  ledger_id     TEXT PRIMARY KEY,
  ledger_data   JSON NOT NULL CHECK (json_valid(ledger_data)),
  updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);