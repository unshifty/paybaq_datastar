CREATE TABLE IF NOT EXISTS events (
  id				INTEGER PRIMARY KEY AUTOINCREMENT,  -- Unique id of the event
  subject_type		TEXT NOT NULL,      -- Type of the subject which the event is related to
  subject_id    	TEXT NOT NULL,      -- Id of the subject which the event is related to
  subject_revision	INTEGER NOT NULL,   -- Revision number of the subject, incremented on each event
  event_type      	TEXT NOT NULL,      -- Type of the event
  event_payload   	JSON NOT NULL CHECK (json_valid(event_payload)),    -- Data that defines the event
  metadata        	JSON CHECK (metadata IS NULL OR json_valid(metadata)),         -- Metadata about the event
  created_at		REAL DEFAULT CURRENT_TIMESTAMP                -- When the event was created
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_subject_revision
  ON events (subject_type, subject_id, subject_revision);

CREATE INDEX IF NOT EXISTS idx_events_type_id
  ON events (subject_type, id);


CREATE TABLE IF NOT EXISTS ledgers (
  id            TEXT PRIMARY KEY,
  data          JSON NOT NULL CHECK (json_valid(data)),
  created_at    REAL DEFAULT CURRENT_TIMESTAMP,
  updated_at    REAL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS user_ledgers (
  user_cookie_id TEXT NOT NULL,
  ledger_id      TEXT NOT NULL,
  created_at     REAL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (user_cookie_id, ledger_id)
);