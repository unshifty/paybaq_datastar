CREATE TABLE IF NOT EXISTS events (
  id				INTEGER PRIMARY KEY AUTOINCREMENT,
  entity_type		TEXT NOT NULL, -- Type of the entity which the event is related to
  entity_id    		TEXT NOT NULL, -- Id of the entity which the event is related to
  entity_version	INTEGER NOT NULL, -- Version of the entity, incremented on each event
  event_type      	TEXT NOT NULL, -- Type of the event
  event_payload   	JSON NOT NULL CHECK (json_valid(event_payload)), -- Data that defines the event
  metadata        	JSON NOT NULL CHECK (json_valid(metadata)), -- Metadata about the event
  created_at		TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- When the event was created
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_stream_version
  ON events (entity_type, entity_id, entity_version);

CREATE INDEX IF NOT EXISTS idx_events_type_id
  ON events (entity_type, id);