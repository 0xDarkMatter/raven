-- raven_bus v2 schema (fresh DB — v2 does not migrate v1 files).
-- Decisions of record: ADR-001 (append-only log + channel-kind read
-- state), ADR-002 (addressing + one host DB).
-- INVARIANT (ADR-001): messages is APPEND-ONLY. The only DELETEs are
-- retention/teardown; there is NO UPDATE path on messages, ever.

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS channels (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT NOT NULL UNIQUE,
    kind           TEXT NOT NULL CHECK (kind IN ('broadcast','queue','stream')),
    retention_s    INTEGER,
    max_deliveries INTEGER NOT NULL DEFAULT 3,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id  INTEGER NOT NULL REFERENCES channels(id),
    sender      TEXT NOT NULL,
    type        TEXT NOT NULL,
    urgency     TEXT NOT NULL DEFAULT 'prompt'
                CHECK (urgency IN ('blocking','prompt','fyi')),
    body        TEXT NOT NULL,
    tags        TEXT NOT NULL DEFAULT '',
    reply_to    INTEGER REFERENCES messages(id),
    thread_id   INTEGER REFERENCES messages(id),
    expires_at  TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id, id);
CREATE INDEX IF NOT EXISTS idx_messages_expiry
    ON messages(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS cursors (
    consumer    TEXT NOT NULL,
    channel_id  INTEGER NOT NULL REFERENCES channels(id),
    last_ack_id INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (consumer, channel_id)
);

CREATE TABLE IF NOT EXISTS claims (
    message_id  INTEGER PRIMARY KEY REFERENCES messages(id),
    consumer    TEXT NOT NULL,
    state       TEXT NOT NULL CHECK (state IN ('leased','done','dead')),
    deliveries  INTEGER NOT NULL DEFAULT 1,
    lease_until TEXT NOT NULL,
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_claims_lease
    ON claims(lease_until) WHERE state = 'leased';

CREATE TABLE IF NOT EXISTS consumers (
    id           TEXT PRIMARY KEY,
    role         TEXT NOT NULL,
    run          TEXT NOT NULL,
    kind         TEXT NOT NULL DEFAULT 'agent'
                 CHECK (kind IN ('agent','orchestrator','observer')),
    last_seen_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_consumers_run ON consumers(run);

CREATE TABLE IF NOT EXISTS bus_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
