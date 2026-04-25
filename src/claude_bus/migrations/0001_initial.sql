-- claude-bus initial schema.
-- Applied idempotently by claude_bus.db.init_db.
-- Schema version recorded in bus_meta under key "schema_version".

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

-- Aliases: deterministic identities per (role, session).
CREATE TABLE IF NOT EXISTS aliases (
    alias        TEXT PRIMARY KEY,
    role         TEXT NOT NULL,
    session_id   TEXT NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_aliases_role ON aliases(role);
CREATE INDEX IF NOT EXISTS idx_aliases_session ON aliases(session_id);

-- Messages: append-only typed message log.
CREATE TABLE IF NOT EXISTS messages (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id        TEXT NOT NULL,
    sender            TEXT NOT NULL,
    recipient         TEXT NOT NULL,
    type              TEXT NOT NULL,
    urgency           TEXT NOT NULL DEFAULT 'prompt',
    body              TEXT NOT NULL,
    tags              TEXT NOT NULL DEFAULT '',
    in_reply_to       INTEGER REFERENCES messages(id),
    conversation_id   INTEGER REFERENCES messages(id),
    ref_id            INTEGER,
    task_id           TEXT,
    status            TEXT NOT NULL DEFAULT 'sent',
    expires_at        TEXT,
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    delivered_at      TEXT,
    resolved_at       TEXT
);
CREATE INDEX IF NOT EXISTS idx_messages_inbox
    ON messages(recipient, status);
CREATE INDEX IF NOT EXISTS idx_messages_session
    ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_sender
    ON messages(sender);
CREATE INDEX IF NOT EXISTS idx_messages_conversation
    ON messages(conversation_id)
    WHERE conversation_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_task
    ON messages(task_id)
    WHERE task_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_expiry
    ON messages(expires_at)
    WHERE expires_at IS NOT NULL AND status != 'resolved';

-- Bus metadata: schema version + per-installation provenance.
CREATE TABLE IF NOT EXISTS bus_meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
