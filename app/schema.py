"""SQLite schema definitions for the wide-to-long migration.

The legacy acquisition program reads and writes ``samples(id, temperature, ph)``
directly.  While migration is running that table remains a real table.  The
cutover replaces it *inside one transaction* with a view over the long table,
backed by INSTEAD OF triggers, so unmodified legacy SQL keeps working.
"""

# Long (analysis) storage: exactly one row per sample and metric.
LONG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sample_metrics (
    sample_id INTEGER NOT NULL,
    metric    TEXT    NOT NULL CHECK (metric IN ('temperature', 'ph')),
    value     REAL,
    PRIMARY KEY (sample_id, metric)
) WITHOUT ROWID
"""

# Bookkeeping for the one logical migration.  Repeated POST requests must not
# rebuild a completed database, so this is the source of truth for state.
MIGRATION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS migration_state (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    run_id         TEXT    NOT NULL,
    state          TEXT    NOT NULL
                   CHECK (state IN ('copying', 'completed', 'failed')),
    created_at     TEXT    NOT NULL,
    started_at     TEXT,
    completed_at   TEXT,
    last_error     TEXT,
    total_samples  INTEGER NOT NULL DEFAULT 0,
    samples_copied INTEGER NOT NULL DEFAULT 0,
    last_id        INTEGER,
    attempt        INTEGER NOT NULL DEFAULT 0
)
"""

MIGRATION_EVENT_SQL = """
CREATE TABLE IF NOT EXISTS migration_events (
    seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT    NOT NULL,
    at     TEXT    NOT NULL,
    event  TEXT    NOT NULL,
    detail TEXT
)
"""

# Helper for the compatibility INSERT trigger: remembers the id allocated when
# the legacy program inserts without an explicit id.  One row only.
INSERT_CONTEXT_SQL = """
CREATE TABLE IF NOT EXISTS _insert_context (
    id        INTEGER PRIMARY KEY CHECK (id = 1),
    sample_id INTEGER NOT NULL
)
"""

# Single-row write watermark.  The mirroring triggers bump it for every
# committed legacy write while migration is active, so the engine can detect
# whether legacy writes happened during a reconciliation pass without scanning
# the whole table.  The post-cutover compatibility triggers never touch it,
# freezing the value at the switch instant.
WATERMARK_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS migration_write_watermark (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    counter INTEGER NOT NULL DEFAULT 0
)
"""

WATERMARK_SEED_SQL = """
INSERT OR IGNORE INTO migration_write_watermark (id, counter) VALUES (1, 0)
"""

# Compatibility view: exactly the legacy column names, values and NULLs.
COMPAT_VIEW_SQL = """
CREATE VIEW samples (id, temperature, ph) AS
SELECT
    sample_id,
    MAX(CASE WHEN metric = 'temperature' THEN value END),
    MAX(CASE WHEN metric = 'ph'          THEN value END)
FROM sample_metrics
GROUP BY sample_id
"""

# INSERT: explicit duplicate ids are rejected like the legacy PRIMARY KEY did.
# An omitted / NULL id is allocated as MAX(id)+1, matching SQLite's legacy
# INTEGER PRIMARY KEY behaviour (no AUTOINCREMENT).
#
# Both metric rows always get the same effective id (given or allocated).  The
# one-row helper also lets a legacy program recover the id allocated for an
# INSERT without an id: SQLite restores ``last_insert_rowid()`` when a trigger
# finishes, so ``cursor.lastrowid`` cannot report it.
COMPAT_INSERT_TRIGGER_SQL = """
CREATE TRIGGER samples_insert INSTEAD OF INSERT ON samples
BEGIN
    SELECT RAISE(ABORT, 'UNIQUE constraint failed: samples.id')
    WHERE NEW.id IS NOT NULL
      AND EXISTS (SELECT 1 FROM sample_metrics WHERE sample_id = NEW.id);

    DELETE FROM _insert_context;
    INSERT INTO _insert_context (id, sample_id)
    VALUES (
        1,
        COALESCE(NEW.id, (SELECT COALESCE(MAX(sample_id), 0) + 1 FROM sample_metrics))
    );

    INSERT INTO sample_metrics (sample_id, metric, value)
    SELECT c.sample_id, m.metric,
           CASE m.metric
               WHEN 'temperature' THEN NEW.temperature
               WHEN 'ph'          THEN NEW.ph
           END
    FROM _insert_context AS c
    CROSS JOIN (SELECT 'temperature' AS metric UNION ALL SELECT 'ph') AS m;
END
"""

# UPDATE: changing the id moves the two rows; changing a value overwrites only
# that metric.  Both rows are kept, even with NULL values.
COMPAT_UPDATE_TRIGGER_SQL = """
CREATE TRIGGER samples_update INSTEAD OF UPDATE ON samples
BEGIN
    -- Match the wide table: updating the INTEGER PRIMARY KEY to NULL was a
    -- "datatype mismatch" error there.
    SELECT RAISE(ABORT, 'datatype mismatch') WHERE NEW.id IS NULL;

    SELECT RAISE(ABORT, 'UNIQUE constraint failed: samples.id')
    WHERE NEW.id IS NOT OLD.id
      AND EXISTS (SELECT 1 FROM sample_metrics WHERE sample_id = NEW.id);

    DELETE FROM sample_metrics
    WHERE sample_id = OLD.id AND NEW.id IS NOT OLD.id;

    INSERT INTO sample_metrics (sample_id, metric, value)
    SELECT NEW.id, m.metric,
           CASE m.metric
               WHEN 'temperature' THEN NEW.temperature
               WHEN 'ph'          THEN NEW.ph
           END
    FROM (SELECT 'temperature' AS metric UNION ALL SELECT 'ph') AS m
    WHERE NEW.id IS NOT NULL
    ON CONFLICT (sample_id, metric) DO UPDATE SET value = excluded.value;
END
"""

COMPAT_DELETE_TRIGGER_SQL = """
CREATE TRIGGER samples_delete INSTEAD OF DELETE ON samples
BEGIN
    DELETE FROM sample_metrics WHERE sample_id = OLD.id;
END
"""

_COMPAT_DROP_SQL = """
DROP TRIGGER IF EXISTS samples_insert;
DROP TRIGGER IF EXISTS samples_update;
DROP TRIGGER IF EXISTS samples_delete;
DROP VIEW IF EXISTS samples;
"""

# Triggers installed on the real wide table for the duration of the migration.
# Every committed legacy write is mirrored immediately, so a chunk that was
# copied earlier can never be rolled back to a stale value.  Each trigger also
# bumps the single-row watermark, an observable counter of legacy writes (kept
# for diagnostics/operability; the engine's correctness does not depend on it).
WATERMARK_BUMP = (
    "UPDATE migration_write_watermark SET counter = counter + 1 WHERE id = 1"
)

MIGRATION_TRIGGERS = (
    f"""
    CREATE TRIGGER _migration_samples_insert AFTER INSERT ON samples
    BEGIN
        INSERT INTO sample_metrics (sample_id, metric, value)
        VALUES (NEW.id, 'temperature', NEW.temperature)
        ON CONFLICT (sample_id, metric) DO UPDATE SET value = excluded.value;
        INSERT INTO sample_metrics (sample_id, metric, value)
        VALUES (NEW.id, 'ph', NEW.ph)
        ON CONFLICT (sample_id, metric) DO UPDATE SET value = excluded.value;
        {WATERMARK_BUMP};
    END
    """,
    f"""
    CREATE TRIGGER _migration_samples_update AFTER UPDATE ON samples
    BEGIN
        DELETE FROM sample_metrics
        WHERE sample_id = OLD.id AND NEW.id IS NOT OLD.id;
        INSERT INTO sample_metrics (sample_id, metric, value)
        VALUES (NEW.id, 'temperature', NEW.temperature)
        ON CONFLICT (sample_id, metric) DO UPDATE SET value = excluded.value;
        INSERT INTO sample_metrics (sample_id, metric, value)
        VALUES (NEW.id, 'ph', NEW.ph)
        ON CONFLICT (sample_id, metric) DO UPDATE SET value = excluded.value;
        {WATERMARK_BUMP};
    END
    """,
    f"""
    CREATE TRIGGER _migration_samples_delete AFTER DELETE ON samples
    BEGIN
        DELETE FROM sample_metrics WHERE sample_id = OLD.id;
        {WATERMARK_BUMP};
    END
    """,
)

_MIGRATION_DROP_SQL = """
DROP TRIGGER IF EXISTS _migration_samples_insert;
DROP TRIGGER IF EXISTS _migration_samples_update;
DROP TRIGGER IF EXISTS _migration_samples_delete;
"""

WIDE_TABLE_SQL = """
CREATE TABLE samples (
    id INTEGER PRIMARY KEY,
    temperature REAL,
    ph REAL
)
"""

# At cutover the legacy wide table is renamed (a metadata-only operation that
# takes ~1 ms even on an 800k-row table) instead of being dropped inside the
# switch transaction.  The engine drops it in a separate short transaction
# immediately after the switch commits; if the process dies in between, the
# archive is harmless and cleaned up by the next run.
ARCHIVE_TABLE = "samples_legacy_archive"


def create_base_schema(conn) -> None:
    """Create long storage and migration bookkeeping (idempotent)."""
    conn.execute(LONG_TABLE_SQL)
    conn.execute(MIGRATION_TABLE_SQL)
    conn.execute(MIGRATION_EVENT_SQL)
    conn.execute(INSERT_CONTEXT_SQL)
    conn.execute(WATERMARK_TABLE_SQL)
    conn.execute(WATERMARK_SEED_SQL)


def ensure_wide_table(conn) -> None:
    """Create the legacy wide table only when ``samples`` does not exist.

    After cutover ``samples`` is a view; ``CREATE TABLE IF NOT EXISTS`` is a
    no-op for views on SQLite, but checking sqlite_master keeps the intent
    explicit and works on older versions too.
    """
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE name = 'samples'"
    ).fetchone()
    if row is None:
        conn.execute(WIDE_TABLE_SQL)


def install_migration_triggers(conn) -> None:
    """Install mirroring triggers on the wide table (inside a transaction)."""
    drop_migration_triggers(conn)
    for sql in MIGRATION_TRIGGERS:
        conn.execute(sql)


def drop_migration_triggers(conn) -> None:
    for statement in _MIGRATION_DROP_SQL.split(";"):
        if statement.strip():
            conn.execute(statement)


def switch_wide_table_to_view(conn, archive: str = ARCHIVE_TABLE) -> None:
    """Atomically replace the wide table with the compatibility view.

    Caller must be inside a short write transaction, after dropping the
    mirroring triggers.  The wide table is renamed rather than dropped:
    ``ALTER TABLE RENAME`` only touches the schema catalog, so this whole
    operation holds the write lock for a few milliseconds regardless of the
    table size.  A drop of a large table would run inside the same lock and
    could exceed a legacy connection's busy timeout.
    """
    conn.execute(f"DROP TABLE IF EXISTS {archive}")
    conn.execute("ALTER TABLE samples RENAME TO " + archive)
    for statement in _COMPAT_DROP_SQL.split(";"):
        if statement.strip():
            conn.execute(statement)
    conn.execute(COMPAT_VIEW_SQL)
    conn.execute(COMPAT_INSERT_TRIGGER_SQL)
    conn.execute(COMPAT_UPDATE_TRIGGER_SQL)
    conn.execute(COMPAT_DELETE_TRIGGER_SQL)


def drop_archived_wide_table(conn, archive: str = ARCHIVE_TABLE) -> None:
    """Drop the archived wide table.  Intended to run in its own transaction."""
    conn.execute(f"DROP TABLE IF EXISTS {archive}")
