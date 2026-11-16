"""Regression tests for the cutover lock-fairness fix.

The original cutover ran the whole switch as one multi-second write
transaction on a large table, so a legacy connection whose ``busy_timeout`` is
one second failed its UPDATE with "database is locked" while the migration
still reported ``completed``.  These tests pin the required behaviour:

* a one-second-busy-timeout writer keeps committing throughout the migration
  (copy *and* cutover) on a chunked wide table without a single lock error;
* committed inserts / updates / deletes stay consistent with both the
  compatibility view and the metric queries;
* deleting a sample and re-creating the same id never brings the old values
  back, even when it happens during the migration.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import pytest

import legacy.acquisition as legacy
from app.config import Settings
from app.db import connect
from app.migration import MigrationEngine
from app.repository import SampleRepository

ROWS = 20_000
CHUNK = 5000


def _build_wide_db(path: Path, rows: int) -> None:
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute(
        "CREATE TABLE samples (id INTEGER PRIMARY KEY, temperature REAL, ph REAL)"
    )
    conn.execute("BEGIN")
    batch = 5000
    for lo in range(1, rows + 1, batch):
        hi = min(lo + batch, rows + 1)
        conn.executemany(
            "INSERT INTO samples (id, temperature, ph) VALUES (?, ?, ?)",
            [(i, 36.5 + (i % 10) / 10.0, 7.0 + (i % 7) / 10.0) for i in range(lo, hi)],
        )
    conn.execute("COMMIT")
    conn.close()


def _engine(tmp_path: Path) -> tuple[MigrationEngine, Settings, Path]:
    db = tmp_path / "locked.db"
    _build_wide_db(db, ROWS)
    settings = Settings(
        database_path=db,
        seed_database_path=None,
        chunk_size=CHUNK,
        chunk_delay_ms=0,
        lock_yield_ms=25,
        busy_timeout_ms=30_000,
        auto_resume=False,
    )
    eng = MigrationEngine(settings)
    eng.initialize()
    return eng, settings, db


def test_one_second_busy_timeout_writer_never_sees_locked(tmp_path):
    eng, settings, db = _engine(tmp_path)
    stop = threading.Event()
    errors: list[str] = []
    committed: list[int] = []
    guard = threading.Lock()

    def legacy_writer():
        # Exactly the old-program profile: busy_timeout = 1000 ms.
        w = sqlite3.connect(db, timeout=1.0, isolation_level=None)
        w.execute("PRAGMA busy_timeout = 1000")
        i = 0
        while not stop.is_set():
            i += 1
            sample_id = 1 + (i * 7919) % ROWS
            try:
                w.execute(
                    "UPDATE samples SET temperature = ? WHERE id = ?",
                    (35.0 + (i % 50) / 10.0, sample_id),
                )
                with guard:
                    committed.append(sample_id)
            except sqlite3.OperationalError as exc:
                with guard:
                    errors.append(str(exc))
            time.sleep(0.002)
        w.close()

    thread = threading.Thread(target=legacy_writer)
    thread.start()
    try:
        state, _ = eng.start()
        assert state["state"] in {"copying", "completed"}
        eng.wait_for_idle(120)
    finally:
        stop.set()
        thread.join()

    final = eng.get_state()
    assert final["state"] == "completed"
    assert final["view_installed"] is True
    assert committed, "legacy writer never committed during the migration"
    assert errors == [], f"legacy writer hit lock errors: {errors[:5]}"

    # Long table and compatibility view agree exactly.
    chk = connect(settings)
    try:
        metrics = chk.execute("SELECT COUNT(*) FROM sample_metrics").fetchone()[0]
        samples = chk.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        assert metrics == samples * 2
        assert chk.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        chk.close()


def test_crud_consistency_and_same_id_recreate_during_migration(tmp_path):
    eng, settings, db = _engine(tmp_path)
    repo = SampleRepository(settings)
    state, _ = eng.start()
    assert state["state"] in {"copying", "completed"}

    conn = legacy.open_db(db)
    try:
        # A correction on an early id, a delete, and a same-id recreate with
        # brand-new values, plus a fresh high id -- all while migration runs.
        legacy.update(conn, 1, temperature=99.9)
        legacy.delete(conn, 2)
        legacy.insert(conn, sample_id=2, temperature=11.1, ph=3.3)
        new_id = legacy.insert(conn, temperature=22.2, ph=4.4)
        # And again after the switch has committed.
        eng.wait_for_idle(120)
        assert eng.get_state()["state"] == "completed"
        legacy.update(conn, 1, ph=8.8)
        legacy.delete(conn, 3)
        legacy.insert(conn, sample_id=3, temperature=44.4, ph=5.5)
    finally:
        conn.close()

    # The recreated ids must expose only their new values, never stale ones.
    assert repo.get_sample(1) == {"id": 1, "temperature": 99.9, "ph": 8.8}
    assert repo.get_sample(2) == {"id": 2, "temperature": 11.1, "ph": 3.3}
    assert repo.get_sample(3) == {"id": 3, "temperature": 44.4, "ph": 5.5}
    assert repo.get_metrics(new_id) == {
        "sample_id": new_id,
        "temperature": 22.2,
        "ph": 4.4,
    }

    chk = connect(settings)
    try:
        # No orphan metric rows and exactly two rows per surviving sample.
        orphans = chk.execute(
            "SELECT COUNT(*) FROM sample_metrics"
            " WHERE sample_id NOT IN (SELECT id FROM samples)"
        ).fetchone()[0]
        assert orphans == 0
        bad = chk.execute(
            "SELECT sample_id FROM sample_metrics GROUP BY sample_id"
            " HAVING COUNT(*) != 2"
        ).fetchall()
        assert bad == []
        # Recreated id 2 must not retain any pre-delete value.
        vals = {
            r["metric"]: r["value"]
            for r in chk.execute(
                "SELECT metric, value FROM sample_metrics WHERE sample_id = 2"
            )
        }
        assert vals == {"temperature": 11.1, "ph": 3.3}
    finally:
        chk.close()


def test_retry_after_failure_keeps_writes_and_completes(tmp_path, monkeypatch):
    eng, settings, db = _engine(tmp_path)
    from app import migration as mig

    def boom(self):
        raise RuntimeError("simulated cutover failure")

    monkeypatch.setattr(mig.MigrationEngine, "_cutover", boom)
    eng.start()
    eng.wait_for_idle(60)
    assert eng.get_state()["state"] == "failed"

    # The wide table stays fully usable after the failure.
    conn = legacy.open_db(db)
    try:
        legacy.update(conn, 1, ph=7.7)
        legacy.delete(conn, 4)
        legacy.insert(conn, sample_id=4, temperature=9.0, ph=9.0)
    finally:
        conn.close()

    monkeypatch.undo()
    state, started = eng.start()
    assert started is True
    eng.wait_for_idle(120)
    final = eng.get_state()
    assert final["state"] == "completed"
    assert final["metric_rows"] == final["metric_rows_expected"]

    repo = SampleRepository(settings)
    assert repo.get_sample(1)["ph"] == 7.7
    assert repo.get_sample(4) == {"id": 4, "temperature": 9.0, "ph": 9.0}
