"""Application configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    database_path: Path
    seed_database_path: Path | None = None
    chunk_size: int = 100
    chunk_delay_ms: int = 100
    # Pause between two migration write transactions.  SQLite allows only one
    # writer at a time; back-to-back migration transactions can starve a legacy
    # connection whose busy_timeout is short (e.g. 1 s) so its UPDATE fails
    # with "database is locked".  Yielding ~25 ms between every chunk gives a
    # legacy correction enough wall-clock time to acquire the write lock and
    # commit.  The value is a trade-off: smaller is faster on a quiet table,
    # larger gives legacy writes more headroom under heavy contention.
    lock_yield_ms: int = 25
    busy_timeout_ms: int = 30_000
    auto_resume: bool = True

    @classmethod
    def from_env(cls) -> Settings:
        seed_raw = os.environ.get("SEED_DATABASE_PATH", "")
        return cls(
            database_path=Path(
                os.environ.get("DATABASE_PATH", "/app/data/lab.db")
            ).expanduser(),
            seed_database_path=Path(seed_raw).expanduser() if seed_raw else None,
            chunk_size=max(1, _env_int("MIGRATION_CHUNK_SIZE", 100)),
            chunk_delay_ms=max(0, _env_int("MIGRATION_CHUNK_DELAY_MS", 100)),
            lock_yield_ms=max(0, _env_int("MIGRATION_LOCK_YIELD_MS", 25)),
            busy_timeout_ms=max(1000, _env_int("SQLITE_BUSY_TIMEOUT_MS", 30_000)),
            auto_resume=os.environ.get("AUTO_RESUME", "true").lower()
            not in {"0", "false", "no"},
        )
