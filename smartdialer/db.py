from __future__ import annotations

import contextlib
import sqlite3
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from .model import AgentState, BorrowerState, CallState, DialMode


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS campaigns (
    id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    provider TEXT NOT NULL,
    answer_rate REAL NOT NULL DEFAULT 0.3,
    avg_talk_time REAL NOT NULL DEFAULT 120,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    state TEXT NOT NULL,
    call_id TEXT,
    version INTEGER NOT NULL DEFAULT 0,
    lease_expires_at REAL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agents_alloc ON agents(campaign_id, state, updated_at);

CREATE TABLE IF NOT EXISTS borrowers (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    phone TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    call_id TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    UNIQUE(campaign_id, phone)
);
CREATE INDEX IF NOT EXISTS idx_borrowers_alloc
    ON borrowers(campaign_id, state, priority DESC, updated_at);

CREATE TABLE IF NOT EXISTS calls (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    borrower_id TEXT NOT NULL REFERENCES borrowers(id),
    agent_id TEXT REFERENCES agents(id),
    provider TEXT NOT NULL,
    provider_call_id TEXT,
    mode TEXT NOT NULL,
    state TEXT NOT NULL,
    state_rank INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 0,
    initiated_at REAL,
    answered_at REAL,
    connected_at REAL,
    completed_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    failure_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_calls_campaign_state ON calls(campaign_id, state);

CREATE TABLE IF NOT EXISTS initiation_jobs (
    call_id TEXT PRIMARY KEY REFERENCES calls(id),
    status TEXT NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL,
    lease_owner TEXT,
    lease_expires_at REAL,
    last_error TEXT,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_pending
    ON initiation_jobs(status, next_attempt_at, lease_expires_at);

CREATE TABLE IF NOT EXISTS provider_events (
    provider TEXT NOT NULL,
    event_id TEXT NOT NULL,
    call_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at REAL NOT NULL,
    processed_at REAL NOT NULL,
    result TEXT NOT NULL,
    PRIMARY KEY(provider, event_id)
);

CREATE TABLE IF NOT EXISTS safety_decisions (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    requested_calls INTEGER NOT NULL,
    approved_calls INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    reason TEXT NOT NULL,
    allocated_calls INTEGER NOT NULL DEFAULT 0,
    consumed INTEGER NOT NULL DEFAULT 0,
    expires_at REAL NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS provider_health (
    provider TEXT PRIMARY KEY,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    successes INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0,
    circuit_open_until REAL NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL
);
"""


class Database:
    """Small SQLite repository with explicit write transactions.

    Every allocator operation uses BEGIN IMMEDIATE. SQLite serializes those short
    transactions, so two workers cannot reserve the same row. Version predicates
    remain in the updates as a second guard and document the intended CAS model
    for a future PostgreSQL implementation.
    """

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._uri = self.path.startswith("file:")
        self._keeper: sqlite3.Connection | None = None
        self._lock = threading.RLock()
        if self.path == ":memory:":
            self.path = f"file:smartdialer-{uuid.uuid4()}?mode=memory&cache=shared"
            self._uri = True
            self._keeper = self._connect()
        with self.connection() as conn:
            conn.executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            uri=self._uri,
            timeout=5,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    @contextlib.contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        # The process lock avoids SQLITE_LOCKED in shared-memory test databases;
        # BEGIN IMMEDIATE remains the cross-process/file-backed serialization
        # mechanism used by real workers.
        with self._lock:
            with self.connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    yield conn
                except BaseException:
                    conn.rollback()
                    raise
                else:
                    conn.commit()

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        with self.connection() as conn:
            conn.execute(sql, params)

    def one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        with self.connection() as conn:
            return conn.execute(sql, params).fetchone()

    def all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self.connection() as conn:
            return list(conn.execute(sql, params).fetchall())

    def create_campaign(
        self,
        campaign_id: str,
        mode: DialMode,
        provider: str,
        answer_rate: float,
        avg_talk_time: float,
        now: float,
    ) -> None:
        self.execute(
            "INSERT INTO campaigns(id, mode, provider, answer_rate, avg_talk_time, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?)",
            (campaign_id, mode.value, provider, answer_rate, avg_talk_time, now),
        )

    def add_agents(self, campaign_id: str, count: int, now: float) -> None:
        with self.transaction() as conn:
            conn.executemany(
                "INSERT INTO agents(id, campaign_id, state, updated_at) VALUES(?, ?, ?, ?)",
                [
                    (f"agent-{uuid.uuid4().hex[:12]}", campaign_id, AgentState.AVAILABLE.value, now)
                    for _ in range(count)
                ],
            )

    def add_borrowers(self, campaign_id: str, count: int, now: float) -> None:
        with self.transaction() as conn:
            conn.executemany(
                "INSERT INTO borrowers(id, campaign_id, phone, priority, state, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?)",
                [
                    (
                        f"borrower-{uuid.uuid4().hex[:12]}",
                        campaign_id,
                        f"+1555{i:07d}",
                        count - i,
                        BorrowerState.READY.value,
                        now,
                    )
                    for i in range(count)
                ],
            )

    def counts(self, campaign_id: str) -> dict[str, int]:
        result: dict[str, int] = {}
        with self.connection() as conn:
            for row in conn.execute(
                "SELECT state, COUNT(*) n FROM agents WHERE campaign_id=? GROUP BY state",
                (campaign_id,),
            ):
                result[f"agents_{row['state'].lower()}"] = row["n"]
            for row in conn.execute(
                "SELECT state, COUNT(*) n FROM calls WHERE campaign_id=? GROUP BY state",
                (campaign_id,),
            ):
                result[f"calls_{row['state'].lower()}"] = row["n"]
            result["borrowers_ready"] = conn.execute(
                "SELECT COUNT(*) FROM borrowers WHERE campaign_id=? AND state=?",
                (campaign_id, BorrowerState.READY.value),
            ).fetchone()[0]
        return result

    def close(self) -> None:
        if self._keeper is not None:
            self._keeper.close()
            self._keeper = None
