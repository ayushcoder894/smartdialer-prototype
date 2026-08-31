from __future__ import annotations

import sqlite3
import uuid

from .db import Database
from .model import (
    AgentState,
    BorrowerState,
    CallState,
    DialMode,
    SafetyDecision,
)
from .safety import SafetyController


class CallAllocator:
    """Consumes a safety capability and atomically allocates calls."""

    def __init__(self, db: Database, safety: SafetyController) -> None:
        self.db = db
        self.safety = safety

    def allocate(self, decision: SafetyDecision, now: float) -> list[str]:
        if not self.safety.verify(decision, now):
            raise PermissionError("invalid or expired Safety Controller permit")
        if decision.approved_calls <= 0:
            return []

        effective_mode = (
            DialMode.PROGRESSIVE
            if decision.outcome == "FALLBACK_PROGRESSIVE"
            else decision.mode
        )
        call_ids: list[str] = []
        with self.db.transaction() as conn:
            persisted = conn.execute(
                "SELECT approved_calls, allocated_calls, consumed FROM safety_decisions WHERE id=?",
                (decision.decision_id,),
            ).fetchone()
            if persisted is None or persisted["approved_calls"] != decision.approved_calls:
                raise PermissionError("Safety Controller decision not found or altered")
            if persisted["consumed"] != 0:
                raise PermissionError("Safety Controller permit has already been consumed")

            for _ in range(decision.approved_calls):
                borrower = conn.execute(
                    "SELECT id, version FROM borrowers WHERE campaign_id=? AND state=? "
                    "ORDER BY priority DESC, updated_at, id LIMIT 1",
                    (decision.campaign_id, BorrowerState.READY.value),
                ).fetchone()
                if borrower is None:
                    break

                agent_id: str | None = None
                agent_version: int | None = None
                if effective_mode is DialMode.PROGRESSIVE:
                    agent = conn.execute(
                        "SELECT id, version FROM agents WHERE campaign_id=? AND state=? "
                        "ORDER BY updated_at, id LIMIT 1",
                        (decision.campaign_id, AgentState.AVAILABLE.value),
                    ).fetchone()
                    if agent is None:
                        break
                    agent_id, agent_version = agent["id"], agent["version"]

                call_id = f"call-{uuid.uuid4().hex}"
                borrower_update = conn.execute(
                    "UPDATE borrowers SET state=?, call_id=?, attempts=attempts+1, "
                    "version=version+1, updated_at=? WHERE id=? AND state=? AND version=?",
                    (
                        BorrowerState.RESERVED.value,
                        call_id,
                        now,
                        borrower["id"],
                        BorrowerState.READY.value,
                        borrower["version"],
                    ),
                )
                if borrower_update.rowcount != 1:
                    continue

                if agent_id is not None:
                    agent_update = conn.execute(
                        "UPDATE agents SET state=?, call_id=?, version=version+1, updated_at=? "
                        "WHERE id=? AND state=? AND version=?",
                        (
                            AgentState.RESERVED.value,
                            call_id,
                            now,
                            agent_id,
                            AgentState.AVAILABLE.value,
                            agent_version,
                        ),
                    )
                    if agent_update.rowcount != 1:
                        raise sqlite3.IntegrityError("agent compare-and-swap failed")

                campaign = conn.execute(
                    "SELECT provider FROM campaigns WHERE id=? AND enabled=1",
                    (decision.campaign_id,),
                ).fetchone()
                if campaign is None:
                    raise ValueError("campaign is missing or disabled")
                conn.execute(
                    "INSERT INTO calls(id, campaign_id, borrower_id, agent_id, provider, mode, "
                    "state, state_rank, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        call_id,
                        decision.campaign_id,
                        borrower["id"],
                        agent_id,
                        campaign["provider"],
                        effective_mode.value,
                        CallState.RESERVED.value,
                        1,
                        now,
                        now,
                    ),
                )
                conn.execute(
                    "INSERT INTO initiation_jobs(call_id, next_attempt_at, updated_at) VALUES(?, ?, ?)",
                    (call_id, now, now),
                )
                call_ids.append(call_id)

            conn.execute(
                "UPDATE safety_decisions SET allocated_calls=?, consumed=1 WHERE id=? AND consumed=0",
                (len(call_ids), decision.decision_id),
            )
        return call_ids
