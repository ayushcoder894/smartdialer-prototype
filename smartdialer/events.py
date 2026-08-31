from __future__ import annotations

from .db import Database
from .model import (
    AgentState,
    BorrowerState,
    CallState,
    ProviderEvent,
    ProviderEventType,
    TERMINAL_CALL_STATES,
)


EVENT_RANK = {
    ProviderEventType.INITIATED: 2,
    ProviderEventType.RINGING: 3,
    ProviderEventType.ANSWERED: 4,
    ProviderEventType.COMPLETED: 6,
    ProviderEventType.FAILED: 6,
}


class EventProcessor:
    """Idempotent monotonic reducer for untrusted provider webhooks."""

    def __init__(self, db: Database, wrap_up_seconds: float = 5.0) -> None:
        self.db = db
        self.wrap_up_seconds = wrap_up_seconds

    def process(self, event: ProviderEvent, now: float) -> str:
        with self.db.transaction() as conn:
            inserted = conn.execute(
                "INSERT OR IGNORE INTO provider_events(provider, event_id, call_id, event_type, "
                "occurred_at, processed_at, result) VALUES(?, ?, ?, ?, ?, ?, 'PROCESSING')",
                (
                    event.provider,
                    event.event_id,
                    event.call_id,
                    event.event_type.value,
                    event.occurred_at,
                    now,
                ),
            )
            if inserted.rowcount == 0:
                return "DUPLICATE_IGNORED"

            call = conn.execute("SELECT * FROM calls WHERE id=?", (event.call_id,)).fetchone()
            if call is None:
                result = "UNKNOWN_CALL"
            elif call["provider"] != event.provider or (
                call["provider_call_id"] is not None
                and call["provider_call_id"] != event.provider_call_id
            ):
                result = "PROVIDER_IDENTITY_MISMATCH"
            elif CallState(call["state"]) in TERMINAL_CALL_STATES:
                result = "TERMINAL_IGNORED"
            elif EVENT_RANK[event.event_type] <= call["state_rank"]:
                result = "STALE_IGNORED"
            elif event.event_type is ProviderEventType.RINGING:
                conn.execute(
                    "UPDATE calls SET state=?, state_rank=3, version=version+1, updated_at=? WHERE id=?",
                    (CallState.RINGING.value, now, event.call_id),
                )
                result = "RINGING"
            elif event.event_type is ProviderEventType.INITIATED:
                conn.execute(
                    "UPDATE calls SET state=?, state_rank=2, version=version+1, updated_at=? WHERE id=?",
                    (CallState.INITIATED.value, now, event.call_id),
                )
                result = "INITIATED"
            elif event.event_type is ProviderEventType.ANSWERED:
                result = self._answer(conn, call, event, now)
            elif event.event_type is ProviderEventType.COMPLETED:
                self._finish(conn, call, CallState.COMPLETED, now)
                result = "COMPLETED"
            else:
                self._finish(conn, call, CallState.FAILED, now)
                result = "FAILED"

            conn.execute(
                "UPDATE provider_events SET result=? WHERE provider=? AND event_id=?",
                (result, event.provider, event.event_id),
            )
            return result

    def _answer(self, conn, call, event: ProviderEvent, now: float) -> str:
        agent_id = call["agent_id"]
        if agent_id is None:
            agent = conn.execute(
                "SELECT id, version FROM agents WHERE campaign_id=? AND state=? "
                "ORDER BY updated_at, id LIMIT 1",
                (call["campaign_id"], AgentState.AVAILABLE.value),
            ).fetchone()
            if agent is not None:
                updated = conn.execute(
                    "UPDATE agents SET state=?, call_id=?, version=version+1, updated_at=? "
                    "WHERE id=? AND state=? AND version=?",
                    (
                        AgentState.CONNECTED.value,
                        call["id"],
                        now,
                        agent["id"],
                        AgentState.AVAILABLE.value,
                        agent["version"],
                    ),
                )
                if updated.rowcount == 1:
                    agent_id = agent["id"]
        else:
            updated = conn.execute(
                "UPDATE agents SET state=?, version=version+1, updated_at=? "
                "WHERE id=? AND state IN (?, ?)",
                (
                    AgentState.CONNECTED.value,
                    now,
                    agent_id,
                    AgentState.RESERVED.value,
                    AgentState.DIALING.value,
                ),
            )
            if updated.rowcount != 1:
                agent_id = None

        if agent_id is None:
            conn.execute(
                "UPDATE calls SET state=?, state_rank=6, answered_at=?, completed_at=?, "
                "failure_reason='no agent available at answer', version=version+1, updated_at=? "
                "WHERE id=?",
                (CallState.ABANDONED.value, event.occurred_at, now, now, call["id"]),
            )
            conn.execute(
                "UPDATE borrowers SET state=?, call_id=NULL, version=version+1, updated_at=? WHERE id=?",
                (BorrowerState.DONE.value, now, call["borrower_id"]),
            )
            return "ABANDONED_NO_AGENT"

        conn.execute(
            "UPDATE calls SET agent_id=?, state=?, state_rank=5, answered_at=?, connected_at=?, "
            "version=version+1, updated_at=? WHERE id=?",
            (
                agent_id,
                CallState.CONNECTED.value,
                event.occurred_at,
                now,
                now,
                call["id"],
            ),
        )
        return "CONNECTED"

    def _finish(self, conn, call, terminal: CallState, now: float) -> None:
        conn.execute(
            "UPDATE calls SET state=?, state_rank=6, completed_at=?, version=version+1, "
            "updated_at=?, failure_reason=CASE WHEN ?=? THEN 'provider failure/no answer' "
            "ELSE failure_reason END WHERE id=?",
            (
                terminal.value,
                now,
                now,
                terminal.value,
                CallState.FAILED.value,
                call["id"],
            ),
        )
        if call["agent_id"]:
            next_state = AgentState.WRAP_UP if terminal is CallState.COMPLETED else AgentState.AVAILABLE
            lease = now + self.wrap_up_seconds if next_state is AgentState.WRAP_UP else None
            conn.execute(
                "UPDATE agents SET state=?, call_id=NULL, lease_expires_at=?, version=version+1, "
                "updated_at=? WHERE id=? AND call_id=?",
                (next_state.value, lease, now, call["agent_id"], call["id"]),
            )
        borrower = conn.execute(
            "SELECT attempts FROM borrowers WHERE id=?", (call["borrower_id"],)
        ).fetchone()
        borrower_state = (
            BorrowerState.DONE
            if terminal is CallState.COMPLETED or (borrower and borrower["attempts"] >= 3)
            else BorrowerState.READY
        )
        conn.execute(
            "UPDATE borrowers SET state=?, call_id=NULL, version=version+1, updated_at=? WHERE id=?",
            (borrower_state.value, now, call["borrower_id"]),
        )

    def release_wrap_up(self, now: float) -> int:
        with self.db.transaction() as conn:
            result = conn.execute(
                "UPDATE agents SET state=?, lease_expires_at=NULL, version=version+1, updated_at=? "
                "WHERE state=? AND lease_expires_at<=?",
                (AgentState.AVAILABLE.value, now, AgentState.WRAP_UP.value, now),
            )
            return result.rowcount
