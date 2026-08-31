from __future__ import annotations

import uuid

from .db import Database
from .model import AgentState, BorrowerState, CallState, DispatchResult
from .providers import ProviderRegistry


class SimulatedWorkerCrash(RuntimeError):
    pass


class Dispatcher:
    """Durable outbox dispatcher for the non-transactional provider side effect."""

    def __init__(
        self,
        db: Database,
        providers: ProviderRegistry,
        *,
        worker_id: str | None = None,
        lease_seconds: float = 3.0,
        circuit_failure_threshold: int = 3,
        circuit_cooldown: float = 10.0,
        max_attempts: int = 3,
    ) -> None:
        self.db = db
        self.providers = providers
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.lease_seconds = lease_seconds
        self.circuit_failure_threshold = circuit_failure_threshold
        self.circuit_cooldown = circuit_cooldown
        self.max_attempts = max_attempts

    def _claim(self, now: float):
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT j.call_id, c.provider, c.borrower_id, b.phone "
                "FROM initiation_jobs j JOIN calls c ON c.id=j.call_id "
                "JOIN borrowers b ON b.id=c.borrower_id "
                "WHERE ((j.status='PENDING' AND j.next_attempt_at<=?) OR "
                "(j.status='IN_PROGRESS' AND j.lease_expires_at<=?)) "
                "ORDER BY j.next_attempt_at, j.call_id LIMIT 1",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            health = conn.execute(
                "SELECT circuit_open_until FROM provider_health WHERE provider=?",
                (row["provider"],),
            ).fetchone()
            if health is not None and health["circuit_open_until"] > now:
                return None
            updated = conn.execute(
                "UPDATE initiation_jobs SET status='IN_PROGRESS', lease_owner=?, "
                "lease_expires_at=?, updated_at=? WHERE call_id=? AND "
                "(status='PENDING' OR lease_expires_at<=?)",
                (
                    self.worker_id,
                    now + self.lease_seconds,
                    now,
                    row["call_id"],
                    now,
                ),
            )
            return row if updated.rowcount == 1 else None

    def _record_success(self, call_id: str, provider: str, provider_call_id: str, now: float) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE initiation_jobs SET status='DONE', attempts=attempts+1, "
                "lease_owner=NULL, lease_expires_at=NULL, updated_at=? WHERE call_id=?",
                (now, call_id),
            )
            conn.execute(
                "UPDATE calls SET provider_call_id=?, state=?, state_rank=MAX(state_rank, 2), "
                "initiated_at=COALESCE(initiated_at, ?), version=version+1, updated_at=? "
                "WHERE id=? AND state NOT IN (?, ?, ?, ?)",
                (
                    provider_call_id,
                    CallState.INITIATED.value,
                    now,
                    now,
                    call_id,
                    CallState.COMPLETED.value,
                    CallState.FAILED.value,
                    CallState.CANCELLED.value,
                    CallState.ABANDONED.value,
                ),
            )
            conn.execute(
                "UPDATE agents SET state=?, version=version+1, updated_at=? "
                "WHERE call_id=? AND state=?",
                (AgentState.DIALING.value, now, call_id, AgentState.RESERVED.value),
            )
            conn.execute(
                "INSERT INTO provider_health(provider, successes, updated_at) VALUES(?, 1, ?) "
                "ON CONFLICT(provider) DO UPDATE SET successes=successes+1, "
                "consecutive_failures=0, updated_at=excluded.updated_at",
                (provider, now),
            )

    def _record_failure(self, call_id: str, provider: str, error: str, now: float) -> None:
        with self.db.transaction() as conn:
            job = conn.execute(
                "SELECT attempts FROM initiation_jobs WHERE call_id=?",
                (call_id,),
            ).fetchone()
            attempts = (job["attempts"] if job else 0) + 1
            delay = min(30.0, 2.0 ** min(attempts, 5))
            if attempts >= self.max_attempts:
                conn.execute(
                    "UPDATE initiation_jobs SET status='FAILED', attempts=?, lease_owner=NULL, "
                    "lease_expires_at=NULL, last_error=?, updated_at=? WHERE call_id=?",
                    (attempts, error, now, call_id),
                )
                call = conn.execute(
                    "SELECT borrower_id FROM calls WHERE id=?", (call_id,)
                ).fetchone()
                conn.execute(
                    "UPDATE calls SET state=?, state_rank=6, completed_at=?, failure_reason=?, "
                    "version=version+1, updated_at=? WHERE id=?",
                    (CallState.FAILED.value, now, error, now, call_id),
                )
                conn.execute(
                    "UPDATE agents SET state=?, call_id=NULL, version=version+1, updated_at=? "
                    "WHERE call_id=?",
                    (AgentState.AVAILABLE.value, now, call_id),
                )
                if call:
                    conn.execute(
                        "UPDATE borrowers SET state=?, call_id=NULL, version=version+1, updated_at=? "
                        "WHERE id=?",
                        (BorrowerState.READY.value, now, call["borrower_id"]),
                    )
            else:
                conn.execute(
                    "UPDATE initiation_jobs SET status='PENDING', attempts=?, next_attempt_at=?, "
                    "lease_owner=NULL, lease_expires_at=NULL, last_error=?, updated_at=? WHERE call_id=?",
                    (attempts, now + delay, error, now, call_id),
                )
            conn.execute(
                "INSERT INTO provider_health(provider, consecutive_failures, failures, updated_at) "
                "VALUES(?, 1, 1, ?) ON CONFLICT(provider) DO UPDATE SET "
                "consecutive_failures=consecutive_failures+1, failures=failures+1, "
                "updated_at=excluded.updated_at",
                (provider, now),
            )
            health = conn.execute(
                "SELECT consecutive_failures FROM provider_health WHERE provider=?",
                (provider,),
            ).fetchone()
            if health and health["consecutive_failures"] >= self.circuit_failure_threshold:
                conn.execute(
                    "UPDATE provider_health SET circuit_open_until=? WHERE provider=?",
                    (now + self.circuit_cooldown, provider),
                )

    def dispatch_one(self, now: float, *, crash_after_provider: bool = False) -> DispatchResult | None:
        job = self._claim(now)
        if job is None:
            return None
        provider = self.providers.get(job["provider"])
        try:
            # call_id is the provider idempotency key. Retrying after a crash cannot
            # create a second outbound call on a compliant provider.
            provider_call_id = provider.start_call(job["call_id"], job["phone"], now)
            if crash_after_provider:
                raise SimulatedWorkerCrash("crashed after provider accepted, before DB commit")
        except SimulatedWorkerCrash:
            raise
        except (TimeoutError, ConnectionError) as exc:
            self._record_failure(job["call_id"], job["provider"], str(exc), now)
            return DispatchResult(job["call_id"], "RETRY", str(exc))
        self._record_success(job["call_id"], job["provider"], provider_call_id, now)
        return DispatchResult(job["call_id"], "DISPATCHED")

    def dispatch_available(self, now: float, limit: int = 100) -> list[DispatchResult]:
        results: list[DispatchResult] = []
        for _ in range(limit):
            result = self.dispatch_one(now)
            if result is None:
                break
            results.append(result)
        return results
