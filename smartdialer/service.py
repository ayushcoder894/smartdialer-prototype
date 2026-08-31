from __future__ import annotations

import time
from pathlib import Path

from .allocation import CallAllocator
from .db import Database
from .dispatch import Dispatcher
from .events import EventProcessor
from .model import (
    AgentState,
    BorrowerState,
    CallState,
    DialMode,
    PacingRequest,
    SafetyDecision,
    SafetySnapshot,
)
from .pacing import PredictivePacer, ProgressivePacer
from .providers import FastReliableProvider, ProviderRegistry, SlowChaoticProvider
from .safety import SafetyController


class SmartDialer:
    """Composition root used by the CLI, simulator, and tests."""

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        *,
        providers: ProviderRegistry | None = None,
        safety_secret: bytes | None = None,
    ) -> None:
        self.db = Database(db_path)
        self.providers = providers or ProviderRegistry(
            [FastReliableProvider(), SlowChaoticProvider()]
        )
        self.safety = SafetyController(self.db, safety_secret)
        self.allocator = CallAllocator(self.db, self.safety)
        self.dispatcher = Dispatcher(self.db, self.providers)
        self.events = EventProcessor(self.db)
        self.progressive = ProgressivePacer()
        self.predictive = PredictivePacer()

    def create_campaign(
        self,
        campaign_id: str,
        *,
        mode: DialMode = DialMode.PREDICTIVE,
        provider: str = "fast-reliable",
        agents: int = 10,
        borrowers: int = 100,
        answer_rate: float = 0.5,
        avg_talk_time: float = 90.0,
        now: float | None = None,
    ) -> None:
        now = time.time() if now is None else now
        self.providers.get(provider)
        self.db.create_campaign(campaign_id, mode, provider, answer_rate, avg_talk_time, now)
        self.db.add_agents(campaign_id, agents, now)
        self.db.add_borrowers(campaign_id, borrowers, now)

    def snapshot(self, campaign_id: str, now: float | None = None) -> SafetySnapshot:
        now = time.time() if now is None else now
        campaign = self.db.one("SELECT provider FROM campaigns WHERE id=?", (campaign_id,))
        if campaign is None:
            raise KeyError(f"unknown campaign: {campaign_id}")
        provider = campaign["provider"]
        health = self.db.one(
            "SELECT successes, failures, circuit_open_until FROM provider_health WHERE provider=?",
            (provider,),
        )
        successes = health["successes"] if health else 0
        failures = health["failures"] if health else 0
        attempts = successes + failures
        failure_rate = failures / attempts if attempts else 0.0
        return SafetySnapshot(
            available_agents=self.db.one(
                "SELECT COUNT(*) n FROM agents WHERE campaign_id=? AND state=?",
                (campaign_id, AgentState.AVAILABLE.value),
            )["n"],
            ringing_calls=self.db.one(
                "SELECT COUNT(*) n FROM calls WHERE campaign_id=? AND agent_id IS NULL "
                "AND state IN (?, ?, ?)",
                (
                    campaign_id,
                    CallState.RESERVED.value,
                    CallState.INITIATED.value,
                    CallState.RINGING.value,
                ),
            )["n"],
            answered_unassigned=self.db.one(
                "SELECT COUNT(*) n FROM calls WHERE campaign_id=? AND state=? AND agent_id IS NULL",
                (campaign_id, CallState.ANSWERED.value),
            )["n"],
            provider_healthy=not health or health["circuit_open_until"] <= now,
            provider_failure_rate=failure_rate,
            ready_borrowers=self.db.one(
                "SELECT COUNT(*) n FROM borrowers WHERE campaign_id=? AND state=?",
                (campaign_id, BorrowerState.READY.value),
            )["n"],
        )

    def pace(self, campaign_id: str, now: float | None = None) -> tuple[PacingRequest, SafetyDecision, list[str]]:
        now = time.time() if now is None else now
        campaign = self.db.one(
            "SELECT mode, answer_rate FROM campaigns WHERE id=? AND enabled=1",
            (campaign_id,),
        )
        if campaign is None:
            raise KeyError(f"unknown or disabled campaign: {campaign_id}")
        snapshot = self.snapshot(campaign_id, now)
        mode = DialMode(campaign["mode"])
        pacer = self.progressive if mode is DialMode.PROGRESSIVE else self.predictive
        request = pacer.propose(campaign_id, snapshot, campaign["answer_rate"], now)
        decision = self.safety.decide(request, snapshot)
        calls = self.allocator.allocate(decision, now)
        return request, decision, calls

    def tick(self, now: float | None = None, dispatch_limit: int = 100) -> dict[str, int]:
        now = time.time() if now is None else now
        released = self.events.release_wrap_up(now)
        dispatches = self.dispatcher.dispatch_available(now, dispatch_limit)
        event_count = 0
        duplicates = 0
        for provider in self.providers.all():
            for event in provider.poll_events(now):
                result = self.events.process(event, now)
                event_count += 1
                duplicates += int(result == "DUPLICATE_IGNORED")
        return {
            "released_agents": released,
            "dispatches": len(dispatches),
            "provider_events": event_count,
            "duplicates_ignored": duplicates,
        }

    def drop_agents(self, campaign_id: str, count: int, now: float) -> dict[str, int]:
        """Take agents offline and cancel any call still being set up for them."""
        cancelled_provider_calls: list[tuple[str, str]] = []
        dropped = 0
        cancelled = 0
        with self.db.transaction() as conn:
            agents = conn.execute(
                "SELECT id, state, call_id FROM agents WHERE campaign_id=? AND state != ? "
                "ORDER BY CASE state WHEN ? THEN 0 WHEN ? THEN 1 WHEN ? THEN 2 ELSE 3 END, id LIMIT ?",
                (
                    campaign_id,
                    AgentState.OFFLINE.value,
                    AgentState.AVAILABLE.value,
                    AgentState.RESERVED.value,
                    AgentState.DIALING.value,
                    count,
                ),
            ).fetchall()
            for agent in agents:
                conn.execute(
                    "UPDATE agents SET state=?, call_id=NULL, version=version+1, updated_at=? WHERE id=?",
                    (AgentState.OFFLINE.value, now, agent["id"]),
                )
                dropped += 1
                if agent["call_id"] and agent["state"] in {
                    AgentState.RESERVED.value,
                    AgentState.DIALING.value,
                }:
                    call = conn.execute(
                        "SELECT borrower_id, provider, provider_call_id, state FROM calls WHERE id=?",
                        (agent["call_id"],),
                    ).fetchone()
                    if call and call["state"] not in {
                        CallState.CONNECTED.value,
                        CallState.COMPLETED.value,
                    }:
                        conn.execute(
                            "UPDATE calls SET state=?, state_rank=6, failure_reason=?, completed_at=?, "
                            "version=version+1, updated_at=? WHERE id=?",
                            (
                                CallState.CANCELLED.value,
                                "agent disappeared during setup",
                                now,
                                now,
                                agent["call_id"],
                            ),
                        )
                        conn.execute(
                            "UPDATE initiation_jobs SET status='CANCELLED', updated_at=? WHERE call_id=?",
                            (now, agent["call_id"]),
                        )
                        conn.execute(
                            "UPDATE borrowers SET state=?, call_id=NULL, version=version+1, updated_at=? "
                            "WHERE id=?",
                            (BorrowerState.READY.value, now, call["borrower_id"]),
                        )
                        if call["provider_call_id"]:
                            cancelled_provider_calls.append((call["provider"], call["provider_call_id"]))
                        cancelled += 1
        for provider_name, provider_call_id in cancelled_provider_calls:
            self.providers.get(provider_name).cancel_call(provider_call_id, now)
        predictive_cancelled = self.reconcile_predictive_exposure(campaign_id, now)
        return {
            "agents_dropped": dropped,
            "setup_calls_cancelled": cancelled,
            "predictive_calls_cancelled": predictive_cancelled,
        }

    def reconcile_predictive_exposure(self, campaign_id: str, now: float) -> int:
        """Cancel excess unassigned attempts after an abrupt capacity drop."""
        campaign = self.db.one(
            "SELECT mode, answer_rate FROM campaigns WHERE id=?", (campaign_id,)
        )
        if campaign is None or campaign["mode"] != DialMode.PREDICTIVE.value:
            return 0
        snapshot = self.snapshot(campaign_id, now)
        empty_exposure = SafetySnapshot(
            available_agents=snapshot.available_agents,
            ringing_calls=0,
            answered_unassigned=snapshot.answered_unassigned,
            provider_healthy=snapshot.provider_healthy,
            provider_failure_rate=snapshot.provider_failure_rate,
            ready_borrowers=snapshot.ready_borrowers,
        )
        allowed_total, _ = self.safety.predictive_limit(
            empty_exposure, campaign["answer_rate"]
        )
        excess = max(0, snapshot.ringing_calls - allowed_total)
        if excess == 0:
            return 0

        provider_cancellations: list[tuple[str, str]] = []
        cancelled = 0
        with self.db.transaction() as conn:
            calls = conn.execute(
                "SELECT id, borrower_id, provider, provider_call_id FROM calls "
                "WHERE campaign_id=? AND agent_id IS NULL AND state IN (?, ?, ?) "
                "ORDER BY created_at DESC, id LIMIT ?",
                (
                    campaign_id,
                    CallState.RESERVED.value,
                    CallState.INITIATED.value,
                    CallState.RINGING.value,
                    excess,
                ),
            ).fetchall()
            for call in calls:
                conn.execute(
                    "UPDATE calls SET state=?, state_rank=6, completed_at=?, failure_reason=?, "
                    "version=version+1, updated_at=? WHERE id=?",
                    (
                        CallState.CANCELLED.value,
                        now,
                        "predictive exposure reduced after agent capacity drop",
                        now,
                        call["id"],
                    ),
                )
                conn.execute(
                    "UPDATE initiation_jobs SET status='CANCELLED', updated_at=? WHERE call_id=?",
                    (now, call["id"]),
                )
                conn.execute(
                    "UPDATE borrowers SET state=?, call_id=NULL, version=version+1, updated_at=? "
                    "WHERE id=?",
                    (BorrowerState.READY.value, now, call["borrower_id"]),
                )
                if call["provider_call_id"]:
                    provider_cancellations.append(
                        (call["provider"], call["provider_call_id"])
                    )
                cancelled += 1
        for provider_name, provider_call_id in provider_cancellations:
            self.providers.get(provider_name).cancel_call(provider_call_id, now)
        return cancelled

    def close(self) -> None:
        self.db.close()
