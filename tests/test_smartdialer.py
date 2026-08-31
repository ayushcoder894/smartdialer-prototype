from __future__ import annotations

import dataclasses
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from smartdialer.allocation import CallAllocator
from smartdialer.dispatch import Dispatcher, SimulatedWorkerCrash
from smartdialer.model import (
    AgentState,
    CallState,
    DialMode,
    PacingRequest,
    ProviderEvent,
    ProviderEventType,
)
from smartdialer.providers import FastReliableProvider, ProviderRegistry, SlowChaoticProvider
from smartdialer.service import SmartDialer
from smartdialer.simulation import run_simulation


class SmartDialerTest(unittest.TestCase):
    def make_dialer(self, provider=None) -> tuple[SmartDialer, FastReliableProvider]:
        provider = provider or FastReliableProvider(answer_probability=1.0, seed=10)
        return SmartDialer(providers=ProviderRegistry([provider]), safety_secret=b"test-secret"), provider

    def test_progressive_never_allocates_more_than_available_agents(self) -> None:
        dialer, provider = self.make_dialer()
        self.addCleanup(dialer.close)
        dialer.create_campaign(
            "p", mode=DialMode.PROGRESSIVE, provider=provider.name,
            agents=3, borrowers=20, now=0.0,
        )
        _, decision, calls = dialer.pace("p", 0.0)
        self.assertEqual(3, decision.approved_calls)
        self.assertEqual(3, len(calls))
        counts = dialer.db.counts("p")
        self.assertEqual(3, counts["agents_reserved"])
        self.assertNotIn("agents_available", counts)

    def test_two_workers_cannot_reserve_same_agent(self) -> None:
        dialer, provider = self.make_dialer()
        self.addCleanup(dialer.close)
        dialer.create_campaign(
            "race", mode=DialMode.PROGRESSIVE, provider=provider.name,
            agents=1, borrowers=2, now=0.0,
        )
        snapshot = dialer.snapshot("race", 0.0)
        request = PacingRequest("race", DialMode.PROGRESSIVE, 1, 1.0, "race", 0.0)
        permits = [dialer.safety.decide(request, snapshot) for _ in range(2)]
        barrier = threading.Barrier(2)

        def allocate(permit):
            barrier.wait()
            return CallAllocator(dialer.db, dialer.safety).allocate(permit, 0.1)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(allocate, permits))
        self.assertEqual(1, sum(map(len, results)))
        self.assertEqual(
            1,
            dialer.db.one(
                "SELECT COUNT(*) n FROM agents WHERE state=?", (AgentState.RESERVED.value,)
            )["n"],
        )

    def test_safety_permit_cannot_be_forged_or_replayed(self) -> None:
        dialer, provider = self.make_dialer()
        self.addCleanup(dialer.close)
        dialer.create_campaign(
            "safe", mode=DialMode.PROGRESSIVE, provider=provider.name,
            agents=2, borrowers=4, now=0.0,
        )
        request, permit, _ = dialer.pace("safe", 0.0)
        forged = dataclasses.replace(permit, approved_calls=permit.approved_calls + 100)
        with self.assertRaises(PermissionError):
            dialer.allocator.allocate(forged, 0.1)
        with self.assertRaises(PermissionError):
            dialer.allocator.allocate(permit, 0.1)

    def test_predictive_controller_reduces_unsafe_proposal(self) -> None:
        dialer, provider = self.make_dialer()
        self.addCleanup(dialer.close)
        dialer.create_campaign(
            "pred", mode=DialMode.PREDICTIVE, provider=provider.name,
            agents=2, borrowers=100, answer_rate=0.5, now=0.0,
        )
        snapshot = dialer.snapshot("pred", 0.0)
        request = PacingRequest("pred", DialMode.PREDICTIVE, 100, 0.5, "malicious pacer", 0.0)
        decision = dialer.safety.decide(request, snapshot)
        self.assertEqual("REDUCE", decision.outcome)
        self.assertLess(decision.approved_calls, 100)
        self.assertIn("binomial guard", decision.reason)

    def test_concurrent_predictive_permits_reserve_exposure(self) -> None:
        dialer, provider = self.make_dialer()
        self.addCleanup(dialer.close)
        dialer.create_campaign(
            "permit-race", mode=DialMode.PREDICTIVE, provider=provider.name,
            agents=2, borrowers=100, answer_rate=0.5, now=0.0,
        )
        stale_snapshot = dialer.snapshot("permit-race", 0.0)
        request = PacingRequest(
            "permit-race", DialMode.PREDICTIVE, 100, 0.5, "concurrent proposal", 0.0
        )
        first = dialer.safety.decide(request, stale_snapshot)
        second = dialer.safety.decide(request, stale_snapshot)
        self.assertGreater(first.approved_calls, 0)
        self.assertEqual(0, second.approved_calls)

    def test_worker_crash_retries_provider_idempotently(self) -> None:
        dialer, provider = self.make_dialer()
        self.addCleanup(dialer.close)
        dialer.create_campaign(
            "crash", mode=DialMode.PROGRESSIVE, provider=provider.name,
            agents=1, borrowers=1, now=0.0,
        )
        dialer.pace("crash", 0.0)
        with self.assertRaises(SimulatedWorkerCrash):
            dialer.dispatcher.dispatch_one(0.0, crash_after_provider=True)
        result = Dispatcher(
            dialer.db, dialer.providers, worker_id="replacement", lease_seconds=3.0
        ).dispatch_one(3.1)
        self.assertEqual("DISPATCHED", result.status)
        self.assertEqual(2, provider.start_attempts)
        self.assertEqual(1, provider.unique_starts)

    def test_duplicate_and_out_of_order_events_are_idempotent(self) -> None:
        dialer, provider = self.make_dialer()
        self.addCleanup(dialer.close)
        dialer.create_campaign(
            "events", mode=DialMode.PROGRESSIVE, provider=provider.name,
            agents=1, borrowers=1, now=0.0,
        )
        _, _, calls = dialer.pace("events", 0.0)
        dialer.dispatcher.dispatch_one(0.0)
        call_id = calls[0]
        provider_call_id = dialer.db.one(
            "SELECT provider_call_id FROM calls WHERE id=?", (call_id,)
        )[0]
        completed = ProviderEvent(
            "complete", provider.name, provider_call_id, call_id,
            ProviderEventType.COMPLETED, 5.0,
        )
        answered = ProviderEvent(
            "answer", provider.name, provider_call_id, call_id,
            ProviderEventType.ANSWERED, 4.0,
        )
        self.assertEqual("COMPLETED", dialer.events.process(completed, 5.0))
        self.assertEqual("TERMINAL_IGNORED", dialer.events.process(answered, 5.1))
        self.assertEqual("DUPLICATE_IGNORED", dialer.events.process(answered, 5.2))
        self.assertEqual(
            CallState.COMPLETED.value,
            dialer.db.one("SELECT state FROM calls WHERE id=?", (call_id,))[0],
        )

    def test_provider_outage_opens_circuit_and_releases_agent(self) -> None:
        provider = SlowChaoticProvider(timeout_rate=1.0, seed=11)
        dialer, _ = self.make_dialer(provider)
        self.addCleanup(dialer.close)
        dialer.create_campaign(
            "outage", mode=DialMode.PROGRESSIVE, provider=provider.name,
            agents=1, borrowers=1, now=0.0,
        )
        dialer.pace("outage", 0.0)
        dialer.dispatcher.dispatch_one(0.0)
        dialer.dispatcher.dispatch_one(2.1)
        dialer.dispatcher.dispatch_one(6.2)
        self.assertFalse(dialer.snapshot("outage", 6.2).provider_healthy)
        self.assertEqual(
            AgentState.AVAILABLE.value,
            dialer.db.one("SELECT state FROM agents")[0],
        )
        self.assertEqual(CallState.FAILED.value, dialer.db.one("SELECT state FROM calls")[0])

    def test_agent_disappearance_cancels_setup_and_releases_borrower(self) -> None:
        dialer, provider = self.make_dialer()
        self.addCleanup(dialer.close)
        dialer.create_campaign(
            "drop", mode=DialMode.PROGRESSIVE, provider=provider.name,
            agents=2, borrowers=2, now=0.0,
        )
        dialer.pace("drop", 0.0)
        result = dialer.drop_agents("drop", 1, 0.1)
        self.assertEqual(1, result["setup_calls_cancelled"])
        self.assertEqual(1, dialer.db.one(
            "SELECT COUNT(*) n FROM calls WHERE state=?", (CallState.CANCELLED.value,)
        )["n"])

    def test_agent_drop_cancels_excess_predictive_exposure(self) -> None:
        dialer, provider = self.make_dialer()
        self.addCleanup(dialer.close)
        dialer.create_campaign(
            "predictive-drop", mode=DialMode.PREDICTIVE, provider=provider.name,
            agents=10, borrowers=100, answer_rate=0.2, now=0.0,
        )
        _, _, calls = dialer.pace("predictive-drop", 0.0)
        self.assertGreater(len(calls), 2)
        result = dialer.drop_agents("predictive-drop", 8, 0.1)
        self.assertGreater(result["predictive_calls_cancelled"], 0)
        snapshot = dialer.snapshot("predictive-drop", 0.1)
        empty = dataclasses.replace(snapshot, ringing_calls=0)
        allowed, _ = dialer.safety.predictive_limit(empty, 0.2)
        self.assertLessEqual(snapshot.ringing_calls, allowed)

    def test_small_simulation_runs_and_reports_metrics(self) -> None:
        result = run_simulation(
            scenario="A", mode=DialMode.PREDICTIVE, agents=5,
            borrowers=50, duration=20, seed=12,
        )
        self.assertGreater(result.calls_initiated, 0)
        self.assertGreaterEqual(result.agent_utilization, 0.0)
        self.assertLessEqual(result.agent_utilization, 1.0)


if __name__ == "__main__":
    unittest.main()
