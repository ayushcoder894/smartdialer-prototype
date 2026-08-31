from __future__ import annotations

import time
from dataclasses import asdict, dataclass

from .model import AgentState, CallState, DialMode
from .providers import FastReliableProvider, ProviderRegistry, SlowChaoticProvider
from .service import SmartDialer


SCENARIOS = {
    "A": {"answer_rate": 0.20, "talk_time": 120.0},
    "B": {"answer_rate": 0.50, "talk_time": 90.0},
    "C": {"answer_rate": 0.70, "talk_time": 180.0},
    "D": {"answer_rate": 0.35, "talk_time": 100.0},
}


@dataclass
class SimulationResult:
    scenario: str
    mode: str
    agents: int
    duration_seconds: int
    agent_utilization: float
    calls_initiated: int
    calls_connected: int
    calls_completed: int
    calls_failed: int
    calls_abandoned: int
    safety_approved: int
    safety_reduced: int
    safety_rejected: int
    safety_fallback: int
    duplicate_events_ignored: int
    final_answer_rate: float
    wall_time_seconds: float

    def to_dict(self) -> dict[str, str | int | float]:
        return asdict(self)


def run_simulation(
    *,
    scenario: str = "B",
    mode: DialMode = DialMode.PREDICTIVE,
    provider_kind: str = "fast",
    agents: int = 50,
    borrowers: int = 2000,
    duration: int = 600,
    seed: int = 42,
) -> SimulationResult:
    config = SCENARIOS[scenario]
    provider_cls = FastReliableProvider if provider_kind == "fast" else SlowChaoticProvider
    provider = provider_cls(
        answer_probability=config["answer_rate"],
        talk_time=config["talk_time"],
        seed=seed,
    )
    dialer = SmartDialer(providers=ProviderRegistry([provider]))
    campaign_id = f"scenario-{scenario.lower()}"
    dialer.create_campaign(
        campaign_id,
        mode=mode,
        provider=provider.name,
        agents=agents,
        borrowers=borrowers,
        answer_rate=config["answer_rate"],
        avg_talk_time=config["talk_time"],
        now=0.0,
    )

    connected_agent_seconds = 0.0
    online_agent_seconds = 0.0
    duplicate_events = 0
    started = time.perf_counter()
    for second in range(duration + 1):
        now = float(second)
        if scenario == "D" and second in {duration // 3, 2 * duration // 3}:
            new_p = 0.72 if second == duration // 3 else 0.10
            new_talk = 160.0 if second == duration // 3 else 45.0
            provider.answer_probability = new_p
            provider.talk_time = new_talk
            # Recent behavior reaches the estimator; the safety margin still
            # protects the change between observations.
            dialer.db.execute(
                "UPDATE campaigns SET answer_rate=?, avg_talk_time=? WHERE id=?",
                (new_p, new_talk, campaign_id),
            )

        tick_metrics = dialer.tick(now, dispatch_limit=1000)
        duplicate_events += tick_metrics["duplicates_ignored"]
        # Pace once per second. Outstanding ringing calls are included in every
        # subsequent proposal and Safety Controller decision.
        dialer.pace(campaign_id, now)

        states = dialer.db.all(
            "SELECT state, COUNT(*) n FROM agents WHERE campaign_id=? GROUP BY state",
            (campaign_id,),
        )
        state_counts = {row["state"]: row["n"] for row in states}
        connected_agent_seconds += state_counts.get(AgentState.CONNECTED.value, 0)
        online_agent_seconds += agents - state_counts.get(AgentState.OFFLINE.value, 0)

    calls = dialer.db.all(
        "SELECT state, COUNT(*) n FROM calls WHERE campaign_id=? GROUP BY state",
        (campaign_id,),
    )
    call_counts = {row["state"]: row["n"] for row in calls}
    decisions = dialer.db.all(
        "SELECT outcome, COUNT(*) n FROM safety_decisions WHERE campaign_id=? GROUP BY outcome",
        (campaign_id,),
    )
    decision_counts = {row["outcome"]: row["n"] for row in decisions}
    campaign = dialer.db.one("SELECT answer_rate FROM campaigns WHERE id=?", (campaign_id,))
    connected_ever = dialer.db.one(
        "SELECT COUNT(*) n FROM calls WHERE campaign_id=? AND connected_at IS NOT NULL",
        (campaign_id,),
    )["n"]
    result = SimulationResult(
        scenario=scenario,
        mode=mode.value,
        agents=agents,
        duration_seconds=duration,
        agent_utilization=(connected_agent_seconds / online_agent_seconds if online_agent_seconds else 0.0),
        calls_initiated=provider.unique_starts,
        calls_connected=connected_ever,
        calls_completed=call_counts.get(CallState.COMPLETED.value, 0),
        calls_failed=call_counts.get(CallState.FAILED.value, 0),
        calls_abandoned=call_counts.get(CallState.ABANDONED.value, 0),
        safety_approved=decision_counts.get("APPROVE", 0),
        safety_reduced=decision_counts.get("REDUCE", 0),
        safety_rejected=decision_counts.get("REJECT", 0),
        safety_fallback=decision_counts.get("FALLBACK_PROGRESSIVE", 0),
        duplicate_events_ignored=duplicate_events,
        final_answer_rate=campaign["answer_rate"],
        wall_time_seconds=time.perf_counter() - started,
    )
    dialer.close()
    return result


def run_load_test(agents: int = 1000, borrowers: int = 10000) -> dict[str, int | float | bool]:
    """Basic allocation/dispatch throughput and invariant check, not a benchmark."""
    provider = FastReliableProvider(answer_probability=0.3, seed=99)
    dialer = SmartDialer(providers=ProviderRegistry([provider]))
    dialer.create_campaign(
        "load",
        mode=DialMode.PREDICTIVE,
        provider=provider.name,
        agents=agents,
        borrowers=borrowers,
        answer_rate=0.3,
        now=0.0,
    )
    started = time.perf_counter()
    _, decision, calls = dialer.pace("load", 0.0)
    dispatched = dialer.dispatcher.dispatch_available(0.0, limit=borrowers)
    elapsed = time.perf_counter() - started
    duplicate_borrowers = dialer.db.one(
        "SELECT COUNT(*) n FROM (SELECT borrower_id FROM calls GROUP BY borrower_id HAVING COUNT(*)>1)"
    )["n"]
    duplicate_agents = dialer.db.one(
        "SELECT COUNT(*) n FROM (SELECT agent_id FROM calls WHERE agent_id IS NOT NULL "
        "GROUP BY agent_id HAVING COUNT(*)>1)"
    )["n"]
    pending_jobs = dialer.db.one(
        "SELECT COUNT(*) n FROM initiation_jobs WHERE status NOT IN ('DONE', 'CANCELLED')"
    )["n"]
    result: dict[str, int | float | bool] = {
        "agents": agents,
        "borrowers": borrowers,
        "safety_approved_calls": decision.approved_calls,
        "allocated_calls": len(calls),
        "dispatched_calls": len(dispatched),
        "elapsed_seconds": elapsed,
        "allocations_per_second": len(calls) / elapsed if elapsed else 0.0,
        "duplicate_borrowers": duplicate_borrowers,
        "duplicate_agents": duplicate_agents,
        "pending_jobs": pending_jobs,
        "invariants_hold": duplicate_borrowers == 0 and duplicate_agents == 0,
    }
    dialer.close()
    return result
