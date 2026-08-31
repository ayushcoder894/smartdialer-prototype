from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict

from .dispatch import Dispatcher, SimulatedWorkerCrash
from .model import DialMode, ProviderEvent, ProviderEventType
from .providers import FastReliableProvider, ProviderRegistry, SlowChaoticProvider
from .service import SmartDialer
from .simulation import SCENARIOS, run_load_test, run_simulation


def _print_json(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def demo_failures() -> dict[str, object]:
    results: dict[str, object] = {}

    # 1. Crash after the provider accepted the side effect. The expired job is
    # retried with the same idempotency key, returning the original provider ID.
    provider = FastReliableProvider(answer_probability=1.0, seed=1)
    crash_dialer = SmartDialer(providers=ProviderRegistry([provider]))
    crash_dialer.create_campaign(
        "crash", mode=DialMode.PROGRESSIVE, provider=provider.name,
        agents=1, borrowers=1, now=0.0,
    )
    _, _, calls = crash_dialer.pace("crash", 0.0)
    try:
        crash_dialer.dispatcher.dispatch_one(0.0, crash_after_provider=True)
    except SimulatedWorkerCrash:
        pass
    recovery = Dispatcher(
        crash_dialer.db, crash_dialer.providers, worker_id="recovery-worker", lease_seconds=3.0
    ).dispatch_one(3.1)
    results["worker_crash"] = {
        "call_id": calls[0],
        "provider_start_attempts": provider.start_attempts,
        "unique_outbound_calls": provider.unique_starts,
        "recovery_status": recovery.status if recovery else None,
        "safe": provider.unique_starts == 1,
    }
    crash_dialer.close()

    # 2. Provider outage opens the circuit after three timeouts. Existing provider
    # events would still be polled; new initiation is suppressed until cooldown.
    outage_provider = SlowChaoticProvider(timeout_rate=1.0, seed=2)
    outage = SmartDialer(providers=ProviderRegistry([outage_provider]))
    outage.create_campaign(
        "outage", mode=DialMode.PROGRESSIVE, provider=outage_provider.name,
        agents=1, borrowers=2, now=0.0,
    )
    outage.pace("outage", 0.0)
    outage.dispatcher.dispatch_one(0.0)
    outage.dispatcher.dispatch_one(2.1)
    outage.dispatcher.dispatch_one(6.2)
    health = outage.db.one("SELECT * FROM provider_health WHERE provider=?", (outage_provider.name,))
    results["provider_outage"] = {
        "failures": health["failures"],
        "consecutive_failures": health["consecutive_failures"],
        "circuit_open_until": health["circuit_open_until"],
        "new_calls_allowed": outage.snapshot("outage", 6.2).provider_healthy,
    }
    outage.close()

    # 3. Progressive setup calls tied to disappearing agents are cancelled and
    # borrowers are released for later retry.
    drop_provider = FastReliableProvider(seed=3)
    drop = SmartDialer(providers=ProviderRegistry([drop_provider]))
    drop.create_campaign(
        "drop", mode=DialMode.PROGRESSIVE, provider=drop_provider.name,
        agents=5, borrowers=10, now=0.0,
    )
    drop.pace("drop", 0.0)
    results["agent_drop"] = drop.drop_agents("drop", 2, 0.1)
    drop.close()

    # 4/5. Feed a duplicate ANSWERED and then stale events after terminal state.
    event_provider = FastReliableProvider(answer_probability=0.0, seed=4)
    hostile = SmartDialer(providers=ProviderRegistry([event_provider]))
    hostile.create_campaign(
        "events", mode=DialMode.PROGRESSIVE, provider=event_provider.name,
        agents=1, borrowers=1, now=0.0,
    )
    _, _, event_calls = hostile.pace("events", 0.0)
    hostile.dispatcher.dispatch_one(0.0)
    call_id = event_calls[0]
    provider_call_id = hostile.db.one("SELECT provider_call_id FROM calls WHERE id=?", (call_id,))[0]
    completed = ProviderEvent(
        "evt-completed", event_provider.name, provider_call_id, call_id,
        ProviderEventType.COMPLETED, 5.0,
    )
    answered = ProviderEvent(
        "evt-answered", event_provider.name, provider_call_id, call_id,
        ProviderEventType.ANSWERED, 4.0,
    )
    first = hostile.events.process(completed, 5.0)
    stale = hostile.events.process(answered, 5.1)
    duplicate = hostile.events.process(answered, 5.2)
    results["hostile_events"] = {
        "completed_first": first,
        "late_answered": stale,
        "duplicate_answered": duplicate,
        "final_call_state": hostile.db.one("SELECT state FROM calls WHERE id=?", (call_id,))[0],
    }
    hostile.close()
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="smartdialer",
        description="Safety-first progressive/predictive SmartDialer prototype",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    simulate = sub.add_parser("simulate", help="run scenario A, B, C, or D")
    simulate.add_argument("--scenario", choices=sorted(SCENARIOS), default="B")
    simulate.add_argument(
        "--mode", choices=[mode.value.lower() for mode in DialMode], default="predictive"
    )
    simulate.add_argument("--provider", choices=["fast", "chaotic"], default="fast")
    simulate.add_argument("--agents", type=int, default=50)
    simulate.add_argument("--borrowers", type=int, default=2000)
    simulate.add_argument("--duration", type=int, default=600)
    simulate.add_argument("--seed", type=int, default=42)

    load = sub.add_parser("load-test", help="run the basic allocation throughput test")
    load.add_argument("--agents", type=int, default=1000)
    load.add_argument("--borrowers", type=int, default=10000)

    sub.add_parser("failure-demo", help="demonstrate the required failure cases")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "simulate":
        result = run_simulation(
            scenario=args.scenario,
            mode=DialMode(args.mode.upper()),
            provider_kind=args.provider,
            agents=args.agents,
            borrowers=args.borrowers,
            duration=args.duration,
            seed=args.seed,
        )
        _print_json(result.to_dict())
    elif args.command == "load-test":
        _print_json(run_load_test(args.agents, args.borrowers))
    elif args.command == "failure-demo":
        _print_json(demo_failures())
    else:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

