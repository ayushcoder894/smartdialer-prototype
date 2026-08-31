# SmartDialer

A safety-first functional prototype of progressive and predictive outbound dialing.
It is intentionally one Python process plus SQLite: enough machinery to prove the
concurrency, state, idempotency, recovery, pacing, and provider-boundary decisions
without hiding them behind infrastructure.

## Run it

Requirements: Python 3.11 or newer. There are no runtime dependencies.

```bash
python -m unittest discover -v
python -m smartdialer simulate --scenario B --mode predictive
python -m smartdialer simulate --scenario D --mode predictive --provider chaotic
python -m smartdialer failure-demo
python -m smartdialer load-test --agents 1000 --borrowers 10000
```

Optional editable install:

```bash
python -m pip install -e .
smartdialer simulate --scenario A --mode progressive
```

Scenarios are the assignment's inputs: A is 20%/120s, B is 50%/90s, C is
70%/180s, and D changes its answer rate and talk time twice while running. Every
command writes structured JSON, so runs can be compared or piped into another tool.

## Architecture

```mermaid
flowchart LR
    C[Campaign scheduler] --> P[Pacing engine]
    P -->|proposal only| S[Safety Controller]
    S -->|signed, expiring, single-use permit| A[Call Allocator]
    A -->|one DB transaction| DB[(SQLite source of truth)]
    A --> O[Durable initiation outbox]
    O --> D[Dispatcher workers]
    D -->|call ID = idempotency key| T[Telecom provider]
    T -->|duplicate / stale / out-of-order events| E[Event reducer]
    E --> DB
    DB --> P
```

The pacing engine has no provider reference. The allocator rejects permits that
are forged, expired, altered, or already consumed. This makes the Safety Controller
a structural boundary rather than an `if safety_enabled` flag.

### Progressive mode

The pacer requests `min(AVAILABLE agents, READY borrowers)`. In the same
`BEGIN IMMEDIATE` transaction, the allocator claims one agent and one borrower,
creates their call, and creates its initiation job. An agent is moved to `RESERVED`
before the provider is called. Two workers cannot both claim it: SQLite serializes
the write transactions, and each update also checks state plus version as a
compare-and-swap predicate.

### Predictive mode

The transparent pacing proposal is:

```text
target_answers    = floor(available_agents * 0.92)
expected_inflight = ringing_calls * estimated_answer_rate
answer_gap        = max(0, target_answers - unassigned_answers - expected_inflight)
requested_calls   = ceil(answer_gap / estimated_answer_rate)
```

The Safety Controller independently recalculates a maximum. It raises the answer
rate by an 8-point uncertainty margin, then finds the largest ringing population
whose binomial probability of answers exceeding currently free capacity is at most
1%. It additionally enforces a 4x hard ring-to-capacity ratio, borrower availability,
a 500-call batch cap, provider circuit state, and permit expiry. It records the full
reason for every approve/reduce/reject/fallback decision.

This is probabilistic risk control, not a claim of impossible abandonment. A
predictive system that dials more live people than it has agents can never offer
the same zero-overflow proof as progressive dialing. When provider failures reach
25%, the controller falls back to progressive allocation; when the circuit opens,
it authorizes no new calls.

## Correctness invariants

- An agent can have at most one active call; reservation is transactional.
- A borrower can have at most one active call; reservation and call creation commit together.
- A provider side effect always originates from a durable initiation job.
- `call_id` is the provider idempotency key, so crash-after-send recovery does not redial.
- Provider events are unique on `(provider, event_id)` and reduce state monotonically.
- A pending Safety permit reserves its approved exposure, so concurrent pacers cannot over-authorize from stale snapshots.
- Terminal call states never reopen, even when `ANSWERED` arrives after `COMPLETED`.
- Predictive pacing cannot call the allocator or provider without a valid Safety permit.
- Progressive calls reserve an agent before initiation; disappearance during setup cancels the call and releases the borrower.
- Initiation retries are exponential and bounded at three; three consecutive transport failures open the provider circuit.

## Failure demonstrations

`python -m smartdialer failure-demo` runs the requested cases:

| Failure | Behavior |
|---|---|
| Worker crashes after provider accepts | Job lease expires; another worker retries the same idempotency key; one unique outbound call exists. |
| Provider times out | Exponential retries at 2s/4s; third failure opens a 10s circuit, fails the setup call, and releases its agent/borrower. Existing call events continue to be polled. |
| Agent availability drops | Progressive setup calls tied to lost agents are cancelled. Predictive in-flight exposure is immediately recomputed and excess newest calls are cancelled best-effort before the next pacing cycle. |
| Duplicate webhook | Unique event insert fails harmlessly and returns `DUPLICATE_IGNORED`. |
| Out-of-order webhook | State rank and terminal dominance ignore stale regressions. |

## Tests and load check

The test suite covers the safety permit, simultaneous workers racing for one agent,
progressive capacity, predictive reduction, crash recovery, provider outage,
agent disappearance, event idempotency/order, and an end-to-end simulation.

The basic load command creates 1,000 agents and 10,000 borrowers by default,
authorizes a bounded batch, allocates and dispatches it, then checks duplicate-agent
and duplicate-borrower invariants. It is a local smoke/load test, not a production
capacity claim. On the development machine, a representative 500-call batch
completed allocation and mock dispatch in about 0.22 seconds with no duplicates.

## Project map

```text
smartdialer/model.py       domain states and messages
smartdialer/db.py          schema and transaction boundary
smartdialer/pacing.py      progressive and predictive proposals
smartdialer/safety.py      risk guardrails and signed permits
smartdialer/allocation.py  atomic agent/borrower allocation
smartdialer/dispatch.py    outbox, leases, retries, circuit breaker
smartdialer/providers.py   fast/reliable and slow/chaotic providers
smartdialer/events.py      idempotent monotonic webhook reducer
smartdialer/simulation.py  scenarios, metrics, basic load test
smartdialer/cli.py         runnable commands and failure demo
tests/                     adversarial automated tests
docs/                      architecture decisions and state machines
```

See [state machines](docs/state-machines.md) and the
[architecture decision record](docs/architecture-decision.md) for the detailed
reasoning, tradeoffs, and scale path.

## Short answer to the final question

I would run an adaptive predictive pacer behind a non-bypassable controller that
owns a small, explicit risk budget. The pacer may optimize utilization, but only
the controller can mint call permits, and it computes limits from conservative
answer-rate bounds, current ringing exposure, immediately usable agent capacity,
provider health, and hard caps. As uncertainty or failures rise, the permitted
batch shrinks automatically and ultimately becomes one pre-reserved agent per call.
That captures predictive gains while conditions are measurable, but makes the safe
progressive behavior the deterministic floor rather than an optional feature.
