# Architecture decision record

## Decision

Use a modular monolith with Python 3.11 and SQLite/WAL. Keep pacing, safety,
allocation, dispatch, provider adapters, and event reduction as separate modules,
but commit all authoritative allocation state in one relational transaction.

## Why

The hardest assignment problems are competing reservations and recovery across a
non-transactional telecom side effect. A relational database directly supplies the
atomicity and uniqueness needed to prove those properties. SQLite makes the
prototype runnable with no services or dependencies and is entirely adequate for
demonstrating the invariants. Kafka, Redis, or microservices would add operational
surfaces without strengthening this prototype's core proof.

The cost is that SQLite has one write coordinator. That is a deliberate prototype
tradeoff, not the intended 100,000-agent datastore.

## Source of truth and stale caches

The database wins. Cache entries, if introduced later, are hints only and carry
the row version. A cache saying `RESERVED` while the database says `AVAILABLE`
cannot allocate or block an agent by itself; the allocator rereads and conditionally
updates the database. Commit events invalidate/update cache entries asynchronously.

## Concurrency and workers

- Allocation transactions are short and start with `BEGIN IMMEDIATE`.
- Candidate updates include old state and version, preserving compare-and-swap semantics.
- Borrower, agent, call, and outbox job change in one commit.
- Job leases allow crash recovery without permanent ownership.
- Provider initiation is idempotent under `call_id`.
- Webhook uniqueness plus a monotonic reducer handles at-least-once, stale, and out-of-order delivery.
- Unconsumed Safety permits reserve agent/exposure capacity in the database; allocation atomically replaces that reservation with calls, closing the concurrent-pacer snapshot race.

With PostgreSQL, the selection queries become `FOR UPDATE SKIP LOCKED`; the state
and version predicates remain. Campaign partitioning lets independent campaigns
allocate concurrently.

## Provider outage policy

Transport timeouts retry after 2s then 4s. The third failure marks the setup call
failed, returns its agent and borrower, and opens the provider circuit for 10s.
New pacing is rejected while open. Already accepted calls are not guessed dead:
their events continue through the reducer. Production reconciliation would also
query provider call status after a webhook silence timeout. A multi-provider routing
policy can move new work only when campaign/compliance rules permit it; it must not
redial an ambiguous call on another provider.

## Predictive safety argument

Progressive mode is deterministically capacity-safe because every live attempt owns
an agent first. Predictive mode cannot prove zero abandonment while over-dialing:
all ringing borrowers may answer. The controller therefore exposes this as a risk
budget, not a false guarantee. It uses an upper answer-rate estimate and a 1%
overflow tail, plus hard caps and circuit state. Every decision is explainable and
auditable. A stricter deployment can set the overflow budget to zero, which reduces
the system to progressive-equivalent exposure.

If regulation counts an answered call waiting in a media bridge as abandoned, a
hold queue does not solve the proof. If a provider supports a legally acceptable
pre-connect signal or agent-first bridge, that feature can provide deterministic
answer slots and allow safe early setup; this prototype does not assume it.

## What breaks as scale grows

### 100 to 1,000 agents

SQLite's serialized writer and repeated aggregate counts become visible first.
Move to PostgreSQL, add indexed campaign/state queues, use `SKIP LOCKED`, and keep
incremental campaign counters rather than scanning states each pacing tick. Batch
allocations and provider requests, but retain one idempotency key per call.

### 1,000 to 10,000 agents

The hot campaign row/counter and a single dispatcher queue become bottlenecks.
Partition work by `campaign_id` (and shard very large campaigns by stable bucket),
use a transactional outbox/CDC stream, and autoscale stateless dispatchers. Make
Safety Controller counters strongly consistent within the campaign partition;
event analytics may be eventually consistent, allocation capacity may not.

### 10,000 to 100,000 agents

Telecom rate limits, webhook ingestion, and per-tick global coordination dominate.
Use hierarchical pacing: a campaign controller grants short-lived capacity tokens
to shards; shards spend tokens locally and return/expire them. Provider adapters
enforce provider/account CPS limits. Webhooks land in a partitioned durable log and
are reduced by call ID. Reconciliation scans are incremental by updated-time bucket.

The first fix is not merely “add servers”; it is removing shared write/scan hot
spots while preserving exclusive capacity ownership.

## Alternatives rejected for the prototype

- **In-memory locks only:** do not survive process crashes or coordinate processes.
- **Redis lock plus database writes:** introduces split ownership and lock-expiry races.
- **Microservices and a message broker:** harder local operation and distributed transactions before they are justified.
- **ML answer model:** improves estimation but not the safety proof; transparent rules are easier to interrogate in this timebox.

## What is intentionally incomplete

- No real telecom credentials, authentication, UI, recording, DNC/time-zone rules, or PII encryption.
- Provider idempotency is modeled; a real adapter must verify the provider's exact contract.
- Answer-rate estimation is injected from campaign observations rather than trained online.
- Circuit and pacing thresholds are prototype configuration, not compliance policy.
- SQLite is not claimed as the production scale store.

Given another week, the first additions would be PostgreSQL integration tests with
real multi-process workers, provider status reconciliation, property-based state
machine tests, and a shadow-mode controller calibrated from recorded campaign data.
