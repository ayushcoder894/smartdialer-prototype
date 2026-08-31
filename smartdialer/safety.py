from __future__ import annotations

import hashlib
import hmac
import math
import secrets
import uuid

from .db import Database
from .model import DialMode, PacingRequest, SafetyDecision, SafetySnapshot


class SafetyController:
    """Mandatory authorization boundary between pacing and allocation.

    Predictive pacing uses a binomial overflow budget plus hard caps. This does
    not claim that predictive dialing can make abandonment mathematically
    impossible; only progressive mode has that property. It bounds risk and
    deterministically falls back or stops when its guardrails say so.
    """

    def __init__(
        self,
        db: Database,
        secret: bytes | None = None,
        *,
        max_overflow_probability: float = 0.01,
        answer_rate_margin: float = 0.08,
        hard_ring_ratio: float = 4.0,
        permit_ttl: float = 5.0,
    ) -> None:
        self.db = db
        self._secret = secret or secrets.token_bytes(32)
        self.max_overflow_probability = max_overflow_probability
        self.answer_rate_margin = answer_rate_margin
        self.hard_ring_ratio = hard_ring_ratio
        self.permit_ttl = permit_ttl

    @staticmethod
    def _binomial_overflow_probability(n: int, p: float, capacity: int) -> float:
        if capacity >= n:
            return 0.0
        if capacity < 0:
            return 1.0
        if n > 200:
            # Continuity-corrected normal approximation keeps the controller
            # fast for the 1k/10k-agent load demonstration.
            mean = n * p
            stddev = math.sqrt(n * p * (1.0 - p))
            if stddev == 0:
                return float(mean > capacity)
            z = (capacity + 0.5 - mean) / stddev
            return 0.5 * math.erfc(z / math.sqrt(2.0))
        # Exact tail for small batches.
        return sum(
            math.comb(n, k) * (p**k) * ((1.0 - p) ** (n - k))
            for k in range(capacity + 1, n + 1)
        )

    def predictive_limit(self, snapshot: SafetySnapshot, p: float) -> tuple[int, str]:
        capacity = max(0, snapshot.available_agents - snapshot.answered_unassigned)
        if capacity == 0:
            return 0, "no immediately available answer capacity"

        p_upper = min(0.98, max(0.02, p + self.answer_rate_margin))
        hard_total = max(capacity, math.floor(capacity * self.hard_ring_ratio))
        allowed_total = 0
        overflow = 0.0
        for total_ringing in range(0, hard_total + 1):
            risk = self._binomial_overflow_probability(total_ringing, p_upper, capacity)
            if risk > self.max_overflow_probability:
                break
            allowed_total = total_ringing
            overflow = risk
        additional = max(0, allowed_total - snapshot.ringing_calls)
        return additional, (
            f"binomial guard: capacity={capacity}, p_upper={p_upper:.3f}, "
            f"existing_ringing={snapshot.ringing_calls}, allowed_total={allowed_total}, "
            f"overflow_risk={overflow:.5f} <= {self.max_overflow_probability:.5f}"
        )

    def _payload(
        self,
        decision_id: str,
        request: PacingRequest,
        approved: int,
        outcome: str,
        expires_at: float,
    ) -> bytes:
        return "|".join(
            (
                decision_id,
                request.campaign_id,
                request.mode.value,
                str(request.requested_calls),
                str(approved),
                outcome,
                f"{expires_at:.6f}",
            )
        ).encode()

    def decide(self, request: PacingRequest, snapshot: SafetySnapshot) -> SafetyDecision:
        # Re-read the capacity while holding the write coordinator. Unconsumed
        # permits reserve exposure, closing the stale-snapshot gap between two
        # concurrent pacing workers. Allocation atomically replaces a reservation
        # with either RESERVED calls or claimed agents.
        with self.db.transaction() as conn:
            campaign = conn.execute(
                "SELECT provider FROM campaigns WHERE id=? AND enabled=1",
                (request.campaign_id,),
            ).fetchone()
            if campaign is None:
                raise KeyError(f"unknown or disabled campaign: {request.campaign_id}")
            pending = conn.execute(
                "SELECT "
                "COALESCE(SUM(CASE WHEN mode=? AND outcome!='FALLBACK_PROGRESSIVE' "
                "THEN approved_calls ELSE 0 END), 0) predictive, "
                "COALESCE(SUM(CASE WHEN mode=? OR outcome='FALLBACK_PROGRESSIVE' "
                "THEN approved_calls ELSE 0 END), 0) progressive "
                "FROM safety_decisions WHERE campaign_id=? AND consumed=0 "
                "AND approved_calls>0 AND expires_at>=?",
                (
                    DialMode.PREDICTIVE.value,
                    DialMode.PROGRESSIVE.value,
                    request.campaign_id,
                    request.now,
                ),
            ).fetchone()
            actual_exposure = conn.execute(
                "SELECT COUNT(*) n FROM calls WHERE campaign_id=? AND agent_id IS NULL "
                "AND state IN ('RESERVED', 'INITIATED', 'RINGING')",
                (request.campaign_id,),
            ).fetchone()["n"]
            available = conn.execute(
                "SELECT COUNT(*) n FROM agents WHERE campaign_id=? AND state='AVAILABLE'",
                (request.campaign_id,),
            ).fetchone()["n"]
            ready = conn.execute(
                "SELECT COUNT(*) n FROM borrowers WHERE campaign_id=? AND state='READY'",
                (request.campaign_id,),
            ).fetchone()["n"]
            answered_unassigned = conn.execute(
                "SELECT COUNT(*) n FROM calls WHERE campaign_id=? AND state='ANSWERED' "
                "AND agent_id IS NULL",
                (request.campaign_id,),
            ).fetchone()["n"]
            health = conn.execute(
                "SELECT successes, failures, circuit_open_until FROM provider_health WHERE provider=?",
                (campaign["provider"],),
            ).fetchone()
            successes = health["successes"] if health else 0
            failures = health["failures"] if health else 0
            attempts = successes + failures
            fresh_snapshot = SafetySnapshot(
                available_agents=max(0, available - pending["progressive"]),
                ringing_calls=actual_exposure + pending["predictive"],
                answered_unassigned=answered_unassigned,
                provider_healthy=not health or health["circuit_open_until"] <= request.now,
                provider_failure_rate=failures / attempts if attempts else 0.0,
                ready_borrowers=max(
                    0, ready - pending["progressive"] - pending["predictive"]
                ),
            )
            decision = self._decide_and_sign(request, fresh_snapshot)
            conn.execute(
                "INSERT INTO safety_decisions(id, campaign_id, mode, requested_calls, "
                "approved_calls, outcome, reason, expires_at, created_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    decision.decision_id,
                    decision.campaign_id,
                    decision.mode.value,
                    decision.requested_calls,
                    decision.approved_calls,
                    decision.outcome,
                    decision.reason,
                    decision.expires_at,
                    request.now,
                ),
            )
            return decision

    def _decide_and_sign(
        self, request: PacingRequest, snapshot: SafetySnapshot
    ) -> SafetyDecision:
        requested = max(0, request.requested_calls)
        reasons: list[str] = []
        outcome = "APPROVE"

        if requested == 0:
            approved = 0
            outcome = "REJECT"
            reasons.append("pacer requested no calls")
        elif not snapshot.provider_healthy:
            approved = 0
            outcome = "REJECT"
            reasons.append("provider circuit is open")
        elif request.mode is DialMode.PROGRESSIVE:
            limit = min(snapshot.available_agents, snapshot.ready_borrowers)
            approved = min(requested, limit)
            reasons.append(f"progressive hard cap=min(free agents, ready borrowers)={limit}")
        elif snapshot.provider_failure_rate >= 0.25:
            # A degraded predictive campaign gets deterministic progressive
            # allocation: every call reserves an agent before initiation.
            limit = min(snapshot.available_agents, snapshot.ready_borrowers)
            approved = min(requested, limit)
            outcome = "FALLBACK_PROGRESSIVE" if approved else "REJECT"
            reasons.append(
                f"provider failure rate {snapshot.provider_failure_rate:.1%} >= 25%; "
                f"fallback cap={limit}"
            )
        else:
            limit, risk_reason = self.predictive_limit(snapshot, request.answer_rate)
            approved = min(requested, limit, snapshot.ready_borrowers)
            reasons.append(risk_reason)

        if approved < requested and outcome == "APPROVE":
            outcome = "REDUCE" if approved else "REJECT"
        reasons.insert(0, request.reason)
        decision_id = f"decision-{uuid.uuid4().hex}"
        expires_at = request.now + self.permit_ttl
        signature = hmac.new(
            self._secret,
            self._payload(decision_id, request, approved, outcome, expires_at),
            hashlib.sha256,
        ).hexdigest()
        return SafetyDecision(
            decision_id=decision_id,
            campaign_id=request.campaign_id,
            mode=request.mode,
            requested_calls=requested,
            approved_calls=approved,
            outcome=outcome,
            reason="; ".join(reasons),
            expires_at=expires_at,
            signature=signature,
        )

    def verify(self, decision: SafetyDecision, now: float) -> bool:
        request = PacingRequest(
            campaign_id=decision.campaign_id,
            mode=decision.mode,
            requested_calls=decision.requested_calls,
            answer_rate=0.0,
            reason="",
            now=now,
        )
        expected = hmac.new(
            self._secret,
            self._payload(
                decision.decision_id,
                request,
                decision.approved_calls,
                decision.outcome,
                decision.expires_at,
            ),
            hashlib.sha256,
        ).hexdigest()
        return now <= decision.expires_at and hmac.compare_digest(expected, decision.signature)
