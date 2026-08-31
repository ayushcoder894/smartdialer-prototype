from __future__ import annotations

import math

from .model import DialMode, PacingRequest, SafetySnapshot


class ProgressivePacer:
    """One request per currently available agent."""

    def propose(
        self,
        campaign_id: str,
        snapshot: SafetySnapshot,
        answer_rate: float,
        now: float,
    ) -> PacingRequest:
        requested = min(snapshot.available_agents, snapshot.ready_borrowers)
        return PacingRequest(
            campaign_id=campaign_id,
            mode=DialMode.PROGRESSIVE,
            requested_calls=requested,
            answer_rate=answer_rate,
            reason=(
                f"one call per free agent: free={snapshot.available_agents}, "
                f"ready_borrowers={snapshot.ready_borrowers}"
            ),
            now=now,
        )


class PredictivePacer:
    """Transparent rule-based pacing proposal.

    The pacer targets enough *expected* answers to fill free agent capacity, but
    subtracts expected answers already in flight. It deliberately has no provider
    dependency; only the SafetyController can authorize work.
    """

    def __init__(self, utilization_target: float = 0.92, max_batch: int = 500) -> None:
        self.utilization_target = utilization_target
        self.max_batch = max_batch

    def propose(
        self,
        campaign_id: str,
        snapshot: SafetySnapshot,
        answer_rate: float,
        now: float,
    ) -> PacingRequest:
        p = min(0.95, max(0.02, answer_rate))
        target_answers = math.floor(snapshot.available_agents * self.utilization_target)
        expected_inflight = snapshot.ringing_calls * p
        answer_gap = max(0.0, target_answers - snapshot.answered_unassigned - expected_inflight)
        requested = min(
            snapshot.ready_borrowers,
            self.max_batch,
            math.ceil(answer_gap / p),
        )
        return PacingRequest(
            campaign_id=campaign_id,
            mode=DialMode.PREDICTIVE,
            requested_calls=requested,
            answer_rate=p,
            reason=(
                f"target={target_answers} answers ({self.utilization_target:.0%} of "
                f"{snapshot.available_agents} free), expected_inflight={expected_inflight:.2f}, "
                f"gap={answer_gap:.2f}, p={p:.3f}, raw_calls=ceil(gap/p)={requested}"
            ),
            now=now,
        )

