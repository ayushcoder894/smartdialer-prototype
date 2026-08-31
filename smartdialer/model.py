from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class DialMode(StrEnum):
    PROGRESSIVE = "PROGRESSIVE"
    PREDICTIVE = "PREDICTIVE"


class AgentState(StrEnum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"


class BorrowerState(StrEnum):
    READY = "READY"
    RESERVED = "RESERVED"
    DONE = "DONE"


class CallState(StrEnum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    ABANDONED = "ABANDONED"


class ProviderEventType(StrEnum):
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


TERMINAL_CALL_STATES = {
    CallState.COMPLETED,
    CallState.FAILED,
    CallState.CANCELLED,
    CallState.ABANDONED,
}

ACTIVE_AGENT_STATES = {
    AgentState.RESERVED,
    AgentState.DIALING,
    AgentState.CONNECTED,
}


@dataclass(frozen=True)
class PacingRequest:
    campaign_id: str
    mode: DialMode
    requested_calls: int
    answer_rate: float
    reason: str
    now: float


@dataclass(frozen=True)
class SafetySnapshot:
    available_agents: int
    ringing_calls: int
    answered_unassigned: int
    provider_healthy: bool
    provider_failure_rate: float
    ready_borrowers: int


@dataclass(frozen=True)
class SafetyDecision:
    decision_id: str
    campaign_id: str
    mode: DialMode
    requested_calls: int
    approved_calls: int
    outcome: str
    reason: str
    expires_at: float
    signature: str


@dataclass(frozen=True)
class ProviderEvent:
    event_id: str
    provider: str
    provider_call_id: str
    call_id: str
    event_type: ProviderEventType
    occurred_at: float


@dataclass(frozen=True)
class DispatchResult:
    call_id: str
    status: str
    detail: str = ""

