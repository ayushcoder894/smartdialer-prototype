from __future__ import annotations

import heapq
import random
import uuid
from dataclasses import dataclass, field
from typing import Protocol

from .model import ProviderEvent, ProviderEventType


class TelecomProvider(Protocol):
    name: str

    def start_call(self, idempotency_key: str, phone: str, now: float) -> str: ...

    def cancel_call(self, provider_call_id: str, now: float) -> None: ...

    def poll_events(self, now: float) -> list[ProviderEvent]: ...


@dataclass(order=True)
class _ScheduledEvent:
    deliver_at: float
    sequence: int
    event: ProviderEvent = field(compare=False)


class MockProvider:
    """Deterministic, seedable telecom simulator with idempotent initiation."""

    name = "base"

    def __init__(
        self,
        *,
        answer_probability: float = 0.5,
        setup_latency: float = 1.0,
        talk_time: float = 90.0,
        timeout_rate: float = 0.0,
        failure_rate: float = 0.0,
        duplicate_rate: float = 0.0,
        out_of_order_rate: float = 0.0,
        seed: int = 7,
    ) -> None:
        self.answer_probability = answer_probability
        self.setup_latency = setup_latency
        self.talk_time = talk_time
        self.timeout_rate = timeout_rate
        self.failure_rate = failure_rate
        self.duplicate_rate = duplicate_rate
        self.out_of_order_rate = out_of_order_rate
        self._rng = random.Random(seed)
        self._calls: dict[str, str] = {}
        self._cancelled: set[str] = set()
        self._events: list[_ScheduledEvent] = []
        self._sequence = 0
        self.start_attempts = 0
        self.unique_starts = 0

    def _schedule(
        self,
        call_id: str,
        provider_call_id: str,
        event_type: ProviderEventType,
        deliver_at: float,
        occurred_at: float | None = None,
        event_id: str | None = None,
    ) -> None:
        event = ProviderEvent(
            event_id=event_id or f"evt-{uuid.uuid4().hex}",
            provider=self.name,
            provider_call_id=provider_call_id,
            call_id=call_id,
            event_type=event_type,
            occurred_at=deliver_at if occurred_at is None else occurred_at,
        )
        self._sequence += 1
        heapq.heappush(self._events, _ScheduledEvent(deliver_at, self._sequence, event))
        if self._rng.random() < self.duplicate_rate:
            self._sequence += 1
            heapq.heappush(
                self._events,
                _ScheduledEvent(deliver_at + 0.05, self._sequence, event),
            )

    def start_call(self, idempotency_key: str, phone: str, now: float) -> str:
        self.start_attempts += 1
        if idempotency_key in self._calls:
            return self._calls[idempotency_key]
        if self._rng.random() < self.timeout_rate:
            raise TimeoutError(f"{self.name} timed out")

        provider_call_id = f"{self.name}-{uuid.uuid4().hex[:16]}"
        self._calls[idempotency_key] = provider_call_id
        self.unique_starts += 1
        ringing_at = now + self.setup_latency
        self._schedule(idempotency_key, provider_call_id, ProviderEventType.RINGING, ringing_at)

        if self._rng.random() < self.failure_rate:
            self._schedule(
                idempotency_key,
                provider_call_id,
                ProviderEventType.FAILED,
                ringing_at + self.setup_latency,
            )
        elif self._rng.random() < self.answer_probability:
            answered_at = ringing_at + self.setup_latency
            completed_at = answered_at + self.talk_time
            if self._rng.random() < self.out_of_order_rate:
                # Delivery order is hostile even though occurrence timestamps are sane.
                self._schedule(
                    idempotency_key,
                    provider_call_id,
                    ProviderEventType.COMPLETED,
                    answered_at + 0.05,
                    occurred_at=completed_at,
                )
                self._schedule(
                    idempotency_key,
                    provider_call_id,
                    ProviderEventType.ANSWERED,
                    answered_at + 0.10,
                    occurred_at=answered_at,
                )
            else:
                self._schedule(
                    idempotency_key,
                    provider_call_id,
                    ProviderEventType.ANSWERED,
                    answered_at,
                )
                self._schedule(
                    idempotency_key,
                    provider_call_id,
                    ProviderEventType.COMPLETED,
                    completed_at,
                )
        else:
            self._schedule(
                idempotency_key,
                provider_call_id,
                ProviderEventType.FAILED,
                ringing_at + self.setup_latency * 3,
            )
        return provider_call_id

    def cancel_call(self, provider_call_id: str, now: float) -> None:
        self._cancelled.add(provider_call_id)

    def poll_events(self, now: float) -> list[ProviderEvent]:
        result: list[ProviderEvent] = []
        while self._events and self._events[0].deliver_at <= now:
            scheduled = heapq.heappop(self._events)
            if scheduled.event.provider_call_id not in self._cancelled:
                result.append(scheduled.event)
        return result


class FastReliableProvider(MockProvider):
    name = "fast-reliable"

    def __init__(self, **kwargs: float | int) -> None:
        defaults: dict[str, float | int] = {
            "setup_latency": 0.5,
            "timeout_rate": 0.0,
            "failure_rate": 0.01,
            "duplicate_rate": 0.0,
            "out_of_order_rate": 0.0,
        }
        defaults.update(kwargs)
        super().__init__(**defaults)


class SlowChaoticProvider(MockProvider):
    name = "slow-chaotic"

    def __init__(self, **kwargs: float | int) -> None:
        defaults: dict[str, float | int] = {
            "setup_latency": 2.0,
            "timeout_rate": 0.12,
            "failure_rate": 0.08,
            "duplicate_rate": 0.30,
            "out_of_order_rate": 0.15,
        }
        defaults.update(kwargs)
        super().__init__(**defaults)


class ProviderRegistry:
    def __init__(self, providers: list[TelecomProvider]) -> None:
        self._providers = {provider.name: provider for provider in providers}

    def get(self, name: str) -> TelecomProvider:
        try:
            return self._providers[name]
        except KeyError as exc:
            raise KeyError(f"unknown provider: {name}") from exc

    def all(self) -> list[TelecomProvider]:
        return list(self._providers.values())

