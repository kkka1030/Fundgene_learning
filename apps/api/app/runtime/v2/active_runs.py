from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event, Lock


class AgentRunCancelled(Exception):
    """Raised when a user asks to stop the active advisor run."""


class AgentRunCancellationToken:
    def __init__(self) -> None:
        self._cancel_requested = Event()
        self._cancel_requested_event_emitted = Event()

    def request_cancel(self) -> None:
        self._cancel_requested.set()

    @property
    def cancel_requested(self) -> bool:
        return self._cancel_requested.is_set()

    def mark_cancel_requested_event_emitted(self) -> bool:
        if self._cancel_requested_event_emitted.is_set():
            return False
        self._cancel_requested_event_emitted.set()
        return True


@dataclass(slots=True)
class ActiveAgentRun:
    run_id: str
    session_id: str
    user_id: str
    started_at: datetime
    cancellation_token: AgentRunCancellationToken
    latest_event_sequence: int = 0


@dataclass(slots=True)
class ActiveAgentRunSnapshot:
    run_id: str
    session_id: str
    user_id: str
    started_at: datetime
    cancel_requested: bool


class ActiveAgentRunRegistry:
    def __init__(self) -> None:
        self._runs: dict[str, ActiveAgentRun] = {}
        self._lock = Lock()

    def register(
        self,
        *,
        run_id: str,
        session_id: str,
        user_id: str,
    ) -> ActiveAgentRun:
        active_run = ActiveAgentRun(
            run_id=run_id,
            session_id=session_id,
            user_id=user_id,
            started_at=datetime.now(timezone.utc),
            cancellation_token=AgentRunCancellationToken(),
        )
        with self._lock:
            self._runs[run_id] = active_run
        return active_run

    def request_cancel(
        self,
        *,
        run_id: str,
        user_id: str,
    ) -> ActiveAgentRunSnapshot | None:
        with self._lock:
            active_run = self._runs.get(run_id)
            if active_run is None or active_run.user_id != user_id:
                return None
            active_run.cancellation_token.request_cancel()
            return self._snapshot(active_run)

    def unregister(self, *, run_id: str) -> None:
        with self._lock:
            self._runs.pop(run_id, None)

    def mark_cancel_requested_event_emitted(
        self,
        *,
        run_id: str,
        user_id: str,
    ) -> bool:
        with self._lock:
            active_run = self._runs.get(run_id)
            if active_run is None or active_run.user_id != user_id:
                return False
            return active_run.cancellation_token.mark_cancel_requested_event_emitted()

    def reserve_event_sequence(self, *, run_id: str) -> int | None:
        with self._lock:
            active_run = self._runs.get(run_id)
            if active_run is None:
                return None
            active_run.latest_event_sequence += 1
            return active_run.latest_event_sequence

    def observe_event_sequence(self, *, run_id: str, sequence: int) -> None:
        with self._lock:
            active_run = self._runs.get(run_id)
            if active_run is not None:
                active_run.latest_event_sequence = max(
                    active_run.latest_event_sequence,
                    sequence,
                )

    def get(self, *, run_id: str) -> ActiveAgentRunSnapshot | None:
        with self._lock:
            active_run = self._runs.get(run_id)
            return self._snapshot(active_run) if active_run is not None else None

    def list_for_user(
        self,
        *,
        user_id: str,
        session_id: str | None = None,
    ) -> list[ActiveAgentRunSnapshot]:
        with self._lock:
            snapshots = [
                self._snapshot(active_run)
                for active_run in self._runs.values()
                if active_run.user_id == user_id
                and (session_id is None or active_run.session_id == session_id)
            ]
        return sorted(snapshots, key=lambda item: item.started_at, reverse=True)

    @staticmethod
    def _snapshot(active_run: ActiveAgentRun) -> ActiveAgentRunSnapshot:
        return ActiveAgentRunSnapshot(
            run_id=active_run.run_id,
            session_id=active_run.session_id,
            user_id=active_run.user_id,
            started_at=active_run.started_at,
            cancel_requested=active_run.cancellation_token.cancel_requested,
        )


_active_agent_run_registry = ActiveAgentRunRegistry()


def get_active_agent_run_registry() -> ActiveAgentRunRegistry:
    return _active_agent_run_registry
