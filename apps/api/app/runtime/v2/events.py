from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

from app.runtime.v2.features import RuntimeCapabilityConfig
from app.schemas.assistant import AgentRunEvent

if TYPE_CHECKING:
    from app.runtime.v2.active_runs import AgentRunCancellationToken


STEP_EVENT_METADATA: dict[str, tuple[str, str]] = {
    "input_guard": ("guardrail", "检查问题边界"),
    "classify_intent": ("plan", "识别任务意图"),
    "build_agent_plan": ("plan", "制定执行计划"),
    "load_context_snapshot": ("context", "读取授权上下文"),
    "validate_plan": ("guardrail", "校验计划安全性"),
    "select_tools": ("plan", "选择只读工具"),
    "execute_tools": ("tool", "执行资料读取"),
    "execute_workers": ("worker", "整理分析结论"),
    "validate_worker_output": ("guardrail", "校验证据支撑"),
    "policy_guard": ("guardrail", "检查回答边界"),
    "compose_response": ("composer", "组织最终回答"),
    "validate_final_response": ("guardrail", "校验最终输出"),
}


def event_phase_for_step(step_name: str) -> str:
    return STEP_EVENT_METADATA.get(step_name, ("runtime", step_name))[0]


def event_title_for_step(step_name: str) -> str:
    return STEP_EVENT_METADATA.get(step_name, ("runtime", step_name))[1]


class AgentRunEventSink(Protocol):
    def emit_existing(self, event: AgentRunEvent) -> None:
        ...

    def emit(
        self,
        *,
        run_id: str,
        event_type: str,
        phase: str,
        title: str,
        status: str,
        at: datetime | None = None,
        duration_ms: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> AgentRunEvent:
        ...


class NullAgentRunEventSink:
    def emit_existing(self, event: AgentRunEvent) -> None:
        return None

    def emit(
        self,
        *,
        run_id: str,
        event_type: str,
        phase: str,
        title: str,
        status: str,
        at: datetime | None = None,
        duration_ms: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> AgentRunEvent:
        return AgentRunEvent(
            id=f"{run_id}:0000",
            run_id=run_id,
            sequence=0,
            event_type=event_type,
            phase=phase,
            title=title,
            status=status,
            at=at,
            duration_ms=duration_ms,
            payload=payload or {},
        )


class QueueAgentRunEventSink:
    def __init__(self, queue: asyncio.Queue[AgentRunEvent]) -> None:
        self.queue = queue
        self._sequences: dict[str, int] = {}
        self.latest_run_id: str | None = None

    def emit_existing(self, event: AgentRunEvent) -> None:
        self._sequences[event.run_id] = max(
            self._sequences.get(event.run_id, 0),
            event.sequence,
        )
        self.latest_run_id = event.run_id
        self.queue.put_nowait(event)

    def emit(
        self,
        *,
        run_id: str,
        event_type: str,
        phase: str,
        title: str,
        status: str,
        at: datetime | None = None,
        duration_ms: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> AgentRunEvent:
        sequence = self._sequences.get(run_id, 0) + 1
        self._sequences[run_id] = sequence
        event = AgentRunEvent(
            id=f"{run_id}:{sequence:04d}",
            run_id=run_id,
            sequence=sequence,
            event_type=event_type,
            phase=phase,
            title=title,
            status=status,
            at=at,
            duration_ms=duration_ms,
            payload=payload or {},
        )
        self.latest_run_id = run_id
        self.queue.put_nowait(event)
        return event


@dataclass(slots=True)
class AgentTurnContext:
    run_id: str
    session_id: str
    user_id: str
    message: str
    model_name: str
    llm_settings_source: str
    page_context: dict[str, object] | None
    runtime_capabilities: RuntimeCapabilityConfig
    current_date: str
    timezone: str
    event_sink: AgentRunEventSink
    cancellation_token: AgentRunCancellationToken | None = None
