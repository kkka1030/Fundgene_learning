import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from app.api.routes import assistant as assistant_routes
from app.models.agent_evidence_ref import AgentEvidenceRef
from app.models.agent_run import AgentRun
from app.models.agent_run_event import AgentRunEventRecord
from app.models.agent_step import AgentStep
from app.models.agent_tool_call import AgentToolCall
from app.models.chat_message import ChatMessage
from app.models.chat_session import ChatSession
from app.models.user import UserProfile
from app.runtime.v2.active_runs import (
    AgentRunCancelled,
    get_active_agent_run_registry,
)
from app.runtime.v2.features import build_runtime_capabilities
from app.runtime.v2.orchestrator import AdvisorOrchestrator
from app.runtime.v2.planner import AgentPlanner
from app.runtime.v2.schemas import (
    AgentPlan,
    EvidenceRef,
    PlannedToolCall,
    PolicyResult,
    ToolSelectionSignal,
    WorkerOutput,
)
from app.runtime.v2.tools.registry import ToolRegistry
from app.schemas.assistant import AdvisorResponse
from app.services.agent_run_events import DurableAgentRunEventSink


def _parse_sse_events(text: str) -> list[tuple[str, dict]]:
    events: list[tuple[str, dict]] = []
    for block in text.strip().split("\n\n"):
        if not block.strip():
            continue
        event_name = "message"
        data = ""
        for line in block.splitlines():
            if line.startswith("event:"):
                event_name = line.removeprefix("event:").strip()
            if line.startswith("data:"):
                data += line.removeprefix("data:").strip()
        if data:
            events.append((event_name, json.loads(data)))
    return events


def _onboard(client: TestClient, *, email: str = "agent-v2@example.com") -> None:
    response = client.post(
        "/api/auth/register",
        json={"email": email, "password": "supersecure123"},
    )
    assert response.status_code == 201

    response = client.post(
        "/api/onboarding/profile",
        json={
            "display_name": "Ava",
            "investing_experience": "beginner",
            "monthly_contribution_band": "under_3000",
            "primary_goal": "建立长期基金投资纪律。",
        },
    )
    assert response.status_code == 200

    response = client.post(
        "/api/behavior/questionnaires",
        json={
            "questionnaire_version": "v1",
            "answers": {
                "volatility_comfort": 4,
                "drawdown_reaction": 3,
                "investment_horizon": 4,
                "panic_sell_impulse": 2,
                "chase_hot_funds": 4,
                "diversification_habit": 3,
            },
        },
    )
    assert response.status_code == 200


def test_agent_runtime_v2_persists_trace_and_exposes_owned_trace(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _onboard(client)

    response = client.post(
        "/api/assistant/messages",
        json={"message": "我想理解基金回撤和风险等级。"},
    )
    assert response.status_code == 200
    payload = response.json()
    assistant_message = payload["messages"][-1]
    run_id = assistant_message["agent_run_id"]
    assert run_id is not None
    assert assistant_message["advisor_response"]["intent"] == "learning"

    trace_response = client.get(f"/api/assistant/runs/{run_id}/trace")
    assert trace_response.status_code == 200
    trace_payload = trace_response.json()
    assert trace_payload["run"]["id"] == run_id
    assert trace_payload["run"]["orchestrator_version"] == "agent_runtime_v2"
    assert trace_payload["run"]["policy_status"] == "allow"
    assert trace_payload["context_snapshot"]["display_name"] == "Ava"
    assert [step["step_name"] for step in trace_payload["steps"]] == [
        "input_guard",
        "classify_intent",
        "build_agent_plan",
        "load_context_snapshot",
        "validate_plan",
        "select_tools",
        "execute_tools",
        "execute_workers",
        "validate_worker_output",
        "policy_guard",
        "compose_response",
        "validate_final_response",
    ]
    plan_step = next(
        step
        for step in trace_payload["steps"]
        if step["step_name"] == "build_agent_plan"
    )
    assert plan_step["output_payload"]["schema_version"] == "agent_plan_v1"
    assert plan_step["output_payload"]["primary_intent"] == "learning"
    assert "LearningWorker" in plan_step["output_payload"]["worker_names"]
    selected_skills = plan_step["output_payload"]["selected_skills"]
    assert selected_skills[0]["skill_name"] == "fund_basics_explainer_v1"
    assert selected_skills[0]["skill_version"] == "2026-05-16"
    assert "learning.evidence_search" in selected_skills[0]["required_tools"]
    assert {
        item["tool_name"] for item in plan_step["output_payload"]["planned_tools"]
    } >= {"learning.concept_map", "learning.evidence_search"}
    concept_signal = next(
        signal
        for signal in plan_step["output_payload"]["tool_selection_signals"]
        if signal["tool_name"] == "learning.concept_map"
    )
    assert concept_signal["source"] in {"required", "scored_search"}
    if concept_signal["source"] == "scored_search":
        assert {"回撤", "风险等级"} & set(concept_signal["matched_terms"])
    validate_plan_step = next(
        step for step in trace_payload["steps"] if step["step_name"] == "validate_plan"
    )
    assert validate_plan_step["output_payload"]["status"] == "pass"
    final_validation_step = next(
        step
        for step in trace_payload["steps"]
        if step["step_name"] == "validate_final_response"
    )
    assert final_validation_step["output_payload"]["schema_version"] == (
        "final_response_validation_v1"
    )
    assert final_validation_step["output_payload"]["status"] == "pass"
    assert {call["tool_name"] for call in trace_payload["tool_calls"]} >= {
        "profile.current",
        "learning.path",
        "learning.concept_map",
    }
    assert trace_payload["evidence_refs"]
    worker_step = next(
        step
        for step in trace_payload["steps"]
        if step["step_name"] == "execute_workers"
    )
    worker_output = worker_step["output_payload"]
    assert worker_output["schema_version"] == "worker_output_v2"
    assert "fund_basics_explainer_v1" in worker_output["skill_names"]
    assert worker_output["learning_outcome"]
    assert worker_output["structured_findings"]
    structured_finding = worker_output["structured_findings"][0]
    assert structured_finding["claim"] in worker_output["findings"]
    assert structured_finding["support_level"] == "direct"
    assert structured_finding["evidence_refs"]
    assert "收益承诺" in structured_finding["safety_boundary"]

    with session_factory() as session:
        run = session.get(AgentRun, run_id)
        assert run is not None
        assert run.intent == "learning"
        assert run.policy_status == "allow"
        assert run.tool_trace["composer"]["composer_mode"] == "deterministic"
        assert run.tool_trace["agent_plan"]["primary_intent"] == "learning"
        assert run.tool_trace["selected_skill_versions"] == [
            "fund_basics_explainer_v1@2026-05-16"
        ]
        assert run.tool_trace["runtime_capabilities"]["schema_version"] == (
            "agent_runtime_capabilities_v1"
        )
        assert {event["name"] for event in run.tool_trace["trace_events"]} >= {
            "agent.plan",
            "agent.skills",
            "agent.tools",
            "agent.guardrails",
        }
        assert run.context_snapshot_version == "context_snapshot_v1"
        assert session.scalar(select(func.count()).select_from(AgentStep)) == 12
        assert session.scalar(select(func.count()).select_from(AgentToolCall)) >= 2
        assert session.scalar(select(func.count()).select_from(AgentEvidenceRef)) >= 2
        assert session.scalar(select(func.count()).select_from(AgentRunEventRecord)) >= 20

    events_response = client.get(f"/api/assistant/runs/{run_id}/events")
    assert events_response.status_code == 200
    events_payload = events_response.json()
    assert events_payload["schema_version"] == "agent_run_events_v1"
    assert events_payload["run_id"] == run_id
    events = events_payload["events"]
    assert events[0]["event_type"] == "turn_started"
    assert events[0]["phase"] == "turn"
    assert events[-1]["event_type"] == "turn_complete"
    assert events[-1]["status"] == "completed"
    assert [event["sequence"] for event in events] == list(range(1, len(events) + 1))
    assert {event["event_type"] for event in events} >= {
        "step_started",
        "step_completed",
        "tool_call_started",
        "tool_call_completed",
        "agent_message",
    }
    assert any(
        event["event_type"] == "tool_call_completed"
        and event["payload"]["permission_level"] == "read"
        for event in events
    )
    assert any(
        event["event_type"] == "agent_message"
        and event["payload"]["message"]
        for event in events
    )

    status_response = client.get(f"/api/assistant/runs/{run_id}/status")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["schema_version"] == "agent_run_status_v1"
    assert status_payload["run_id"] == run_id
    assert status_payload["status"] == "completed"
    assert status_payload["run_status"] == "completed"
    assert status_payload["active"] is False
    assert status_payload["cancel_requested"] is False


def test_assistant_message_stream_emits_live_events_and_final_conversation(
    client: TestClient,
) -> None:
    _onboard(client, email="agent-stream@example.com")

    with client.stream(
        "POST",
        "/api/assistant/messages/stream",
        json={"message": "帮我检查今天应该先看组合还是新闻。"},
    ) as response:
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]
        raw_stream = "".join(response.iter_text())

    stream_events = _parse_sse_events(raw_stream)
    agent_events = [
        payload for event_name, payload in stream_events if event_name == "agent_event"
    ]
    conversation_payloads = [
        payload for event_name, payload in stream_events if event_name == "conversation"
    ]

    assert conversation_payloads
    assert conversation_payloads[-1]["messages"][-1]["role"] == "assistant"
    assert conversation_payloads[-1]["messages"][-1]["agent_run_id"]
    assert [event["sequence"] for event in agent_events] == list(
        range(1, len(agent_events) + 1)
    )
    assert all(f"id: {event['id']}\n" in raw_stream for event in agent_events)
    assert agent_events[0]["event_type"] == "turn_started"
    assert agent_events[-1]["event_type"] == "turn_complete"
    assert {event["event_type"] for event in agent_events} >= {
        "step_started",
        "step_completed",
        "tool_call_started",
        "tool_call_completed",
        "agent_message",
    }


def test_assistant_message_stream_error_includes_recoverable_run_id(
    client: TestClient,
    monkeypatch,
) -> None:
    _onboard(client, email="agent-stream-error-run-id@example.com")

    async def failing_stream_send_message(*args, **kwargs):  # noqa: ANN002, ANN003
        event_sink = kwargs["event_sink"]
        event_sink.emit(
            run_id="run_stream_error_e2e",
            event_type="turn_started",
            phase="turn",
            title="开始处理任务",
            status="running",
            payload={},
        )
        raise RuntimeError("stream transport failed after run started")

    monkeypatch.setattr(assistant_routes, "send_message", failing_stream_send_message)

    with client.stream(
        "POST",
        "/api/assistant/messages/stream",
        json={"message": "触发流式错误恢复。"},
    ) as response:
        assert response.status_code == 200
        raw_stream = "".join(response.iter_text())

    stream_events = _parse_sse_events(raw_stream)
    assert [event_name for event_name, _ in stream_events] == [
        "agent_event",
        "error",
    ]
    error_payload = stream_events[-1][1]
    assert error_payload["run_id"] == "run_stream_error_e2e"
    assert error_payload["recoverable"] is True


def test_assistant_runtime_failure_persists_failed_status_and_events(
    client: TestClient,
    monkeypatch,
) -> None:
    _onboard(client, email="agent-failure@example.com")

    async def failing_run(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise RuntimeError("internal test failure")

    monkeypatch.setattr(AdvisorOrchestrator, "run", failing_run)

    response = client.post(
        "/api/assistant/messages",
        json={"message": "请帮我检查一下组合风险。"},
    )
    assert response.status_code == 200
    assistant_message = response.json()["messages"][-1]
    assert "没有顺利完成" in assistant_message["content"]
    run_id = assistant_message["agent_run_id"]

    status_response = client.get(f"/api/assistant/runs/{run_id}/status")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["status"] == "failed"
    assert status_payload["active"] is False
    assert "没有顺利完成" in status_payload["message"]

    events_response = client.get(f"/api/assistant/runs/{run_id}/events")
    assert events_response.status_code == 200
    events = events_response.json()["events"]
    assert [event["event_type"] for event in events] == [
        "turn_started",
        "turn_error",
        "agent_message",
        "turn_complete",
    ]
    assert events[-1]["status"] == "failed"
    assert events[1]["payload"]["error_type"] == "RuntimeError"

    trace_response = client.get(f"/api/assistant/runs/{run_id}/trace")
    assert trace_response.status_code == 200
    trace_payload = trace_response.json()
    assert trace_payload["run"]["run_status"] == "failed"
    assert trace_payload["run"]["fallback_reason"] == "runtime_error"
    assert trace_payload["run"]["tool_trace"]["error_type"] == "RuntimeError"


def test_active_run_stays_registered_until_terminal_commit(
    client: TestClient,
    monkeypatch,
) -> None:
    _onboard(client, email="agent-terminal-commit@example.com")

    registry = get_active_agent_run_registry()
    observed: dict[str, object] = {}
    original_commit = Session.commit

    def observing_commit(self: Session) -> None:
        terminal_run = next(
            (
                item
                for item in self.dirty
                if isinstance(item, AgentRun) and item.completed_at is not None
            ),
            None,
        )
        if terminal_run is not None:
            active_snapshot = registry.get(run_id=terminal_run.id)
            observed["run_id"] = terminal_run.id
            observed["active_during_terminal_commit"] = active_snapshot is not None
        original_commit(self)

    monkeypatch.setattr(Session, "commit", observing_commit)

    response = client.post(
        "/api/assistant/messages",
        json={"message": "请帮我看今天应该先检查什么。"},
    )
    assert response.status_code == 200
    run_id = response.json()["messages"][-1]["agent_run_id"]
    assert observed == {
        "run_id": run_id,
        "active_during_terminal_commit": True,
    }
    assert registry.get(run_id=run_id) is None


def test_active_run_unregisters_when_terminal_flush_fails(
    client: TestClient,
    monkeypatch,
) -> None:
    _onboard(client, email="agent-terminal-flush-failure@example.com")

    registry = get_active_agent_run_registry()
    observed: dict[str, object] = {}
    original_flush = Session.flush

    def failing_terminal_flush(self: Session, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        terminal_message = next(
            (
                item
                for item in self.new
                if isinstance(item, ChatMessage)
                and item.role == "assistant"
                and item.agent_run_id is not None
            ),
            None,
        )
        if terminal_message is not None:
            observed["run_id"] = terminal_message.agent_run_id
            observed["active_during_terminal_flush"] = (
                registry.get(run_id=terminal_message.agent_run_id) is not None
            )
            raise RuntimeError("terminal flush failed")
        return original_flush(self, *args, **kwargs)

    monkeypatch.setattr(Session, "flush", failing_terminal_flush)

    with pytest.raises(RuntimeError, match="terminal flush failed"):
        client.post(
            "/api/assistant/messages",
            json={"message": "请帮我看今天应该先检查什么。"},
        )

    run_id = observed["run_id"]
    assert observed["active_during_terminal_flush"] is True
    assert registry.get(run_id=run_id) is None


def test_active_run_unregisters_when_turn_started_event_sink_fails(
    client: TestClient,
    monkeypatch,
) -> None:
    _onboard(client, email="agent-turn-start-event-failure@example.com")

    registry = get_active_agent_run_registry()
    observed: dict[str, object] = {}
    original_emit = DurableAgentRunEventSink.emit

    def failing_turn_started_emit(
        self: DurableAgentRunEventSink,
        *args,
        **kwargs,
    ):  # noqa: ANN002, ANN003
        if kwargs.get("event_type") == "turn_started":
            run_id = str(kwargs["run_id"])
            observed["run_id"] = run_id
            observed["active_during_turn_started_emit"] = (
                registry.get(run_id=run_id) is not None
            )
            raise RuntimeError("turn started emit failed")
        return original_emit(self, *args, **kwargs)

    monkeypatch.setattr(DurableAgentRunEventSink, "emit", failing_turn_started_emit)

    with pytest.raises(RuntimeError, match="turn started emit failed"):
        client.post(
            "/api/assistant/messages",
            json={"message": "请帮我看今天应该先检查什么。"},
        )

    run_id = observed["run_id"]
    assert observed["active_during_turn_started_emit"] is True
    assert registry.get(run_id=run_id) is None


def test_assistant_runtime_cancel_uses_closed_event_not_complete(
    client: TestClient,
    monkeypatch,
) -> None:
    _onboard(client, email="agent-cancel-terminal@example.com")

    async def cancelling_run(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise AgentRunCancelled("user_requested_cancel")

    monkeypatch.setattr(AdvisorOrchestrator, "run", cancelling_run)

    response = client.post(
        "/api/assistant/messages",
        json={"message": "请帮我整理一下组合，必要时我会停止。"},
    )
    assert response.status_code == 200
    assistant_message = response.json()["messages"][-1]
    assert "已停止这次整理" in assistant_message["content"]
    run_id = assistant_message["agent_run_id"]

    status_response = client.get(f"/api/assistant/runs/{run_id}/status")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["status"] == "cancelled"
    assert status_payload["active"] is False
    assert "已停止" in status_payload["message"]

    events_response = client.get(f"/api/assistant/runs/{run_id}/events")
    assert events_response.status_code == 200
    events = events_response.json()["events"]
    assert [event["event_type"] for event in events] == [
        "turn_started",
        "agent_message",
        "turn_closed",
    ]
    assert events[1]["title"] == "同步停止说明"
    assert events[1]["status"] == "cancelled"
    assert events[-1]["status"] == "cancelled"
    assert "turn_complete" not in {event["event_type"] for event in events}

    trace_response = client.get(f"/api/assistant/runs/{run_id}/trace")
    assert trace_response.status_code == 200
    trace_payload = trace_response.json()
    assert trace_payload["run"]["run_status"] == "cancelled"
    assert trace_payload["run"]["fallback_reason"] == "user_requested_cancel"


def test_inactive_running_run_is_closed_as_interrupted(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _onboard(client, email="agent-interrupted-stale@example.com")

    with session_factory() as db:
        user = db.scalar(
            select(UserProfile).where(
                UserProfile.display_name == "Ava"
            )
        )
        assert user is not None
        started_at = datetime.now(timezone.utc)
        session = ChatSession(
            user_id=user.id,
            topic="中断恢复测试",
            context_type="coach",
            created_at=started_at,
            updated_at=started_at,
        )
        db.add(session)
        db.flush()
        db.add(
            ChatMessage(
                session_id=session.id,
                role="user",
                content="这轮会在没有终态时中断。",
                message_type="user_prompt",
                created_at=started_at,
            )
        )
        run = AgentRun(
            session_id=session.id,
            user_id=user.id,
            model_name="deterministic",
            schema_version="assistant_message_v1",
            run_status="running",
            run_type="advisor_orchestrator",
            orchestrator_version="test",
            input_payload={"message": "这轮会在没有终态时中断。"},
            output_payload={},
            tool_trace={},
            started_at=started_at,
            created_at=started_at,
        )
        db.add(run)
        db.flush()
        event_sink = DurableAgentRunEventSink(db)
        event_sink.emit(
            run_id=run.id,
            event_type="turn_started",
            phase="turn",
            title="开始整理",
            status="running",
            at=started_at,
            payload={"context_snapshot_version": 1},
        )
        db.commit()
        run_id = run.id

    trace_response = client.get(f"/api/assistant/runs/{run_id}/trace")
    assert trace_response.status_code == 200
    trace_payload = trace_response.json()
    assert trace_payload["run"]["run_status"] == "interrupted"
    assert trace_payload["run"]["policy_status"] == "interrupted"
    assert trace_payload["run"]["fallback_reason"] == "inactive_running_run"
    assert trace_payload["run"]["tool_trace"]["interrupted"] is True

    status_response = client.get(f"/api/assistant/runs/{run_id}/status")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["status"] == "interrupted"
    assert status_payload["run_status"] == "interrupted"
    assert status_payload["active"] is False
    assert "中断" in status_payload["message"]

    events_response = client.get(f"/api/assistant/runs/{run_id}/events")
    assert events_response.status_code == 200
    events = events_response.json()["events"]
    assert [event["event_type"] for event in events] == [
        "turn_started",
        "turn_interrupted",
        "agent_message",
        "turn_closed",
    ]
    assert [event["sequence"] for event in events] == [1, 2, 3, 4]
    assert events[1]["payload"]["reason"] == "inactive_running_run"
    assert events[-1]["status"] == "interrupted"
    assert "turn_complete" not in {event["event_type"] for event in events}

    incremental_response = client.get(
        f"/api/assistant/runs/{run_id}/events",
        params={"after_sequence": 2},
    )
    assert incremental_response.status_code == 200
    incremental_events = incremental_response.json()["events"]
    assert [event["sequence"] for event in incremental_events] == [3, 4]
    assert [event["event_type"] for event in incremental_events] == [
        "agent_message",
        "turn_closed",
    ]
    empty_incremental_response = client.get(
        f"/api/assistant/runs/{run_id}/events",
        params={"after_sequence": 4},
    )
    assert empty_incremental_response.status_code == 200
    assert empty_incremental_response.json()["events"] == []
    invalid_incremental_response = client.get(
        f"/api/assistant/runs/{run_id}/events",
        params={"after_sequence": -1},
    )
    assert invalid_incremental_response.status_code == 422

    repeat_trace_response = client.get(f"/api/assistant/runs/{run_id}/trace")
    assert repeat_trace_response.status_code == 200
    with session_factory() as db:
        stored_run = db.get(AgentRun, run_id)
        assert stored_run is not None
        assert stored_run.run_status == "interrupted"
        assistant_messages = list(
            db.scalars(
                select(ChatMessage).where(
                    ChatMessage.session_id == stored_run.session_id,
                    ChatMessage.role == "assistant",
                )
            )
        )
        assert len(assistant_messages) == 1
        assert "上次整理中断" in assistant_messages[0].content


def test_legacy_agent_run_events_support_after_sequence_filter(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _onboard(client, email="agent-legacy-events-cursor@example.com")

    with session_factory() as db:
        user = db.scalar(select(UserProfile).where(UserProfile.display_name == "Ava"))
        assert user is not None
        started_at = datetime.now(timezone.utc)
        completed_at = started_at + timedelta(milliseconds=1)
        session = ChatSession(
            user_id=user.id,
            topic="旧事件回放测试",
            context_type="coach",
            created_at=started_at,
            updated_at=completed_at,
        )
        db.add(session)
        db.flush()
        run = AgentRun(
            session_id=session.id,
            user_id=user.id,
            model_name="deterministic",
            schema_version="assistant_message_v1",
            run_status="completed",
            run_type="advisor_orchestrator",
            orchestrator_version="legacy-test",
            intent="learning",
            policy_status="safe",
            input_payload={"message": "旧记录没有持久事件。"},
            output_payload={
                "answer": "旧记录回答。",
                "intent": "learning",
                "citations": [],
                "recommended_actions": [],
            },
            tool_trace={},
            latency_ms=1,
            started_at=started_at,
            completed_at=completed_at,
            created_at=started_at,
        )
        db.add(run)
        db.commit()
        run_id = run.id

    response = client.get(
        f"/api/assistant/runs/{run_id}/events",
        params={"after_sequence": 1},
    )
    assert response.status_code == 200
    events = response.json()["events"]
    assert [event["sequence"] for event in events] == [2, 3]
    assert [event["event_type"] for event in events] == [
        "agent_message",
        "turn_complete",
    ]


def test_active_agent_run_can_be_cancelled_by_owner(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _onboard(client, email="agent-cancel@example.com")

    with session_factory() as session:
        user = session.scalar(select(UserProfile).where(UserProfile.display_name == "Ava"))
        assert user is not None
        user_id = user.id

    registry = get_active_agent_run_registry()
    run_id = "run-cancel-test"
    registry.register(
        run_id=run_id,
        session_id="session-cancel-test",
        user_id=user_id,
    )
    try:
        status_response = client.get(f"/api/assistant/runs/{run_id}/status")
        assert status_response.status_code == 200
        status_payload = status_response.json()
        assert status_payload["run_id"] == run_id
        assert status_payload["status"] == "running"
        assert status_payload["active"] is True
        assert status_payload["cancel_requested"] is False

        response = client.post(f"/api/assistant/runs/{run_id}/cancel")
        assert response.status_code == 200
        payload = response.json()
        assert payload["run_id"] == run_id
        assert payload["status"] == "cancelling"
        assert payload["cancel_requested"] is True
        cancelling_status_response = client.get(f"/api/assistant/runs/{run_id}/status")
        assert cancelling_status_response.status_code == 200
        cancelling_status_payload = cancelling_status_response.json()
        assert cancelling_status_payload["status"] == "cancelling"
        assert cancelling_status_payload["active"] is True
        assert cancelling_status_payload["cancel_requested"] is True
        active_response = client.get("/api/assistant/runs/active")
        assert active_response.status_code == 200
        active_payload = active_response.json()
        assert active_payload["schema_version"] == "active_agent_runs_v1"
        assert [item["run_id"] for item in active_payload["runs"]] == [run_id]
        active_for_session_response = client.get(
            "/api/assistant/runs/active",
            params={"session_id": "session-cancel-test"},
        )
        assert active_for_session_response.status_code == 200
        assert [
            item["run_id"]
            for item in active_for_session_response.json()["runs"]
        ] == [run_id]
        active_for_other_session_response = client.get(
            "/api/assistant/runs/active",
            params={"session_id": "session-other"},
        )
        assert active_for_other_session_response.status_code == 200
        assert active_for_other_session_response.json()["runs"] == []
        snapshot = registry.get(run_id=run_id)
        assert snapshot is not None
        assert snapshot.cancel_requested is True
    finally:
        registry.unregister(run_id=run_id)


def test_active_agent_run_event_sequences_do_not_reuse_uncommitted_numbers(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _onboard(client, email="agent-active-sequence@example.com")

    with session_factory() as db:
        user = db.scalar(select(UserProfile).where(UserProfile.display_name == "Ava"))
        assert user is not None
        user_id = user.id
        started_at = datetime.now(timezone.utc)
        session = ChatSession(
            user_id=user.id,
            topic="事件序号并发测试",
            context_type="coach",
            created_at=started_at,
            updated_at=started_at,
        )
        db.add(session)
        db.flush()
        run = AgentRun(
            session_id=session.id,
            user_id=user.id,
            model_name="deterministic",
            schema_version="assistant_message_v1",
            run_status="running",
            run_type="advisor_orchestrator",
            orchestrator_version="test",
            input_payload={"message": "测试事件序号。"},
            output_payload={},
            tool_trace={},
            started_at=started_at,
            created_at=started_at,
        )
        db.add(run)
        db.commit()
        run_id = run.id
        session_id = session.id

    registry = get_active_agent_run_registry()
    registry.register(run_id=run_id, session_id=session_id, user_id=user_id)
    try:
        with session_factory() as main_db:
            main_sink = DurableAgentRunEventSink(main_db)
            first_event = main_sink.emit(
                run_id=run_id,
                event_type="turn_started",
                phase="turn",
                title="开始处理任务",
                status="running",
            )
            assert first_event.sequence == 1

            with session_factory() as side_db:
                side_sink = DurableAgentRunEventSink(side_db)
                side_event = side_sink.emit(
                    run_id=run_id,
                    event_type="turn_cancel_requested",
                    phase="control",
                    title="正在停止本次整理",
                    status="cancelling",
                )
                assert side_event.sequence == 2
                side_db.commit()

            main_event = main_sink.emit(
                run_id=run_id,
                event_type="agent_message",
                phase="message",
                title="同步停止说明",
                status="cancelled",
            )
            assert main_event.sequence == 3
            main_db.commit()
    finally:
        registry.unregister(run_id=run_id)

    with session_factory() as db:
        events = list(
            db.scalars(
                select(AgentRunEventRecord)
                .where(AgentRunEventRecord.run_id == run_id)
                .order_by(AgentRunEventRecord.sequence.asc())
            )
        )
    assert [event.sequence for event in events] == [1, 2, 3]
    assert [event.event_type for event in events] == [
        "turn_started",
        "turn_cancel_requested",
        "agent_message",
    ]


def test_active_agent_run_accepts_persisted_queued_follow_up(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _onboard(client, email="agent-queue@example.com")

    message_response = client.post(
        "/api/assistant/messages",
        json={"message": "请先帮我解释组合风险。"},
    )
    assert message_response.status_code == 200
    message_payload = message_response.json()
    session_id = message_payload["session"]["id"]
    run_id = message_payload["messages"][-1]["agent_run_id"]
    assert run_id is not None

    with session_factory() as session:
        user = session.scalar(select(UserProfile).where(UserProfile.display_name == "Ava"))
        assert user is not None
        user_id = user.id

    registry = get_active_agent_run_registry()
    registry.register(run_id=run_id, session_id=session_id, user_id=user_id)
    try:
        queue_response = client.post(
            f"/api/assistant/runs/{run_id}/queued-follow-up",
            json={"message": "然后再帮我看新闻会不会影响它。"},
        )
        assert queue_response.status_code == 200
        queue_payload = queue_response.json()
        assert queue_payload["schema_version"] == "queued_follow_up_v1"
        assert queue_payload["queued_follow_up"]["session_id"] == session_id
        assert queue_payload["queued_follow_up"]["queued_after_run_id"] == run_id
        assert queue_payload["queued_follow_up"]["message"] == (
            "然后再帮我看新闻会不会影响它。"
        )
        assert queue_payload["queued_follow_up"]["status"] == "queued"
        queued_follow_up_id = queue_payload["queued_follow_up"]["id"]

        status_response = client.get(f"/api/assistant/runs/{run_id}/status")
        assert status_response.status_code == 200
        status_payload = status_response.json()
        assert status_payload["active"] is True
        assert status_payload["queued_follow_up"]["message"] == (
            "然后再帮我看新闻会不会影响它。"
        )
        events_after_queue_response = client.get(f"/api/assistant/runs/{run_id}/events")
        assert events_after_queue_response.status_code == 200
        events_after_queue = events_after_queue_response.json()["events"]
        assert events_after_queue[-1]["event_type"] == "input_queued"
        assert events_after_queue[-1]["title"] == "下一句已排队"
        assert events_after_queue[-1]["payload"]["message_preview"] == (
            "然后再帮我看新闻会不会影响它。"
        )

        read_response = client.get(
            f"/api/assistant/sessions/{session_id}/queued-follow-up"
        )
        assert read_response.status_code == 200
        assert read_response.json()["queued_follow_up"]["status"] == "queued"

        premature_submitted_response = client.post(
            f"/api/assistant/sessions/{session_id}/queued-follow-up/submitted"
        )
        assert premature_submitted_response.status_code == 409
        assert "previous run completes successfully" in premature_submitted_response.text
        registry.request_cancel(run_id=run_id, user_id=user_id)
        cancelling_submitted_response = client.post(
            f"/api/assistant/sessions/{session_id}/queued-follow-up/submitted"
        )
        assert cancelling_submitted_response.status_code == 409
        assert "previous run completes successfully" in cancelling_submitted_response.text
        registry.unregister(run_id=run_id)

        wrong_item_submitted_response = client.post(
            f"/api/assistant/sessions/{session_id}/queued-follow-up/submitted",
            json={"queued_follow_up_id": "queued-follow-up-that-was-replaced"},
        )
        assert wrong_item_submitted_response.status_code == 409
        assert "Queued follow-up has changed" in wrong_item_submitted_response.text

        submitted_response = client.post(
            f"/api/assistant/sessions/{session_id}/queued-follow-up/submitted",
            json={"queued_follow_up_id": queued_follow_up_id},
        )
        assert submitted_response.status_code == 200
        assert submitted_response.json()["queued_follow_up"]["status"] == "submitted"
        events_after_submitted_response = client.get(
            f"/api/assistant/runs/{run_id}/events"
        )
        assert events_after_submitted_response.status_code == 200
        events_after_submitted = events_after_submitted_response.json()["events"]
        assert [event["sequence"] for event in events_after_submitted] == list(
            range(1, len(events_after_submitted) + 1)
        )
        assert events_after_submitted[-2]["event_type"] == "input_queued"
        assert events_after_submitted[-1]["event_type"] == "input_submitted"
        assert events_after_submitted[-1]["title"] == "排队的下一句已发送"

        empty_response = client.get(
            f"/api/assistant/sessions/{session_id}/queued-follow-up"
        )
        assert empty_response.status_code == 200
        assert empty_response.json()["queued_follow_up"] is None
    finally:
        registry.unregister(run_id=run_id)


@pytest.mark.parametrize("source_status", ["failed", "cancelled"])
def test_queued_follow_up_submitted_rejects_unfinished_source_run(
    client: TestClient,
    session_factory: sessionmaker[Session],
    source_status: str,
) -> None:
    _onboard(client, email=f"agent-queue-{source_status}-source@example.com")

    message_response = client.post(
        "/api/assistant/messages",
        json={"message": "请先帮我看一下组合风险。"},
    )
    assert message_response.status_code == 200
    message_payload = message_response.json()
    session_id = message_payload["session"]["id"]
    run_id = message_payload["messages"][-1]["agent_run_id"]
    assert run_id is not None

    with session_factory() as session:
        user = session.scalar(select(UserProfile).where(UserProfile.display_name == "Ava"))
        assert user is not None
        user_id = user.id

    registry = get_active_agent_run_registry()
    registry.register(run_id=run_id, session_id=session_id, user_id=user_id)
    try:
        queue_response = client.post(
            f"/api/assistant/runs/{run_id}/queued-follow-up",
            json={"message": f"上一条{source_status}时，这句要先保留确认。"},
        )
        assert queue_response.status_code == 200
    finally:
        registry.unregister(run_id=run_id)

    with session_factory() as session:
        agent_run = session.get(AgentRun, run_id)
        assert agent_run is not None
        agent_run.run_status = source_status
        session.add(agent_run)
        session.commit()

    submitted_response = client.post(
        f"/api/assistant/sessions/{session_id}/queued-follow-up/submitted"
    )
    assert submitted_response.status_code == 409
    assert "previous run completes successfully" in submitted_response.text

    read_response = client.get(
        f"/api/assistant/sessions/{session_id}/queued-follow-up"
    )
    assert read_response.status_code == 200
    queued = read_response.json()["queued_follow_up"]
    assert queued["status"] == "queued"
    assert queued["message"] == f"上一条{source_status}时，这句要先保留确认。"
    events_response = client.get(f"/api/assistant/runs/{run_id}/events")
    assert events_response.status_code == 200
    assert "input_submitted" not in {
        event["event_type"] for event in events_response.json()["events"]
    }


def test_queued_follow_up_rejects_inactive_and_stopping_runs(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _onboard(client, email="agent-queue-stop@example.com")

    message_response = client.post(
        "/api/assistant/messages",
        json={"message": "请先帮我看一下风险。"},
    )
    assert message_response.status_code == 200
    message_payload = message_response.json()
    session_id = message_payload["session"]["id"]
    run_id = message_payload["messages"][-1]["agent_run_id"]
    assert run_id is not None

    inactive_response = client.post(
        f"/api/assistant/runs/{run_id}/queued-follow-up",
        json={"message": "这句不应该排队成功。"},
    )
    assert inactive_response.status_code == 409

    with session_factory() as session:
        user = session.scalar(select(UserProfile).where(UserProfile.display_name == "Ava"))
        assert user is not None
        user_id = user.id

    registry = get_active_agent_run_registry()
    registry.register(run_id=run_id, session_id=session_id, user_id=user_id)
    registry.request_cancel(run_id=run_id, user_id=user_id)
    try:
        stopping_response = client.post(
            f"/api/assistant/runs/{run_id}/queued-follow-up",
            json={"message": "停止中的下一句也不能自动排队。"},
        )
        assert stopping_response.status_code == 409
    finally:
        registry.unregister(run_id=run_id)


def test_agent_run_cancel_persists_cancel_request_and_discards_queued_follow_up(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _onboard(client, email="agent-cancel-queue@example.com")

    message_response = client.post(
        "/api/assistant/messages",
        json={"message": "请先帮我看今天应该检查什么。"},
    )
    assert message_response.status_code == 200
    message_payload = message_response.json()
    session_id = message_payload["session"]["id"]
    run_id = message_payload["messages"][-1]["agent_run_id"]
    assert run_id is not None

    with session_factory() as session:
        user = session.scalar(select(UserProfile).where(UserProfile.display_name == "Ava"))
        assert user is not None
        user_id = user.id

    registry = get_active_agent_run_registry()
    registry.register(run_id=run_id, session_id=session_id, user_id=user_id)
    try:
        queue_response = client.post(
            f"/api/assistant/runs/{run_id}/queued-follow-up",
            json={"message": "如果它还没结束，就别自动发这句。"},
        )
        assert queue_response.status_code == 200

        cancel_response = client.post(f"/api/assistant/runs/{run_id}/cancel")
        assert cancel_response.status_code == 200
        cancel_payload = cancel_response.json()
        assert cancel_payload["status"] == "cancelling"
        assert cancel_payload["cancel_requested"] is True
        assert "不会自动发送" in cancel_payload["message"]

        queued_response = client.get(
            f"/api/assistant/sessions/{session_id}/queued-follow-up"
        )
        assert queued_response.status_code == 200
        assert queued_response.json()["queued_follow_up"] is None

        events_response = client.get(f"/api/assistant/runs/{run_id}/events")
        assert events_response.status_code == 200
        events = events_response.json()["events"]
        assert [event["sequence"] for event in events] == list(
            range(1, len(events) + 1)
        )
        assert [event["event_type"] for event in events[-3:]] == [
            "input_queued",
            "turn_cancel_requested",
            "input_discarded",
        ]
        assert events[-2]["title"] == "正在停止本次整理"
        assert events[-1]["title"] == "停止后已取消排队下一句"
    finally:
        registry.unregister(run_id=run_id)


def test_agent_policy_guard_blocks_trade_execution_request(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _onboard(client, email="policy-guard@example.com")

    response = client.post(
        "/api/assistant/messages",
        json={"message": "请直接告诉我今天应该买入哪只基金，并保证收益。"},
    )
    assert response.status_code == 200
    payload = response.json()
    assistant_message = payload["messages"][-1]
    advisor_response = assistant_message["advisor_response"]
    assert advisor_response["intent"] == "learning"
    assert "不能给出买卖" in advisor_response["answer"]

    run_id = assistant_message["agent_run_id"]
    trace_response = client.get(f"/api/assistant/runs/{run_id}/trace")
    assert trace_response.status_code == 200
    trace_payload = trace_response.json()
    assert trace_payload["run"]["policy_status"] == "block_with_guidance"
    worker_step = next(
        step
        for step in trace_payload["steps"]
        if step["step_name"] == "execute_workers"
    )
    structured_finding = worker_step["output_payload"]["structured_findings"][0]
    assert structured_finding["support_level"] == "contextual"
    assert "不提供买卖" in structured_finding["safety_boundary"]
    plan_step = next(
        step
        for step in trace_payload["steps"]
        if step["step_name"] == "build_agent_plan"
    )
    assert {
        item["tool_name"] for item in plan_step["output_payload"]["planned_tools"]
    } == {"profile.current", "safety.boundary_rules"}
    assert plan_step["output_payload"]["selected_skills"] == []

    with session_factory() as session:
        run = session.get(AgentRun, run_id)
        assert run is not None
        assert run.policy_status == "block_with_guidance"


def test_assistant_contract_remains_stable(client: TestClient) -> None:
    _onboard(client, email="contract@example.com")

    response = client.post(
        "/api/assistant/messages",
        json={"message": "基金风险等级是什么意思？"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert set(payload.keys()) == {"session", "messages"}
    assert payload["session"]["context_type"] == "coach"

    assistant_message = payload["messages"][-1]
    assert set(assistant_message.keys()) == {
        "id",
        "role",
        "content",
        "message_type",
        "created_at",
        "agent_run_id",
        "advisor_response",
    }
    assert set(assistant_message["advisor_response"].keys()) == {
        "answer",
        "intent",
        "citations",
        "risk_notice",
        "recommended_actions",
        "recommended_action_targets",
        "follow_up_questions",
    }
    targets = assistant_message["advisor_response"]["recommended_action_targets"]
    assert targets
    assert all(target["href"].startswith("/") for target in targets)
    assert all(target["kind"] == "internal_link" for target in targets)


def test_agent_answers_state_and_next_step_questions_directly(
    client: TestClient,
) -> None:
    _onboard(client, email="direct-state@example.com")

    learning_response = client.post(
        "/api/assistant/messages",
        json={"message": "我现在的风险等级是什么？"},
    )
    assert learning_response.status_code == 200
    learning_answer = learning_response.json()["messages"][-1]["advisor_response"][
        "answer"
    ]
    assert "直接回答：" not in learning_answer
    assert "风险等级是" in learning_answer
    assert "学习进度" not in learning_answer.split("。", 1)[0]
    learning_run_id = learning_response.json()["messages"][-1]["agent_run_id"]
    learning_trace = client.get(f"/api/assistant/runs/{learning_run_id}/trace")
    assert learning_trace.status_code == 200
    assert learning_trace.json()["run"]["policy_status"] == "allow"

    portfolio_response = client.post(
        "/api/assistant/messages",
        json={"message": "我现在有没有组合报告？"},
    )
    assert portfolio_response.status_code == 200
    portfolio_answer = portfolio_response.json()["messages"][-1]["advisor_response"][
        "answer"
    ]
    assert "直接回答：" not in portfolio_answer
    assert "还没有可用的组合报告" in portfolio_answer

    behavior_response = client.post(
        "/api/assistant/messages",
        json={"message": "我的行为画像现在是什么？"},
    )
    assert behavior_response.status_code == 200
    behavior_answer = behavior_response.json()["messages"][-1]["advisor_response"][
        "answer"
    ]
    assert "直接回答：" not in behavior_answer
    assert "当前行为画像显示" in behavior_answer
    assert "no_major_bias_detected" not in behavior_answer

    simulation_response = client.post(
        "/api/assistant/messages",
        json={"message": "我现在适合做什么情境演练？"},
    )
    assert simulation_response.status_code == 200
    simulation_answer = simulation_response.json()["messages"][-1]["advisor_response"][
        "answer"
    ]
    assert "直接回答：" not in simulation_answer
    assert "推荐从" in simulation_answer
    assert "no_major_bias_detected" not in simulation_answer


def test_agent_runtime_v2_fans_out_cross_domain_plan(client: TestClient) -> None:
    _onboard(client, email="cross-domain@example.com")

    response = client.post(
        "/api/assistant/messages",
        json={"message": "政策新闻会不会影响我的持仓配置和追涨冲动？"},
    )
    assert response.status_code == 200
    payload = response.json()
    assistant_message = payload["messages"][-1]
    assert assistant_message["advisor_response"]["intent"] == "portfolio"

    trace_response = client.get(
        f"/api/assistant/runs/{assistant_message['agent_run_id']}/trace"
    )
    assert trace_response.status_code == 200
    trace_payload = trace_response.json()
    plan_step = next(
        step
        for step in trace_payload["steps"]
        if step["step_name"] == "build_agent_plan"
    )
    assert plan_step["output_payload"]["detected_intents"] == [
        "portfolio",
        "behavior",
        "news",
    ]
    assert plan_step["output_payload"]["worker_names"] == [
        "PortfolioWorker",
        "BehaviorWorker",
        "NewsWorker",
    ]
    assert {
        skill["skill_name"] for skill in plan_step["output_payload"]["selected_skills"]
    } >= {
        "portfolio_concentration_review_v1",
        "behavior_bias_reflection_v1",
        "news_policy_impact_path_v1",
        "cross_domain_synthesis_v1",
    }
    assert {call["tool_name"] for call in trace_payload["tool_calls"]} >= {
        "portfolio.latest_report",
        "portfolio.risk_lens",
        "behavior.training_plan",
        "news.impact_lens",
    }
    worker_step = next(
        step
        for step in trace_payload["steps"]
        if step["step_name"] == "execute_workers"
    )
    assert worker_step["output_payload"]["worker_name"] == "AdvisorSynthesisWorker"
    assert len(worker_step["output_payload"]["worker_outputs"]) == 3
    validation_step = next(
        step
        for step in trace_payload["steps"]
        if step["step_name"] == "validate_worker_output"
    )
    assert validation_step["output_payload"]["status"] == "pass"


def test_simulation_intent_uses_simulation_worker(client: TestClient) -> None:
    _onboard(client, email="simulation-worker@example.com")

    response = client.post(
        "/api/assistant/messages",
        json={"message": "历史情境演练能训练什么？"},
    )
    assert response.status_code == 200
    payload = response.json()
    assistant_message = payload["messages"][-1]
    assert assistant_message["advisor_response"]["intent"] == "simulation"

    trace_response = client.get(
        f"/api/assistant/runs/{assistant_message['agent_run_id']}/trace"
    )
    assert trace_response.status_code == 200
    trace_payload = trace_response.json()
    plan_step = next(
        step
        for step in trace_payload["steps"]
        if step["step_name"] == "build_agent_plan"
    )
    assert "SimulationWorker" in plan_step["output_payload"]["worker_names"]
    assert {
        skill["skill_name"] for skill in plan_step["output_payload"]["selected_skills"]
    } >= {"simulation_review_coach_v1"}
    worker_step = next(
        step
        for step in trace_payload["steps"]
        if step["step_name"] == "execute_workers"
    )
    worker_names = {
        output["worker_name"]
        for output in worker_step["output_payload"]["worker_outputs"]
    }
    assert "SimulationWorker" in worker_names


def test_behavior_worker_persists_pending_state_proposal(client: TestClient) -> None:
    _onboard(client, email="behavior-proposal@example.com")

    response = client.post(
        "/api/assistant/messages",
        json={"message": "我总想追涨热门基金，这是什么偏差？"},
    )
    assert response.status_code == 200
    run_id = response.json()["messages"][-1]["agent_run_id"]

    trace_response = client.get(f"/api/assistant/runs/{run_id}/trace")
    assert trace_response.status_code == 200
    trace_payload = trace_response.json()
    proposals = trace_payload["state_update_proposals"]
    assert proposals
    assert proposals[0]["target_type"] == "behavior_profile_note"
    assert proposals[0]["validator_status"] == "pending"


def test_plan_validation_failure_falls_back_before_unknown_tool_execution(
    monkeypatch,
    client: TestClient,
) -> None:
    _onboard(client, email="bad-plan@example.com")

    def bad_plan(self, *, message: str, blocked: bool = False) -> AgentPlan:  # noqa: ARG001
        return AgentPlan(
            primary_intent="learning",
            detected_intents=["learning"],
            worker_names=["LearningWorker"],
            planned_tools=[
                PlannedToolCall(
                    tool_name="unknown.external_tool",
                    purpose="This tool must never execute.",
                )
            ],
            tool_selection_signals=[
                ToolSelectionSignal(
                    tool_name="unknown.external_tool",
                    score=100,
                    source="required",
                    reason="Injected invalid plan for test.",
                )
            ],
            rationale="Injected invalid plan for test.",
        )

    monkeypatch.setattr(AgentPlanner, "build_plan", bad_plan)

    response = client.post(
        "/api/assistant/messages",
        json={"message": "基金风险等级是什么意思？"},
    )
    assert response.status_code == 200
    run_id = response.json()["messages"][-1]["agent_run_id"]

    trace_response = client.get(f"/api/assistant/runs/{run_id}/trace")
    assert trace_response.status_code == 200
    trace_payload = trace_response.json()
    assert {call["tool_name"] for call in trace_payload["tool_calls"]} == {
        "profile.current",
        "safety.boundary_rules",
    }
    validate_plan_step = next(
        step for step in trace_payload["steps"] if step["step_name"] == "validate_plan"
    )
    assert validate_plan_step["output_payload"]["status"] == "fail"
    worker_step = next(
        step
        for step in trace_payload["steps"]
        if step["step_name"] == "execute_workers"
    )
    assert worker_step["output_payload"]["worker_name"] == "SafetyPolicyWorker"
    assert trace_payload["run"]["policy_status"] == "runtime_repair"


def test_plan_validation_fails_when_skill_required_tools_are_omitted() -> None:
    orchestrator = AdvisorOrchestrator(
        model_name="deepseek:deepseek-v4-pro",
        agent_mode="deterministic",
        max_tool_calls=2,
    )
    plan = orchestrator.planner.build_plan(message="基金回撤是什么意思？")
    assert "fund_basics_explainer_v1" in plan.skill_names
    assert set(plan.tool_names) < set(plan.selected_skills[0].required_tools)

    constraints = orchestrator._validate_plan(plan, blocked=False)

    required_tool_constraint = next(
        constraint
        for constraint in constraints
        if constraint.name == "skill_required_tools"
    )
    assert required_tool_constraint.status == "fail"


def test_agent_planner_capability_flags_disable_scored_fanout() -> None:
    planner = AgentPlanner(
        tool_registry=ToolRegistry(),
        capabilities=build_runtime_capabilities(
            flags="strict_final_validation,trace_observability",
            max_tool_calls=4,
            max_workers=1,
        ),
    )

    plan = planner.build_plan(message="政策新闻会不会影响我的持仓配置和追涨冲动？")

    assert plan.detected_intents == ["portfolio"]
    assert plan.worker_names == ["PortfolioWorker"]
    assert plan.max_tool_calls == 4
    assert {signal.source for signal in plan.tool_selection_signals} == {
        "intent_fallback"
    }


def test_final_response_validation_filters_unsupported_output() -> None:
    orchestrator = AdvisorOrchestrator(
        model_name="deepseek:deepseek-v4-pro",
        agent_mode="deterministic",
    )
    worker_output = WorkerOutput(
        worker_name="LearningWorker",
        intent="learning",
        findings=["先理解回撤，再看风险承受能力。"],
        evidence_refs=[
            EvidenceRef(
                worker_name="LearningWorker",
                source_type="course_section",
                source_id="lesson-1",
                quote_or_summary="回撤是阶段高点后的下跌幅度。",
                claim="回撤用于理解短期波动压力。",
            )
        ],
    )
    response = AdvisorResponse(
        answer="先理解回撤，再看风险承受能力。",
        intent="learning",
        citations=["made_up:1", "course_section:lesson-1"],
        risk_notice="FundGene 提供的是学习与决策支持，不是收益承诺、交易执行指令或自动下单系统。",
        recommended_actions=["今天买入这只基金。", "回到 Learning 继续学习回撤。"],
        follow_up_questions=["帮我把风险等级翻译成新手能懂的话。"],
    )

    normalized, validation = orchestrator._normalize_and_validate_final_response(
        response=response,
        worker_output=worker_output,
        policy_status="allow",
    )

    assert normalized.citations == ["course_section:lesson-1"]
    assert normalized.recommended_actions == ["回到 Learning 继续学习回撤。"]
    assert normalized.recommended_action_targets[0].href.startswith("/learning")
    assert normalized.recommended_action_targets[0].target_params == {
        "from": "coach",
        "focus": "risk-basics",
    }
    assert normalized.recommended_action_targets[0].safety_note
    assert validation.status == "warn"
    assert validation.unsupported_citations == ["made_up:1"]
    assert validation.removed_unsafe_actions == ["今天买入这只基金。"]


def test_deterministic_response_hides_runtime_prompt_markers() -> None:
    orchestrator = AdvisorOrchestrator(
        model_name="deepseek:deepseek-v4-pro",
        agent_mode="deterministic",
    )
    worker_output = WorkerOutput(
        worker_name="PortfolioWorker",
        intent="portfolio",
        findings=[
            "组合分析先看集中度。 运行时风险约束：组合解释按集中度、资产类型分散、目标匹配、行为冲动四个维度展开；只给风险结构和再平衡原则，不给交易指令。",
            "新闻解读先分清事实。 检索证据提示：长期资金入市政策继续推进 capital_market_policy",
        ],
        evidence_refs=[],
        recommended_actions=["回到 Portfolio 查看组合报告。"],
    )

    response = orchestrator._compose_deterministic_response(
        intent="portfolio",
        worker_output=worker_output,
        input_policy=PolicyResult(status="allow", reason="test"),
    )

    assert "组合分析先看集中度" in response.answer
    assert "新闻解读先分清事实" in response.answer
    assert "运行时风险约束" not in response.answer
    assert "检索证据提示" not in response.answer
    assert "capital_market_policy" not in response.answer
