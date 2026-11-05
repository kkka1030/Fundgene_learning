import asyncio
import json
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.database import get_db_session
from app.models.user import UserProfile
from app.runtime.deps import get_advisor_runtime
from app.runtime.v2.events import QueueAgentRunEventSink
from app.schemas.assistant import (
    ActiveAgentRunsResponse,
    AgentRunCancelResponse,
    AgentRunEvent,
    AgentRunEventsResponse,
    AgentRunStatusResponse,
    AgentRunTraceResponse,
    AssistantConversationResponse,
    AssistantMessageRequest,
    AssistantSessionListResponse,
    QueuedFollowUpRequest,
    QueuedFollowUpResponse,
    QueuedFollowUpSubmittedRequest,
)
from app.services.assistant import (
    discard_session_queued_follow_up,
    get_conversation_by_id,
    get_current_conversation as get_current_assistant_conversation,
)
from app.services.assistant import get_agent_run_events
from app.services.assistant import get_agent_run_status
from app.services.assistant import get_agent_run_trace
from app.services.assistant import get_session_queued_follow_up
from app.services.assistant import list_active_agent_runs
from app.services.assistant import list_conversations
from app.services.assistant import mark_session_queued_follow_up_submitted
from app.services.assistant import queue_agent_run_follow_up
from app.services.assistant import request_agent_run_cancel
from app.services.assistant import send_message

router = APIRouter(prefix="/assistant", tags=["assistant"])


@router.get("/session", response_model=AssistantConversationResponse)
def read_current_conversation(
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> AssistantConversationResponse:
    return get_current_assistant_conversation(db, user=user)


@router.get("/sessions", response_model=AssistantSessionListResponse)
def read_conversations(
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> AssistantSessionListResponse:
    return list_conversations(db, user=user)


@router.get("/sessions/{session_id}", response_model=AssistantConversationResponse)
def read_conversation(
    session_id: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> AssistantConversationResponse:
    return get_conversation_by_id(db, user=user, session_id=session_id)


@router.get(
    "/sessions/{session_id}/queued-follow-up",
    response_model=QueuedFollowUpResponse,
)
def read_session_queued_follow_up(
    session_id: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> QueuedFollowUpResponse:
    return get_session_queued_follow_up(db, user=user, session_id=session_id)


@router.post(
    "/sessions/{session_id}/queued-follow-up/submitted",
    response_model=QueuedFollowUpResponse,
)
def mark_session_queued_follow_up_sent(
    session_id: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
    payload: Annotated[QueuedFollowUpSubmittedRequest | None, Body()] = None,
) -> QueuedFollowUpResponse:
    return mark_session_queued_follow_up_submitted(
        db,
        user=user,
        session_id=session_id,
        queued_follow_up_id=payload.queued_follow_up_id if payload else None,
    )


@router.delete(
    "/sessions/{session_id}/queued-follow-up",
    response_model=QueuedFollowUpResponse,
)
def delete_session_queued_follow_up(
    session_id: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> QueuedFollowUpResponse:
    return discard_session_queued_follow_up(db, user=user, session_id=session_id)


@router.post("/messages", response_model=AssistantConversationResponse)
async def post_message(
    payload: AssistantMessageRequest,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> AssistantConversationResponse:
    runtime = get_advisor_runtime()
    return await send_message(db, user=user, payload=payload, runtime=runtime)


@router.post("/messages/stream")
async def stream_message(
    payload: AssistantMessageRequest,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> StreamingResponse:
    runtime = get_advisor_runtime()
    queue: asyncio.Queue[AgentRunEvent] = asyncio.Queue()
    event_sink = QueueAgentRunEventSink(queue)

    async def event_stream() -> AsyncIterator[str]:
        task = asyncio.create_task(
            send_message(
                db,
                user=user,
                payload=payload,
                runtime=runtime,
                event_sink=event_sink,
            )
        )

        while True:
            if task.done() and queue.empty():
                break
            try:
                event = await asyncio.wait_for(queue.get(), timeout=0.2)
            except TimeoutError:
                continue
            yield _format_sse("agent_event", event.model_dump(mode="json"))

        try:
            conversation = await task
        except Exception as exc:
            yield _format_sse(
                "error",
                {
                    "message": str(exc),
                    "run_id": event_sink.latest_run_id,
                    "recoverable": event_sink.latest_run_id is not None,
                },
            )
            return

        yield _format_sse("conversation", conversation.model_dump(mode="json"))

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/runs/{run_id}/trace", response_model=AgentRunTraceResponse)
def read_agent_run_trace(
    run_id: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> AgentRunTraceResponse:
    return get_agent_run_trace(db, user=user, run_id=run_id)


@router.get("/runs/{run_id}/events", response_model=AgentRunEventsResponse)
def read_agent_run_events(
    run_id: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
    after_sequence: Annotated[int | None, Query(ge=0)] = None,
) -> AgentRunEventsResponse:
    return get_agent_run_events(
        db,
        user=user,
        run_id=run_id,
        after_sequence=after_sequence,
    )


@router.get("/runs/active", response_model=ActiveAgentRunsResponse)
def read_active_agent_runs(
    user: Annotated[UserProfile, Depends(get_current_user)],
    session_id: Annotated[str | None, Query(max_length=36)] = None,
) -> ActiveAgentRunsResponse:
    return list_active_agent_runs(user=user, session_id=session_id)


@router.get("/runs/{run_id}/status", response_model=AgentRunStatusResponse)
def read_agent_run_status(
    run_id: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> AgentRunStatusResponse:
    return get_agent_run_status(db, user=user, run_id=run_id)


@router.post(
    "/runs/{run_id}/queued-follow-up",
    response_model=QueuedFollowUpResponse,
)
def create_agent_run_queued_follow_up(
    run_id: str,
    payload: QueuedFollowUpRequest,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> QueuedFollowUpResponse:
    return queue_agent_run_follow_up(db, user=user, run_id=run_id, payload=payload)


@router.post("/runs/{run_id}/cancel", response_model=AgentRunCancelResponse)
async def cancel_agent_run(
    run_id: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> AgentRunCancelResponse:
    return request_agent_run_cancel(db, user=user, run_id=run_id)


def _format_sse(event: str, data: dict) -> str:
    event_id = data.get("id")
    return (
        f"id: {event_id}\n" if isinstance(event_id, str) and event_id else ""
    ) + (
        f"event: {event}\n"
        f"data: {json.dumps(data, ensure_ascii=False, separators=(',', ':'))}\n\n"
    )
