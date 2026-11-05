from functools import lru_cache

from app.core.config import get_settings
from app.runtime.advisor_agent import AdvisorAgentRuntime


@lru_cache
def get_advisor_runtime() -> AdvisorAgentRuntime:
    settings = get_settings()
    return AdvisorAgentRuntime(
        model_name=settings.advisor_model,
        agent_mode=settings.agent_mode,
        model_timeout_ms=settings.agent_model_timeout_ms,
        runtime_flags=settings.agent_runtime_flags,
        max_tool_calls=settings.agent_max_tool_calls,
        max_workers=settings.agent_max_workers,
    )
