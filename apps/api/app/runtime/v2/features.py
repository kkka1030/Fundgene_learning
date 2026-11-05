from typing import Literal

from pydantic import BaseModel, Field


RuntimeCapability = Literal[
    "scored_tool_discovery",
    "multi_worker_fanout",
    "strict_final_validation",
    "trace_observability",
]

DEFAULT_RUNTIME_CAPABILITIES: tuple[RuntimeCapability, ...] = (
    "scored_tool_discovery",
    "multi_worker_fanout",
    "strict_final_validation",
    "trace_observability",
)


class RuntimeCapabilityConfig(BaseModel):
    schema_version: str = "agent_runtime_capabilities_v1"
    enabled_flags: list[RuntimeCapability] = Field(
        default_factory=lambda: list(DEFAULT_RUNTIME_CAPABILITIES)
    )
    max_tool_calls: int = Field(default=8, ge=1, le=16)
    max_workers: int = Field(default=3, ge=1, le=5)

    def enabled(self, flag: RuntimeCapability) -> bool:
        return flag in self.enabled_flags


def build_runtime_capabilities(
    *,
    flags: str | None = None,
    max_tool_calls: int = 8,
    max_workers: int = 3,
) -> RuntimeCapabilityConfig:
    return RuntimeCapabilityConfig(
        enabled_flags=_parse_flags(flags),
        max_tool_calls=max_tool_calls,
        max_workers=max_workers,
    )


def _parse_flags(raw_flags: str | None) -> list[RuntimeCapability]:
    if raw_flags is None or raw_flags.strip().lower() in {"", "default", "all"}:
        return list(DEFAULT_RUNTIME_CAPABILITIES)
    if raw_flags.strip().lower() == "none":
        return []

    valid_flags = set(DEFAULT_RUNTIME_CAPABILITIES)
    parsed: list[RuntimeCapability] = []
    for raw_flag in raw_flags.replace(";", ",").split(","):
        flag = raw_flag.strip().lower().replace("-", "_")
        if flag in valid_flags:
            parsed.append(flag)  # type: ignore[arg-type]
    return list(dict.fromkeys(parsed))
