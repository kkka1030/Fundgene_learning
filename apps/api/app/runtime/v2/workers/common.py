from app.runtime.v2.schemas import (
    EvidenceRef,
    SkillSelection,
    ToolResult,
    WorkerFinding,
)


def tool_payload(tool_results: list[ToolResult], tool_name: str) -> dict:
    for result in tool_results:
        if result.tool_name == tool_name:
            return result.output_payload
    return {}


def collect_evidence(tool_results: list[ToolResult]) -> list[EvidenceRef]:
    return [evidence for result in tool_results for evidence in result.evidence_refs]


def evidence_keys(evidence_refs: list[EvidenceRef]) -> list[str]:
    keys = [
        f"{evidence.source_type}:{evidence.source_id}"
        if evidence.source_id
        else evidence.source_type
        for evidence in evidence_refs
    ]
    return list(dict.fromkeys(keys))


def finding_from_evidence(
    *,
    claim: str,
    evidence_refs: list[EvidenceRef],
    explanation: str | None = None,
    safety_boundary: str | None = None,
) -> WorkerFinding:
    return WorkerFinding(
        claim=claim,
        explanation=explanation,
        evidence_refs=evidence_keys(evidence_refs),
        support_level="direct" if evidence_refs else "unsupported",
        safety_boundary=safety_boundary,
    )


def skill_names(skills: list[SkillSelection] | None) -> list[str]:
    return [skill.skill_name for skill in skills or []]


def skill_output_guidance(skills: list[SkillSelection] | None) -> list[str]:
    return list(
        dict.fromkeys(
            guidance for skill in skills or [] for guidance in skill.output_guidance
        )
    )


def skill_forbidden_language(skills: list[SkillSelection] | None) -> list[str]:
    return list(
        dict.fromkeys(
            term for skill in skills or [] for term in skill.forbidden_language
        )
    )


def learning_outcome_for(
    skills: list[SkillSelection] | None,
    *,
    intent: str,
) -> str | None:
    if not skills:
        return None
    for skill in skills:
        if intent in skill.skill_name:
            return skill.learning_outcome
    return skills[0].learning_outcome


def asks_for_list_or_status(message: str) -> bool:
    normalized = message.lower()
    return any(
        term in normalized
        for term in (
            "有哪些",
            "有什么",
            "哪些",
            "几",
            "列",
            "列表",
            "现在",
            "当前",
            "目前",
            "有没有",
            "是否",
            "进度",
            "状态",
            "是什么",
            "什么课",
            "什么情境",
            "下一步",
            "先做什么",
            "该做什么",
            "应该做什么",
            "what",
            "which",
            "list",
            "status",
            "progress",
            "next",
        )
    )


def display_bias_tags(tags: list[str]) -> list[str]:
    labels = {
        "no_major_bias_detected": "暂无显著行为偏差标签",
    }
    return [labels.get(tag, tag) for tag in tags]
