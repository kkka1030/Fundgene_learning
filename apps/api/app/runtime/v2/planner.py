from app.runtime.v2.features import RuntimeCapabilityConfig, build_runtime_capabilities
from app.runtime.v2.schemas import (
    AgentPlan,
    Intent,
    PlanConstraint,
    PlannedToolCall,
    SkillSelection,
    ToolSelectionSignal,
)
from app.runtime.v2.skills import SkillRegistry
from app.runtime.v2.tools.registry import ToolRegistry


MAX_TOOL_CALLS_PER_RUN = 8
MAX_WORKERS_PER_RUN = 3

_INTENT_KEYWORDS: dict[Intent, tuple[str, ...]] = {
    "portfolio": (
        "portfolio",
        "allocation",
        "holding",
        "holdings",
        "仓位",
        "持仓",
        "配置",
        "集中",
        "组合",
    ),
    "behavior": (
        "behavior",
        "bias",
        "emotion",
        "panic",
        "chase",
        "画像",
        "标签",
        "追涨",
        "偏差",
        "情绪",
        "恐慌",
        "冲动",
    ),
    "news": (
        "news",
        "policy",
        "macro",
        "新闻",
        "政策",
        "宏观",
        "加息",
    ),
    "simulation": (
        "simulation",
        "scenario",
        "情景",
        "情境",
        "演练",
        "复盘",
    ),
    "learning": (
        "learning",
        "learn",
        "concept",
        "course",
        "学习",
        "课程",
        "概念",
        "回撤",
        "风险等级",
    ),
}

_WORKER_BY_INTENT: dict[Intent, str] = {
    "learning": "LearningWorker",
    "portfolio": "PortfolioWorker",
    "behavior": "BehaviorWorker",
    "simulation": "SimulationWorker",
    "news": "NewsWorker",
}

_TOOLS_BY_INTENT: dict[Intent, tuple[tuple[str, str], ...]] = {
    "learning": (
        ("profile.current", "读取当前用户画像，确定解释口径。"),
        ("learning.path", "读取学习进度和下一门推荐课程。"),
        ("learning.concept_map", "读取内置基金概念图谱，约束回答术语。"),
        ("learning.evidence_search", "检索课程材料，支撑学习型回答。"),
        ("behavior.profile", "读取风险等级和行为标签，避免泛化建议。"),
    ),
    "portfolio": (
        ("profile.current", "读取当前用户目标和经验。"),
        ("portfolio.latest_report", "读取最近一份持久化组合报告。"),
        ("portfolio.risk_lens", "读取组合风险解释维度。"),
        ("behavior.profile", "读取行为画像，识别调整时的冲动风险。"),
    ),
    "behavior": (
        ("profile.current", "读取当前用户画像。"),
        ("behavior.profile", "读取行为画像和偏差标签。"),
        ("behavior.training_plan", "读取训练焦点和建议情境。"),
        ("simulation.latest_review", "读取最近情境训练复盘。"),
    ),
    "simulation": (
        ("profile.current", "读取当前用户画像。"),
        ("behavior.profile", "读取行为画像和偏差标签。"),
        ("behavior.training_plan", "读取训练焦点和建议情境。"),
        ("simulation.latest_review", "读取最近情境训练复盘。"),
    ),
    "news": (
        ("profile.current", "读取当前用户画像。"),
        ("news.latest_analysis", "读取最近一次持久化新闻/政策解读。"),
        ("news.impact_lens", "读取新闻影响路径解释模板。"),
        ("news.policy_evidence_search", "检索新闻/政策证据。"),
    ),
}


class AgentPlanner:
    def __init__(
        self,
        *,
        tool_registry: ToolRegistry | None = None,
        capabilities: RuntimeCapabilityConfig | None = None,
    ) -> None:
        self.tool_registry = tool_registry or ToolRegistry()
        self.skill_registry = SkillRegistry(tool_registry=self.tool_registry)
        self.capabilities = capabilities or build_runtime_capabilities()

    def build_plan(self, *, message: str, blocked: bool = False) -> AgentPlan:
        if blocked:
            return AgentPlan(
                primary_intent="learning",
                detected_intents=["learning"],
                worker_names=["SafetyPolicyWorker"],
                selected_skills=[],
                planned_tools=[
                    PlannedToolCall(
                        tool_name="profile.current",
                        purpose="安全拦截仍读取当前用户画像，保持回复有上下文。",
                    ),
                    PlannedToolCall(
                        tool_name="safety.boundary_rules",
                        purpose="读取 FundGene 产品安全边界，支撑拒答和改写建议。",
                    ),
                ],
                tool_selection_signals=[
                    ToolSelectionSignal(
                        tool_name="profile.current",
                        score=100,
                        source="required",
                        reason="安全拦截仍需要当前用户画像。",
                    ),
                    ToolSelectionSignal(
                        tool_name="safety.boundary_rules",
                        score=100,
                        source="required",
                        reason="安全拦截必须读取产品边界规则。",
                    ),
                ],
                constraints=[
                    PlanConstraint(
                        name="blocked_request_read_only",
                        status="pass",
                        detail="命中安全边界后只允许只读上下文和安全规则工具。",
                    )
                ],
                requires_human_review=False,
                rationale="输入触及产品边界，计划降级为学习型安全引导。",
            )

        detected_intents = self.detect_intents(message)
        if not self.capabilities.enabled("multi_worker_fanout"):
            detected_intents = detected_intents[:1]
        primary_intent = detected_intents[0]
        worker_names = self._worker_names_for(detected_intents)
        selected_skills = self.skill_registry.select_skills(
            message=message,
            intents=detected_intents,
        )
        required_tool_names = _required_tools_for_skills(selected_skills)
        if self.capabilities.enabled("scored_tool_discovery"):
            planned_tools, tool_selection_signals = self.tool_registry.recommend_tools(
                message=message,
                intents=detected_intents,
                max_budget=self.capabilities.max_tool_calls,
                required_tool_names=required_tool_names,
            )
        else:
            planned_tools = self._tools_for(
                detected_intents,
                required_tool_names=required_tool_names,
            )
            tool_selection_signals = [
                ToolSelectionSignal(
                    tool_name=tool.tool_name,
                    score=0,
                    source="intent_fallback",
                    reason="scored_tool_discovery 未启用，回退到 intent 映射。",
                )
                for tool in planned_tools
            ]
        return AgentPlan(
            primary_intent=primary_intent,
            detected_intents=detected_intents,
            worker_names=worker_names,
            selected_skills=selected_skills,
            planned_tools=planned_tools,
            tool_selection_signals=tool_selection_signals,
            constraints=[
                PlanConstraint(
                    name="selected_skills",
                    status="pass",
                    detail=(
                        "已选择 skills："
                        + ", ".join(
                            plan_skill.skill_name for plan_skill in selected_skills
                        )
                        if selected_skills
                        else "未选择业务 skill。"
                    ),
                ),
                PlanConstraint(
                    name="max_workers",
                    status=(
                        "pass"
                        if len(worker_names) <= self.capabilities.max_workers
                        else "warn"
                    ),
                    detail=f"本次计划使用 {len(worker_names)} 个内部 worker。",
                ),
                PlanConstraint(
                    name="read_only_tools",
                    status="pass",
                    detail="当前 Agent Runtime v2 只允许只读工具，状态变更只能作为 proposal 持久化。",
                ),
                PlanConstraint(
                    name="runtime_capabilities",
                    status="pass",
                    detail="启用能力："
                    + ", ".join(self.capabilities.enabled_flags or ["none"]),
                ),
            ],
            max_tool_calls=self.capabilities.max_tool_calls,
            requires_human_review=False,
            rationale=self._rationale(detected_intents),
        )

    def detect_intents(self, message: str) -> list[Intent]:
        normalized = message.lower()
        matches: list[Intent] = []
        for intent, keywords in _INTENT_KEYWORDS.items():
            if any(keyword.lower() in normalized for keyword in keywords):
                matches.append(intent)
        if not matches:
            return ["learning"]
        if "simulation" in matches and "behavior" not in matches:
            matches.append("behavior")
        return _dedupe(matches)[: self.capabilities.max_workers]

    def _worker_names_for(self, intents: list[Intent]) -> list[str]:
        return _dedupe([_WORKER_BY_INTENT[intent] for intent in intents])[
            : self.capabilities.max_workers
        ]

    def _tools_for(
        self,
        intents: list[Intent],
        *,
        required_tool_names: list[str] | None = None,
    ) -> list[PlannedToolCall]:
        planned: list[PlannedToolCall] = []
        seen: set[str] = set()
        for tool_name in required_tool_names or []:
            definition = self.tool_registry.definitions.get(tool_name)
            if definition is None or tool_name in seen:
                continue
            planned.append(
                PlannedToolCall(
                    tool_name=tool_name,
                    purpose=definition.purpose,
                    required=True,
                )
            )
            seen.add(tool_name)
            if len(planned) >= self.capabilities.max_tool_calls:
                return planned
        for intent in intents:
            for tool_name, purpose in _TOOLS_BY_INTENT[intent]:
                if tool_name in seen:
                    continue
                planned.append(PlannedToolCall(tool_name=tool_name, purpose=purpose))
                seen.add(tool_name)
                if len(planned) >= self.capabilities.max_tool_calls:
                    return planned
        return planned

    def _rationale(self, intents: list[Intent]) -> str:
        if len(intents) == 1:
            return f"单意图问题，使用 {intents[0]} worker 和相关只读工具。"
        labels = "、".join(intents)
        return f"跨域问题，受控 fan-out 到 {labels}，再聚合为一个新手可读答案。"


def _dedupe(values: list) -> list:
    return list(dict.fromkeys(values))


def _required_tools_for_skills(skills: list[SkillSelection]) -> list[str]:
    return list(
        dict.fromkeys(tool for skill in skills for tool in skill.required_tools)
    )
