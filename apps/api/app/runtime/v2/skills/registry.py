from pydantic import BaseModel, Field

from app.runtime.v2.schemas import Intent, SkillSelection
from app.runtime.v2.tools.registry import ToolRegistry


class SkillDefinition(BaseModel):
    schema_version: str = "fundgene_skill_v1"
    name: str
    version: str
    title: str
    description: str
    applicable_intents: list[Intent]
    required_tools: list[str] = Field(default_factory=list)
    match_terms: list[str] = Field(default_factory=list)
    forbidden_language: list[str] = Field(default_factory=list)
    output_guidance: list[str] = Field(default_factory=list)
    eval_case_ids: list[str] = Field(default_factory=list)
    learning_outcome: str
    priority: int = Field(default=50, ge=0, le=100)
    min_intent_count: int = Field(default=1, ge=1)

    @property
    def skill_key(self) -> str:
        return f"{self.name}@{self.version}"


class SkillRegistry:
    def __init__(self, *, tool_registry: ToolRegistry | None = None) -> None:
        self.tool_registry = tool_registry or ToolRegistry()
        self.definitions = self._build_definition_map(_builtin_skills())
        self._validate_definitions()

    def select_skills(
        self,
        *,
        message: str,
        intents: list[Intent],
    ) -> list[SkillSelection]:
        if not intents:
            return []

        normalized = message.lower()
        selected: list[SkillDefinition] = []
        selected_names: set[str] = set()

        for intent in intents:
            candidate = self._best_skill_for_intent(
                intent=intent,
                intents=intents,
                normalized_message=normalized,
                excluded=selected_names,
            )
            if candidate is None:
                continue
            selected.append(candidate)
            selected_names.add(candidate.name)

        if len(intents) > 1:
            synthesis = self.definitions.get("cross_domain_synthesis_v1")
            if synthesis is not None and synthesis.name not in selected_names:
                selected.append(synthesis)

        return [
            self._selection_for(
                definition=definition,
                normalized_message=normalized,
                intents=intents,
            )
            for definition in selected
        ]

    def _best_skill_for_intent(
        self,
        *,
        intent: Intent,
        intents: list[Intent],
        normalized_message: str,
        excluded: set[str],
    ) -> SkillDefinition | None:
        candidates = [
            definition
            for definition in self.definitions.values()
            if definition.name not in excluded
            and definition.min_intent_count <= len(intents)
            and intent in definition.applicable_intents
            and definition.name != "cross_domain_synthesis_v1"
        ]
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda definition: self._score(
                definition=definition,
                normalized_message=normalized_message,
                intents=intents,
            ),
        )

    def _selection_for(
        self,
        *,
        definition: SkillDefinition,
        normalized_message: str,
        intents: list[Intent],
    ) -> SkillSelection:
        matched_terms = _matched_terms(definition.match_terms, normalized_message)
        return SkillSelection(
            skill_name=definition.name,
            skill_version=definition.version,
            score=self._score(
                definition=definition,
                normalized_message=normalized_message,
                intents=intents,
            ),
            matched_terms=matched_terms,
            required_tools=definition.required_tools,
            output_guidance=definition.output_guidance,
            forbidden_language=definition.forbidden_language,
            learning_outcome=definition.learning_outcome,
            reason=(
                f"技能适配 intents={','.join(intents)}；"
                f"required_tools={','.join(definition.required_tools) or 'none'}。"
            ),
        )

    def _score(
        self,
        *,
        definition: SkillDefinition,
        normalized_message: str,
        intents: list[Intent],
    ) -> float:
        intent_hits = len(set(definition.applicable_intents) & set(intents))
        matched_terms = _matched_terms(definition.match_terms, normalized_message)
        return float(definition.priority + intent_hits * 20 + len(matched_terms) * 8)

    def _build_definition_map(
        self,
        definitions: list[SkillDefinition],
    ) -> dict[str, SkillDefinition]:
        names: set[str] = set()
        mapped: dict[str, SkillDefinition] = {}
        for definition in definitions:
            if definition.name in names:
                raise ValueError(f"Duplicate skill name: {definition.name}")
            names.add(definition.name)
            mapped[definition.name] = definition
        return mapped

    def _validate_definitions(self) -> None:
        known_tools = set(self.tool_registry.definitions)
        for definition in self.definitions.values():
            unknown_tools = [
                tool for tool in definition.required_tools if tool not in known_tools
            ]
            if unknown_tools:
                raise ValueError(
                    f"Skill {definition.skill_key} references unknown tools: "
                    + ", ".join(unknown_tools)
                )


def _matched_terms(terms: list[str], normalized_message: str) -> list[str]:
    return [term for term in terms if term.lower() in normalized_message]


def _builtin_skills() -> list[SkillDefinition]:
    return [
        SkillDefinition(
            name="fund_basics_explainer_v1",
            version="2026-05-16",
            title="基金基础解释",
            description="把基金概念拆成资产、波动、费用和风险承受四层解释。",
            applicable_intents=["learning"],
            required_tools=[
                "profile.current",
                "learning.path",
                "learning.concept_map",
                "learning.evidence_search",
            ],
            match_terms=[
                "基金",
                "回撤",
                "风险等级",
                "净值",
                "波动",
                "费用",
                "concept",
                "drawdown",
                "learning",
            ],
            forbidden_language=["稳赚", "保本", "保证收益", "直接买入"],
            output_guidance=[
                "先解释术语，再说明它为什么影响新手决策。",
                "把下一步学习动作绑定到当前 learning path。",
            ],
            eval_case_ids=[
                "learning_drawdown",
                "learning_fund_basics",
                "citation_faithfulness_learning",
            ],
            learning_outcome="用户能用自己的话解释一个基金概念，并知道下一节学习内容。",
            priority=70,
        ),
        SkillDefinition(
            name="portfolio_concentration_review_v1",
            version="2026-05-16",
            title="组合集中度复核",
            description="按集中度、重复暴露、目标匹配和行为冲动解释组合风险。",
            applicable_intents=["portfolio"],
            required_tools=[
                "profile.current",
                "portfolio.latest_report",
                "portfolio.risk_lens",
                "behavior.profile",
            ],
            match_terms=[
                "组合",
                "持仓",
                "仓位",
                "配置",
                "集中",
                "第一大",
                "portfolio",
                "allocation",
                "holding",
            ],
            forbidden_language=["立即卖出", "清仓", "满仓", "调仓指令"],
            output_guidance=[
                "只解释结构风险和原则，不输出交易动作。",
                "显式区分报告事实、风险含义和下一步检查。",
            ],
            eval_case_ids=[
                "portfolio_concentration",
                "portfolio_allocation",
                "portfolio_holding_weight",
            ],
            learning_outcome="用户能识别组合是否存在集中或重复暴露，并转到报告页继续检查。",
            priority=75,
        ),
        SkillDefinition(
            name="behavior_bias_reflection_v1",
            version="2026-05-16",
            title="行为偏差反思",
            description="把追涨、恐慌和冲动拆成触发点、证据和可训练动作。",
            applicable_intents=["behavior"],
            required_tools=[
                "profile.current",
                "behavior.profile",
                "behavior.training_plan",
            ],
            match_terms=[
                "追涨",
                "恐慌",
                "冲动",
                "偏差",
                "情绪",
                "behavior",
                "bias",
                "panic",
                "chase",
            ],
            forbidden_language=["你就是", "永久标签", "必须买", "必须卖"],
            output_guidance=[
                "行为标签只作为训练线索，不作为诊断。",
                "推荐动作应是记录、复盘或情境训练。",
            ],
            eval_case_ids=["behavior_chasing", "behavior_panic"],
            learning_outcome="用户能把一次投资冲动拆成触发情绪、证据和下一次训练动作。",
            priority=72,
        ),
        SkillDefinition(
            name="simulation_review_coach_v1",
            version="2026-05-16",
            title="情境训练复盘",
            description="把历史情境练习转成可复盘的决策训练，不做涨跌预测。",
            applicable_intents=["simulation"],
            required_tools=[
                "profile.current",
                "behavior.profile",
                "behavior.training_plan",
                "simulation.latest_review",
            ],
            match_terms=[
                "情境",
                "情景",
                "演练",
                "复盘",
                "历史",
                "simulation",
                "scenario",
                "review",
            ],
            forbidden_language=["预测涨跌", "猜对", "保证胜率", "交易指令"],
            output_guidance=[
                "强调训练目的不是猜行情，而是留下决策证据。",
                "把复盘转成下一次训练焦点。",
            ],
            eval_case_ids=["simulation_review"],
            learning_outcome="用户能理解历史情境训练的价值，并知道下一次如何复盘。",
            priority=74,
        ),
        SkillDefinition(
            name="news_policy_impact_path_v1",
            version="2026-05-16",
            title="新闻政策影响路径",
            description="按事实、影响路径、不确定性和安全学习动作解释新闻政策。",
            applicable_intents=["news"],
            required_tools=[
                "profile.current",
                "news.latest_analysis",
                "news.impact_lens",
                "news.policy_evidence_search",
            ],
            match_terms=[
                "新闻",
                "政策",
                "宏观",
                "事实",
                "不确定性",
                "影响",
                "news",
                "policy",
                "macro",
            ],
            forbidden_language=["马上买入", "立即卖出", "利好必涨", "确定影响"],
            output_guidance=[
                "标题情绪不能直接变成投资结论。",
                "必须把事实、影响路径和不确定性分开。",
            ],
            eval_case_ids=["news_policy", "news_uncertainty", "safe_next_action"],
            learning_outcome="用户能把一条新闻拆成事实、影响路径和不确定性，而不是直接行动。",
            priority=73,
        ),
        SkillDefinition(
            name="cross_domain_synthesis_v1",
            version="2026-05-16",
            title="跨域综合回答",
            description="把多个 worker 的结论压缩成一个新手可执行的学习/检查顺序。",
            applicable_intents=[
                "learning",
                "portfolio",
                "behavior",
                "simulation",
                "news",
            ],
            required_tools=[],
            match_terms=["我的", "同时", "影响", "配置", "情绪", "冲动"],
            forbidden_language=["多智能体争论", "内部工具", "run id", "tool name"],
            output_guidance=[
                "只输出一个统一答案，不暴露内部 worker 对话。",
                "推荐动作按学习、检查、复盘排序。",
            ],
            eval_case_ids=[],
            learning_outcome="用户能看到跨模块问题的下一步顺序，而不是内部 agent 细节。",
            priority=60,
            min_intent_count=2,
        ),
    ]
