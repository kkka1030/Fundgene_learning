from app.runtime.v2.schemas import (
    ContextSnapshot,
    EvidenceRef,
    PlannedToolCall,
    ToolDefinition,
    ToolResult,
    ToolSelectionSignal,
)
from app.services.evidence import EvidenceService


class ToolRegistry:
    def __init__(self) -> None:
        self.definitions = {
            "profile.current": ToolDefinition(
                name="profile.current",
                category="context",
                purpose="Read current user profile and risk context.",
                allowed_intents=[
                    "learning",
                    "portfolio",
                    "behavior",
                    "simulation",
                    "news",
                ],
                search_terms=["profile", "context", "画像", "目标", "经验", "风险等级"],
                evidence_required=True,
            ),
            "safety.boundary_rules": ToolDefinition(
                name="safety.boundary_rules",
                category="guardrail",
                purpose="Read FundGene product safety boundaries.",
                allowed_intents=[
                    "learning",
                    "portfolio",
                    "behavior",
                    "simulation",
                    "news",
                ],
                search_terms=[
                    "safety",
                    "boundary",
                    "买入",
                    "卖出",
                    "收益",
                    "保证",
                    "交易",
                ],
                evidence_required=True,
            ),
            "learning.path": ToolDefinition(
                name="learning.path",
                category="learning",
                purpose="Read persisted learning progress and recommendation.",
                allowed_intents=["learning"],
                search_terms=["learning", "course", "progress", "学习", "课程", "进度"],
                evidence_required=True,
            ),
            "learning.concept_map": ToolDefinition(
                name="learning.concept_map",
                category="learning",
                purpose="Read curated concept explanations for beginner fund education.",
                allowed_intents=["learning"],
                search_terms=[
                    "concept",
                    "term",
                    "基金",
                    "概念",
                    "术语",
                    "回撤",
                    "风险等级",
                ],
                evidence_required=True,
            ),
            "learning.evidence_search": ToolDefinition(
                name="learning.evidence_search",
                category="learning",
                purpose="Search course-section evidence.",
                allowed_intents=["learning"],
                search_terms=[
                    "evidence",
                    "course",
                    "material",
                    "证据",
                    "课程",
                    "材料",
                    "解释",
                ],
                evidence_required=True,
            ),
            "portfolio.latest_report": ToolDefinition(
                name="portfolio.latest_report",
                category="portfolio",
                purpose="Read latest persisted portfolio report.",
                allowed_intents=["portfolio"],
                search_terms=[
                    "portfolio",
                    "report",
                    "holding",
                    "组合",
                    "持仓",
                    "报告",
                    "配置",
                ],
                evidence_required=True,
            ),
            "portfolio.risk_lens": ToolDefinition(
                name="portfolio.risk_lens",
                category="portfolio",
                purpose="Read portfolio risk-analysis lens and safe explanation constraints.",
                allowed_intents=["portfolio"],
                search_terms=[
                    "risk",
                    "allocation",
                    "concentration",
                    "风险",
                    "集中",
                    "分散",
                    "仓位",
                ],
                evidence_required=True,
            ),
            "behavior.profile": ToolDefinition(
                name="behavior.profile",
                category="behavior",
                purpose="Read behavior profile and bias tags.",
                allowed_intents=["learning", "portfolio", "behavior", "simulation"],
                search_terms=[
                    "behavior",
                    "bias",
                    "emotion",
                    "行为",
                    "偏差",
                    "情绪",
                    "追涨",
                    "恐慌",
                ],
                evidence_required=True,
            ),
            "behavior.training_plan": ToolDefinition(
                name="behavior.training_plan",
                category="behavior",
                purpose="Read behavior training focus and recommended scenario.",
                allowed_intents=["behavior", "simulation"],
                search_terms=[
                    "training",
                    "practice",
                    "scenario",
                    "训练",
                    "练习",
                    "情境",
                    "冲动",
                ],
                evidence_required=True,
            ),
            "simulation.latest_review": ToolDefinition(
                name="simulation.latest_review",
                category="simulation",
                purpose="Read latest simulation review summary.",
                allowed_intents=["behavior", "simulation"],
                search_terms=[
                    "simulation",
                    "review",
                    "scenario",
                    "模拟",
                    "情境",
                    "复盘",
                    "演练",
                ],
                evidence_required=True,
            ),
            "news.latest_analysis": ToolDefinition(
                name="news.latest_analysis",
                category="news",
                purpose="Read latest persisted news or policy analysis.",
                allowed_intents=["news"],
                search_terms=[
                    "news",
                    "policy",
                    "analysis",
                    "新闻",
                    "政策",
                    "解读",
                    "宏观",
                ],
                evidence_required=True,
            ),
            "news.impact_lens": ToolDefinition(
                name="news.impact_lens",
                category="news",
                purpose="Read news impact-path explanation lens and uncertainty constraints.",
                allowed_intents=["news"],
                search_terms=[
                    "impact",
                    "uncertainty",
                    "path",
                    "影响",
                    "路径",
                    "不确定",
                    "标题",
                ],
                evidence_required=True,
            ),
            "news.policy_evidence_search": ToolDefinition(
                name="news.policy_evidence_search",
                category="news",
                purpose="Search persisted news and policy evidence.",
                allowed_intents=["news"],
                search_terms=[
                    "policy",
                    "evidence",
                    "source",
                    "政策",
                    "证据",
                    "来源",
                    "材料",
                ],
                evidence_required=True,
            ),
        }

    def recommend_tools(
        self,
        *,
        message: str,
        intents: list[str],
        max_budget: int,
        required_tool_names: list[str] | None = None,
    ) -> tuple[list[PlannedToolCall], list[ToolSelectionSignal]]:
        planned: list[PlannedToolCall] = []
        signals: list[ToolSelectionSignal] = []
        selected: set[str] = set()
        remaining_budget = max_budget

        def add_tool(
            tool_name: str,
            *,
            source: str,
            score: float,
            matched_terms: list[str] | None = None,
            reason: str,
        ) -> None:
            nonlocal remaining_budget
            if tool_name in selected or tool_name not in self.definitions:
                return
            definition = self.definitions[tool_name]
            if definition.budget_cost > remaining_budget:
                return
            planned.append(
                PlannedToolCall(
                    tool_name=tool_name,
                    purpose=definition.purpose or reason,
                    required=source == "required",
                )
            )
            signals.append(
                ToolSelectionSignal(
                    tool_name=tool_name,
                    score=score,
                    matched_terms=matched_terms or [],
                    source=source,  # type: ignore[arg-type]
                    reason=reason,
                )
            )
            selected.add(tool_name)
            remaining_budget -= definition.budget_cost

        add_tool(
            "profile.current",
            source="required",
            score=100,
            reason="所有 advisor run 都需要当前用户画像作为上下文。",
        )
        for tool_name in required_tool_names or []:
            add_tool(
                tool_name,
                source="required",
                score=95,
                reason="已选 coaching skill 要求该只读工具作为证据来源。",
            )

        candidates: list[tuple[float, str, list[str], str]] = []
        for tool_name, definition in self.definitions.items():
            if tool_name in selected or tool_name == "safety.boundary_rules":
                continue
            if definition.allowed_intents and not any(
                intent in definition.allowed_intents for intent in intents
            ):
                continue
            score, matched_terms = _score_tool(
                definition, message=message, intents=intents
            )
            if score <= 0:
                continue
            candidates.append(
                (
                    score,
                    tool_name,
                    matched_terms,
                    f"工具与 detected intents={','.join(intents)} 和用户问题匹配。",
                )
            )

        for score, tool_name, matched_terms, reason in sorted(
            candidates,
            key=lambda item: (-item[0], item[1]),
        ):
            add_tool(
                tool_name,
                source="scored_search",
                score=score,
                matched_terms=matched_terms,
                reason=reason,
            )
        return planned, signals

    def select_tools(self, intent: str, *, blocked: bool = False) -> list[str]:
        if blocked:
            return ["profile.current"]
        if intent == "portfolio":
            return ["profile.current", "portfolio.latest_report", "behavior.profile"]
        if intent in {"behavior", "simulation"}:
            return ["profile.current", "behavior.profile", "simulation.latest_review"]
        if intent == "news":
            return [
                "profile.current",
                "news.latest_analysis",
                "news.policy_evidence_search",
            ]
        return [
            "profile.current",
            "learning.path",
            "learning.evidence_search",
            "behavior.profile",
        ]

    def execute(
        self,
        *,
        tool_name: str,
        snapshot: ContextSnapshot,
        query: str = "",
        evidence_service: EvidenceService | None = None,
    ) -> ToolResult:
        if tool_name not in self.definitions:
            raise KeyError(f"Unknown runtime tool: {tool_name}")

        if tool_name == "profile.current":
            return ToolResult(
                tool_name=tool_name,
                output_payload={
                    "user_id": snapshot.user_id,
                    "display_name": snapshot.display_name,
                    "investing_experience": snapshot.investing_experience,
                    "primary_goal": snapshot.primary_goal,
                    "risk_level": snapshot.risk_level,
                },
                evidence_refs=[
                    EvidenceRef(
                        worker_name="context_loader",
                        source_type="user_profile",
                        source_id=snapshot.user_id,
                        source_version=snapshot.schema_version,
                        quote_or_summary=(
                            f"{snapshot.display_name} / {snapshot.investing_experience}"
                        ),
                        claim="回答使用当前登录用户画像作为上下文。",
                    )
                ],
            )

        if tool_name == "safety.boundary_rules":
            summary = (
                "FundGene 只能提供基金学习、组合结构解释、行为训练和新闻/政策理解；"
                "不能提供买卖、清仓、满仓、保本收益、收益保证或自动交易。"
            )
            return ToolResult(
                tool_name=tool_name,
                output_payload={
                    "direct_trade_execution_allowed": False,
                    "return_promise_allowed": False,
                    "broker_connection_allowed": False,
                    "safe_response_mode": "learning_guidance",
                    "summary": summary,
                },
                evidence_refs=[
                    EvidenceRef(
                        worker_name="SafetyPolicyWorker",
                        source_type="product_policy",
                        source_id="fundgene_scope_v1",
                        source_version="agent_runtime_v2",
                        quote_or_summary=summary,
                        claim="回答必须保持在学习和决策支持边界内。",
                        support_summary="产品边界来自 FundGene canonical agent context。",
                    )
                ],
            )

        if tool_name == "learning.path":
            return ToolResult(
                tool_name=tool_name,
                output_payload={
                    "progress": snapshot.learning_progress_percentage,
                    "completed_courses": snapshot.learning_completed_courses_count,
                    "total_courses": snapshot.learning_total_courses,
                    "recommended_course_slug": snapshot.learning_recommended_course_slug,
                    "recommended_course_title": snapshot.learning_recommended_course_title,
                },
                evidence_refs=[
                    EvidenceRef(
                        worker_name="LearningWorker",
                        source_type="learning_path",
                        source_id=snapshot.learning_recommended_course_slug,
                        source_version=snapshot.schema_version,
                        quote_or_summary=(
                            snapshot.learning_recommended_course_title or "暂无推荐课程"
                        ),
                        claim="学习建议基于当前学习进度和下一门推荐课程。",
                    )
                ],
            )

        if tool_name == "learning.concept_map":
            concepts = _concepts_for_query(query)
            summary = "；".join(
                f"{item['term']}：{item['definition']}" for item in concepts
            )
            return ToolResult(
                tool_name=tool_name,
                output_payload={"concepts": concepts, "summary": summary},
                evidence_refs=[
                    EvidenceRef(
                        worker_name="LearningWorker",
                        source_type="fundgene_concept_map",
                        source_id="concept_map_v1",
                        source_version="curated_v1",
                        quote_or_summary=summary,
                        claim="学习回答使用内置基金概念图谱约束术语解释。",
                        support_summary="概念图谱只用于基础教育，不产生投资建议。",
                    )
                ],
            )

        if tool_name == "learning.evidence_search":
            hits = (
                evidence_service.search_learning(query, limit=3)
                if evidence_service is not None
                else []
            )
            return ToolResult(
                tool_name=tool_name,
                output_payload={"hits": [hit.as_payload() for hit in hits]},
                evidence_refs=[
                    EvidenceRef(
                        worker_name="LearningWorker",
                        source_type=hit.source_type,
                        source_id=hit.source_id,
                        source_version="keyword_v1",
                        quote_or_summary=hit.snippet,
                        claim="学习回答引用课程材料作为解释证据。",
                        support_summary=hit.support_summary,
                    )
                    for hit in hits
                ],
            )

        if tool_name == "portfolio.latest_report":
            return ToolResult(
                tool_name=tool_name,
                output_payload={
                    "has_report": snapshot.portfolio_has_report,
                    "summary": snapshot.portfolio_latest_summary,
                    "snapshot_date": (
                        snapshot.portfolio_latest_snapshot_date.isoformat()
                        if snapshot.portfolio_latest_snapshot_date
                        else None
                    ),
                },
                evidence_refs=[
                    EvidenceRef(
                        worker_name="PortfolioWorker",
                        source_type="portfolio_analysis",
                        source_id=(
                            snapshot.portfolio_latest_snapshot_date.isoformat()
                            if snapshot.portfolio_latest_snapshot_date
                            else None
                        ),
                        source_version=snapshot.schema_version,
                        quote_or_summary=(
                            snapshot.portfolio_latest_summary or "当前用户尚无组合报告"
                        ),
                        claim="组合解释优先引用最近一份持久化组合报告。",
                    )
                ],
            )

        if tool_name == "portfolio.risk_lens":
            summary = (
                "组合解释按集中度、资产类型分散、目标匹配、行为冲动四个维度展开；"
                "只给风险结构和再平衡原则，不给交易指令。"
            )
            return ToolResult(
                tool_name=tool_name,
                output_payload={
                    "dimensions": [
                        "concentration",
                        "asset_type_balance",
                        "goal_alignment",
                        "behavior_impulse",
                    ],
                    "safe_mode": "principles_only",
                    "summary": summary,
                },
                evidence_refs=[
                    EvidenceRef(
                        worker_name="PortfolioWorker",
                        source_type="portfolio_risk_lens",
                        source_id="portfolio_lens_v1",
                        source_version="curated_v1",
                        quote_or_summary=summary,
                        claim="组合回答必须限定在风险结构解释和配置原则。",
                        support_summary="组合工具约束防止输出买卖或仓位执行建议。",
                    )
                ],
            )

        if tool_name == "behavior.profile":
            return ToolResult(
                tool_name=tool_name,
                output_payload={
                    "risk_level": snapshot.risk_level,
                    "bias_tags": snapshot.bias_tags,
                    "training_focus": snapshot.behavior_training_focus,
                    "training_guidance": snapshot.behavior_training_guidance,
                },
                evidence_refs=[
                    EvidenceRef(
                        worker_name="BehaviorWorker",
                        source_type="behavior_profile",
                        source_id=snapshot.user_id,
                        source_version=snapshot.schema_version,
                        quote_or_summary=", ".join(snapshot.bias_tags)
                        or "暂无显著偏差标签",
                        claim="行为建议基于风险问卷和行为画像。",
                    )
                ],
            )

        if tool_name == "behavior.training_plan":
            focus = snapshot.behavior_training_focus or "先记录投资动作前的触发情绪"
            guidance = (
                snapshot.behavior_training_guidance or "把情绪、证据和动作分开写下来。"
            )
            scenario = snapshot.simulation_recommended_scenario_title or "回撤纪律训练"
            summary = f"训练焦点：{focus}；建议情境：{scenario}；练习方式：{guidance}"
            return ToolResult(
                tool_name=tool_name,
                output_payload={
                    "training_focus": focus,
                    "training_guidance": guidance,
                    "recommended_scenario_slug": snapshot.simulation_recommended_scenario_slug,
                    "recommended_scenario_title": scenario,
                    "summary": summary,
                },
                evidence_refs=[
                    EvidenceRef(
                        worker_name="BehaviorWorker",
                        source_type="behavior_training_plan",
                        source_id=snapshot.user_id,
                        source_version=snapshot.schema_version,
                        quote_or_summary=summary,
                        claim="行为回答引用当前训练焦点和推荐情境。",
                        support_summary="训练计划来自用户行为画像和情境训练状态。",
                    )
                ],
            )

        if tool_name == "simulation.latest_review":
            return ToolResult(
                tool_name=tool_name,
                output_payload={
                    "completed_sessions_count": snapshot.simulation_completed_sessions_count,
                    "recommended_scenario_slug": snapshot.simulation_recommended_scenario_slug,
                    "recommended_scenario_title": snapshot.simulation_recommended_scenario_title,
                    "latest_review_summary": snapshot.simulation_latest_review_summary,
                },
                evidence_refs=[
                    EvidenceRef(
                        worker_name="BehaviorWorker",
                        source_type="simulation_review",
                        source_id=snapshot.simulation_recommended_scenario_slug,
                        source_version=snapshot.schema_version,
                        quote_or_summary=(
                            snapshot.simulation_latest_review_summary
                            or snapshot.simulation_recommended_scenario_title
                            or "暂无情境训练复盘"
                        ),
                        claim="行为训练建议参考最近情境复盘和推荐训练场景。",
                    )
                ],
            )

        if tool_name == "news.latest_analysis":
            return ToolResult(
                tool_name=tool_name,
                output_payload={
                    "has_analysis": snapshot.news_has_analysis,
                    "analysis_id": snapshot.news_latest_analysis_id,
                    "title": snapshot.news_latest_title,
                    "source_name": snapshot.news_latest_source_name,
                    "beginner_translation": snapshot.news_latest_beginner_translation,
                    "recommended_action": snapshot.news_latest_recommended_action,
                },
                evidence_refs=[
                    EvidenceRef(
                        worker_name="NewsWorker",
                        source_type="news_analysis",
                        source_id=snapshot.news_latest_analysis_id,
                        source_version=snapshot.schema_version,
                        quote_or_summary=(
                            snapshot.news_latest_beginner_translation
                            or snapshot.news_latest_title
                            or "暂无新闻/政策解读"
                        ),
                        claim="新闻解释优先引用最近一次持久化新闻/政策解读。",
                    )
                ],
            )

        if tool_name == "news.impact_lens":
            summary = (
                "新闻/政策解读按事实、影响路径、不确定性、可做的学习动作四层展开；"
                "标题情绪不能直接变成买卖结论。"
            )
            return ToolResult(
                tool_name=tool_name,
                output_payload={
                    "dimensions": [
                        "facts",
                        "impact_paths",
                        "uncertainty",
                        "safe_learning_actions",
                    ],
                    "summary": summary,
                },
                evidence_refs=[
                    EvidenceRef(
                        worker_name="NewsWorker",
                        source_type="news_impact_lens",
                        source_id="news_lens_v1",
                        source_version="curated_v1",
                        quote_or_summary=summary,
                        claim="新闻回答必须先区分事实、路径和不确定性。",
                        support_summary="影响路径模板用于约束新闻解释，不生成买卖指令。",
                    )
                ],
            )

        hits = (
            evidence_service.search_news_policy(
                query,
                user_id=snapshot.user_id,
                limit=3,
            )
            if evidence_service is not None
            else []
        )
        return ToolResult(
            tool_name=tool_name,
            output_payload={"hits": [hit.as_payload() for hit in hits]},
            evidence_refs=[
                EvidenceRef(
                    worker_name="NewsWorker",
                    source_type=hit.source_type,
                    source_id=hit.source_id,
                    source_version="keyword_v1",
                    quote_or_summary=hit.snippet,
                    claim="新闻/政策回答引用检索到的来源材料作为解释证据。",
                    support_summary=hit.support_summary,
                )
                for hit in hits
            ],
        )


def _concepts_for_query(query: str) -> list[dict[str, str]]:
    normalized = query.lower()
    concepts = []
    if "回撤" in query or "drawdown" in normalized:
        concepts.append(
            {
                "term": "回撤",
                "definition": "从阶段高点下跌的幅度，用来理解账户短期波动压力。",
            }
        )
    if "风险" in query or "risk" in normalized:
        concepts.append(
            {
                "term": "风险等级",
                "definition": "风险等级描述波动承受能力，不等于收益承诺。",
            }
        )
    if "费用" in query or "fee" in normalized:
        concepts.append(
            {
                "term": "基金费用",
                "definition": "申购、赎回、管理和托管等费用会影响长期持有结果。",
            }
        )
    if not concepts:
        concepts.append(
            {
                "term": "基金",
                "definition": "基金把多位投资者资金集合起来，按合同投资一组资产。",
            }
        )
    return concepts


def _score_tool(
    definition: ToolDefinition,
    *,
    message: str,
    intents: list[str],
) -> tuple[float, list[str]]:
    matched_terms = _matched_terms(message, definition.search_terms)
    score = 0.0
    for index, intent in enumerate(intents):
        if intent in definition.allowed_intents:
            score += 3.0 if index == 0 else 1.5
    score += len(matched_terms) * 4.0
    if definition.evidence_required:
        score += 0.2
    if definition.category in intents:
        score += 0.5
    return score, matched_terms


def _matched_terms(message: str, terms: list[str]) -> list[str]:
    normalized = message.lower()
    return [term for term in terms if term.lower() in normalized]
