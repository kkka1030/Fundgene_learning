from app.runtime.v2.schemas import SkillSelection, ToolResult, WorkerOutput
from app.runtime.v2.workers.common import (
    asks_for_list_or_status,
    collect_evidence,
    finding_from_evidence,
    learning_outcome_for,
    skill_forbidden_language,
    skill_names,
    skill_output_guidance,
    tool_payload,
)


class PortfolioWorker:
    name = "PortfolioWorker"
    intent = "portfolio"

    def run(
        self,
        *,
        message: str,
        tool_results: list[ToolResult],
        skills: list[SkillSelection] | None = None,
    ) -> WorkerOutput:
        profile = tool_payload(tool_results, "profile.current")
        report = tool_payload(tool_results, "portfolio.latest_report")
        risk_lens = tool_payload(tool_results, "portfolio.risk_lens")
        behavior = tool_payload(tool_results, "behavior.profile")

        goal = profile.get("primary_goal")
        summary = report.get("summary")
        snapshot_date = report.get("snapshot_date")
        if asks_for_list_or_status(message):
            if summary:
                date_text = f"（快照日期 {snapshot_date}）" if snapshot_date else ""
                finding = f"直接回答：当前已有最近组合报告{date_text}：{summary}"
            else:
                finding = "直接回答：当前还没有可用的组合报告；需要先录入一份持仓快照，系统才能判断集中度、风险桶和配置结构。"
        elif summary:
            finding = (
                "组合分析先看集中度、重复风险和整体风险是否匹配目标。"
                f" 最近一份组合报告显示：{summary}"
            )
        else:
            finding = (
                "组合分析需要先录入持仓快照；没有原始持仓时，系统不会伪造组合结论。"
            )

        if goal:
            finding += f" 当前解释应围绕“{goal}”这个目标。"
        if risk_lens.get("summary"):
            finding += (
                " 接下来更适合先看四件事：集中度、资产类型是否分散、"
                "和目标是否匹配，以及遇到波动时会不会被情绪推着操作。"
            )

        risk_flags = ["组合解释不是交易指令，只能作为风险结构和配置原则的学习支持。"]
        if behavior.get("bias_tags"):
            risk_flags.append("当前行为画像会影响组合调整时的冲动风险判断。")

        actions = [
            "先回到 Portfolio 查看最近一份组合报告，再围绕它继续提问。",
            "优先检查最大仓位和前两大持仓是否承担了重复风险。",
        ]
        if not report.get("has_report"):
            actions = [
                "先手工录入当前持仓快照。",
                "拿到报告后，再讨论集中度、风险桶和再平衡原则。",
            ]

        evidence_refs = collect_evidence(tool_results)
        return WorkerOutput(
            worker_name=self.name,
            intent=self.intent,
            skill_names=skill_names(skills),
            learning_outcome=learning_outcome_for(skills, intent=self.intent),
            skill_output_guidance=skill_output_guidance(skills),
            skill_forbidden_language=skill_forbidden_language(skills),
            findings=[finding],
            structured_findings=[
                finding_from_evidence(
                    claim=finding,
                    evidence_refs=evidence_refs,
                    explanation="组合 worker 只使用用户画像、最近组合报告和行为画像来解释风险结构。",
                    safety_boundary="组合解释只覆盖风险结构和配置原则，不生成交易指令。",
                )
            ],
            evidence_refs=evidence_refs,
            risk_flags=risk_flags,
            recommended_actions=actions,
            confidence=0.76,
            limitations=["当前组合解释只引用最近一份持久化报告摘要。"],
        )
