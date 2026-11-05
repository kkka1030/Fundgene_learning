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


class LearningWorker:
    name = "LearningWorker"
    intent = "learning"

    def run(
        self,
        *,
        message: str,
        tool_results: list[ToolResult],
        skills: list[SkillSelection] | None = None,
    ) -> WorkerOutput:
        learning_payload = tool_payload(tool_results, "learning.path")
        concept_payload = tool_payload(tool_results, "learning.concept_map")
        evidence_payload = tool_payload(tool_results, "learning.evidence_search")
        behavior_payload = tool_payload(tool_results, "behavior.profile")
        recommended_course = learning_payload.get("recommended_course_title")
        risk_level = behavior_payload.get("risk_level")
        concept_summary = concept_payload.get("summary")
        evidence_hits = evidence_payload.get("hits") or []
        progress = learning_payload.get("progress")
        completed = learning_payload.get("completed_courses")
        total = learning_payload.get("total_courses")
        asks_risk_level_status = (
            asks_for_list_or_status(message) and "风险等级" in message
        )

        prefix = ""
        if risk_level == "growth":
            prefix = "结合你目前偏进取的风险承受度，"
        elif risk_level == "balanced":
            prefix = "结合你目前偏平衡的风险承受度，"
        elif risk_level == "conservative":
            prefix = "结合你目前偏稳健的风险承受度，"

        finding = f"{prefix}基金学习先看三件事：基金买什么、波动来自哪里、费用如何影响长期结果。"
        if asks_risk_level_status and risk_level:
            finding = f"直接回答：你当前的风险等级是 {risk_level}。"
        elif asks_for_list_or_status(message):
            status_parts = []
            if progress is not None:
                status_parts.append(f"学习进度约 {progress}%")
            if completed is not None and total is not None:
                status_parts.append(f"已完成 {completed}/{total} 门课程")
            if recommended_course:
                status_parts.append(f"下一门推荐《{recommended_course}》")
            if risk_level:
                status_parts.append(f"当前风险等级是 {risk_level}")
            if status_parts:
                finding = "直接回答：" + "；".join(status_parts) + "。"
        if "回撤" in message or "drawdown" in message.lower():
            finding = "回撤是从阶段高点回落的幅度，核心作用是帮助你判断自己能否承受账户短期下跌。"
        if concept_summary:
            finding += f" 概念图谱提示：{concept_summary}"
        if evidence_hits:
            first_hit = evidence_hits[0]
            finding += f" 课程证据提示：{first_hit.get('snippet')}"

        actions = [
            "回到 Dashboard，确认当前风险等级和下一步动作。",
            "把今天最关键的概念写成自己的话，确认你真的理解了。",
        ]
        if recommended_course:
            actions[0] = (
                f"进入 Learning，优先完成《{recommended_course}》的下一节内容。"
            )

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
                    explanation="学习 worker 将学习进度、风险画像和课程检索证据合并成一个新手解释。",
                    safety_boundary="学习建议不能被理解成收益承诺。",
                )
            ],
            evidence_refs=evidence_refs,
            risk_flags=["不要把风险等级理解成收益承诺。"],
            recommended_actions=actions,
            confidence=0.78,
            limitations=["当前学习建议基于学习进度、行为画像摘要和课程关键词证据。"],
        )
