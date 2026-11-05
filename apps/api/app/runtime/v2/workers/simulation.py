from app.runtime.v2.schemas import SkillSelection, ToolResult, WorkerOutput
from app.runtime.v2.workers.common import (
    asks_for_list_or_status,
    collect_evidence,
    display_bias_tags,
    finding_from_evidence,
    learning_outcome_for,
    skill_forbidden_language,
    skill_names,
    skill_output_guidance,
    tool_payload,
)


class SimulationWorker:
    name = "SimulationWorker"
    intent = "simulation"

    def run(
        self,
        *,
        message: str,
        tool_results: list[ToolResult],
        skills: list[SkillSelection] | None = None,
    ) -> WorkerOutput:
        behavior = tool_payload(tool_results, "behavior.profile")
        training_plan = tool_payload(tool_results, "behavior.training_plan")
        simulation = tool_payload(tool_results, "simulation.latest_review")
        latest_review = simulation.get("latest_review_summary")
        completed_sessions = simulation.get("completed_sessions_count")
        scenario_title = (
            simulation.get("recommended_scenario_title")
            or training_plan.get("recommended_scenario_title")
            or "回撤纪律训练"
        )
        bias_tags = behavior.get("bias_tags") or []
        display_tags = display_bias_tags(bias_tags)

        asks_status = asks_for_list_or_status(message)
        if asks_status:
            completed_text = (
                f"已完成 {completed_sessions} 次情境训练"
                if completed_sessions is not None
                else "当前还没有完成训练记录"
            )
            if latest_review:
                finding = (
                    f"直接回答：{completed_text}。最近一次训练复盘提示："
                    f"{latest_review} 这次更适合先把复盘拆成下一次行动前检查清单，"
                    "而不是立刻重复同一轮训练。"
                )
            else:
                finding = f"直接回答：{completed_text}；推荐从“{scenario_title}”开始。"
        else:
            finding = (
                "历史情境训练不是预测涨跌，而是把当时能看到的信息、你的选择、"
                "事后复盘分开记录。"
            )
        if latest_review and not asks_status:
            finding += (
                f" 最近一次复盘提示：{latest_review} 先沿着这条复盘整理下一次行动前"
                "要检查的问题，不需要马上重复同一轮训练。"
            )
        elif not latest_review and not asks_status:
            finding += f" 当前更适合从“{scenario_title}”开始练习。"
        if display_tags:
            finding += f" 训练时重点观察“{display_tags[0]}”是否影响了动作。"

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
                    explanation="情境 worker 将训练计划、行为画像和最近复盘转成下一次练习焦点。",
                    safety_boundary="情境训练用于复盘决策过程，不用于预测涨跌或生成交易指令。",
                )
            ],
            evidence_refs=evidence_refs,
            risk_flags=["情境训练不是行情预测，也不是交易胜率承诺。"],
            recommended_actions=(
                [
                    "把最近一次复盘整理成下一次行动前检查清单。",
                    "回到 Profile 确认是否把这条行为线索保存为待观察记录。",
                ]
                if latest_review
                else [
                    "进入 Simulation，完成一轮当前推荐情境。",
                    "复盘时分别写下当时信息、你的选择、事后结果和下一次规则。",
                ]
            ),
            confidence=0.75,
            limitations=["情境建议基于当前训练计划和最近复盘摘要。"],
        )
