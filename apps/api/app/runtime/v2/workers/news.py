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


def _format_news_hit(hit: dict, index: int) -> str:
    title = str(hit.get("source_title") or "未命名资讯").strip()
    snippet = str(hit.get("snippet") or "").strip()
    published_at = str(hit.get("published_at") or "").strip()
    freshness = f"{published_at[:10]}，" if published_at else ""
    if snippet and snippet != title:
        return f"{index}. “{title}”：{freshness}{snippet}"
    return f"{index}. “{title}”：{freshness}当前同步到的新闻/政策标题。"


class NewsWorker:
    name = "NewsWorker"
    intent = "news"

    def run(
        self,
        *,
        message: str,
        tool_results: list[ToolResult],
        skills: list[SkillSelection] | None = None,
    ) -> WorkerOutput:
        news = tool_payload(tool_results, "news.latest_analysis")
        impact_lens = tool_payload(tool_results, "news.impact_lens")
        evidence_payload = tool_payload(tool_results, "news.policy_evidence_search")
        title = news.get("title")
        translation = news.get("beginner_translation")
        evidence_hits = evidence_payload.get("hits") or []

        finding = "新闻解读先分清事实、影响路径和不确定性，不要把标题情绪直接当成投资结论。"
        if translation:
            finding += f" 最近一条真实新闻/政策分析是“{title}”：{translation}"
        elif title:
            finding += f" 当前最近信息是“{title}”，但还需要生成结构化解读。"
        elif evidence_hits:
            formatted_hits = [
                _format_news_hit(hit, index)
                for index, hit in enumerate(evidence_hits[:3], start=1)
            ]
            finding += (
                ("直接回答：当前已同步资讯中最近几条是：" if asks_for_list_or_status(message) else " 当前已同步资讯中最近几条是：")
                + "；".join(formatted_hits)
                + "。如果用户问“今天有什么新闻”，先给出这些具体标题，再提醒它们需要逐条解读。"
            )
        if impact_lens.get("summary"):
            finding += (
                " 这类信息适合先按事实、影响路径、不确定性和你能做的学习动作来拆，"
                "不要直接跳到买卖结论。"
            )
        if evidence_hits and (translation or title):
            first_hit = evidence_hits[0]
            snippet = str(first_hit.get("snippet") or "").strip()
            if snippet:
                finding += f" 相关背景是：{snippet}"

        action = (
            news.get("recommended_action")
            or "进入 News，选择最相关的一条新闻或政策生成结构化解读。"
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
                    explanation="新闻 worker 将最近持久化解读和政策检索证据转成影响路径解释。",
                    safety_boundary="新闻/政策解读不能被当作买卖建议。",
                )
            ],
            evidence_refs=evidence_refs,
            risk_flags=["新闻/政策解读不能被当作买卖建议。"],
            recommended_actions=[
                action,
                "继续追问这条信息通过什么路径影响你正在学习或持有的基金类型。",
            ],
            confidence=0.72,
            limitations=[
                "当前新闻解释引用最近持久化解读和关键词检索到的新闻/政策证据。"
            ],
        )
