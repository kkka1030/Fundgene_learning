from __future__ import annotations

import asyncio
import json
import os
import sys

from app.core.config import get_settings
from app.runtime.v2.composer import ResponseComposer
from app.runtime.v2.orchestrator import RISK_NOTICE
from app.runtime.v2.schemas import EvidenceRef, PolicyResult, WorkerOutput
from app.schemas.assistant import AdvisorResponse


async def main() -> int:
    if not (
        os.getenv("FUNDGENE_DEEPSEEK_API_KEY") or os.getenv("DEEPSEEK_API_KEY")
    ):
        print(
            "DeepSeek smoke skipped: set FUNDGENE_DEEPSEEK_API_KEY or DEEPSEEK_API_KEY in the shell environment.",
            file=sys.stderr,
        )
        return 0

    settings = get_settings()
    composer = ResponseComposer(
        model_name=settings.advisor_model,
        agent_mode="model",
        timeout_ms=settings.agent_model_timeout_ms,
        risk_notice=RISK_NOTICE,
    )
    deterministic_response = AdvisorResponse(
        answer="基金回撤可以理解为从阶段高点下跌的幅度，先看它是否超过你的承受范围。",
        intent="learning",
        citations=["smoke_fixture:fund_basics"],
        risk_notice=RISK_NOTICE,
        recommended_actions=["先记录自己能接受的最大回撤。"],
        follow_up_questions=["我想用一个数字例子理解回撤。"],
    )
    result = await composer.compose(
        intent="learning",
        worker_output=WorkerOutput(
            worker_name="SmokeLearningWorker",
            intent="learning",
            findings=[
                "回撤是基金从阶段高点到低点的跌幅，新手应先理解它和风险承受力的关系。"
            ],
            evidence_refs=[
                EvidenceRef(
                    worker_name="SmokeLearningWorker",
                    source_type="smoke_fixture",
                    source_id="fund_basics",
                    quote_or_summary="回撤用于描述净值从高点下跌的幅度。",
                    claim="回撤是理解基金风险的重要指标。",
                )
            ],
            recommended_actions=["先记录自己能接受的最大回撤。"],
            confidence=0.9,
        ),
        input_policy=PolicyResult(status="allow", reason="smoke test input"),
        deterministic_response=deterministic_response,
    )
    print(
        json.dumps(
            {
                "metadata": result.metadata,
                "fallback_reason": result.fallback_reason,
                "response": result.response.model_dump(mode="json"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
