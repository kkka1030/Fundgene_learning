import asyncio
import json

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

import app.services.ai_enhancement as ai_enhancement
from app.core.config import get_settings
from app.runtime.deps import get_advisor_runtime
from app.runtime.v2.composer import ResponseComposer
from app.runtime.v2.orchestrator import RISK_NOTICE
from app.runtime.v2.schemas import EvidenceRef, PolicyResult, WorkerOutput
from app.schemas.assistant import AdvisorResponse


def _deterministic_response() -> AdvisorResponse:
    return AdvisorResponse(
        answer="规则回答：先理解风险和回撤，再讨论组合结构。",
        intent="learning",
        citations=["agent_runtime_v2"],
        risk_notice=RISK_NOTICE,
        recommended_actions=["先完成一节风险课程。"],
        follow_up_questions=["我想用一个数字例子理解回撤。"],
    )


def _worker_output() -> WorkerOutput:
    return WorkerOutput(
        worker_name="LearningWorker",
        intent="learning",
        findings=["新手应先把风险等级和回撤理解清楚。"],
        recommended_actions=["先完成一节风险课程。"],
        confidence=0.9,
    )


def _onboard(client: TestClient, *, email: str = "deepseek@example.com") -> None:
    response = client.post(
        "/api/auth/register",
        json={"email": email, "password": "supersecure123"},
    )
    assert response.status_code == 201
    response = client.post(
        "/api/onboarding/profile",
        json={
            "display_name": "Ava",
            "investing_experience": "beginner",
            "monthly_contribution_band": "under_3000",
            "primary_goal": "建立长期基金投资纪律。",
        },
    )
    assert response.status_code == 200


def _clear_runtime_caches() -> None:
    get_settings.cache_clear()
    get_advisor_runtime.cache_clear()


def test_deepseek_hybrid_without_key_reports_configuration_required(monkeypatch) -> None:
    monkeypatch.setenv("FUNDGENE_DEEPSEEK_API_KEY", "")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "")
    _clear_runtime_caches()

    composer = ResponseComposer(
        model_name="deepseek:deepseek-v4-pro",
        agent_mode="hybrid",
        timeout_ms=1000,
        risk_notice=RISK_NOTICE,
    )
    result = asyncio.run(
        composer.compose(
            intent="learning",
            worker_output=_worker_output(),
            input_policy=PolicyResult(status="allow", reason="test"),
            deterministic_response=_deterministic_response(),
        )
    )

    assert "没有接入可用的 LLM API" in result.response.answer
    assert "规则回答" not in result.response.answer
    assert result.response.recommended_action_targets[0].href.startswith("/profile")
    assert result.metadata["composer_mode"] == "model_unconfigured"
    assert result.metadata["provider"] == "deepseek"
    assert result.metadata["model_name"] == "deepseek:deepseek-v4-pro"
    assert result.metadata["fallback_reason"] == "model_not_configured"
    assert result.fallback_reason == "model_not_configured"


def test_profile_llm_settings_can_supply_user_deepseek_key(
    monkeypatch,
    client: TestClient,
) -> None:
    monkeypatch.setenv("FUNDGENE_AGENT_MODE", "hybrid")
    monkeypatch.setenv("FUNDGENE_DEEPSEEK_API_KEY", "")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "")
    _clear_runtime_caches()

    async def fake_model(self, **kwargs):  # noqa: ANN001
        deterministic = kwargs["deterministic_response"]
        return deterministic.model_copy(
            update={"answer": "用户 Key 模型回答：先把风险等级和回撤拆开看。"}
        )

    monkeypatch.setattr(ResponseComposer, "_run_model_composer", fake_model)
    _onboard(client, email="user-llm-settings@example.com")

    empty_settings = client.get("/api/profile/llm-settings")
    assert empty_settings.status_code == 200
    assert empty_settings.json()["source"] == "none"
    assert empty_settings.json()["configured"] is False

    save_settings = client.put(
        "/api/profile/llm-settings",
        json={
            "provider": "deepseek",
            "model_name": "deepseek:deepseek-v4-pro",
            "api_key": "sk-user-demo-key",
            "enabled": True,
        },
    )
    assert save_settings.status_code == 200
    saved_payload = save_settings.json()["settings"]
    assert saved_payload["source"] == "user"
    assert saved_payload["configured"] is True
    assert saved_payload["masked_api_key"].startswith("sk-use")
    assert "sk-user-demo-key" not in json.dumps(saved_payload)

    response = client.post(
        "/api/assistant/messages",
        json={"message": "基金回撤是什么意思？"},
    )
    assert response.status_code == 200
    assistant_message = response.json()["messages"][-1]
    assert assistant_message["advisor_response"]["answer"].startswith("用户 Key 模型回答")

    trace_response = client.get(
        f"/api/assistant/runs/{assistant_message['agent_run_id']}/trace"
    )
    assert trace_response.status_code == 200
    composer = trace_response.json()["run"]["tool_trace"]["composer"]
    assert composer["composer_mode"] == "model"


def test_deepseek_model_failure_keeps_deterministic_response(monkeypatch) -> None:
    monkeypatch.setenv("FUNDGENE_DEEPSEEK_API_KEY", "test-key")
    _clear_runtime_caches()

    async def fail_model(self, **kwargs):  # noqa: ANN001
        raise TimeoutError("mock timeout")

    monkeypatch.setattr(ResponseComposer, "_run_model_composer", fail_model)
    composer = ResponseComposer(
        model_name="deepseek:deepseek-v4-pro",
        agent_mode="model",
        timeout_ms=1000,
        risk_notice=RISK_NOTICE,
    )
    result = asyncio.run(
        composer.compose(
            intent="learning",
            worker_output=_worker_output(),
            input_policy=PolicyResult(status="allow", reason="test"),
            deterministic_response=_deterministic_response(),
        )
    )

    assert result.response.answer.startswith("规则回答")
    assert result.metadata["composer_mode"] == "deterministic_fallback"
    assert result.metadata["fallback_reason"] == "mock timeout"
    assert result.metadata["provider"] == "deepseek"


def test_coach_mock_deepseek_success_records_model_trace(
    monkeypatch,
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    monkeypatch.setenv("FUNDGENE_AGENT_MODE", "hybrid")
    monkeypatch.setenv("FUNDGENE_DEEPSEEK_API_KEY", "test-key")
    _clear_runtime_caches()

    async def fake_model(self, **kwargs):  # noqa: ANN001
        deterministic = kwargs["deterministic_response"]
        return deterministic.model_copy(
            update={"answer": "模型增强回答：先把回撤看成风险承受力测试。"}
        )

    monkeypatch.setattr(ResponseComposer, "_run_model_composer", fake_model)
    _onboard(client, email="trace-model@example.com")

    response = client.post(
        "/api/assistant/messages",
        json={"message": "基金回撤是什么意思？"},
    )
    assert response.status_code == 200
    run_id = response.json()["messages"][-1]["agent_run_id"]
    trace_response = client.get(f"/api/assistant/runs/{run_id}/trace")
    assert trace_response.status_code == 200
    composer = trace_response.json()["run"]["tool_trace"]["composer"]
    assert composer["composer_mode"] == "model"
    assert composer["provider"] == "deepseek"
    assert composer["model_name"] == "deepseek:deepseek-v4-pro"

    with session_factory() as session:
        from app.models.agent_run import AgentRun

        run = session.get(AgentRun, run_id)
        assert run is not None
        assert run.fallback_reason is None


def test_news_model_composer_keeps_concrete_headlines(monkeypatch) -> None:
    monkeypatch.setenv("FUNDGENE_DEEPSEEK_API_KEY", "test-key")
    _clear_runtime_caches()

    async def generic_news_model(self, **kwargs):  # noqa: ANN001
        deterministic = kwargs["deterministic_response"]
        return deterministic.model_copy(
            update={
                "answer": (
                    "想看懂财经新闻，可以先拆成事实、影响路径和不确定性，"
                    "再判断和自己的学习计划是否相关。"
                )
            }
        )

    monkeypatch.setattr(ResponseComposer, "_run_model_composer", generic_news_model)
    composer = ResponseComposer(
        model_name="deepseek:deepseek-v4-pro",
        agent_mode="model",
        timeout_ms=1000,
        risk_notice=RISK_NOTICE,
    )
    worker_output = WorkerOutput(
        worker_name="NewsWorker",
        intent="news",
        findings=[
            "当前已同步资讯中最近几条是："
            "1. “SEC Charges 21 Individuals With Alleged Wide-Reaching Insider Trading Scheme”："
            "2026-05-06，SEC charged 21 individuals."
        ],
        evidence_refs=[
            EvidenceRef(
                worker_name="NewsWorker",
                source_type="policy_item",
                source_id="policy-1",
                quote_or_summary="SEC charged 21 individuals.",
                claim="新闻回答引用检索到的来源材料作为解释证据。",
            )
        ],
        recommended_actions=["进入 News，选择最相关的一条新闻或政策生成结构化解读。"],
    )
    deterministic = AdvisorResponse(
        answer=worker_output.findings[0],
        intent="news",
        citations=["policy_item:policy-1"],
        risk_notice=RISK_NOTICE,
        recommended_actions=worker_output.recommended_actions,
        follow_up_questions=["请用第一条新闻说明它可能影响哪些基金类型。"],
    )

    result = asyncio.run(
        composer.compose(
            intent="news",
            worker_output=worker_output,
            input_policy=PolicyResult(status="allow", reason="test"),
            deterministic_response=deterministic,
        )
    )

    assert result.response.answer == deterministic.answer
    assert result.metadata["composer_mode"] == "deterministic_fallback"
    assert result.metadata["fallback_reason"] == "news_concrete_items_dropped"


def test_model_composer_keeps_direct_state_answer(monkeypatch) -> None:
    monkeypatch.setenv("FUNDGENE_DEEPSEEK_API_KEY", "test-key")
    _clear_runtime_caches()

    async def generic_state_model(self, **kwargs):  # noqa: ANN001
        deterministic = kwargs["deterministic_response"]
        return deterministic.model_copy(
            update={"answer": "组合分析通常要先看集中度、重复风险和目标匹配。"}
        )

    monkeypatch.setattr(ResponseComposer, "_run_model_composer", generic_state_model)
    composer = ResponseComposer(
        model_name="deepseek:deepseek-v4-pro",
        agent_mode="model",
        timeout_ms=1000,
        risk_notice=RISK_NOTICE,
    )
    worker_output = WorkerOutput(
        worker_name="PortfolioWorker",
        intent="portfolio",
        findings=["直接回答：当前还没有可用的组合报告；需要先录入一份持仓快照。"],
        recommended_actions=["先手工录入当前持仓快照。"],
    )
    deterministic = AdvisorResponse(
        answer=worker_output.findings[0],
        intent="portfolio",
        citations=["portfolio_analysis"],
        risk_notice=RISK_NOTICE,
        recommended_actions=worker_output.recommended_actions,
        follow_up_questions=["请告诉我录入持仓快照需要哪些字段。"],
    )

    result = asyncio.run(
        composer.compose(
            intent="portfolio",
            worker_output=worker_output,
            input_policy=PolicyResult(status="allow", reason="test"),
            deterministic_response=deterministic,
        )
    )

    assert result.response.answer == deterministic.answer
    assert result.metadata["composer_mode"] == "deterministic_fallback"
    assert result.metadata["fallback_reason"] == "direct_answer_dropped"


def test_news_unsafe_model_enhancement_falls_back_to_rule_payload(
    monkeypatch,
    client: TestClient,
) -> None:
    monkeypatch.setenv("FUNDGENE_AGENT_MODE", "hybrid")
    monkeypatch.setenv("FUNDGENE_DEEPSEEK_API_KEY", "test-key")
    _clear_runtime_caches()

    def unsafe_model(**kwargs):  # noqa: ANN003
        return ai_enhancement.NewsAnalysisEnhancement(
            facts=["模型声称保证收益。", "标题已经足够判断。"],
            impact_paths=["可以直接买入。", "不需要再看组合。"],
            uncertainty_notes=["没有不确定性。", "短期确定上涨。"],
            beginner_translation="这是一段包含保证收益和直接买入的错误模型文本。",
            recommended_next_actions=["直接买入相关基金。", "满仓等待上涨。"],
        )

    monkeypatch.setattr(ai_enhancement, "_run_model_enhancement", unsafe_model)
    _onboard(client, email="unsafe-news@example.com")

    response = client.post(
        "/api/news/analyze",
        json={
            "headline": "长期资金入市政策继续推进",
            "body": "政策强调长期资金和资本市场稳定，但具体节奏、落地方式与基金组合影响仍需要进一步观察。",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    serialized = json.dumps(payload, ensure_ascii=False)
    assert "模型声称保证收益" not in serialized
    assert "直接买入相关基金" not in serialized
    assert "不构成收益承诺" in payload["risk_notice"]
    assert payload["facts"][0].endswith("发布或更新了一条新闻信息。")


def test_portfolio_unsafe_model_enhancement_falls_back_to_rule_payload(
    monkeypatch,
    client: TestClient,
) -> None:
    monkeypatch.setenv("FUNDGENE_AGENT_MODE", "hybrid")
    monkeypatch.setenv("FUNDGENE_DEEPSEEK_API_KEY", "test-key")
    _clear_runtime_caches()

    def unsafe_model(**kwargs):  # noqa: ANN003
        return ai_enhancement.PortfolioAnalysisEnhancement(
            summary="模型错误总结：这份组合保证收益，应该直接买入并满仓。",
            risk_exposure=["保证收益。"],
            concentration_flags=["直接买入第一大持仓。"],
            allocation_balance=["清仓其他资产。"],
            recommended_next_actions=["满仓。", "自动交易。"],
        )

    monkeypatch.setattr(ai_enhancement, "_run_model_enhancement", unsafe_model)
    _onboard(client, email="unsafe-portfolio@example.com")

    response = client.post(
        "/api/portfolio/snapshots",
        json={
            "snapshot_date": "2026-04-30",
            "cash_value": 5000,
            "holdings": [
                {
                    "fund_code": "161725",
                    "fund_name": "白酒指数",
                    "fund_type": "equity",
                    "market_value": 12000,
                },
                {
                    "fund_code": "006327",
                    "fund_name": "中短债基金",
                    "fund_type": "bond",
                    "market_value": 8000,
                },
            ],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    serialized = json.dumps(payload, ensure_ascii=False)
    assert "保证收益" not in serialized
    assert "直接买入第一大持仓" not in serialized
    assert "这份快照总资产约为 25000.00 元" in payload["summary"]
