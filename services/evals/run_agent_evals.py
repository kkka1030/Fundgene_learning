from __future__ import annotations

import json
import os
import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any

os.environ.setdefault("FUNDGENE_AGENT_MODE", "deterministic")

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base, get_db_session
from app.main import app


CASE_PATH = Path(__file__).with_name("agent_eval_cases.json")


def main() -> int:
    cases = json.loads(CASE_PATH.read_text(encoding="utf-8"))
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    session_factory = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
        expire_on_commit=False,
    )
    Base.metadata.create_all(bind=engine)

    def override_get_db_session() -> Generator[Session, None, None]:
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db_session] = override_get_db_session
    try:
        with TestClient(app) as client:
            _seed_eval_user(client)
            results = [_run_case(client, case) for case in cases]
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(bind=engine)
        engine.dispose()

    failed = [result for result in results if not result["passed"]]
    print(
        json.dumps(
            {"total": len(results), "failed": failed}, ensure_ascii=False, indent=2
        )
    )
    return 1 if failed else 0


def _seed_eval_user(client: TestClient) -> None:
    _expect_status(
        client.post(
            "/api/auth/register",
            json={"email": "agent-evals@fundgene.test", "password": "supersecure123"},
        ),
        201,
    )
    _expect_status(
        client.post(
            "/api/onboarding/profile",
            json={
                "display_name": "Eval 用户",
                "investing_experience": "beginner",
                "monthly_contribution_band": "under_3000",
                "primary_goal": "建立长期基金投资纪律。",
            },
        ),
        200,
    )
    _expect_status(
        client.post(
            "/api/behavior/questionnaires",
            json={
                "questionnaire_version": "v1",
                "answers": {
                    "volatility_comfort": 4,
                    "drawdown_reaction": 3,
                    "investment_horizon": 4,
                    "panic_sell_impulse": 2,
                    "chase_hot_funds": 4,
                    "diversification_habit": 3,
                },
            },
        ),
        200,
    )
    _expect_status(client.get("/api/learning/path"), 200)
    _expect_status(
        client.post(
            "/api/portfolio/snapshots",
            json={
                "snapshot_date": "2026-04-29",
                "cash_value": 6000,
                "holdings": [
                    {
                        "fund_code": "161725",
                        "fund_name": "招商中证白酒指数",
                        "fund_type": "equity",
                        "market_value": 18000,
                    },
                    {
                        "fund_code": "110027",
                        "fund_name": "易方达安心债券",
                        "fund_type": "bond",
                        "market_value": 16000,
                    },
                    {
                        "fund_code": "000071",
                        "fund_name": "华夏恒生 ETF 联接",
                        "fund_type": "international",
                        "market_value": 12000,
                    },
                ],
            },
        ),
        200,
    )
    _expect_status(
        client.post(
            "/api/news/analyze",
            json={
                "headline": "长期资金入市政策继续推进",
                "body": "政策强调长期资金和资本市场稳定，但具体节奏、落地方式与基金组合影响仍需要进一步观察。",
            },
        ),
        200,
    )


def _run_case(client: TestClient, case: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    response = client.post("/api/assistant/messages", json={"message": case["message"]})
    if response.status_code != 200:
        return {
            "id": case["id"],
            "passed": False,
            "failures": [f"assistant_status:{response.status_code}"],
        }

    payload = response.json()
    assistant_message = payload["messages"][-1]
    advisor_response = assistant_message["advisor_response"]
    run_id = assistant_message["agent_run_id"]
    trace_response = client.get(f"/api/assistant/runs/{run_id}/trace")
    if trace_response.status_code != 200:
        return {
            "id": case["id"],
            "passed": False,
            "failures": [f"trace_status:{trace_response.status_code}"],
        }
    trace = trace_response.json()

    if advisor_response["intent"] != case["expected_intent"]:
        failures.append(f"intent:{advisor_response['intent']}")
    if trace["run"]["policy_status"] != case["expected_policy_status"]:
        failures.append(f"policy:{trace['run']['policy_status']}")

    answer = advisor_response["answer"]
    for expected in case.get("answer_contains", []):
        if expected not in answer:
            failures.append(f"missing_answer:{expected}")
    for forbidden in case.get("forbidden_answer_terms", []):
        if forbidden in answer:
            failures.append(f"forbidden_answer:{forbidden}")
    max_answer_chars = case.get("max_answer_chars")
    if max_answer_chars and len(answer) > int(max_answer_chars):
        failures.append(f"answer_too_long:{len(answer)}")

    tool_names = {item["tool_name"] for item in trace["tool_calls"]}
    for tool_name in case.get("required_tools", []):
        if tool_name not in tool_names:
            failures.append(f"missing_tool:{tool_name}")
    for tool_name in case.get("forbidden_tools", []):
        if tool_name in tool_names:
            failures.append(f"forbidden_tool:{tool_name}")

    tool_trace = trace["run"].get("tool_trace") or {}
    agent_plan = tool_trace.get("agent_plan") or {}
    skill_names = {item["skill_name"] for item in agent_plan.get("selected_skills", [])}
    for skill_name in case.get("required_skills", []):
        if skill_name not in skill_names:
            failures.append(f"missing_skill:{skill_name}")

    trace_events = {item["name"] for item in tool_trace.get("trace_events", [])}
    for event_name in case.get("required_trace_events", []):
        if event_name not in trace_events:
            failures.append(f"missing_trace_event:{event_name}")

    max_unsupported_findings = case.get("max_unsupported_findings")
    if max_unsupported_findings is not None:
        worker_validation = tool_trace.get("worker_validation") or {}
        unsupported = int(worker_validation.get("unsupported_findings") or 0)
        if unsupported > int(max_unsupported_findings):
            failures.append(f"unsupported_findings:{unsupported}")

    evidence_types = {item["source_type"] for item in trace["evidence_refs"]}
    for source_type in case.get("required_evidence_types", []):
        if source_type not in evidence_types:
            failures.append(f"missing_evidence:{source_type}")

    if case.get("require_citations") and not advisor_response["citations"]:
        failures.append("missing_citations")

    allowed_targets = set(case.get("allowed_action_targets", []))
    action_targets = advisor_response.get("recommended_action_targets") or []
    if case.get("require_action_targets") and not action_targets:
        failures.append("missing_action_targets")
    for target in action_targets:
        href = target.get("href")
        if not isinstance(href, str) or not href.startswith("/"):
            failures.append(f"unsafe_action_target:{href}")
        href_path = href.split("?", 1)[0] if isinstance(href, str) else href
        if allowed_targets and href_path not in allowed_targets:
            failures.append(f"unexpected_action_target:{href}")

    return {
        "id": case["id"],
        "passed": not failures,
        "failures": failures,
        "observed": {
            "intent": advisor_response["intent"],
            "policy_status": trace["run"]["policy_status"],
            "tools": sorted(tool_names),
            "skills": sorted(skill_names),
            "trace_events": sorted(trace_events),
            "evidence_types": sorted(evidence_types),
            "action_targets": [target.get("href") for target in action_targets],
        },
    }


def _expect_status(response, expected_status: int) -> None:
    if response.status_code != expected_status:
        raise RuntimeError(
            f"Expected {expected_status}, got {response.status_code}: {response.text}"
        )


if __name__ == "__main__":
    sys.exit(main())
