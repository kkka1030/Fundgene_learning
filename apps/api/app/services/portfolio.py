from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.portfolio_analysis import PortfolioAnalysis
from app.models.portfolio_holding import PortfolioHolding
from app.models.portfolio_snapshot import PortfolioSnapshot
from app.models.user import UserProfile
from app.schemas.dashboard import DashboardPortfolioStatus
from app.schemas.portfolio import (
    PortfolioHistoryItem,
    PortfolioHistoryResponse,
    PortfolioReportResponse,
    PortfolioSnapshotCreateRequest,
)
from app.services.ai_enhancement import enhance_portfolio_analysis_text

PORTFOLIO_ANALYSIS_VERSION = "portfolio_report_v1"


@dataclass(slots=True)
class PortfolioAnalysisResult:
    total_value: float
    holdings_with_weight: list[dict[str, object]]
    summary: str
    risk_exposure: list[str]
    concentration_flags: list[str]
    allocation_balance: list[str]
    recommended_next_actions: list[str]


def _normalize_bucket(fund_type: str) -> str:
    normalized = fund_type.strip().lower()
    if normalized in {"equity", "international", "commodity"}:
        return "growth"
    if normalized in {"mixed"}:
        return "balanced"
    if normalized in {"bond", "money_market"}:
        return "defensive"
    return "other"


def _round_money(value: float) -> float:
    return round(float(value), 2)


def _analyze_snapshot(
    *,
    user: UserProfile,
    cash_value: float,
    holdings: list[dict[str, object]],
) -> PortfolioAnalysisResult:
    holdings_total = sum(float(item["market_value"]) for item in holdings)
    total_value = cash_value + holdings_total
    holdings_with_weight: list[dict[str, object]] = []
    type_weights: dict[str, float] = defaultdict(float)

    for holding in holdings:
        market_value = float(holding["market_value"])
        weight = round((market_value / total_value) * 100, 1) if total_value > 0 else 0.0
        bucket = _normalize_bucket(str(holding["fund_type"]))
        type_weights[bucket] += weight
        holdings_with_weight.append({**holding, "weight": weight})

    sorted_holdings = sorted(
        holdings_with_weight,
        key=lambda item: float(item["weight"]),
        reverse=True,
    )
    top_weight = float(sorted_holdings[0]["weight"]) if sorted_holdings else 0.0
    top_two_weight = sum(float(item["weight"]) for item in sorted_holdings[:2])
    cash_ratio = round((cash_value / total_value) * 100, 1) if total_value > 0 else 0.0
    growth_weight = round(type_weights.get("growth", 0.0), 1)
    balanced_weight = round(type_weights.get("balanced", 0.0), 1)
    defensive_weight = round(type_weights.get("defensive", 0.0), 1)

    concentration_flags: list[str] = []
    if top_weight >= 40:
        concentration_flags.append(
            "当前最大单一基金权重已达到 40% 以上，组合对单一产品波动会明显敏感。"
        )
    elif top_weight >= 25:
        concentration_flags.append(
            "当前至少有一只基金权重超过 25%，建议优先检查是否承担了过多单一主题风险。"
        )
    else:
        concentration_flags.append(
            "当前没有单只基金权重超过 25%，单产品集中度压力相对可控。"
        )

    if top_two_weight >= 70:
        concentration_flags.append(
            "前两大持仓合计超过 70%，组合层面的重复风险仍然偏高。"
        )

    risk_exposure: list[str] = []
    if growth_weight >= 70:
        risk_exposure.append(
            "权益与高波动资产占比较高，组合更适合能承受中长期波动的用户。"
        )
    elif growth_weight >= 40:
        risk_exposure.append(
            "组合里既有成长暴露，也保留了一定缓冲，整体更接近平衡型风险暴露。"
        )
    else:
        risk_exposure.append(
            "高波动资产占比较低，组合整体更偏防守，回撤弹性可能相对有限。"
        )

    if cash_ratio >= 30:
        risk_exposure.append(
            "现金占比较高，说明你保留了较多等待空间，但也可能拖慢长期资金利用效率。"
        )
    else:
        risk_exposure.append(
            "现金占比处于可控区间，组合主要风险来自已持有基金而不是大量闲置现金。"
        )

    allocation_balance: list[str] = []
    if len(type_weights) <= 1:
        allocation_balance.append(
            "当前持仓类型过于单一，更像押注一个风险桶，而不是完成组合配置。"
        )
    else:
        allocation_balance.append(
            "组合已经覆盖多个类型，但仍要继续检查这些基金是否承担了重复主题暴露。"
        )

    if defensive_weight == 0 and growth_weight >= 60:
        allocation_balance.append(
            "组合几乎没有防守垫层，遇到阶段性下跌时更容易触发情绪压力。"
        )
    elif defensive_weight > 0:
        allocation_balance.append(
            "组合里已经有一定缓冲资产，可以继续观察它们是否真的承担了风险缓释作用。"
        )

    goal_hint = (
        f"你的当前目标是“{user.primary_goal}”，"
        if user.primary_goal
        else "当前组合分析会优先围绕风险暴露和配置纪律来解释，"
    )
    summary = (
        f"{goal_hint}这份快照总资产约为 {total_value:.2f} 元。"
        f" 当前最大单一基金权重约 {top_weight:.1f}%，"
        f"成长型暴露约 {growth_weight:.1f}%，防守型暴露约 {defensive_weight:.1f}%。"
    )

    recommended_next_actions = [
        "回看最大仓位，确认它是否承担了过多单一主题或风格风险。",
        "进入 Coach，把这份组合报告作为上下文继续追问配置逻辑。",
        "保留一份新的持仓快照，后续才能比较你的配置纪律是否在改善。",
    ]
    if cash_ratio >= 30:
        recommended_next_actions[0] = "先判断高现金占比是主动留白，还是因为还没有形成明确配置计划。"
    elif top_weight >= 25:
        recommended_next_actions[0] = "先检查第一大持仓是否过重，再判断是否需要讨论分散原则。"

    enhancement = enhance_portfolio_analysis_text(
        portfolio_context={
            "primary_goal": user.primary_goal,
            "total_value": _round_money(total_value),
            "cash_ratio": cash_ratio,
            "top_weight": top_weight,
            "top_two_weight": top_two_weight,
            "growth_weight": growth_weight,
            "balanced_weight": balanced_weight,
            "defensive_weight": defensive_weight,
            "holdings": holdings_with_weight,
        },
        summary=summary,
        risk_exposure=risk_exposure,
        concentration_flags=concentration_flags,
        allocation_balance=allocation_balance,
        recommended_next_actions=recommended_next_actions,
    )

    return PortfolioAnalysisResult(
        total_value=_round_money(total_value),
        holdings_with_weight=holdings_with_weight,
        summary=enhancement.summary or summary,
        risk_exposure=enhancement.risk_exposure or risk_exposure,
        concentration_flags=enhancement.concentration_flags or concentration_flags,
        allocation_balance=enhancement.allocation_balance or allocation_balance,
        recommended_next_actions=(
            enhancement.recommended_next_actions or recommended_next_actions
        ),
    )


def _serialize_report(
    snapshot: PortfolioSnapshot,
    analysis: PortfolioAnalysis,
    holdings: list[PortfolioHolding],
) -> PortfolioReportResponse:
    return PortfolioReportResponse(
        snapshot_id=snapshot.id,
        snapshot_date=snapshot.snapshot_date,
        total_value=_round_money(snapshot.total_value),
        cash_value=_round_money(snapshot.cash_value),
        summary=analysis.summary,
        risk_exposure=list(analysis.risk_exposure or []),
        concentration_flags=list(analysis.concentration_flags or []),
        allocation_balance=list(analysis.allocation_balance or []),
        recommended_next_actions=list(analysis.recommended_next_actions or []),
        holdings=[
            {
                "fund_code": holding.fund_code,
                "fund_name": holding.fund_name,
                "fund_type": holding.fund_type,
                "market_value": _round_money(holding.market_value),
                "weight": round(float(holding.weight), 1),
            }
            for holding in holdings
        ],
        generated_at=analysis.generated_at,
    )


def create_portfolio_snapshot(
    db: Session,
    *,
    user: UserProfile,
    payload: PortfolioSnapshotCreateRequest,
) -> PortfolioReportResponse:
    holdings_payload = [
        {
            "fund_code": holding.fund_code,
            "fund_name": holding.fund_name,
            "fund_type": holding.fund_type,
            "market_value": float(holding.market_value),
        }
        for holding in payload.holdings
    ]
    analysis_result = _analyze_snapshot(
        user=user,
        cash_value=float(payload.cash_value),
        holdings=holdings_payload,
    )
    now = datetime.now(timezone.utc)
    snapshot = PortfolioSnapshot(
        user_id=user.id,
        snapshot_date=payload.snapshot_date,
        cash_value=_round_money(payload.cash_value),
        total_value=analysis_result.total_value,
        created_at=now,
    )
    db.add(snapshot)
    db.flush()

    holdings: list[PortfolioHolding] = []
    for holding_payload in analysis_result.holdings_with_weight:
        holding = PortfolioHolding(
            snapshot_id=snapshot.id,
            fund_code=str(holding_payload["fund_code"]),
            fund_name=str(holding_payload["fund_name"]),
            fund_type=str(holding_payload["fund_type"]),
            market_value=_round_money(float(holding_payload["market_value"])),
            weight=round(float(holding_payload["weight"]), 1),
            created_at=now,
        )
        db.add(holding)
        holdings.append(holding)

    analysis = PortfolioAnalysis(
        user_id=user.id,
        snapshot_id=snapshot.id,
        analysis_version=PORTFOLIO_ANALYSIS_VERSION,
        summary=analysis_result.summary,
        risk_exposure=analysis_result.risk_exposure,
        concentration_flags=analysis_result.concentration_flags,
        allocation_balance=analysis_result.allocation_balance,
        recommended_next_actions=analysis_result.recommended_next_actions,
        total_value=analysis_result.total_value,
        generated_at=now,
    )
    db.add(analysis)
    db.commit()
    db.refresh(snapshot)
    db.refresh(analysis)
    return _serialize_report(snapshot, analysis, holdings)


def get_latest_portfolio_report(
    db: Session,
    *,
    user_id: str,
) -> PortfolioReportResponse | None:
    latest_row = db.execute(
        select(PortfolioSnapshot, PortfolioAnalysis)
        .join(PortfolioAnalysis, PortfolioAnalysis.snapshot_id == PortfolioSnapshot.id)
        .where(PortfolioSnapshot.user_id == user_id)
        .order_by(
            PortfolioAnalysis.generated_at.desc(),
            PortfolioSnapshot.created_at.desc(),
        )
    ).first()
    if latest_row is None:
        return None

    snapshot, analysis = latest_row
    holdings = list(
        db.scalars(
            select(PortfolioHolding)
            .where(PortfolioHolding.snapshot_id == snapshot.id)
            .order_by(PortfolioHolding.weight.desc(), PortfolioHolding.created_at.asc())
        )
    )
    return _serialize_report(snapshot, analysis, holdings)


def get_portfolio_history(
    db: Session,
    *,
    user_id: str,
) -> PortfolioHistoryResponse:
    rows = list(
        db.execute(
            select(PortfolioSnapshot, PortfolioAnalysis)
            .join(PortfolioAnalysis, PortfolioAnalysis.snapshot_id == PortfolioSnapshot.id)
            .where(PortfolioSnapshot.user_id == user_id)
            .order_by(
                PortfolioAnalysis.generated_at.desc(),
                PortfolioSnapshot.created_at.desc(),
            )
        )
    )
    return PortfolioHistoryResponse(
        items=[
            PortfolioHistoryItem(
                snapshot_id=snapshot.id,
                snapshot_date=snapshot.snapshot_date,
                total_value=_round_money(snapshot.total_value),
                summary=analysis.summary,
                generated_at=analysis.generated_at,
            )
            for snapshot, analysis in rows
        ]
    )


def get_portfolio_overview(
    db: Session,
    *,
    user_id: str,
) -> DashboardPortfolioStatus:
    latest = get_latest_portfolio_report(db, user_id=user_id)
    if latest is None:
        return DashboardPortfolioStatus(has_report=False)

    return DashboardPortfolioStatus(
        has_report=True,
        latest_snapshot_id=latest.snapshot_id,
        latest_snapshot_date=latest.snapshot_date,
        total_value=latest.total_value,
        summary=latest.summary,
    )
