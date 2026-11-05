from __future__ import annotations

import asyncio
import hashlib
import html
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

import feedparser
import httpx
from fastapi import HTTPException, status
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.agent_citation import AgentCitation
from app.models.news_analysis import NewsAnalysis
from app.models.news_item import NewsItem
from app.models.policy_item import PolicyItem
from app.models.user import UserProfile
from app.schemas.dashboard import DashboardNewsStatus
from app.schemas.news import (
    AgentCitationResponse,
    NewsAgentProcessStep,
    NewsAnalysisResponse,
    NewsAnalyzeRequest,
    NewsItemDetailResponse,
    NewsItemSummary,
    NewsListResponse,
    NewsItemType,
)
from app.services.ai_enhancement import enhance_news_analysis_payload

NEWS_ANALYSIS_VERSION = "news_policy_analysis_v1"
CHINA_NEWS_TIMEZONE = timezone(timedelta(hours=8))
NEWS_RISK_NOTICE = (
    "FundGene 对新闻和政策的解读只用于学习与决策支持，不构成收益承诺、买卖建议或交易指令。"
)
BEGINNER_TRANSLATION_PROMPT_PREFIXES = (
    "把这条政策先翻译成一句新手能执行的话：",
    "把这条新闻先翻译成一句新手能执行的话：",
)
FEED_REQUEST_HEADERS = {
    "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
    "User-Agent": (
        "FundGene/0.1 local research prototype "
        "(beginner fund education; contact: demo@fundgene.local)"
    ),
}
POLICY_KEYWORDS = {
    "policy",
    "regulation",
    "regulatory",
    "rule",
    "federal reserve",
    "treasury",
    "sec",
    "interest rate",
    "monetary",
    "fiscal",
    "监管",
    "政策",
    "利率",
    "降息",
    "加息",
    "央行",
    "财政",
}


def _trim(value: str, *, limit: int) -> str:
    normalized = " ".join(value.strip().split())
    if len(normalized) <= limit:
        return normalized
    return normalized[:limit].rstrip()


def _display_beginner_translation(value: str) -> str:
    normalized = " ".join(value.strip().split())
    for prefix in BEGINNER_TRANSLATION_PROMPT_PREFIXES:
        if normalized.startswith(prefix):
            return normalized.removeprefix(prefix).strip()
    return normalized


def _news_agent_process_steps(analysis: NewsAnalysis) -> list[NewsAgentProcessStep]:
    model_label = analysis.model_name or "DeepSeek"
    model_status = analysis.model_status or "legacy"
    model_step_status = "completed" if model_status == "enhanced" else "warning"
    model_detail = (
        f"{model_label} 已完成结构化解读。"
        if model_status == "enhanced"
        else (
            "本次没有拿到可用模型增强结果，页面展示的是规则兜底分析。"
            if model_status in {"fallback", "skipped"}
            else "这条历史解读缺少模型运行元数据，建议重新生成以确认 DeepSeek 是否参与。"
        )
    )
    if analysis.fallback_reason:
        model_detail = f"{model_detail} 原因：{analysis.fallback_reason}。"

    return [
        NewsAgentProcessStep(
            key="read_source",
            label="读取真实资讯来源",
            status="completed",
            detail="已读取标题、摘要、发布时间和来源链接。",
        ),
        NewsAgentProcessStep(
            key="split_facts",
            label="拆分新闻事实",
            status="completed",
            detail="已把标题和摘要拆成事实、影响对象和不确定性。",
        ),
        NewsAgentProcessStep(
            key="deepseek_enhancement",
            label="调用 DeepSeek 生成解读",
            status=model_step_status,  # type: ignore[arg-type]
            detail=model_detail,
        ),
        NewsAgentProcessStep(
            key="safety_guard",
            label="检查安全边界",
            status="completed",
            detail="已过滤收益承诺、买卖指令、满仓/清仓等不适合新手的表达。",
        ),
        NewsAgentProcessStep(
            key="persist_result",
            label="保存分析结果",
            status="completed",
            detail="已保存为可被 Today、Agent 和自动任务引用的 news analysis。",
        ),
    ]


def _clean_text(value: Any, *, limit: int | None = None) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = " ".join(text.strip().split())
    if limit is not None:
        return _trim(text, limit=limit)
    return text


def _stable_external_id(entry: dict[str, Any], *, fallback: str) -> str:
    raw = str(
        entry.get("id")
        or entry.get("guid")
        or entry.get("link")
        or entry.get("title")
        or fallback
    ).strip()
    if len(raw) <= 512:
        return raw
    return f"sha256:{hashlib.sha256(raw.encode('utf-8')).hexdigest()}"


def _source_name(parsed_feed: Any, *, feed_url: str) -> str:
    feed_title = getattr(parsed_feed, "feed", {}).get("title") if parsed_feed else None
    if feed_title:
        return _clean_text(feed_title, limit=160)
    parsed_url = urlparse(feed_url)
    return parsed_url.netloc or "Curated feed"


def _entry_datetime(entry: dict[str, Any]) -> datetime | None:
    parsed_time = entry.get("published_parsed") or entry.get("updated_parsed")
    if parsed_time is None:
        return None
    return datetime(*parsed_time[:6], tzinfo=timezone.utc)


def _item_type_for_entry(
    *,
    source_name: str,
    title: str,
    summary: str | None,
) -> NewsItemType:
    haystack = f"{source_name} {title} {summary or ''}".lower()
    if any(keyword in haystack for keyword in POLICY_KEYWORDS):
        return "policy"
    return "news"


def _policy_area(title: str, summary: str | None) -> str:
    haystack = f"{title} {summary or ''}".lower()
    if any(keyword in haystack for keyword in ("rate", "interest", "monetary", "利率", "降息", "加息")):
        return "monetary_policy"
    if any(keyword in haystack for keyword in ("sec", "regulation", "rule", "监管", "规则")):
        return "regulatory_policy"
    if any(keyword in haystack for keyword in ("treasury", "fiscal", "tax", "财政", "税")):
        return "fiscal_policy"
    return "general_policy"


def _first_sentence(text: str | None) -> str | None:
    if not text:
        return None
    parts = re.split(r"(?<=[。.!?])\s+", text)
    return _trim(parts[0], limit=180) if parts else _trim(text, limit=180)


def _upsert_news_item(
    db: Session,
    *,
    user_id: str | None = None,
    source_name: str,
    source_url: str,
    external_id: str,
    title: str,
    summary: str | None,
    url: str,
    published_at: datetime | None,
    fetched_at: datetime,
) -> NewsItem:
    item = db.scalar(
        select(NewsItem).where(
            NewsItem.source_url == source_url,
            NewsItem.external_id == external_id,
        )
    )
    if item is None:
        item = NewsItem(
            user_id=user_id,
            source_name=source_name,
            source_url=source_url,
            external_id=external_id,
            title=title,
            summary=summary,
            url=url,
            published_at=published_at,
            fetched_at=fetched_at,
        )
        db.add(item)
        db.flush()
        return item

    item.user_id = user_id
    item.source_name = source_name
    item.title = title
    item.summary = summary
    item.url = url
    item.published_at = published_at
    item.fetched_at = fetched_at
    db.add(item)
    return item


def _upsert_policy_item(
    db: Session,
    *,
    source_name: str,
    source_url: str,
    external_id: str,
    title: str,
    summary: str | None,
    url: str,
    published_at: datetime | None,
    fetched_at: datetime,
) -> PolicyItem:
    item = db.scalar(
        select(PolicyItem).where(
            PolicyItem.source_url == source_url,
            PolicyItem.external_id == external_id,
        )
    )
    if item is None:
        item = PolicyItem(
            source_name=source_name,
            source_url=source_url,
            external_id=external_id,
            title=title,
            summary=summary,
            url=url,
            policy_area=_policy_area(title, summary),
            published_at=published_at,
            fetched_at=fetched_at,
        )
        db.add(item)
        db.flush()
        return item

    item.source_name = source_name
    item.title = title
    item.summary = summary
    item.url = url
    item.policy_area = _policy_area(title, summary)
    item.published_at = published_at
    item.fetched_at = fetched_at
    db.add(item)
    return item


def ingest_feed_document(
    db: Session,
    *,
    feed_url: str,
    content: str,
    limit_per_feed: int = 20,
) -> list[NewsItem | PolicyItem]:
    parsed = feedparser.parse(content)
    source_name = _source_name(parsed, feed_url=feed_url)
    fetched_at = datetime.now(timezone.utc)
    items: list[NewsItem | PolicyItem] = []

    for index, entry in enumerate(parsed.entries[:limit_per_feed]):
        title = _clean_text(entry.get("title"), limit=300)
        if not title:
            continue
        summary = _clean_text(
            entry.get("summary") or entry.get("description"),
            limit=2000,
        )
        summary = summary or None
        url = _trim(str(entry.get("link") or feed_url), limit=1000)
        external_id = _stable_external_id(
            entry,
            fallback=f"{feed_url}:{index}:{title}",
        )
        published_at = _entry_datetime(entry)
        item_type = _item_type_for_entry(
            source_name=source_name,
            title=title,
            summary=summary,
        )

        if item_type == "policy":
            item = _upsert_policy_item(
                db,
                source_name=source_name,
                source_url=_trim(feed_url, limit=500),
                external_id=external_id,
                title=title,
                summary=summary,
                url=url,
                published_at=published_at,
                fetched_at=fetched_at,
            )
        else:
            item = _upsert_news_item(
                db,
                user_id=None,
                source_name=source_name,
                source_url=_trim(feed_url, limit=500),
                external_id=external_id,
                title=title,
                summary=summary,
                url=url,
                published_at=published_at,
                fetched_at=fetched_at,
            )
        items.append(item)

    db.commit()
    return items


async def refresh_news_feeds(
    db: Session,
    *,
    feed_urls: list[str] | None = None,
    limit_per_feed: int = 20,
) -> list[NewsItem | PolicyItem]:
    urls = feed_urls or get_settings().news_feeds
    ingested: list[NewsItem | PolicyItem] = []
    async with httpx.AsyncClient(
        headers=FEED_REQUEST_HEADERS,
        timeout=10.0,
        follow_redirects=True,
        trust_env=True,
    ) as client:
        responses = await asyncio.gather(
            *(client.get(feed_url) for feed_url in urls),
            return_exceptions=True,
        )

    for feed_url, response in zip(urls, responses, strict=False):
        if isinstance(response, Exception):
            continue
        try:
            response.raise_for_status()
        except httpx.HTTPError:
            continue
        ingested.extend(
            ingest_feed_document(
                db,
                feed_url=feed_url,
                content=response.text,
                limit_per_feed=limit_per_feed,
            )
        )
    return ingested


def refresh_news_feeds_sync(
    db: Session,
    *,
    feed_urls: list[str] | None = None,
    limit_per_feed: int = 20,
) -> list[NewsItem | PolicyItem]:
    urls = feed_urls or get_settings().news_feeds
    ingested: list[NewsItem | PolicyItem] = []
    with httpx.Client(
        headers=FEED_REQUEST_HEADERS,
        timeout=10.0,
        follow_redirects=True,
        trust_env=True,
    ) as client:
        for feed_url in urls:
            try:
                response = client.get(feed_url)
                response.raise_for_status()
            except httpx.HTTPError:
                continue
            ingested.extend(
                ingest_feed_document(
                    db,
                    feed_url=feed_url,
                    content=response.text,
                    limit_per_feed=limit_per_feed,
                )
            )
    return ingested


def _manual_external_id(*, user_id: str, headline: str, body: str) -> str:
    digest = hashlib.sha256(
        f"{user_id}\0{headline}\0{body}".encode("utf-8")
    ).hexdigest()
    return f"manual:{digest}"


def _upsert_manual_news_item(
    db: Session,
    *,
    user: UserProfile,
    headline: str,
    body: str,
) -> NewsItem:
    return _upsert_news_item(
        db,
        user_id=user.id,
        source_name="用户粘贴内容",
        source_url=_trim(f"manual://fundgene/users/{user.id}", limit=500),
        external_id=_manual_external_id(
            user_id=user.id,
            headline=headline,
            body=body,
        ),
        title=_trim(headline, limit=300),
        summary=_clean_text(body, limit=2000),
        url="",
        published_at=None,
        fetched_at=datetime.now(timezone.utc),
    )


def _latest_analysis_id_for_item(
    db: Session,
    *,
    user_id: str,
    item_id: str,
    item_type: NewsItemType,
) -> str | None:
    item_filter = (
        NewsAnalysis.news_item_id == item_id
        if item_type == "news"
        else NewsAnalysis.policy_item_id == item_id
    )
    analysis = db.scalar(
        select(NewsAnalysis)
        .where(NewsAnalysis.user_id == user_id, item_filter)
        .order_by(NewsAnalysis.generated_at.desc(), NewsAnalysis.id.desc())
    )
    return analysis.id if analysis is not None else None


def _serialize_item(
    db: Session,
    *,
    user_id: str,
    item: NewsItem | PolicyItem,
    item_type: NewsItemType,
) -> NewsItemSummary:
    return NewsItemSummary(
        id=item.id,
        item_type=item_type,
        title=item.title,
        summary=item.summary,
        url=item.url,
        source_name=item.source_name,
        source_url=item.source_url,
        published_at=item.published_at,
        fetched_at=item.fetched_at,
        latest_analysis_id=_latest_analysis_id_for_item(
            db,
            user_id=user_id,
            item_id=item.id,
            item_type=item_type,
        ),
    )


def _sort_timestamp(item: NewsItem | PolicyItem) -> datetime:
    return item.published_at or item.fetched_at


def _sort_date(item: NewsItem | PolicyItem) -> date:
    return _sort_timestamp(item).astimezone(CHINA_NEWS_TIMEZONE).date()


def _latest_daily_rows(
    rows: list[tuple[NewsItemType, NewsItem | PolicyItem]],
) -> list[tuple[NewsItemType, NewsItem | PolicyItem]]:
    if not rows:
        return rows

    latest_date = max(_sort_date(item) for _, item in rows)
    return [row for row in rows if _sort_date(row[1]) == latest_date]


def _is_demo_fixture_item(item: NewsItem | PolicyItem) -> bool:
    return (item.source_name or "").strip().lower() == "fundgene dev fixture"


def _prefer_real_items(
    rows: list[tuple[NewsItemType, NewsItem | PolicyItem]],
) -> list[tuple[NewsItemType, NewsItem | PolicyItem]]:
    real_rows = [row for row in rows if not _is_demo_fixture_item(row[1])]
    return real_rows or rows


def list_news_items(
    db: Session,
    *,
    user_id: str,
    limit: int = 20,
) -> NewsListResponse:
    candidate_limit = max(limit * 6, 100)
    news_items = list(
        db.scalars(
            select(NewsItem)
            .where(or_(NewsItem.user_id.is_(None), NewsItem.user_id == user_id))
            .order_by(NewsItem.fetched_at.desc())
            .limit(candidate_limit)
        )
    )
    policy_items = list(
        db.scalars(
            select(PolicyItem)
            .order_by(PolicyItem.fetched_at.desc())
            .limit(candidate_limit)
        )
    )
    combined: list[tuple[NewsItemType, NewsItem | PolicyItem]] = [
        ("news", item) for item in news_items
    ] + [("policy", item) for item in policy_items]
    combined.sort(key=lambda row: _sort_timestamp(row[1]), reverse=True)
    latest_daily_combined = _latest_daily_rows(_prefer_real_items(combined))

    return NewsListResponse(
        items=[
            _serialize_item(db, user_id=user_id, item=item, item_type=item_type)
            for item_type, item in latest_daily_combined[:limit]
        ]
    )


def _get_item(
    db: Session,
    *,
    item_id: str,
    user_id: str | None = None,
    item_type: NewsItemType | None = None,
) -> tuple[NewsItemType, NewsItem | PolicyItem]:
    if item_type in (None, "news"):
        news_item = db.get(NewsItem, item_id)
        if news_item is not None and (
            news_item.user_id is None or news_item.user_id == user_id
        ):
            return "news", news_item
    if item_type in (None, "policy"):
        policy_item = db.get(PolicyItem, item_id)
        if policy_item is not None:
            return "policy", policy_item

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Requested news or policy item does not exist.",
    )


def get_news_item_detail(
    db: Session,
    *,
    user_id: str,
    item_id: str,
) -> NewsItemDetailResponse:
    item_type, item = _get_item(db, item_id=item_id, user_id=user_id)
    return NewsItemDetailResponse(
        item=_serialize_item(db, user_id=user_id, item=item, item_type=item_type)
    )


def _build_rule_analysis(
    *,
    item: NewsItem | PolicyItem,
    item_type: NewsItemType,
) -> dict[str, list[str] | str]:
    title = item.title
    summary_sentence = _first_sentence(item.summary)
    lower_text = f"{title} {item.summary or ''}".lower()
    label = "政策" if item_type == "policy" else "新闻"

    facts = [
        f"{item.source_name} 发布或更新了一条{label}信息。",
        f"标题是：{title}",
    ]
    if item.published_at is not None:
        facts.append(f"发布时间可追溯到 {item.published_at.date().isoformat()}。")
    if summary_sentence:
        facts.append(f"摘要要点：{summary_sentence}")

    impact_paths = [
        "先判断它影响的是基金底层资产、市场情绪、资金成本，还是产品规则本身。",
    ]
    if any(keyword in lower_text for keyword in ("rate", "interest", "fed", "利率", "降息", "加息")):
        impact_paths.append(
            "利率或货币政策信息通常会先影响债券价格、估值折现和市场风险偏好，再间接影响基金净值。"
        )
    if any(keyword in lower_text for keyword in ("sec", "regulation", "rule", "监管", "规则")):
        impact_paths.append(
            "监管规则信息更可能改变产品运作、披露或合规边界，需要先看它是否直接涉及你持有的基金类型。"
        )
    if any(keyword in lower_text for keyword in ("inflation", "jobs", "gdp", "macro", "通胀", "就业", "宏观")):
        impact_paths.append(
            "宏观数据通常通过增长预期、通胀预期和风险偏好传导，不能直接等同于某只基金的短期方向。"
        )
    if len(impact_paths) == 1:
        impact_paths.append(
            "如果它只改变短期情绪而没有改变基金底层资产逻辑，就不应该直接触发买卖动作。"
        )

    uncertainty_notes = [
        "单条新闻无法直接证明一只基金应该买入、卖出或长期持有。",
        "政策从发布到落地存在时间差，实际影响还取决于执行细节和市场预期是否已经提前反映。",
        "同一信息对权益基金、债券基金、货币基金和混合基金的影响方向可能不同。",
    ]

    beginner_translation = (
        f"它提醒你关注“{title}”背后的风险来源；下一步先核对自己的基金类型、"
        "持有期限和组合暴露，而不是被标题直接推着操作。"
    )
    related_learning_topics = ["fund-basics", "risk-and-drawdown"]
    if item_type == "policy":
        related_learning_topics.append("allocation-principles")

    recommended_next_actions = [
        "先标记这条信息属于宏观、政策、产品规则还是市场情绪，不要直接跳到买卖结论。",
        "回到 Portfolio，检查最近组合是否真的暴露在这条信息影响的资产或主题上。",
        "如果还不确定影响路径，进入 Coach 继续追问“这条信息通过什么路径影响我的基金”。",
    ]

    return {
        "facts": facts,
        "impact_paths": impact_paths,
        "uncertainty_notes": uncertainty_notes,
        "beginner_translation": beginner_translation,
        "related_learning_topics": related_learning_topics,
        "recommended_next_actions": recommended_next_actions,
        "risk_notice": NEWS_RISK_NOTICE,
    }


def _serialize_citation(citation: AgentCitation) -> AgentCitationResponse:
    return AgentCitationResponse(
        id=citation.id,
        source_type=citation.source_type,  # type: ignore[arg-type]
        source_item_id=citation.source_item_id,
        source_name=citation.source_name,
        title=citation.title,
        url=citation.url,
    )


def _list_citations(db: Session, *, analysis_id: str) -> list[AgentCitation]:
    return list(
        db.scalars(
            select(AgentCitation)
            .where(AgentCitation.analysis_id == analysis_id)
            .order_by(AgentCitation.created_at.asc(), AgentCitation.id.asc())
        )
    )


def _analysis_item(
    db: Session,
    *,
    analysis: NewsAnalysis,
) -> tuple[NewsItemType, NewsItem | PolicyItem]:
    if analysis.news_item_id is not None:
        item = db.get(NewsItem, analysis.news_item_id)
        if item is not None:
            return "news", item
    if analysis.policy_item_id is not None:
        item = db.get(PolicyItem, analysis.policy_item_id)
        if item is not None:
            return "policy", item
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="News analysis source item is unavailable.",
    )


def _serialize_analysis(
    db: Session,
    *,
    user_id: str,
    analysis: NewsAnalysis,
) -> NewsAnalysisResponse:
    item_type, item = _analysis_item(db, analysis=analysis)
    return NewsAnalysisResponse(
        id=analysis.id,
        item=_serialize_item(db, user_id=user_id, item=item, item_type=item_type),
        facts=list(analysis.facts),
        impact_paths=list(analysis.impact_paths),
        uncertainty_notes=list(analysis.uncertainty_notes),
        beginner_translation=_display_beginner_translation(analysis.beginner_translation),
        related_learning_topics=list(analysis.related_learning_topics),
        recommended_next_actions=list(analysis.recommended_next_actions),
        risk_notice=analysis.risk_notice,
        citations=[
            _serialize_citation(citation)
            for citation in _list_citations(db, analysis_id=analysis.id)
        ],
        model_status=analysis.model_status or "legacy",  # type: ignore[arg-type]
        model_provider=analysis.model_provider,
        model_name=analysis.model_name,
        fallback_reason=analysis.fallback_reason,
        agent_process=_news_agent_process_steps(analysis),
        generated_at=analysis.generated_at,
    )


def create_news_analysis(
    db: Session,
    *,
    user: UserProfile,
    payload: NewsAnalyzeRequest,
) -> NewsAnalysisResponse:
    from app.services.profile import get_effective_llm_config

    if payload.item_id is not None:
        item_type, item = _get_item(
            db,
            item_id=payload.item_id,
            user_id=user.id,
            item_type=payload.item_type,
        )
    else:
        item_type = "news"
        item = _upsert_manual_news_item(
            db,
            user=user,
            headline=payload.headline or "",
            body=payload.body or "",
        )
    rule_analysis_payload = _build_rule_analysis(item=item, item_type=item_type)
    effective_model_name, deepseek_api_key_override, _llm_source = (
        get_effective_llm_config(db, user=user)
    )
    enhancement = enhance_news_analysis_payload(
        item_context={
            "item_type": item_type,
            "source_name": item.source_name,
            "title": item.title,
            "summary": item.summary,
            "published_at": (
                item.published_at.isoformat() if item.published_at is not None else None
            ),
            "url": item.url,
        },
        rule_payload=rule_analysis_payload,
        configured_model_name=effective_model_name,
        deepseek_api_key_override=deepseek_api_key_override,
    )
    analysis_payload = enhancement.payload or rule_analysis_payload
    now = datetime.now(timezone.utc)
    analysis = NewsAnalysis(
        user_id=user.id,
        news_item_id=item.id if item_type == "news" else None,
        policy_item_id=item.id if item_type == "policy" else None,
        analysis_version=NEWS_ANALYSIS_VERSION,
        facts=analysis_payload["facts"],
        impact_paths=analysis_payload["impact_paths"],
        uncertainty_notes=analysis_payload["uncertainty_notes"],
        beginner_translation=str(analysis_payload["beginner_translation"]),
        related_learning_topics=analysis_payload["related_learning_topics"],
        recommended_next_actions=analysis_payload["recommended_next_actions"],
        risk_notice=str(analysis_payload["risk_notice"]),
        model_status=enhancement.status,
        model_provider=(
            enhancement.model_info.provider if enhancement.model_info is not None else None
        ),
        model_name=(
            enhancement.model_info.configured_model_name
            if enhancement.model_info is not None
            else None
        ),
        fallback_reason=enhancement.fallback_reason,
        generated_at=now,
    )
    db.add(analysis)
    db.flush()

    citation = AgentCitation(
        analysis_id=analysis.id,
        source_type=item_type,
        source_item_id=item.id,
        source_name=item.source_name,
        title=item.title,
        url=item.url,
        created_at=now,
    )
    db.add(citation)
    db.commit()
    db.refresh(analysis)
    return _serialize_analysis(db, user_id=user.id, analysis=analysis)


def get_news_analysis(
    db: Session,
    *,
    user: UserProfile,
    analysis_id: str,
) -> NewsAnalysisResponse:
    analysis = db.scalar(
        select(NewsAnalysis).where(
            NewsAnalysis.id == analysis_id,
            NewsAnalysis.user_id == user.id,
        )
    )
    if analysis is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requested news analysis does not exist for the current user.",
        )
    return _serialize_analysis(db, user_id=user.id, analysis=analysis)


def get_news_overview(
    db: Session,
    *,
    user_id: str,
) -> DashboardNewsStatus:
    analyses = list(
        db.scalars(
            select(NewsAnalysis)
            .where(NewsAnalysis.user_id == user_id)
            .order_by(NewsAnalysis.generated_at.desc(), NewsAnalysis.id.desc())
            .limit(12)
        )
    )
    if not analyses:
        return DashboardNewsStatus(has_analysis=False)

    analysis: NewsAnalysis | None = None
    item_type: NewsItemType | None = None
    item: NewsItem | PolicyItem | None = None
    for candidate in analyses:
        candidate_item_type, candidate_item = _analysis_item(db, analysis=candidate)
        if _is_demo_fixture_item(candidate_item):
            continue
        analysis = candidate
        item_type = candidate_item_type
        item = candidate_item
        break

    if analysis is None or item_type is None or item is None:
        return DashboardNewsStatus(has_analysis=False)

    recommended_actions = list(analysis.recommended_next_actions or [])
    return DashboardNewsStatus(
        has_analysis=True,
        latest_analysis_id=analysis.id,
        latest_item_id=item.id,
        latest_item_type=item_type,
        latest_title=item.title,
        latest_summary=item.summary,
        source_name=item.source_name,
        beginner_translation=_display_beginner_translation(analysis.beginner_translation),
        recommended_action=recommended_actions[0] if recommended_actions else None,
        generated_at=analysis.generated_at,
    )
