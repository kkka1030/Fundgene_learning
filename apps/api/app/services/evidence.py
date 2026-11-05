from __future__ import annotations

from datetime import datetime, timezone
import re
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.course import Course
from app.models.course_section import CourseSection
from app.models.news_analysis import NewsAnalysis
from app.models.news_item import NewsItem
from app.models.policy_item import PolicyItem
from app.services.learning import ensure_learning_catalog


_ALIASES = {
    "drawdown": {"drawdown", "回撤", "下跌", "波动"},
    "risk": {"risk", "风险", "风险等级", "波动", "承受"},
    "allocation": {"allocation", "配置", "组合", "分散", "集中", "仓位"},
    "policy": {"policy", "政策", "监管", "利率", "宏观", "资金"},
    "news": {"news", "新闻", "资讯", "标题", "影响"},
}


@dataclass(slots=True)
class EvidenceHit:
    source_type: str
    source_id: str
    source_title: str
    snippet: str
    support_summary: str
    citation_key: str
    score: int
    published_at: datetime | None = None

    def as_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "source_type": self.source_type,
            "source_id": self.source_id,
            "source_title": self.source_title,
            "snippet": self.snippet,
            "support_summary": self.support_summary,
            "citation_key": self.citation_key,
            "score": self.score,
        }
        if self.published_at is not None:
            payload["published_at"] = self.published_at.isoformat()
        return payload


class EvidenceService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def search_learning(self, query: str, *, limit: int = 3) -> list[EvidenceHit]:
        ensure_learning_catalog(self.db)
        rows = list(
            self.db.execute(
                select(Course, CourseSection)
                .join(CourseSection, CourseSection.course_id == Course.id)
                .order_by(Course.position.asc(), CourseSection.position.asc())
            )
        )
        hits = [
            self._score_hit(
                source_type="course_section",
                source_id=f"{course.slug}:{section.slug}",
                source_title=f"{course.title} / {section.title}",
                text=f"{course.focus} {course.description} {section.summary}",
                query=query,
            )
            for course, section in rows
        ]
        return self._top_hits(hits, limit=limit)

    def search_news_policy(
        self,
        query: str,
        *,
        user_id: str,
        limit: int = 3,
    ) -> list[EvidenceHit]:
        if _is_broad_latest_news_query(query):
            return self._latest_news_policy_hits(user_id=user_id, limit=limit)

        hits: list[EvidenceHit] = []
        analyses = list(
            self.db.scalars(
                select(NewsAnalysis)
                .where(NewsAnalysis.user_id == user_id)
                .order_by(NewsAnalysis.generated_at.desc(), NewsAnalysis.id.desc())
                .limit(20)
            )
        )
        for analysis in analyses:
            title = "最近新闻/政策分析"
            if analysis.news_item_id:
                item = self.db.get(NewsItem, analysis.news_item_id)
                title = item.title if item is not None else title
            elif analysis.policy_item_id:
                item = self.db.get(PolicyItem, analysis.policy_item_id)
                title = item.title if item is not None else title
            text = " ".join(
                [
                    title,
                    analysis.beginner_translation,
                    " ".join(analysis.facts or []),
                    " ".join(analysis.impact_paths or []),
                    " ".join(analysis.uncertainty_notes or []),
                ]
            )
            hits.append(
                self._score_hit(
                    source_type="news_analysis",
                    source_id=analysis.id,
                    source_title=title,
                    text=text,
                    query=query,
                )
            )

        for item in self.db.scalars(select(PolicyItem).order_by(PolicyItem.fetched_at.desc()).limit(20)):
            hits.append(
                self._score_hit(
                    source_type="policy_item",
                    source_id=item.id,
                    source_title=item.title,
                    text=f"{item.title} {item.summary or ''} {item.policy_area or ''}",
                    query=query,
                )
            )

        for item in self.db.scalars(select(NewsItem).order_by(NewsItem.fetched_at.desc()).limit(20)):
            if item.user_id not in (None, user_id):
                continue
            hits.append(
                self._score_hit(
                    source_type="news_item",
                    source_id=item.id,
                    source_title=item.title,
                    text=f"{item.title} {item.summary or ''}",
                    query=query,
                )
            )

        return self._top_hits(hits, limit=limit)

    def _latest_news_policy_hits(
        self,
        *,
        user_id: str,
        limit: int,
    ) -> list[EvidenceHit]:
        hits: list[EvidenceHit] = []
        for item in self.db.scalars(
            select(PolicyItem)
            .order_by(PolicyItem.published_at.desc(), PolicyItem.fetched_at.desc())
            .limit(20)
        ):
            hits.append(
                self._score_hit(
                    source_type="policy_item",
                    source_id=item.id,
                    source_title=item.title,
                    text=f"{item.title} {item.summary or ''} {item.policy_area or ''}",
                    query="news policy",
                    published_at=item.published_at or item.fetched_at,
                )
            )

        for item in self.db.scalars(
            select(NewsItem)
            .order_by(NewsItem.published_at.desc(), NewsItem.fetched_at.desc())
            .limit(20)
        ):
            if item.user_id not in (None, user_id):
                continue
            hits.append(
                self._score_hit(
                    source_type="news_item",
                    source_id=item.id,
                    source_title=item.title,
                    text=f"{item.title} {item.summary or ''}",
                    query="news",
                    published_at=item.published_at or item.fetched_at,
                )
            )

        return sorted(hits, key=_published_timestamp, reverse=True)[:limit]

    def _score_hit(
        self,
        *,
        source_type: str,
        source_id: str,
        source_title: str,
        text: str,
        query: str,
        published_at: datetime | None = None,
    ) -> EvidenceHit:
        normalized_text = _normalize(text)
        terms = _query_terms(query)
        score = 0
        for term in terms:
            if term and term in normalized_text:
                score += 3 if len(term) > 1 else 1
        if source_title and _normalize(source_title) in normalized_text:
            score += 1

        snippet = _trim(text, limit=220)
        return EvidenceHit(
            source_type=source_type,
            source_id=source_id,
            source_title=source_title,
            snippet=snippet,
            support_summary=f"{source_title} 提供了与本次问题相关的解释证据。",
            citation_key=f"{source_type}:{source_id}",
            score=score,
            published_at=published_at,
        )

    def _top_hits(self, hits: list[EvidenceHit], *, limit: int) -> list[EvidenceHit]:
        ranked = sorted(hits, key=lambda item: (item.score, item.source_title), reverse=True)
        positive = [hit for hit in ranked if hit.score > 0]
        return (positive or ranked)[:limit]


def _query_terms(query: str) -> set[str]:
    normalized = _normalize(query)
    terms = {term for term in re.split(r"[^0-9a-zA-Z\u4e00-\u9fff]+", normalized) if term}
    for alias_terms in _ALIASES.values():
        if any(_normalize(term) in normalized for term in alias_terms):
            terms.update(_normalize(term) for term in alias_terms)
    return {term for term in terms if term}


def _is_broad_latest_news_query(query: str) -> bool:
    normalized = _normalize(query)
    has_news_word = any(term in normalized for term in ("新闻", "资讯", "news"))
    asks_latest = any(term in normalized for term in ("今天", "今日", "最近", "最新", "有什么", "哪些"))
    return has_news_word and asks_latest


def _published_timestamp(hit: EvidenceHit) -> float:
    value = hit.published_at
    if value is None:
        return 0
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


def _normalize(value: str) -> str:
    return " ".join(str(value or "").lower().split())


def _trim(value: str, *, limit: int) -> str:
    normalized = " ".join(str(value or "").strip().split())
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[:limit].rstrip()}..."
