"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useRef, useState } from "react";
import {
  AlertTriangle,
  ArrowRight,
  Bookmark,
  CheckCircle2,
  RefreshCw,
  Search,
  TrendingUp,
} from "lucide-react";

import {
  ApiError,
  analyzeNews,
  getNewsCatalog,
  getNewsAnalysis,
  getSessionUser,
  type NewsAnalysis,
  type NewsCatalogState,
  type NewsItem,
} from "@/lib/api";
import {
  formatNewsCategory,
  formatProductCopy,
  formatReferenceLabel,
  formatSourceLabel,
} from "@/lib/display-labels";
import { buildAgentPromptHref } from "@/lib/navigation";
import { SectionBlock } from "./section-block";

const interpretationFrames = [
  {
    label: "事实",
    title: "先确认发生了什么",
    detail: "把标题情绪拆成事实、来源、时间和涉及范围。",
  },
  {
    label: "路径",
    title: "再看影响路径",
    detail: "区分宏观、政策、行业和基金组合层面的影响链。",
  },
  {
    label: "不确定性",
    title: "最后标出不确定性",
    detail: "不把一条新闻直接翻译成买卖动作。",
  },
];

const visibleNewsLimit = 7;
const newsFilters = ["全部", "政策", "市场", "基金"] as const;
const newsCatalogQueryKey = ["news-catalog"] as const;
const newsCatalogStaleTimeMs = 5 * 60 * 1000;
const newsCatalogGcTimeMs = 30 * 60 * 1000;

const emptyImpactPathStages = [
  {
    label: "新闻事实",
    title: "选择一条真实资讯开始",
    detail: "这里会展示来源标题、摘要、发布时间和链接，不把空模板当成分析结果。",
  },
  {
    label: "市场路径",
    title: "再看它可能通过哪里传导",
    detail: "系统会把政策、宏观、行业和基金类型的影响链拆开。",
  },
  {
    label: "我的组合暴露",
    title: "检查它和我的持仓有什么关系",
    detail: "只提示需要核对的资产、行业或基金类型，不直接变成买卖动作。",
  },
  {
    label: "我的行为风险",
    title: "识别我可能被哪种情绪带走",
    detail: "追热点、恐慌、过度确认等行为风险会和不确定性一起展示。",
  },
  {
    label: "安全动作",
    title: "最后落到可执行但克制的下一步",
    detail: "安全动作优先是学习、复盘、核对组合或继续追问。",
  },
];

const portfolioExposurePattern =
  /组合|持仓|基金|权益|债|固收|货币|行业|主题|资产|久期|利率|汇率|海外|指数|仓位|配置|暴露/;
const behaviorRiskPattern =
  /情绪|追|恐慌|卖出|买入|满仓|清仓|冲动|确认|波动|短期|热点|亏损|回撤|贪婪|焦虑|纪律/;

function isApiError(error: unknown): error is ApiError {
  return error instanceof ApiError;
}

function isPlannedEndpointError(error: unknown): boolean {
  return (
    isApiError(error) &&
    (error.status === 404 || error.status === 405 || error.status === 501)
  );
}

function formatDate(value: string | null): string {
  if (!value) {
    return "待标注";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("zh-CN", {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function buildImpactSummary(item: NewsItem): string {
  const areas = item.impactAreas.length > 0 ? item.impactAreas : item.tags;
  if (areas.length === 0) {
    return item.itemType === "policy" ? "政策影响路径" : "市场信息路径";
  }

  return areas.slice(0, 3).join(" / ");
}

function formatRelevanceLabel(score: number | null): string {
  if (score === null) {
    return "待判断";
  }
  if (score >= 80) {
    return "高度相关";
  }
  if (score >= 55) {
    return "中等相关";
  }
  return "较低相关";
}

function getRelevanceTone(score: number | null): string {
  if (score === null) {
    return "news-relevance-neutral";
  }
  if (score >= 80) {
    return "news-relevance-high";
  }
  if (score >= 55) {
    return "news-relevance-mid";
  }
  return "news-relevance-low";
}

function itemMatchesFilter(item: NewsItem, filter: (typeof newsFilters)[number]) {
  if (filter === "全部") {
    return true;
  }
  const haystack = [
    item.itemType,
    item.category,
    item.title,
    item.summary,
    ...item.tags,
    ...item.impactAreas,
  ]
    .join(" ")
    .toLowerCase();
  if (filter === "政策") {
    return item.itemType === "policy" || haystack.includes("policy") || haystack.includes("政策");
  }
  if (filter === "基金") {
    return haystack.includes("基金") || haystack.includes("fund");
  }
  return haystack.includes("市场") || haystack.includes("market") || item.itemType === "news";
}

function itemMatchesSearch(item: NewsItem, query: string) {
  const normalized = query.trim().toLowerCase();
  if (!normalized) {
    return true;
  }
  return [
    item.title,
    item.summary,
    item.source ?? "",
    item.category ?? "",
    ...item.tags,
    ...item.impactAreas,
  ]
    .join(" ")
    .toLowerCase()
    .includes(normalized);
}

function sourceCoverageBuckets(items: NewsItem[]) {
  const sources = new Set(items.map((item) => item.source).filter(Boolean));
  const official = items.filter(
    (item) =>
      item.itemType === "policy" ||
      /sec|fed|央行|财政|监管|政府|政策/i.test(`${item.source ?? ""} ${item.title}`),
  ).length;
  const finance = items.filter((item) =>
    /证券|财经|财新|经济|finance|market/i.test(`${item.source ?? ""} ${item.title}`),
  ).length;
  const fund = items.filter((item) =>
    /基金|fund|etf|公募/i.test(`${item.source ?? ""} ${item.title}`),
  ).length;

  return {
    totalSources: Math.max(sources.size, items.length > 0 ? 1 : 0),
    official,
    finance,
    fund,
  };
}

function modelStatusLabel(analysis: NewsAnalysis): string {
  if (analysis.modelStatus === "enhanced") {
    return "已生成解读";
  }
  if (analysis.modelStatus === "fallback") {
    return "模型未完成";
  }
  if (analysis.modelStatus === "skipped") {
    return "模型未配置";
  }
  return "历史解读";
}

function modelStatusTone(analysis: NewsAnalysis): string {
  if (analysis.modelStatus === "enhanced") {
    return "border-[rgba(52,199,89,0.28)] bg-[rgba(52,199,89,0.1)] text-[color:var(--accent-moss)]";
  }
  return "border-[rgba(255,159,10,0.28)] bg-[rgba(255,159,10,0.1)] text-[color:var(--accent-clay)]";
}

function isExternalHttpUrl(value: string | null): value is string {
  return value !== null && /^https?:\/\//.test(value);
}

function formatBeginnerSummary(value: string | null | undefined): string {
  if (!value) {
    return "这条资讯还没有摘要，先从标题和来源判断是否需要解读。";
  }

  return formatProductCopy(value);
}

function applyAnalysisToCatalog(
  catalog: NewsCatalogState | undefined,
  analysis: NewsAnalysis,
): NewsCatalogState | undefined {
  if (!catalog || !analysis.itemId || !analysis.id) {
    return catalog;
  }

  const markItem = (item: NewsItem): NewsItem =>
    item.id === analysis.itemId
      ? {
          ...(analysis.item ?? item),
          latestAnalysisId: analysis.id,
        }
      : item;

  return {
    ...catalog,
    items: catalog.items.map(markItem),
    policyItems: catalog.policyItems.map(markItem),
  };
}

function NewsCard({
  item,
  active,
  disabled,
  onSelect,
  onAnalyze,
}: {
  item: NewsItem;
  active: boolean;
  disabled: boolean;
  onSelect: (item: NewsItem) => void;
  onAnalyze: (item: NewsItem) => void;
}) {
  function handleSelect() {
    onSelect(item);
  }

  return (
    <article
      className={`news-list-item transition duration-200 ${
        active
          ? "news-list-item-active"
          : ""
      }`}
    >
      <button
        type="button"
        aria-pressed={active}
        aria-label={`查看资讯：${item.title}`}
        className="news-list-select"
        onClick={handleSelect}
      >
        <span className="flex items-start justify-between gap-3">
          <span className="min-w-0">
            <span className="flex min-w-0 items-center gap-2">
              <span className="grid size-5 flex-none place-items-center rounded-full bg-[rgba(0,113,227,0.1)] text-[0.62rem] font-bold text-[color:var(--accent-teal)]">
                {formatSourceLabel(item.source).slice(0, 1)}
              </span>
              <span className="truncate text-xs font-semibold text-[color:var(--ink-muted)]">
                {formatSourceLabel(item.source)}
              </span>
            </span>
            <span className="news-list-title mt-2 line-clamp-2 text-sm font-semibold leading-6 text-[color:var(--ink-strong)]">
              {item.title}
            </span>
          </span>
          <span className="flex-none text-xs font-medium text-[color:var(--ink-muted)]">
            {formatDate(item.publishedAt)}
          </span>
        </span>
        <span className="news-list-summary mt-2 line-clamp-2 text-xs leading-5 text-[color:var(--ink-soft)]">
          {formatBeginnerSummary(item.summary)}
        </span>
        <span className="news-list-tags mt-3 flex flex-wrap items-center gap-2 text-xs text-[color:var(--ink-soft)]">
          <span className="rounded-full border border-[rgba(255,159,10,0.18)] bg-[rgba(255,159,10,0.08)] px-2.5 py-1 text-[color:var(--accent-gold)]">
            {buildImpactSummary(item)}
          </span>
          <span className={`rounded-full border px-2.5 py-1 ${getRelevanceTone(item.relevanceScore)}`}>
            {formatRelevanceLabel(item.relevanceScore)}
          </span>
        </span>
      </button>
      <div className="news-list-actions mt-3 flex flex-wrap items-center justify-between gap-2">
        <button
          type="button"
          className="news-mini-button"
          onClick={() => {
            onAnalyze(item);
          }}
          disabled={disabled}
        >
          {disabled ? "生成中" : item.latestAnalysisId ? "重新解读" : "生成解读"}
        </button>
        {isExternalHttpUrl(item.url) ? (
          <a
            href={item.url}
            target="_blank"
            rel="noreferrer"
            className="news-plain-link"
          >
            查看原文
          </a>
        ) : null}
      </div>
    </article>
  );
}

function splitInsightText(value: string): string[] {
  const parts = value.match(/[^。！？；]+[。！？；]?/g) ?? [value];
  return parts.map((part) => part.trim()).filter(Boolean);
}

function filterInsightItems(items: string[], pattern: RegExp): string[] {
  return items.filter((item) => pattern.test(item));
}

function AnalysisCanvas({ analysis }: { analysis: NewsAnalysis }) {
  const factItems = splitInsightText(analysis.factSummary);
  const riskItems = splitInsightText(analysis.riskNotice);
  const sourceItem = analysis.item;
  const exposureItems = filterInsightItems(
    [...analysis.impactPath, ...analysis.recommendedActions],
    portfolioExposurePattern,
  );
  const behaviorItems = filterInsightItems(
    [...analysis.uncertainty, ...riskItems, ...analysis.recommendedActions],
    behaviorRiskPattern,
  );

  return (
    <div className="news-analysis-canvas">
      <div className="news-analysis-head">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="section-kicker">真实资讯解读</p>
            <h2 className="mt-3 text-[1.35rem] font-semibold leading-tight text-[color:var(--ink-strong)] sm:text-[1.55rem]">
              {sourceItem?.title ?? analysis.headline}
            </h2>
          </div>
          <span
            className={`rounded-full border px-3 py-1 text-xs font-semibold ${modelStatusTone(analysis)}`}
          >
            {modelStatusLabel(analysis)}
          </span>
        </div>
        {sourceItem ? (
          <div
            data-testid="news-selected-source"
            className="mt-4 flex flex-wrap items-center gap-3 text-xs text-[color:var(--ink-muted)]"
          >
            <span>{formatSourceLabel(sourceItem.source)}</span>
            <span>{formatDate(sourceItem.publishedAt)}</span>
            <span>{formatNewsCategory(sourceItem.category)}</span>
            {isExternalHttpUrl(sourceItem.url) ? (
              <a
                href={sourceItem.url}
                target="_blank"
                rel="noreferrer"
                className="font-semibold text-[color:var(--accent-blue)] hover:underline"
              >
                查看原文
              </a>
            ) : null}
          </div>
        ) : null}
        {analysis.modelStatus !== "enhanced" ? (
          <div className="mt-4 rounded-lg border border-[rgba(255,159,10,0.28)] bg-[rgba(255,159,10,0.1)] px-4 py-3 text-sm font-medium leading-6 text-[color:var(--accent-clay)]">
            {analysis.modelStatus === "skipped"
              ? "这次没有可用的 LLM API，下面只显示来源事实和规则拆解，不会伪装成模型解读。请到资料中心配置模型 Key 后重新生成。"
              : "这次模型增强没有成功，下面只显示来源事实和规则拆解。"}
            {analysis.fallbackReason ? ` 原因：${analysis.fallbackReason}。` : null}
          </div>
        ) : null}
      </div>

      <div
        data-testid="news-analysis-process"
        className="sr-only"
      >
        从真实来源到安全解释
      </div>

      <div className="news-interpretation-stack">
        <section className="news-interpretation-card">
          <div className="news-section-title">
            <CheckCircle2 aria-hidden="true" className="size-5 text-[color:var(--accent-teal)]" />
            <h3>事实是什么</h3>
          </div>
          <div className="mt-4 space-y-2 text-sm leading-7 text-[color:var(--ink-soft)]">
            {(factItems.length > 0 ? factItems : ["暂无事实摘要。"]).slice(0, 4).map((item) => (
              <p key={item}>• {formatProductCopy(item)}</p>
            ))}
          </div>
        </section>

        <section className="news-interpretation-card">
          <div className="news-section-title">
            <TrendingUp aria-hidden="true" className="size-5 text-[color:var(--accent-blue)]" />
            <h3>和我的组合可能有什么关系</h3>
          </div>
          <div className="mt-4 space-y-2 text-sm leading-7 text-[color:var(--ink-soft)]">
            {(exposureItems.length > 0
              ? exposureItems
              : analysis.impactPath.length > 0
                ? analysis.impactPath
                : ["先核对你的基金类型、行业主题和集中度，再决定是否需要去组合页复盘。"]
            )
              .slice(0, 4)
              .map((item) => (
                <p key={item}>• {formatProductCopy(item)}</p>
              ))}
          </div>
        </section>

        <section className="news-interpretation-card news-interpretation-warning">
          <div className="news-section-title">
            <AlertTriangle aria-hidden="true" className="size-5 text-[color:var(--accent-gold)]" />
            <h3>不确定性</h3>
          </div>
          <div className="mt-4 space-y-2 text-sm leading-7 text-[color:var(--ink-soft)]">
            {Array.from(
              new Set([
                ...riskItems,
                ...analysis.uncertainty,
                ...behaviorItems,
              ]),
            )
              .slice(0, 4)
              .map((item) => (
                <p key={item}>• {formatProductCopy(item)}</p>
              ))}
          </div>
        </section>
      </div>

      <div className="news-safe-actions">
        <p className="section-kicker">安全下一步</p>
        <div className="mt-3 flex flex-wrap gap-2">
          {(analysis.recommendedActions.length > 0
            ? analysis.recommendedActions
            : ["带着这条资讯去 Agent，结合我的组合继续拆解。"]
          )
            .slice(0, 3)
            .map((item) => (
              <span key={item}>{formatProductCopy(item)}</span>
            ))}
        </div>
      </div>

      <div className="flex flex-wrap gap-2">
        {[...analysis.relatedLearning, ...analysis.citations].map((item) => (
          <span
            key={item}
            className="rounded-full border border-[color:var(--line-soft)] bg-white/70 px-3 py-1 text-xs text-[color:var(--ink-soft)]"
          >
            {formatReferenceLabel(item)}
          </span>
        ))}
      </div>
    </div>
  );
}

function EmptyImpactPathMap() {
  return (
    <div className="grid gap-3 xl:grid-cols-5">
      {emptyImpactPathStages.map((item, index) => (
        <div
          key={item.label}
          className="rounded-lg border border-[color:var(--line-soft)] bg-white/72 p-4"
        >
          <div className="flex items-center gap-2">
            <span className="grid size-7 place-items-center rounded-full bg-[rgba(0,113,227,0.11)] text-xs font-bold text-[color:var(--accent-teal)]">
              {index + 1}
            </span>
            <p className="section-kicker">{item.label}</p>
          </div>
          <h3 className="mt-3 text-base font-semibold text-[color:var(--ink-strong)]">
            {item.title}
          </h3>
          <p className="mt-3 text-sm leading-7 text-[color:var(--ink-soft)]">
            {item.detail}
          </p>
        </div>
      ))}
    </div>
  );
}

function SourcePreviewCanvas({ item }: { item: NewsItem }) {
  const factItems = splitInsightText(formatBeginnerSummary(item.summary));
  const impactAreas = item.impactAreas.length > 0 ? item.impactAreas : item.tags;

  return (
    <div className="news-analysis-canvas">
      <div className="news-analysis-head">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="section-kicker">{formatSourceLabel(item.source)}</p>
            <h2 className="mt-3 text-[1.45rem] font-semibold leading-tight text-[color:var(--ink-strong)]">
              {item.title}
            </h2>
          </div>
          <Bookmark aria-hidden="true" className="size-5 text-[color:var(--accent-blue)]" />
        </div>
        <div className="mt-4 flex flex-wrap items-center gap-3 text-xs text-[color:var(--ink-muted)]">
          <span>{formatDate(item.publishedAt)}</span>
          <span>{formatNewsCategory(item.category)}</span>
          <span>{formatRelevanceLabel(item.relevanceScore)}</span>
        </div>
      </div>

      <div className="news-interpretation-stack">
        <section className="news-interpretation-card">
          <div className="news-section-title">
            <CheckCircle2 aria-hidden="true" className="size-5 text-[color:var(--accent-teal)]" />
            <h3>事实是什么</h3>
          </div>
          <div className="mt-4 space-y-2 text-sm leading-7 text-[color:var(--ink-soft)]">
            {(factItems.length > 0 ? factItems : [item.title]).slice(0, 3).map((text) => (
              <p key={text}>• {formatProductCopy(text)}</p>
            ))}
          </div>
        </section>

        <section className="news-interpretation-card">
          <div className="news-section-title">
            <TrendingUp aria-hidden="true" className="size-5 text-[color:var(--accent-blue)]" />
            <h3>和我的组合可能有什么关系</h3>
          </div>
          <div className="mt-4 space-y-2 text-sm leading-7 text-[color:var(--ink-soft)]">
            {(impactAreas.length > 0
              ? impactAreas
              : ["先核对你的基金类型、行业主题和集中度，再决定是否需要去组合页复盘。"]
            )
              .slice(0, 3)
              .map((text) => (
                <p key={text}>• {formatProductCopy(text)}</p>
              ))}
          </div>
        </section>

        <section className="news-interpretation-card news-interpretation-warning">
          <div className="news-section-title">
            <AlertTriangle aria-hidden="true" className="size-5 text-[color:var(--accent-gold)]" />
            <h3>不确定性</h3>
          </div>
          <div className="mt-4 space-y-2 text-sm leading-7 text-[color:var(--ink-soft)]">
            {[
              "单条资讯不能直接变成买卖动作。",
              "需要继续核对政策节奏、市场反应和你的持仓暴露。",
              "如果情绪被标题带动，先回到原计划和风险边界。",
            ].map((text) => (
              <p key={text}>• {text}</p>
            ))}
          </div>
        </section>
      </div>

      <div className="news-safe-actions">
        <p className="section-kicker">安全下一步</p>
        <div className="mt-3 flex flex-wrap gap-2">
          <span>生成解读后再结合组合查看</span>
          <span>先核对持仓暴露</span>
          <span>不把标题当成操作指令</span>
        </div>
      </div>
    </div>
  );
}

export function NewsWorkspace() {
  const queryClient = useQueryClient();
  const searchParams = useSearchParams();
  const sourceAnalysisId = searchParams.get("source_id");
  const [headline, setHeadline] = useState("");
  const [body, setBody] = useState("");
  const [selectedItemId, setSelectedItemId] = useState<string | null>(null);
  const [analysis, setAnalysis] = useState<NewsAnalysis | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [showAllItems, setShowAllItems] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [activeFilter, setActiveFilter] = useState<(typeof newsFilters)[number]>("全部");
  const analysisRef = useRef<HTMLDivElement | null>(null);

  const sessionQuery = useQuery({
    queryKey: ["session-user"],
    queryFn: getSessionUser,
    retry: false,
  });

  const newsQuery = useQuery({
    queryKey: newsCatalogQueryKey,
    queryFn: () => getNewsCatalog({ refresh: false }),
    enabled: Boolean(sessionQuery.data),
    staleTime: newsCatalogStaleTimeMs,
    gcTime: newsCatalogGcTimeMs,
    retry: false,
  });

  const sourceAnalysisQuery = useQuery({
    queryKey: ["news-analysis", sourceAnalysisId],
    queryFn: () => getNewsAnalysis(sourceAnalysisId ?? ""),
    enabled: Boolean(sessionQuery.data && sourceAnalysisId),
    retry: false,
  });

  const refreshNewsMutation = useMutation({
    mutationFn: () => getNewsCatalog({ refresh: true }),
    onSuccess: (result) => {
      queryClient.setQueryData(newsCatalogQueryKey, result);
    },
  });

  const analyzeMutation = useMutation({
    mutationFn: analyzeNews,
    onSuccess: (result) => {
      setAnalysis(result);
      setSelectedItemId(result.itemId);
      setSubmitError(null);
      queryClient.setQueryData<NewsCatalogState | undefined>(
        newsCatalogQueryKey,
        (current) => applyAnalysisToCatalog(current, result),
      );
      window.setTimeout(() => {
        analysisRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
        analysisRef.current?.focus();
      }, 80);
    },
    onError: (error) => {
      setSubmitError(error instanceof Error ? error.message : "解读失败，请稍后重试。");
    },
  });

  if (sessionQuery.isLoading) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/60 px-5 py-4 text-sm text-[color:var(--ink-soft)]">
        正在初始化新闻解读工作区...
      </div>
    );
  }

  if (isApiError(sessionQuery.error) && sessionQuery.error.status === 401) {
    return (
      <SectionBlock
        eyebrow="资讯解读"
        title="新闻解读需要先登录。"
        description="登录后，资讯解读会结合你的画像、组合报告和学习状态。"
      >
        <div className="paper-panel-strong rounded-lg px-5 py-6">
          <div className="flex flex-wrap gap-3">
            <Link href="/start" className="action-button">
              去登录 / 注册
            </Link>
            <Link href="/" className="action-button-secondary">
              回到总览
            </Link>
          </div>
        </div>
      </SectionBlock>
    );
  }

  if (sessionQuery.error || !sessionQuery.data) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
        当前新闻工作区不可用：{sessionQuery.error?.message ?? "未知错误"}
      </div>
    );
  }

  const catalog = newsQuery.data;
  const items = [...(catalog?.items ?? []), ...(catalog?.policyItems ?? [])];
  const filteredItems = items.filter(
    (item) => itemMatchesFilter(item, activeFilter) && itemMatchesSearch(item, searchTerm),
  );
  const visibleItems = showAllItems ? filteredItems : filteredItems.slice(0, visibleNewsLimit);
  const endpointPending = isPlannedEndpointError(newsQuery.error);
  const activeAnalysis = analysis ?? sourceAnalysisQuery.data ?? null;
  const activeItemId = selectedItemId ?? activeAnalysis?.itemId ?? null;
  const activeSourceItem =
    activeAnalysis?.item ??
    items.find((item) => item.id === activeItemId) ??
    visibleItems[0] ??
    null;
  const agentNewsPrompt = activeSourceItem?.title
    ? `请结合我的组合，解释《${activeSourceItem.title}》可能影响什么、哪些地方不能当成买卖信号？`
    : "请结合我的组合，解释当前选中的资讯可能影响什么、哪些地方不能当成买卖信号？";
  const coverage = sourceCoverageBuckets(items);

  function handleAnalyzeItem(item: NewsItem) {
    setSelectedItemId(item.id);
    setSubmitError(null);
    analyzeMutation.mutate({
      itemId: item.id,
      headline: item.title,
      body: item.summary,
    });
  }

  function handleSelectItem(item: NewsItem) {
    setSelectedItemId(item.id);
    setAnalysis(null);
    setSubmitError(null);
  }

  function handleAnalyzeDraft(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitError(null);

    const normalizedHeadline = headline.trim();
    const normalizedBody = body.trim();
    if (normalizedHeadline.length < 4 || normalizedBody.length < 12) {
      setSubmitError("请至少填写一条标题和一段需要解读的新闻或政策内容。");
      return;
    }

    setSelectedItemId(null);
    analyzeMutation.mutate({
      headline: normalizedHeadline,
      body: normalizedBody,
    });
  }

  return (
    <div className="news-command-page">
      <header className="news-command-topbar">
        <div>
          <p className="section-kicker">影响路径图</p>
          <h1>资讯解读</h1>
          <p>聚焦重要政策与市场动态，连接您的组合。</p>
        </div>
        <button
          type="button"
          className="action-button-secondary"
          disabled={refreshNewsMutation.isPending}
          onClick={() => refreshNewsMutation.mutate()}
        >
          <RefreshCw aria-hidden="true" className="size-4" />
          {refreshNewsMutation.isPending ? "同步中" : "同步真实资讯"}
        </button>
      </header>
      {refreshNewsMutation.error ? (
        <div className="command-inline-error">
          同步真实资讯失败：
          {refreshNewsMutation.error instanceof Error
            ? refreshNewsMutation.error.message
            : "请稍后重试。"}
        </div>
      ) : null}

      {endpointPending ? (
        <SectionBlock
          eyebrow="资讯源"
          title="当前资讯源暂不可用。"
          description="你仍然可以粘贴新闻或政策内容，生成同样结构的解读。"
        >
          <div className="grid gap-4 lg:grid-cols-3">
            {interpretationFrames.map((item) => (
              <div
                key={item.label}
                className="paper-panel-strong rounded-lg p-5"
              >
                <p className="section-kicker">{item.label}</p>
                <h3 className="mt-3 text-xl font-semibold">{item.title}</h3>
                <p className="mt-3 text-sm leading-7 text-[color:var(--ink-soft)]">
                  {item.detail}
                </p>
              </div>
            ))}
          </div>
        </SectionBlock>
      ) : null}

      <main className="news-command-grid">
        <aside className="news-feed-panel" aria-label="资讯列表">
          <label className="news-search-box">
            <Search aria-hidden="true" className="size-4" />
            <input
              value={searchTerm}
              onChange={(event) => setSearchTerm(event.target.value)}
              placeholder="搜索资讯或主题"
            />
          </label>
          <div className="news-filter-row" role="tablist" aria-label="资讯筛选">
            {newsFilters.map((filter) => (
              <button
                key={filter}
                type="button"
                role="tab"
                aria-selected={activeFilter === filter}
                className={activeFilter === filter ? "news-filter-active" : ""}
                onClick={() => {
                  setActiveFilter(filter);
                  setShowAllItems(false);
                }}
              >
                {filter}
              </button>
            ))}
          </div>
          <div className="news-feed-list">
            {newsQuery.isLoading ? (
              <div className="command-empty-line">正在读取资讯列表...</div>
            ) : newsQuery.error && !endpointPending && !activeAnalysis ? (
              <div className="command-inline-error">
                新闻列表加载失败：{newsQuery.error.message}
              </div>
            ) : newsQuery.error && !endpointPending && activeAnalysis ? (
              <div className="command-empty-line">
                列表刷新暂时失败，但刚生成的解读已保留在右侧，可继续查看或稍后同步。
              </div>
            ) : visibleItems.length > 0 ? (
              <>
                {visibleItems.map((item) => (
                  <NewsCard
                    key={item.id}
                    item={item}
                    active={activeSourceItem?.id === item.id}
                    disabled={analyzeMutation.isPending}
                    onSelect={handleSelectItem}
                    onAnalyze={handleAnalyzeItem}
                  />
                ))}
                {filteredItems.length > visibleNewsLimit ? (
                  <button
                    type="button"
                    className="news-show-more"
                    onClick={() => setShowAllItems((current) => !current)}
                  >
                    {showAllItems
                      ? `收起到前 ${visibleNewsLimit} 条`
                      : `查看全部 ${filteredItems.length} 条`}
                  </button>
                ) : null}
              </>
            ) : (
              <div className="command-empty-line">
                当前没有已同步资讯。可以点击“同步真实资讯”，或手动粘贴材料。
              </div>
            )}
          </div>
          <p className="news-feed-count">已显示 {visibleItems.length} 条资讯</p>
        </aside>

        <section className="news-main-panel" tabIndex={0} aria-label="资讯解读画布">
          <div
            ref={analysisRef}
            tabIndex={-1}
            aria-live="polite"
            aria-atomic="true"
            className="min-w-0 scroll-mt-6 focus:outline-none"
          >
            {analyzeMutation.isPending ? (
              <div data-testid="news-analysis-pending" className="news-pending-panel">
                <p className="section-kicker">正在解读</p>
                <h2>正在把资讯拆成事实、组合关系和不确定性。</h2>
                <ol>
                  {["读取来源", "拆分事实", "检查影响路径", "保存安全边界"].map(
                    (step, index) => (
                      <li key={step}>
                        <span>{index + 1}</span>
                        {step}
                      </li>
                    ),
                  )}
                </ol>
              </div>
            ) : sourceAnalysisQuery.isLoading ? (
              <div data-testid="news-analysis-pending" className="news-pending-panel">
                正在读取 Today 引用的新闻解读...
              </div>
            ) : activeAnalysis ? (
              <AnalysisCanvas analysis={activeAnalysis} />
            ) : activeSourceItem ? (
              <SourcePreviewCanvas item={activeSourceItem} />
            ) : (
              <div className="news-analysis-canvas">
                <EmptyImpactPathMap />
              </div>
            )}
          </div>
          {sourceAnalysisQuery.error ? (
            <div className="command-inline-error" role="alert">
              Today 引用的历史解读读取失败。你可以从左侧选择同一条资讯重新生成解读。
            </div>
          ) : null}
          {submitError ? (
            <div className="command-inline-error" role="alert">
              {submitError}
            </div>
          ) : null}
        </section>

        <aside className="news-context-panel">
          <div className="news-source-card">
            <p className="section-kicker">来源覆盖</p>
            <div className="news-source-ring">
              <strong>{coverage.totalSources}</strong>
              <span>类来源</span>
            </div>
            <dl>
              <div>
                <dt>官方/政策</dt>
                <dd>{coverage.official}</dd>
              </div>
              <div>
                <dt>财经媒体</dt>
                <dd>{coverage.finance}</dd>
              </div>
              <div>
                <dt>基金相关</dt>
                <dd>{coverage.fund}</dd>
              </div>
            </dl>
          </div>

          <div className="news-agent-card">
            <h2>结合我的组合进行解读</h2>
            <p>基于您的持仓与风险偏好，从多维度解释这条资讯可能带来的影响。</p>
            <Link
              href={buildAgentPromptHref({
                focus: "news",
                from: "news",
                prompt: agentNewsPrompt,
                sourceIds: {
                  news_item_id: activeSourceItem?.id,
                },
              })}
              className="action-button"
            >
              让 Agent 结合我的组合解释
            </Link>
          </div>

          <div className="news-portfolio-card">
            <div className="flex items-center justify-between gap-3">
              <h2>我的组合概览</h2>
              <Link href="/portfolio">查看组合</Link>
            </div>
            <dl>
              <div>
                <dt>用户</dt>
                <dd>{sessionQuery.data.displayName ?? "已登录"}</dd>
              </div>
              <div>
                <dt>主要目标</dt>
                <dd>{sessionQuery.data.primaryGoal ?? "长期学习与稳健判断"}</dd>
              </div>
              <div>
                <dt>解释原则</dt>
                <dd>不追热点</dd>
              </div>
            </dl>
          </div>
        </aside>

        <section className="news-related-panel">
          <div className="flex items-center justify-between gap-3">
            <h2>相关资讯</h2>
            <button
              type="button"
              className="news-plain-link"
              onClick={() => setShowAllItems(true)}
            >
              查看更多 <ArrowRight aria-hidden="true" className="size-4" />
            </button>
          </div>
          <div className="news-related-grid">
            {items.slice(0, 3).map((item) => (
              <button
                key={item.id}
                type="button"
                className="news-related-card"
                onClick={() => handleAnalyzeItem(item)}
                disabled={analyzeMutation.isPending}
              >
                <span>{formatSourceLabel(item.source)}</span>
                <strong>{item.title}</strong>
                <em>{formatNewsCategory(item.category)}</em>
              </button>
            ))}
          </div>
        </section>
      </main>

      <details className="news-manual-panel">
        <summary>手动粘贴新闻或政策材料</summary>
        <form onSubmit={handleAnalyzeDraft}>
          <label>
            <span>新闻或政策标题</span>
            <input
              className="field-input"
              value={headline}
              onChange={(event) => setHeadline(event.target.value)}
              placeholder="例如：某项长期资金入市政策发布"
            />
          </label>
          <label>
            <span>正文或摘要</span>
            <textarea
              className="field-input min-h-[120px]"
              value={body}
              onChange={(event) => setBody(event.target.value)}
              placeholder="粘贴需要拆解的新闻摘要。"
            />
          </label>
          <div className="flex flex-wrap gap-3">
            <button
              type="submit"
              className="action-button"
              disabled={analyzeMutation.isPending}
            >
              {analyzeMutation.isPending ? "正在生成..." : "生成解读"}
            </button>
            <button
              type="button"
              className="action-button-secondary"
              onClick={() => {
                setHeadline("");
                setBody("");
                setSubmitError(null);
              }}
              disabled={analyzeMutation.isPending}
            >
              清空
            </button>
          </div>
        </form>
      </details>
    </div>
  );
}
