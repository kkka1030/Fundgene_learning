"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import * as echarts from "echarts";
import { useEffect, useMemo, useRef, useState } from "react";

import {
  ApiError,
  createPortfolioSnapshot,
  getLatestPortfolioReport,
  getPortfolioHistory,
  getSessionUser,
  type PortfolioHolding,
  type PortfolioHoldingInput,
  type PortfolioReport,
} from "@/lib/api";
import { MetricCard } from "./metric-card";
import { SectionBlock } from "./section-block";

const fundTypeOptions = [
  { value: "equity", label: "权益基金" },
  { value: "mixed", label: "混合基金" },
  { value: "bond", label: "债券基金" },
  { value: "money_market", label: "货币基金" },
  { value: "international", label: "海外 / QDII" },
  { value: "commodity", label: "商品 / 黄金" },
];

const allocationPalette = [
  "#0071e3",
  "#32ade6",
  "#34c759",
  "#5856d6",
  "#ff9f0a",
  "#ff6b5f",
  "#8e8e93",
];

type HoldingDraft = {
  id: string;
  fundCode: string;
  fundName: string;
  fundType: string;
  marketValue: string;
};

type HoldingField = keyof Omit<HoldingDraft, "id">;
type HoldingFieldErrors = Partial<Record<HoldingField, string>>;

type DraftHoldingPreview = {
  id: string;
  fundCode: string;
  fundName: string;
  fundType: string;
  marketValue: number;
  weight: number;
};

type DraftPortfolioPreview = {
  cashValue: number;
  cashWeight: number;
  duplicateCodes: string[];
  holdings: DraftHoldingPreview[];
  totalValue: number;
};

const exampleHoldings: Array<Omit<HoldingDraft, "id">> = [
  {
    fundCode: "161725",
    fundName: "招商中证白酒指数",
    fundType: "equity",
    marketValue: "18000",
  },
  {
    fundCode: "110027",
    fundName: "易方达安心债券",
    fundType: "bond",
    marketValue: "16000",
  },
  {
    fundCode: "000071",
    fundName: "华夏恒生 ETF 联接",
    fundType: "international",
    marketValue: "12000",
  },
];

function isApiError(error: unknown): error is ApiError {
  return error instanceof ApiError;
}

function createEmptyHolding(id: string): HoldingDraft {
  return {
    id,
    fundCode: "",
    fundName: "",
    fundType: "equity",
    marketValue: "",
  };
}

function createHoldingId(prefix = "holding"): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return `${prefix}-${crypto.randomUUID()}`;
  }

  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function formatCurrency(value: number | null): string {
  if (value === null) {
    return "待生成";
  }

  return new Intl.NumberFormat("zh-CN", {
    style: "currency",
    currency: "CNY",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatDate(value: string | null): string {
  if (!value) {
    return "待生成";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "numeric",
    day: "numeric",
  }).format(date);
}

function formatPercent(value: number): string {
  return `${Math.round(value * 10) / 10}%`;
}

function getFundTypeLabel(value: string): string {
  return (
    fundTypeOptions.find((option) => option.value === value)?.label ??
    value
      .split("_")
      .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
      .join(" ")
  );
}

function clampPercent(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }

  return Math.max(0, Math.min(100, value));
}

function normalizeFundCode(value: string): string {
  return value.trim().toUpperCase();
}

function parseAmountInput(value: string): number | null {
  const trimmed = value.trim().replaceAll(",", "").replaceAll("，", "");
  if (!trimmed) {
    return null;
  }

  const withoutCurrency = trimmed.replace(/^[¥￥]\s*/, "").replace(/\s*元$/, "");
  const wanMatch = withoutCurrency.match(/^([0-9]+(?:\.[0-9]+)?)\s*万$/);
  const parsed = wanMatch ? Number(wanMatch[1]) * 10000 : Number(withoutCurrency);
  return Number.isFinite(parsed) ? parsed : null;
}

function buildDraftPortfolioPreview(
  cashValue: string,
  holdings: HoldingDraft[],
): DraftPortfolioPreview {
  const parsedCashValue = parseAmountInput(cashValue);
  const normalizedCashValue =
    parsedCashValue !== null && parsedCashValue >= 0 ? parsedCashValue : 0;
  const codeCounts = new Map<string, number>();
  const validHoldings = holdings.flatMap((holding) => {
    const marketValue = parseAmountInput(holding.marketValue);
    const fundCode = normalizeFundCode(holding.fundCode);
    const fundName = holding.fundName.trim();

    if (fundCode) {
      codeCounts.set(fundCode, (codeCounts.get(fundCode) ?? 0) + 1);
    }

    if (marketValue === null || marketValue <= 0) {
      return [];
    }

    return [
      {
        id: holding.id,
        fundCode,
        fundName: fundName || "未命名持仓",
        fundType: holding.fundType,
        marketValue,
        weight: 0,
      },
    ];
  });

  const holdingsValue = validHoldings.reduce(
    (sum, holding) => sum + holding.marketValue,
    0,
  );
  const totalValue = normalizedCashValue + holdingsValue;
  const previewHoldings = validHoldings.map((holding) => ({
    ...holding,
    weight: totalValue > 0 ? (holding.marketValue / totalValue) * 100 : 0,
  }));

  return {
    cashValue: normalizedCashValue,
    cashWeight: totalValue > 0 ? (normalizedCashValue / totalValue) * 100 : 0,
    duplicateCodes: Array.from(codeCounts.entries())
      .filter(([, count]) => count > 1)
      .map(([code]) => code),
    holdings: previewHoldings.sort((a, b) => b.marketValue - a.marketValue),
    totalValue,
  };
}

function buildAllocationBreakdown(report: PortfolioReport) {
  const buckets = new Map<string, number>();
  if (report.cashValue > 0 && report.totalValue > 0) {
    buckets.set("现金", (report.cashValue / report.totalValue) * 100);
  }

  for (const holding of report.holdings) {
    const label = getFundTypeLabel(holding.fundType);
    buckets.set(label, (buckets.get(label) ?? 0) + holding.weight);
  }

  return Array.from(buckets.entries())
    .map(([label, value], index) => ({
      label,
      value: clampPercent(value),
      color: allocationPalette[index % allocationPalette.length],
    }))
    .filter((item) => item.value > 0)
    .sort((a, b) => b.value - a.value);
}

function getLargestHolding(report: PortfolioReport): PortfolioHolding | null {
  return report.holdings.reduce<PortfolioHolding | null>((largest, holding) => {
    if (!largest || holding.weight > largest.weight) {
      return holding;
    }

    return largest;
  }, null);
}

function getReportPosture(report: PortfolioReport): string {
  const largest = getLargestHolding(report);
  if (largest && largest.weight >= 40) {
    return "集中度偏高";
  }

  const cashRatio =
    report.totalValue > 0 ? (report.cashValue / report.totalValue) * 100 : 0;
  if (cashRatio >= 30) {
    return "现金留白较多";
  }

  if (report.holdings.length >= 4) {
    return "结构较分散";
  }

  return "待继续观察";
}

function DraftPreviewPanel({ preview }: { preview: DraftPortfolioPreview }) {
  const largestHolding =
    preview.holdings.length > 0
      ? preview.holdings.reduce((largest, holding) =>
          holding.weight > largest.weight ? holding : largest,
        )
      : null;

  return (
    <section className="mt-5 rounded-lg border border-[color:var(--line-soft)] bg-white/70 px-4 py-4 shadow-sm backdrop-blur-2xl">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <p className="section-kicker">提交前预览</p>
          <h3 className="mt-2 text-lg font-semibold text-[color:var(--ink-strong)]">
            先检查权重，再生成报告。
          </h3>
          <p className="mt-1 text-sm leading-6 text-[color:var(--ink-soft)]">
            这里按当前草稿估算，不接实时行情，也不是最终分析结论。
          </p>
        </div>
        <div className="grid min-w-[11rem] gap-1 rounded-lg border border-[color:var(--line-soft)] bg-white/72 px-4 py-3 text-sm shadow-sm">
          <span className="text-[color:var(--ink-muted)]">草稿总资产</span>
          <strong className="text-lg text-[color:var(--ink-strong)]">
            {formatCurrency(preview.totalValue)}
          </strong>
        </div>
      </div>

      <div className="mt-4 grid gap-3 sm:grid-cols-3">
        <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/64 px-3 py-3">
          <p className="text-xs font-semibold text-[color:var(--ink-muted)]">现金比例</p>
          <p className="mt-2 text-base font-semibold text-[color:var(--ink-strong)]">
            {formatPercent(preview.cashWeight)}
          </p>
        </div>
        <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/64 px-3 py-3">
          <p className="text-xs font-semibold text-[color:var(--ink-muted)]">持仓数量</p>
          <p className="mt-2 text-base font-semibold text-[color:var(--ink-strong)]">
            {preview.holdings.length} 只
          </p>
        </div>
        <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/64 px-3 py-3">
          <p className="text-xs font-semibold text-[color:var(--ink-muted)]">最大持仓</p>
          <p className="mt-2 text-base font-semibold text-[color:var(--ink-strong)]">
            {largestHolding ? formatPercent(largestHolding.weight) : "待填写"}
          </p>
        </div>
      </div>

      {preview.duplicateCodes.length > 0 ? (
        <div className="mt-4 rounded-lg border border-[rgba(255,59,48,0.22)] bg-[rgba(255,59,48,0.08)] px-4 py-3 text-sm leading-6 text-[color:var(--danger)]">
          有重复基金代码：{preview.duplicateCodes.join("、")}。提交前需要合并或删除重复行。
        </div>
      ) : null}

      <div className="mt-4 space-y-3">
        {preview.holdings.length > 0 ? (
          preview.holdings.map((holding) => (
            <div key={holding.id} className="space-y-2">
              <div className="flex items-center justify-between gap-3 text-sm">
                <div className="min-w-0">
                  <p className="truncate font-semibold text-[color:var(--ink-strong)]">
                    {holding.fundName}
                  </p>
                  <p className="mt-0.5 text-xs text-[color:var(--ink-muted)]">
                    {holding.fundCode || "待填写代码"} · {getFundTypeLabel(holding.fundType)}
                  </p>
                </div>
                <div className="shrink-0 text-right">
                  <p className="font-semibold text-[color:var(--ink-strong)]">
                    {formatPercent(holding.weight)}
                  </p>
                  <p className="text-xs text-[color:var(--ink-muted)]">
                    {formatCurrency(holding.marketValue)}
                  </p>
                </div>
              </div>
              <div className="h-2 overflow-hidden rounded-full bg-[rgba(118,118,128,0.14)]">
                <div
                  className="h-full rounded-full bg-[linear-gradient(90deg,var(--accent-teal),var(--accent-cyan))]"
                  style={{ width: `${clampPercent(holding.weight)}%` }}
                />
              </div>
            </div>
          ))
        ) : (
          <div className="rounded-lg border border-dashed border-[color:var(--line-soft)] px-4 py-4 text-sm leading-6 text-[color:var(--ink-soft)]">
            先填写至少一只基金和持仓金额，这里会显示草稿权重。
          </div>
        )}
      </div>
    </section>
  );
}

function AllocationVisual({ report }: { report: PortfolioReport }) {
  const chartRef = useRef<HTMLDivElement | null>(null);
  const segments = useMemo(() => buildAllocationBreakdown(report), [report]);
  const largest = getLargestHolding(report);
  const cashRatio =
    report.totalValue > 0 ? clampPercent((report.cashValue / report.totalValue) * 100) : 0;

  useEffect(() => {
    if (!chartRef.current) {
      return;
    }

    const chart = echarts.init(chartRef.current, undefined, {
      renderer: "canvas",
    });

    chart.setOption({
      color: segments.map((segment) => segment.color),
      tooltip: {
        trigger: "item",
        formatter: "{b}: {d}%",
      },
      series: [
        {
          name: "资产配置",
          type: "pie",
          radius: ["58%", "78%"],
          center: ["50%", "50%"],
          avoidLabelOverlap: true,
          itemStyle: {
            borderColor: "#ffffff",
            borderWidth: 2,
          },
          label: {
            show: false,
          },
          labelLine: {
            show: false,
          },
          data: segments.map((segment) => ({
            name: segment.label,
            value: segment.value,
          })),
        },
      ],
    });

    const handleResize = () => chart.resize();
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.dispose();
    };
  }, [segments]);

  return (
    <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/70 p-4 shadow-sm backdrop-blur-2xl">
      <div className="flex flex-col gap-5 lg:flex-row lg:items-center">
        <div className="chart-shell relative mx-auto h-[280px] w-full max-w-[360px] flex-none p-2">
          <div
            ref={chartRef}
            className="h-full w-full"
            role="img"
            aria-label={`组合配置分布：${segments
              .map((item) => `${item.label} ${formatPercent(item.value)}`)
              .join("，")}`}
          />
          <div className="pointer-events-none absolute left-1/2 top-1/2 grid size-24 -translate-x-1/2 -translate-y-1/2 place-items-center rounded-full border border-[color:var(--line-soft)] bg-white/86 text-center shadow-[0_10px_24px_rgba(29,29,31,0.1)] backdrop-blur-xl">
            <span className="px-2 text-sm font-semibold leading-tight text-[color:var(--ink-strong)]">
              {getReportPosture(report)}
            </span>
          </div>
        </div>

        <div className="min-w-0 flex-1 space-y-4">
          <div className="grid gap-3 sm:grid-cols-3 lg:grid-cols-1 2xl:grid-cols-3">
            <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/64 px-4 py-4">
              <p className="section-kicker">总资产</p>
              <p className="mt-2 text-lg font-semibold">
                {formatCurrency(report.totalValue)}
              </p>
            </div>
            <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/64 px-4 py-4">
              <p className="section-kicker">现金</p>
              <p className="mt-2 text-lg font-semibold">{formatPercent(cashRatio)}</p>
            </div>
            <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/64 px-4 py-4">
              <p className="section-kicker">第一大持仓</p>
              <p className="mt-2 text-lg font-semibold">
                {largest ? formatPercent(largest.weight) : "待生成"}
              </p>
            </div>
          </div>

          <div className="space-y-3">
            {segments.map((segment) => (
              <div key={segment.label} className="space-y-2">
                <div className="flex items-center justify-between gap-3 text-sm">
                  <div className="flex items-center gap-2">
                    <span
                      className="h-3 w-3 rounded-full"
                      style={{ backgroundColor: segment.color }}
                    />
                    <span className="font-medium">{segment.label}</span>
                  </div>
                  <span className="text-[color:var(--ink-soft)]">
                    {formatPercent(segment.value)}
                  </span>
                </div>
                <div className="h-2 overflow-hidden rounded-full bg-[rgba(118,118,128,0.16)]">
                  <div
                    className="h-full rounded-full"
                    style={{
                      width: `${segment.value}%`,
                      backgroundColor: segment.color,
                    }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function ReportSignalCard({
  title,
  items,
  emptyText,
  tone,
}: {
  title: string;
  items: string[];
  emptyText: string;
  tone: "moss" | "gold" | "clay" | "ink";
}) {
  const toneClass = {
    moss: "border-[rgba(0,113,227,0.18)] bg-[linear-gradient(145deg,rgba(0,113,227,0.08),rgba(255,255,255,0.88))]",
    gold: "border-[rgba(255,159,10,0.2)] bg-[linear-gradient(145deg,rgba(255,159,10,0.08),rgba(255,255,255,0.88))]",
    clay: "border-[rgba(255,59,48,0.18)] bg-[linear-gradient(145deg,rgba(255,59,48,0.06),rgba(255,255,255,0.88))]",
    ink: "border-[rgba(29,37,31,0.12)] bg-[linear-gradient(145deg,rgba(29,37,31,0.06),rgba(255,255,255,0.82))]",
  } as const;

  return (
    <div className={`rounded-lg border p-4 ${toneClass[tone]}`}>
      <p className="section-kicker">{title}</p>
      <div className="mt-3 space-y-2.5 text-sm leading-6 text-[color:var(--ink-soft)]">
        {items.length > 0
          ? items.map((item, index) => (
              <div key={`${title}-${item}`} className="flex gap-3">
                <span className="mt-1 flex h-6 w-6 flex-none items-center justify-center rounded-full bg-white/78 text-xs font-semibold text-[color:var(--accent-teal)] shadow-sm">
                  {index + 1}
                </span>
                <span>{item}</span>
              </div>
            ))
          : emptyText}
      </div>
    </div>
  );
}

function HoldingWeightRows({
  holdings,
  compactOnMobile = false,
}: {
  holdings: PortfolioHolding[];
  compactOnMobile?: boolean;
}) {
  if (holdings.length === 0) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/72 px-5 py-5 text-sm text-[color:var(--ink-soft)]">
        暂无持仓明细。
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {[...holdings]
        .sort((a, b) => b.weight - a.weight)
        .map((holding, index) => (
          <div
            key={`${holding.fundCode}-${holding.fundName}`}
            className={`rounded-lg border border-[color:var(--line-soft)] bg-white/72 px-4 py-4 ${
              compactOnMobile && index >= 2 ? "hidden sm:block" : ""
            }`}
          >
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div className="min-w-0">
                <p className="section-kicker">持仓 0{index + 1}</p>
                <h3 className="mt-2 text-base font-semibold">
                  {holding.fundName}
                </h3>
                <p className="mt-1 text-xs text-[color:var(--ink-soft)]">
                  {holding.fundCode} · {getFundTypeLabel(holding.fundType)}
                </p>
              </div>
              <div className="text-sm font-semibold">
                {formatCurrency(holding.marketValue)}
              </div>
            </div>
            <div className="mt-4 flex items-center gap-3">
              <div className="h-2 flex-1 overflow-hidden rounded-full bg-[rgba(36,49,39,0.08)]">
                <div
                  className="h-full rounded-full bg-[linear-gradient(90deg,var(--accent-teal),var(--accent-cyan))]"
                  style={{ width: `${clampPercent(holding.weight)}%` }}
                />
              </div>
              <span className="w-14 text-right text-sm text-[color:var(--ink-soft)]">
                {formatPercent(holding.weight)}
              </span>
            </div>
          </div>
        ))}
      {compactOnMobile && holdings.length > 2 ? (
        <div className="rounded-lg border border-dashed border-[color:var(--line-soft)] bg-white/54 px-4 py-3 text-sm text-[color:var(--ink-soft)] sm:hidden">
          其余 {holdings.length - 2} 只持仓已收起。桌面端会展示完整权重列表。
        </div>
      ) : null}
    </div>
  );
}

function buildAllocationGradient(
  segments: Array<{ label: string; value: number; color: string }>,
): string {
  let cursor = 0;
  const stops = segments.map((segment) => {
    const start = cursor;
    const end = cursor + segment.value;
    cursor = end;
    return `${segment.color} ${start}% ${end}%`;
  });
  return `conic-gradient(${stops.join(", ")})`;
}

function CompactAllocationDonut({ report }: { report: PortfolioReport }) {
  const segments = buildAllocationBreakdown(report);
  const largest = getLargestHolding(report);

  return (
    <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/74 p-3 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="section-kicker">资产分布</p>
          <h3 className="mt-1 text-base font-semibold text-[color:var(--ink-strong)]">
            权重和缓冲一屏看完
          </h3>
        </div>
        <span className="rounded-full border border-[color:var(--line-soft)] bg-white/80 px-2.5 py-1 text-xs font-semibold text-[color:var(--ink-soft)]">
          {report.holdings.length} 只基金
        </span>
      </div>

      <div className="mt-3 grid grid-cols-[9.5rem_minmax(0,1fr)] items-center gap-4">
        <div
          className="relative grid aspect-square place-items-center rounded-full shadow-[inset_0_0_0_1px_rgba(15,23,42,0.08)]"
          style={{ background: buildAllocationGradient(segments) }}
          role="img"
          aria-label={`组合配置分布：${segments
            .map((item) => `${item.label} ${formatPercent(item.value)}`)
            .join("，")}`}
        >
          <div className="grid size-[5.1rem] place-items-center rounded-full border border-[color:var(--line-soft)] bg-white/92 text-center shadow-sm">
            <span className="px-2 text-xs font-bold leading-tight text-[color:var(--ink-strong)]">
              {largest ? `最大 ${formatPercent(largest.weight)}` : "待生成"}
            </span>
          </div>
        </div>

        <div className="space-y-2">
          {segments.slice(0, 4).map((segment) => (
            <div key={segment.label} className="grid gap-1">
              <div className="flex items-center justify-between gap-2 text-xs">
                <span className="flex min-w-0 items-center gap-2 font-semibold text-[color:var(--ink-strong)]">
                  <span
                    className="size-2.5 shrink-0 rounded-full"
                    style={{ backgroundColor: segment.color }}
                  />
                  <span className="truncate">{segment.label}</span>
                </span>
                <span className="font-semibold text-[color:var(--ink-soft)]">
                  {formatPercent(segment.value)}
                </span>
              </div>
              <div className="h-1.5 overflow-hidden rounded-full bg-[rgba(118,118,128,0.16)]">
                <div
                  className="h-full rounded-full"
                  style={{
                    width: `${segment.value}%`,
                    backgroundColor: segment.color,
                  }}
                />
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function CompactHoldingRows({ holdings }: { holdings: PortfolioHolding[] }) {
  const sortedHoldings = [...holdings].sort((a, b) => b.weight - a.weight);

  return (
    <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/74 p-3 shadow-sm">
      <div className="flex items-center justify-between gap-3">
        <div>
          <p className="section-kicker">持仓权重</p>
          <h3 className="mt-1 text-base font-semibold text-[color:var(--ink-strong)]">
            谁在承担主要波动
          </h3>
        </div>
        <span className="text-xs font-semibold text-[color:var(--ink-muted)]">
          按权重排序
        </span>
      </div>

      <div className="mt-3 space-y-2">
        {sortedHoldings.slice(0, 4).map((holding, index) => (
          <div
            key={`${holding.fundCode}-${holding.fundName}`}
            className="rounded-lg border border-[color:var(--line-soft)] bg-white/72 px-3 py-2.5"
          >
            <div className="flex items-start justify-between gap-3 text-sm">
              <div className="min-w-0">
                <p className="truncate font-semibold text-[color:var(--ink-strong)]">
                  {index + 1}. {holding.fundName}
                </p>
                <p className="mt-0.5 text-xs text-[color:var(--ink-muted)]">
                  {holding.fundCode} · {getFundTypeLabel(holding.fundType)}
                </p>
              </div>
              <div className="shrink-0 text-right">
                <p className="font-semibold text-[color:var(--ink-strong)]">
                  {formatPercent(holding.weight)}
                </p>
                <p className="text-xs text-[color:var(--ink-muted)]">
                  {formatCurrency(holding.marketValue)}
                </p>
              </div>
            </div>
            <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-[rgba(118,118,128,0.14)]">
              <div
                className="h-full rounded-full bg-[linear-gradient(90deg,var(--accent-teal),var(--accent-cyan))]"
                style={{ width: `${clampPercent(holding.weight)}%` }}
              />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function AgentPortfolioOnePage({
  report,
  reportPosture,
  reportCashRatio,
  evidencePathItems,
  isConcentrationFocus,
}: {
  report: PortfolioReport;
  reportPosture: string;
  reportCashRatio: number;
  evidencePathItems: Array<{ label: string; title: string; detail: string }>;
  isConcentrationFocus: boolean;
}) {
  const largest = getLargestHolding(report);
  const prioritySignals = [
    report.riskExposure[0],
    report.concentrationFlags[0],
    report.allocationBalance[0],
    report.recommendedNextActions[0],
  ].filter((item): item is string => Boolean(item));

  return (
    <div className="portfolio-agent-onepage">
      <section
        id="portfolio-latest-report"
        className="portfolio-agent-board"
        aria-label="来自 Agent 的持仓分析任务"
      >
        <div className="portfolio-agent-header">
          <div className="min-w-0">
            <p className="section-kicker">来自 Agent 的持仓分析</p>
            <h2>
              {isConcentrationFocus
                ? "先看第一大持仓、资产分布和集中度。"
                : "已根据刚才的问题打开组合分析结果。"}
            </h2>
            <p>
              这是一屏检查面板，只解释风险来源和下一步检查，不生成买卖或调仓指令。
            </p>
          </div>
          <div className="portfolio-agent-actions">
            <Link href="/agent?new=1&focus=portfolio" className="action-button-secondary">
              回 Agent 追问
            </Link>
            <a href="#portfolio-snapshot-form" className="action-button">
              更新快照
            </a>
          </div>
        </div>

        <div className="portfolio-agent-layout">
          <div className="portfolio-agent-main">
            <div className="portfolio-agent-summary">
              <div>
                <p className="section-kicker">组合画像结论</p>
                <h3>{reportPosture}</h3>
                <p>{report.summary}</p>
              </div>
              <div className="portfolio-agent-metrics">
                <div>
                  <span>总资产</span>
                  <strong>{formatCurrency(report.totalValue)}</strong>
                </div>
                <div>
                  <span>现金留白</span>
                  <strong>{formatPercent(reportCashRatio)}</strong>
                </div>
                <div>
                  <span>第一大持仓</span>
                  <strong>{largest ? formatPercent(largest.weight) : "待生成"}</strong>
                </div>
              </div>
            </div>

            <div className="portfolio-agent-signals">
              {prioritySignals.slice(0, 4).map((signal, index) => (
                <div key={signal}>
                  <span>{index + 1}</span>
                  <p>{signal}</p>
                </div>
              ))}
            </div>

            <CompactAllocationDonut report={report} />
          </div>

          <aside className="portfolio-agent-side">
            <CompactHoldingRows holdings={report.holdings} />
            <div className="portfolio-agent-path">
              <p className="section-kicker">证据路径</p>
              <div>
                {evidencePathItems.slice(0, 4).map((item, index) => (
                  <article key={item.label}>
                    <span>0{index + 1}</span>
                    <div>
                      <strong>{item.title}</strong>
                      <p>{item.detail}</p>
                    </div>
                  </article>
                ))}
              </div>
            </div>
          </aside>
        </div>
      </section>
    </div>
  );
}

export function PortfolioWorkspace() {
  const queryClient = useQueryClient();
  const searchParams = useSearchParams();
  const [snapshotDate, setSnapshotDate] = useState(
    () => new Date().toISOString().slice(0, 10),
  );
  const [cashValue, setCashValue] = useState("0");
  const [holdings, setHoldings] = useState<HoldingDraft[]>(() => [
    createEmptyHolding(createHoldingId()),
  ]);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [readyToSave, setReadyToSave] = useState(false);
  const [snapshotDateError, setSnapshotDateError] = useState<string | null>(null);
  const [cashError, setCashError] = useState<string | null>(null);
  const [holdingErrors, setHoldingErrors] = useState<Record<string, HoldingFieldErrors>>({});
  const latestReportRef = useRef<HTMLDivElement | null>(null);
  const draftPreviewRef = useRef<HTMLDivElement | null>(null);

  const sessionQuery = useQuery({
    queryKey: ["session-user"],
    queryFn: getSessionUser,
    retry: false,
  });

  const latestReportQuery = useQuery({
    queryKey: ["portfolio-latest"],
    queryFn: getLatestPortfolioReport,
    enabled: Boolean(sessionQuery.data?.profileId),
    retry: false,
  });

  const historyQuery = useQuery({
    queryKey: ["portfolio-history"],
    queryFn: getPortfolioHistory,
    enabled: Boolean(sessionQuery.data?.profileId),
    retry: false,
  });

  const snapshotMutation = useMutation({
    mutationFn: async (payload: {
      snapshotDate: string;
      cashValue: number;
      holdings: PortfolioHoldingInput[];
    }) => createPortfolioSnapshot(payload),
    onSuccess: async (report) => {
      setSubmitError(null);
      setSuccessMessage(`已生成 ${formatDate(report.snapshotDate)} 的组合报告，最近报告已刷新。`);
      setReadyToSave(false);
      setSnapshotDateError(null);
      setCashError(null);
      setHoldingErrors({});
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["portfolio-latest"] }),
        queryClient.invalidateQueries({ queryKey: ["portfolio-history"] }),
        queryClient.invalidateQueries({ queryKey: ["dashboard"] }),
      ]);
      window.requestAnimationFrame(() => {
        latestReportRef.current?.scrollIntoView({
          behavior: "smooth",
          block: "start",
        });
        latestReportRef.current?.focus({ preventScroll: true });
      });
    },
    onError: (error) => {
      setSuccessMessage(null);
      setSubmitError(error instanceof Error ? error.message : "提交失败，请稍后重试。");
    },
  });

  if (sessionQuery.isLoading) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/60 px-5 py-4 text-sm text-[color:var(--ink-soft)]">
        正在初始化组合分析上下文…
      </div>
    );
  }

  if (isApiError(sessionQuery.error) && sessionQuery.error.status === 401) {
    return (
      <SectionBlock
        eyebrow="组合分析"
        title="组合分析需要先登录。"
        description="登录后才能保存组合快照、分析报告和历史记录。"
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
        当前组合工作区不可用：{sessionQuery.error?.message ?? "未知错误"}
      </div>
    );
  }

  const sessionUser = sessionQuery.data;
  if (!sessionUser.profileId) {
    return (
      <SectionBlock
        eyebrow="组合分析"
        title="先建立基础画像，再录入第一份持仓快照。"
        description="组合分析要跟你的目标、经验和后续教练上下文连起来，因此至少要先在建档中保存基础资料。"
      >
        <div className="grid gap-4 lg:grid-cols-3">
          <MetricCard
            label="当前用户"
            value={sessionUser.displayName ?? "已登录用户"}
            detail={sessionUser.email ?? sessionUser.id}
            accent="moss"
          />
          <MetricCard
            label="组合输入"
            value="待解锁"
            detail="先建立画像，再把第一份快照作为后续分析基线。"
            accent="gold"
          />
          <MetricCard
            label="安全边界"
            value="不做交易执行"
            detail="这里解释风险和结构，不生成交易执行指令。"
            accent="clay"
          />
        </div>
        <div className="flex flex-wrap gap-3">
          <Link href="/onboarding" className="action-button">
            去建立基础画像
          </Link>
          <Link href="/learning" className="action-button-secondary">
            先去学习
          </Link>
        </div>
      </SectionBlock>
    );
  }

  if (latestReportQuery.isLoading || historyQuery.isLoading) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/60 px-5 py-4 text-sm text-[color:var(--ink-soft)]">
        正在同步最近组合报告和历史快照…
      </div>
    );
  }

  if (latestReportQuery.error || historyQuery.error) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
        组合数据加载失败：
        {latestReportQuery.error?.message ??
          historyQuery.error?.message ??
          "未知错误"}
      </div>
    );
  }

  const latestReport = latestReportQuery.data?.report ?? null;
  const historyItems = historyQuery.data?.items ?? [];
  const fromAgent = searchParams.get("from") === "agent";
  const portfolioFocus = searchParams.get("focus");
  const isConcentrationFocus =
    portfolioFocus === "concentration" || portfolioFocus === "portfolio";
  const draftPreview = buildDraftPortfolioPreview(cashValue, holdings);
  const reportPosture = latestReport ? getReportPosture(latestReport) : "待建立组合画像";
  const largestReportHolding = latestReport ? getLargestHolding(latestReport) : null;
  const reportCashRatio =
    latestReport && latestReport.totalValue > 0
      ? (latestReport.cashValue / latestReport.totalValue) * 100
      : 0;
  const evidencePathItems = latestReport
    ? [
        {
          label: "画像结论",
          title: reportPosture,
          detail: latestReport.summary,
        },
        {
          label: "组合暴露",
          title: largestReportHolding
            ? `${largestReportHolding.fundName} ${formatPercent(largestReportHolding.weight)}`
            : `${latestReport.holdings.length} 只基金`,
          detail:
            latestReport.riskExposure[0] ??
            "当前报告会先解释资产类型和持仓权重，而不是直接讨论操作。",
        },
        {
          label: "行为提醒",
          title: latestReport.concentrationFlags[0] ?? "避免被单一持仓牵动",
          detail:
            latestReport.allocationBalance[0] ??
            "把结构先看清，再决定是否需要进一步学习或训练。",
        },
        {
          label: "安全动作",
          title: latestReport.recommendedNextActions[0] ?? "让教练解释这份报告",
          detail: "下一步只用于理解和复盘，不触发交易执行。",
        },
      ]
    : [
        {
          label: "画像结论",
          title: "还没有组合画像",
          detail: "先保存一份手动快照，系统才会生成可追溯报告。",
        },
        {
          label: "组合暴露",
          title: "待录入",
          detail: "录入基金类型、市值和现金后，才会计算权重和集中度。",
        },
        {
          label: "行为提醒",
          title: "先不要凭感觉调整",
          detail: "没有结构证据前，不把市场情绪当成个人动作依据。",
        },
        {
          label: "安全动作",
          title: "检查草稿权重",
          detail: "先看总资产、现金比例、最大持仓，再保存为历史快照。",
        },
      ];

  function focusField(elementId: string) {
    window.requestAnimationFrame(() => {
      document.getElementById(elementId)?.focus();
    });
  }

  function resetDraftConfirmation() {
    setReadyToSave(false);
  }

  function handleHoldingChange(
    targetId: string,
    field: HoldingField,
    value: string,
  ) {
    setSubmitError(null);
    setSuccessMessage(null);
    resetDraftConfirmation();
    setHoldingErrors((current) => ({
      ...current,
      ...Object.fromEntries(
        Object.entries(current).map(([id, errors]) => [
          id,
          field === "fundCode" && errors.fundCode?.includes("重复")
            ? { ...errors, fundCode: undefined }
            : errors,
        ]),
      ),
      [targetId]: { ...current[targetId], [field]: undefined },
    }));
    setHoldings((current) =>
      current.map((holding) =>
        holding.id === targetId ? { ...holding, [field]: value } : holding,
      ),
    );
  }

  function handleSnapshotDateChange(value: string) {
    setSnapshotDate(value);
    setSnapshotDateError(null);
    setSubmitError(null);
    setSuccessMessage(null);
    resetDraftConfirmation();
  }

  function handleCashChange(value: string) {
    setCashValue(value);
    setCashError(null);
    setSubmitError(null);
    setSuccessMessage(null);
    resetDraftConfirmation();
  }

  function handleAddHolding() {
    setHoldings((current) => [
      ...current,
      createEmptyHolding(createHoldingId()),
    ]);
    setSuccessMessage(null);
    resetDraftConfirmation();
  }

  function handleUseExampleSnapshot() {
    setCashValue("6000");
    setHoldings(
      exampleHoldings.map((holding, index) => ({
        ...holding,
        id: createHoldingId(`example-${index + 1}`),
      })),
    );
    setSnapshotDateError(null);
    setCashError(null);
    setHoldingErrors({});
    setSubmitError(null);
    setSuccessMessage(null);
    resetDraftConfirmation();
  }

  function handleRemoveHolding(targetId: string) {
    setHoldings((current) => {
      if (current.length === 1) {
        return current;
      }
      return current.filter((holding) => holding.id !== targetId);
    });
    setHoldingErrors((current) => {
      const next = { ...current };
      delete next[targetId];
      return next;
    });
    setSuccessMessage(null);
    resetDraftConfirmation();
  }

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitError(null);
    setSuccessMessage(null);

    if (!snapshotDate) {
      setSnapshotDateError("请选择快照日期。");
      setSubmitError("请先补齐快照日期，再检查草稿。");
      focusField("portfolio-snapshot-date");
      return;
    }

    const normalizedCashValue = parseAmountInput(cashValue);
    if (normalizedCashValue === null) {
      setCashError("请填写现金金额；没有现金留白可填 0。");
      setSubmitError("请先补齐现金金额，再检查草稿。");
      focusField("portfolio-cash-value");
      return;
    }
    if (normalizedCashValue < 0) {
      setCashError("现金金额需要是 0 或正数。");
      setSubmitError("请先修正标红字段，再检查草稿。");
      focusField("portfolio-cash-value");
      return;
    }

    const normalizedHoldings = holdings.map((holding) => ({
      fundCode: normalizeFundCode(holding.fundCode),
      fundName: holding.fundName.trim(),
      fundType: holding.fundType,
      marketValue: parseAmountInput(holding.marketValue),
    }));
    const codeToIds = new Map<string, string[]>();
    normalizedHoldings.forEach((holding, index) => {
      if (!holding.fundCode) {
        return;
      }
      const ids = codeToIds.get(holding.fundCode) ?? [];
      ids.push(holdings[index].id);
      codeToIds.set(holding.fundCode, ids);
    });
    const duplicateCodes = new Set(
      Array.from(codeToIds.entries())
        .filter(([, ids]) => ids.length > 1)
        .map(([code]) => code),
    );
    const nextErrors = holdings.reduce<Record<string, HoldingFieldErrors>>(
      (errors, holding, index) => {
        const normalized = normalizedHoldings[index];
        const fieldErrors: HoldingFieldErrors = {};
        if (normalized.fundCode.length === 0) {
          fieldErrors.fundCode = "请填写基金代码。";
        } else if (duplicateCodes.has(normalized.fundCode)) {
          fieldErrors.fundCode = `基金代码 ${normalized.fundCode} 已重复，请合并金额或删除其中一行。`;
        }
        if (normalized.fundName.length === 0) {
          fieldErrors.fundName = "请填写基金名称。";
        }
        if (normalized.marketValue === null || normalized.marketValue <= 0) {
          fieldErrors.marketValue = "当前市值需要大于 0，可输入 12000、12,000 或 1.2万。";
        }
        if (Object.keys(fieldErrors).length > 0) {
          errors[holding.id] = fieldErrors;
        }
        return errors;
      },
      {},
    );

    if (Object.keys(nextErrors).length > 0) {
      setHoldingErrors(nextErrors);
      setSubmitError(
        duplicateCodes.size > 0
          ? "请先合并或删除重复基金代码，再检查草稿。"
          : "请先补齐标红的持仓字段，再检查草稿。",
      );
      const firstErrorId = holdings.find((holding) => nextErrors[holding.id])?.id;
      const firstField = firstErrorId ? Object.keys(nextErrors[firstErrorId])[0] : null;
      if (firstErrorId && firstField) {
        focusField(`${firstErrorId}-${firstField}`);
      }
      return;
    }

    if (!readyToSave) {
      setSnapshotDateError(null);
      setCashError(null);
      setHoldingErrors({});
      setReadyToSave(true);
      window.requestAnimationFrame(() => {
        draftPreviewRef.current?.scrollIntoView({
          behavior: "smooth",
          block: "center",
        });
        draftPreviewRef.current?.focus({ preventScroll: true });
      });
      return;
    }

    setCashError(null);
    setHoldingErrors({});
    setReadyToSave(false);
    snapshotMutation.mutate({
      snapshotDate,
      cashValue: normalizedCashValue,
      holdings: normalizedHoldings.map((holding) => ({
        fundCode: holding.fundCode,
        fundName: holding.fundName,
        fundType: holding.fundType,
        marketValue: holding.marketValue ?? 0,
      })),
    });
  }

  if (fromAgent && latestReport) {
    return (
      <AgentPortfolioOnePage
        report={latestReport}
        reportPosture={reportPosture}
        reportCashRatio={reportCashRatio}
        evidencePathItems={evidencePathItems}
        isConcentrationFocus={isConcentrationFocus}
      />
    );
  }

  return (
    <div className="space-y-6">
      <section className="agent-hero overflow-hidden">
        <div className="grid gap-0 xl:grid-cols-[minmax(0,1fr)_minmax(360px,0.72fr)]">
          <div className="px-5 py-6 sm:px-6 lg:px-8 lg:py-8">
            <p className="section-kicker">组合画像结论</p>
            <h2 className="mt-4 max-w-3xl text-3xl font-semibold leading-tight sm:text-4xl lg:text-5xl">
              {latestReport ? reportPosture : "先建立组合画像，再谈配置。"}
            </h2>
            <p className="mt-5 max-w-3xl text-sm leading-7 text-[color:var(--ink-soft)] sm:text-base">
              {latestReport
                ? latestReport.summary
                : "组合页的主任务不是填写表单，而是把你的持仓结构解释成可回看的画像结论。快照录入会被放在次级区域，保存后再回到结论和证据路径。"}
            </p>
            <div className="mt-7 grid gap-3 sm:grid-cols-3">
              <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/66 px-4 py-3">
                <p className="text-xs font-black text-[color:var(--ink-muted)]">总资产</p>
                <p className="mt-2 text-lg font-semibold text-[color:var(--ink-strong)]">
                  {formatCurrency(latestReport?.totalValue ?? null)}
                </p>
              </div>
              <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/66 px-4 py-3">
                <p className="text-xs font-black text-[color:var(--ink-muted)]">现金留白</p>
                <p className="mt-2 text-lg font-semibold text-[color:var(--ink-strong)]">
                  {latestReport ? formatPercent(reportCashRatio) : "待生成"}
                </p>
              </div>
              <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/66 px-4 py-3">
                <p className="text-xs font-black text-[color:var(--ink-muted)]">最大持仓</p>
                <p className="mt-2 text-lg font-semibold text-[color:var(--ink-strong)]">
                  {largestReportHolding ? formatPercent(largestReportHolding.weight) : "待生成"}
                </p>
              </div>
            </div>
          </div>

          <aside className="signal-strip px-5 py-6 sm:px-6 xl:border-l xl:border-white/10">
            <p className="section-kicker">证据路径</p>
            <div className="flow-line mt-6 space-y-4">
              {evidencePathItems.map((item, index) => (
                <div key={item.label} className="flow-node">
                  <span className="flow-dot" />
                  <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/70 px-4 py-3">
                    <p className="text-xs font-black text-[color:var(--ink-muted)]">
                      0{index + 1} · {item.label}
                    </p>
                    <h3 className="mt-2 text-sm font-semibold leading-6 text-[color:var(--ink-strong)]">
                      {item.title}
                    </h3>
                    <p className="mt-1 text-xs leading-5 text-[color:var(--ink-soft)]">
                      {item.detail}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </aside>

          <div className="border-t border-[color:var(--line-soft)] bg-white/72 px-5 py-4 sm:px-6 lg:px-8 xl:col-span-2">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <p className="text-sm font-semibold leading-6 text-[color:var(--ink-strong)]">
                安全下一步：{latestReport ? "先解释这份报告，再决定是否需要更新快照。" : "先检查草稿权重，再生成第一份报告。"}
              </p>
              <div className="flex flex-wrap gap-2">
                <a href={latestReport ? "#portfolio-latest-report" : "#portfolio-snapshot-form"} className="action-button">
                  {latestReport ? "看证据" : "录入快照"}
                </a>
                <a href="#portfolio-snapshot-form" className="action-button-secondary">
                  更新快照
                </a>
              </div>
            </div>
          </div>
        </div>
      </section>

      {fromAgent ? (
        <section
          className="rounded-lg border border-[rgba(0,113,227,0.18)] bg-[linear-gradient(135deg,rgba(0,113,227,0.09),rgba(255,255,255,0.86))] px-5 py-4 shadow-sm sm:px-6"
          aria-label="来自 Agent 的持仓分析任务"
        >
          <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <p className="section-kicker">来自 Agent 的持仓分析</p>
              <h3 className="mt-2 text-lg font-semibold text-[color:var(--ink-strong)]">
                {isConcentrationFocus
                  ? "先看第一大持仓、资产分布和集中度。"
                  : "已根据刚才的问题打开组合分析结果。"}
              </h3>
              <p className="mt-1 text-sm leading-6 text-[color:var(--ink-soft)]">
                这里展示的是解释和检查路径，不是买卖或调仓指令；需要更新资料时仍要你确认保存。
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <a href="#portfolio-latest-report" className="action-button">
                看持仓报告
              </a>
              <Link href="/agent?new=1&focus=portfolio" className="action-button-secondary">
                回 Agent 追问
              </Link>
            </div>
          </div>
        </section>
      ) : null}

      <nav
        aria-label="组合页面快捷切换"
        className="sticky top-2 z-20 flex rounded-full border border-[color:var(--line-soft)] bg-white/86 p-1 shadow-[0_12px_28px_rgba(29,29,31,0.1)] backdrop-blur-2xl xl:hidden"
      >
        <a
          href="#portfolio-latest-report"
          className={
            latestReport
              ? "action-button flex-1 justify-center px-3"
              : "action-button-secondary flex-1 justify-center px-3"
          }
        >
          报告
        </a>
        <a
          href="#portfolio-snapshot-form"
          className={
            latestReport
              ? "action-button-secondary flex-1 justify-center px-3"
              : "action-button flex-1 justify-center px-3"
          }
        >
          录入
        </a>
        <a href="#portfolio-history" className="action-button-secondary flex-1 justify-center px-3">
          历史
        </a>
      </nav>

      <div className="grid gap-5">
        <div
          id="portfolio-snapshot-form"
          className={latestReport ? "order-2" : "order-1"}
        >
          <SectionBlock
            eyebrow="更新组合资料"
            title="先填现金，再填基金金额，保存前先预览权重。"
            description="新手可以先用示例生成一份草稿；这里不接实时行情，也不会生成交易指令。"
          >
          <form
            className="paper-panel-strong p-4 sm:p-5"
            onSubmit={handleSubmit}
          >
            <div className="grid gap-4 sm:grid-cols-2">
              <label className="space-y-2 text-sm">
                <span className="font-medium">快照日期</span>
                <input
                  id="portfolio-snapshot-date"
                  name="snapshotDate"
                  className="field-input"
                  type="date"
                  value={snapshotDate}
                  onChange={(event) => handleSnapshotDateChange(event.target.value)}
                  aria-invalid={Boolean(snapshotDateError)}
                  aria-describedby={
                    snapshotDateError ? "portfolio-snapshot-date-error" : undefined
                  }
                />
                {snapshotDateError ? (
                  <span
                    id="portfolio-snapshot-date-error"
                    className="text-xs text-red-700"
                  >
                    {snapshotDateError}
                  </span>
                ) : (
                  <span className="text-xs text-[color:var(--ink-muted)]">
                    这会成为一份可回看的历史快照日期。
                  </span>
                )}
              </label>
              <label className="space-y-2 text-sm">
                <span className="font-medium">现金金额（元）</span>
                <input
                  id="portfolio-cash-value"
                  name="cashValue"
                  className="field-input"
                  inputMode="decimal"
                  value={cashValue}
                  onChange={(event) => handleCashChange(event.target.value)}
                  placeholder="0、6,000 或 0.6万"
                  autoComplete="off"
                  aria-invalid={Boolean(cashError)}
                  aria-describedby={cashError ? "portfolio-cash-error" : undefined}
                />
                {cashError ? (
                  <span id="portfolio-cash-error" className="text-xs text-red-700">
                    {cashError}
                  </span>
                ) : (
                  <span className="text-xs text-[color:var(--ink-muted)]">
                    没有现金留白填 0；这里记录当前现金，不是收益率。
                  </span>
                )}
              </label>
            </div>

            <div className="mt-4 space-y-3">
              {holdings.map((holding, index) => (
                <div
                  key={holding.id}
                  className="rounded-lg border border-[color:var(--line-soft)] bg-white/55 p-3.5"
                >
                  <div className="flex items-center justify-between gap-3">
                    <p className="section-kicker">持仓 0{index + 1}</p>
                    {holdings.length > 1 ? (
                      <button
                        type="button"
                        className="action-button-secondary"
                        onClick={() => handleRemoveHolding(holding.id)}
                        disabled={snapshotMutation.isPending}
                      >
                        删除
                      </button>
                    ) : (
                      <span className="text-xs font-semibold text-[color:var(--ink-muted)]">
                        至少保留一行
                      </span>
                    )}
                  </div>

                  <div className="mt-4 grid gap-4 sm:grid-cols-2">
                    <label className="space-y-2 text-sm">
                      <span className="font-medium">基金代码</span>
                      <input
                        id={`${holding.id}-fundCode`}
                        name={`holdings.${index}.fundCode`}
                        className="field-input"
                        value={holding.fundCode}
                        onChange={(event) =>
                          handleHoldingChange(holding.id, "fundCode", event.target.value)
                        }
                        placeholder="例如：161725"
                        autoComplete="off"
                        aria-invalid={Boolean(holdingErrors[holding.id]?.fundCode)}
                        aria-describedby={
                          holdingErrors[holding.id]?.fundCode
                            ? `${holding.id}-fund-code-error`
                            : undefined
                        }
                      />
                      {holdingErrors[holding.id]?.fundCode ? (
                        <span
                          id={`${holding.id}-fund-code-error`}
                          className="text-xs text-red-700"
                        >
                          {holdingErrors[holding.id]?.fundCode}
                        </span>
                      ) : null}
                    </label>
                    <label className="space-y-2 text-sm">
                      <span className="font-medium">基金名称</span>
                      <input
                        id={`${holding.id}-fundName`}
                        name={`holdings.${index}.fundName`}
                        className="field-input"
                        value={holding.fundName}
                        onChange={(event) =>
                          handleHoldingChange(holding.id, "fundName", event.target.value)
                        }
                        placeholder="例如：招商中证白酒指数"
                        autoComplete="off"
                        aria-invalid={Boolean(holdingErrors[holding.id]?.fundName)}
                        aria-describedby={
                          holdingErrors[holding.id]?.fundName
                            ? `${holding.id}-fund-name-error`
                            : undefined
                        }
                      />
                      {holdingErrors[holding.id]?.fundName ? (
                        <span
                          id={`${holding.id}-fund-name-error`}
                          className="text-xs text-red-700"
                        >
                          {holdingErrors[holding.id]?.fundName}
                        </span>
                      ) : null}
                    </label>
                    <label className="space-y-2 text-sm">
                      <span className="font-medium">基金类型</span>
                      <select
                        id={`${holding.id}-fundType`}
                        name={`holdings.${index}.fundType`}
                        className="field-input"
                        value={holding.fundType}
                        onChange={(event) =>
                          handleHoldingChange(holding.id, "fundType", event.target.value)
                        }
                      >
                        {fundTypeOptions.map((option) => (
                          <option key={option.value} value={option.value}>
                            {option.label}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label className="space-y-2 text-sm">
                      <span className="font-medium">当前市值（元）</span>
                      <input
                        id={`${holding.id}-marketValue`}
                        name={`holdings.${index}.marketValue`}
                        className="field-input"
                        inputMode="decimal"
                        value={holding.marketValue}
                        onChange={(event) =>
                          handleHoldingChange(holding.id, "marketValue", event.target.value)
                        }
                        placeholder="例如：12000、12,000 或 1.2万"
                        autoComplete="off"
                        aria-invalid={Boolean(holdingErrors[holding.id]?.marketValue)}
                        aria-describedby={
                          holdingErrors[holding.id]?.marketValue
                            ? `${holding.id}-market-value-error`
                            : undefined
                        }
                      />
                      {holdingErrors[holding.id]?.marketValue ? (
                        <span
                          id={`${holding.id}-market-value-error`}
                          className="text-xs text-red-700"
                        >
                          {holdingErrors[holding.id]?.marketValue}
                        </span>
                      ) : (
                        <span className="text-xs text-[color:var(--ink-muted)]">
                          填当前市值，不是买入本金，也不是收益率。
                        </span>
                      )}
                    </label>
                  </div>
                </div>
              ))}
            </div>

            <div ref={draftPreviewRef} tabIndex={-1}>
              <DraftPreviewPanel preview={draftPreview} />
            </div>

            {readyToSave ? (
              <div
                className="mt-4 rounded-lg border border-[rgba(70,208,172,0.35)] bg-[rgba(70,208,172,0.12)] px-4 py-3 text-sm leading-6 text-[color:var(--ink-soft)]"
                role="status"
                aria-live="polite"
              >
                草稿已检查。确认总资产和权重无误后，再保存为一份新的历史快照。
              </div>
            ) : null}

            <div className="sticky bottom-3 z-20 mt-5 flex flex-wrap gap-3 rounded-2xl border border-[color:var(--line-soft)] bg-white/88 p-2 shadow-[0_14px_34px_rgba(29,29,31,0.12)] backdrop-blur-2xl md:static md:border-0 md:bg-transparent md:p-0 md:shadow-none md:backdrop-blur-none">
              <button
                type="button"
                className="action-button flex-1 justify-center md:flex-none"
                onClick={handleUseExampleSnapshot}
                disabled={snapshotMutation.isPending}
                aria-label="用示例覆盖当前输入"
              >
                用示例开始
              </button>
              <button
                type="button"
                className="action-button-secondary flex-1 justify-center md:flex-none"
                onClick={handleAddHolding}
                disabled={snapshotMutation.isPending}
                aria-label="添加一行持仓"
              >
                添加
              </button>
              <button
                type="submit"
                className="action-button-secondary flex-[1.4] justify-center md:flex-none"
                disabled={snapshotMutation.isPending}
              >
                {snapshotMutation.isPending
                  ? "正在生成报告…"
                  : readyToSave
                    ? "确认并保存快照"
                    : "检查草稿权重"}
              </button>
            </div>

            {successMessage ? (
              <div
                className="mt-4 rounded-lg border border-[rgba(70,208,172,0.35)] bg-[rgba(70,208,172,0.12)] px-4 py-3 text-sm text-[color:var(--ink-soft)]"
                role="status"
                aria-live="polite"
              >
                {successMessage}
              </div>
            ) : null}

            {submitError ? (
              <div
                className="mt-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
                role="alert"
              >
                {submitError}
              </div>
            ) : null}
          </form>
          </SectionBlock>
        </div>

        <div
          id="portfolio-latest-report"
          ref={latestReportRef}
          tabIndex={-1}
          className={latestReport ? "order-1" : "order-2"}
        >
          <SectionBlock
            eyebrow="最近报告"
            title={latestReport ? "最近一份组合体检报告" : "还没有组合报告"}
            description="报告分为配置分布、持仓权重、风险暴露和下一步动作。"
          >
          {latestReport ? (
            <div className="space-y-5">
              <div className="relative overflow-hidden rounded-lg border border-[color:var(--line-soft)] bg-white/76 p-4 text-[color:var(--ink-strong)] shadow-sm backdrop-blur-2xl">
                <div className="relative">
                  <div className="flex flex-wrap items-start justify-between gap-4">
                    <div className="max-w-2xl">
                      <p className="text-[11px] uppercase tracking-normal text-[color:var(--accent-teal)]">
                        快照 {formatDate(latestReport.snapshotDate)}
                      </p>
                      <h3 className="mt-2 font-serif text-2xl leading-tight">
                        {getReportPosture(latestReport)}
                      </h3>
                    </div>
                    <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/64 px-4 py-3 text-sm text-[color:var(--ink-soft)]">
                      <p>报告生成</p>
                      <p className="mt-1 font-semibold">
                        {formatDate(latestReport.generatedAt)}
                      </p>
                    </div>
                  </div>
                  <p className="mt-3 text-sm leading-7 text-[color:var(--ink-soft)]">
                    {latestReport.summary}
                  </p>
                </div>
              </div>

              <AllocationVisual report={latestReport} />

              <div className="grid gap-4 lg:grid-cols-2">
                <ReportSignalCard
                  title="风险暴露"
                  items={latestReport.riskExposure}
                  emptyText="暂无风险暴露说明。"
                  tone="moss"
                />
                <ReportSignalCard
                  title="集中度提示"
                  items={latestReport.concentrationFlags}
                  emptyText="暂无集中度提示。"
                  tone="clay"
                />
                <ReportSignalCard
                  title="配置平衡"
                  items={latestReport.allocationBalance}
                  emptyText="暂无配置平衡说明。"
                  tone="gold"
                />
                <ReportSignalCard
                  title="下一步动作"
                  items={latestReport.recommendedNextActions}
                  emptyText="暂无下一步动作。"
                  tone="ink"
                />
              </div>

              <div className="rounded-lg border border-[color:var(--line-soft)] bg-[linear-gradient(145deg,rgba(255,255,255,0.82),rgba(246,236,218,0.94))] p-4">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <p className="section-kicker">持仓权重</p>
                    <h3 className="mt-2 text-xl font-semibold">
                      每一只基金在组合里承担多少波动。
                    </h3>
                  </div>
                  <span className="rounded-full border border-[color:var(--line-soft)] bg-white/75 px-3 py-1 text-xs text-[color:var(--ink-soft)]">
                    {latestReport.holdings.length} 只基金
                  </span>
                </div>
                <div className="mt-5">
                  <HoldingWeightRows holdings={latestReport.holdings} compactOnMobile />
                </div>
              </div>
            </div>
          ) : (
            <div className="paper-panel-strong rounded-lg px-5 py-5 text-sm leading-7 text-[color:var(--ink-soft)]">
              先录入第一份持仓快照。生成后，这里会显示最新报告，工作台也会同步看到组合体检状态。
            </div>
          )}
          </SectionBlock>
        </div>
      </div>

      <div className="hidden gap-3 sm:grid sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label="最近报告"
          value={latestReport ? "已生成" : "待录入"}
          detail="每次提交都会生成一份新的持仓快照和分析记录。"
          accent="moss"
        />
        <MetricCard
          label="最近估值"
          value={formatCurrency(latestReport?.totalValue ?? null)}
          detail="总资产按现金加持仓快照估算，不接实时行情。"
          accent="gold"
        />
        <MetricCard
          label="最近快照"
          value={formatDate(latestReport?.snapshotDate ?? null)}
          detail="后续教练会优先参考最近一份组合报告。"
          accent="clay"
        />
        <MetricCard
          label="历史记录"
          value={`${historyItems.length} 份`}
          detail="每次录入都保留历史，不用最新状态覆盖一切。"
          accent="moss"
        />
      </div>

      <div id="portfolio-history">
        <details className="paper-panel-strong rounded-lg px-4 py-4 sm:hidden">
          <summary className="cursor-pointer text-sm font-semibold text-[color:var(--ink-strong)]">
            历史快照（{historyItems.length} 份）
          </summary>
          <div className="mt-4 space-y-3">
            {historyItems.length > 0 ? (
              historyItems.map((item) => (
                <div
                  key={item.snapshotId}
                  className="rounded-lg border border-[color:var(--line-soft)] bg-white/72 px-4 py-3"
                >
                  <p className="section-kicker">{formatDate(item.snapshotDate)}</p>
                  <p className="mt-2 text-sm leading-6 text-[color:var(--ink-soft)]">
                    {item.summary}
                  </p>
                  <p className="mt-2 text-sm font-semibold text-[color:var(--ink-strong)]">
                    {formatCurrency(item.totalValue)}
                  </p>
                </div>
              ))
            ) : (
              <p className="text-sm leading-7 text-[color:var(--ink-soft)]">
                还没有历史快照。第一份报告生成后，这里会开始积累记录。
              </p>
            )}
          </div>
        </details>

        <div className="hidden sm:block">
          <SectionBlock
            eyebrow="历史记录"
            title="历史快照"
            description="每次录入都会保留一份历史，便于对照结构变化。"
          >
            <div className="space-y-3">
          {historyItems.length > 0 ? (
            historyItems.map((item) => (
              <div
                key={item.snapshotId}
                className="paper-panel-strong rounded-lg px-5 py-4"
              >
                <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                  <div>
                    <p className="section-kicker">{formatDate(item.snapshotDate)}</p>
                    <p className="mt-2 text-sm leading-7 text-[color:var(--ink-soft)]">
                      {item.summary}
                    </p>
                  </div>
                  <div className="text-sm text-[color:var(--ink-soft)]">
                    {formatCurrency(item.totalValue)}
                  </div>
                </div>
              </div>
            ))
          ) : (
            <div className="paper-panel-strong rounded-lg px-5 py-4 text-sm leading-7 text-[color:var(--ink-soft)]">
              还没有历史快照。第一份报告生成后，这里会开始积累可回看的组合记录。
            </div>
          )}
            </div>
          </SectionBlock>
        </div>
      </div>
    </div>
  );
}
