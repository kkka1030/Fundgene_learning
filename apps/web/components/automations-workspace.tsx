"use client";

import { useState } from "react";
import Link from "next/link";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  BellRing,
  CalendarClock,
  Clock3,
  ListChecks,
  Newspaper,
  ShieldCheck,
  type LucideIcon,
} from "lucide-react";

import {
  ApiError,
  getAutomationsState,
  runAutomationNow,
  updateAutomationSetting,
  type AutomationItem,
  type AutomationKey,
  type AutomationListState,
  type AutomationRunResult,
} from "@/lib/api";
import {
  Button,
  ErrorState,
  LoadingState,
  Panel,
  StatusPill,
} from "./ui/primitives";

const automationIcons: Record<AutomationKey, LucideIcon> = {
  daily_brief: CalendarClock,
  weekly_portfolio: ShieldCheck,
  news_watch: Newspaper,
  behavior_observation: BellRing,
};

function isApiError(error: unknown): error is ApiError {
  return error instanceof ApiError;
}

function formatDateTime(value: string | null): string {
  if (!value) {
    return "等待首次生成";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("zh-CN", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function getBriefCoverageCount(state: AutomationListState | undefined): number {
  const coverage = state?.dailyBriefSummary?.sourceCoverage;
  if (!coverage) {
    return 0;
  }

  return Object.values(coverage).filter(Boolean).length;
}

function statusTone(item: AutomationItem): "positive" | "warning" | "neutral" {
  if (item.status === "ready") {
    return "positive";
  }
  if (item.status === "disabled") {
    return "warning";
  }
  return "neutral";
}

function statusLabel(item: AutomationItem): string {
  if (item.status === "ready") {
    return "可运行";
  }
  if (item.status === "disabled") {
    return "待授权";
  }
  if (item.status === "needs_profile") {
    return "需补齐资料";
  }
  return "暂不可用";
}

const pendingRunSteps: AutomationRunResult["steps"] = [
  {
    key: "submit_run",
    label: "接收运行请求",
    status: "completed",
    detail: "已收到这次手动检查请求。",
  },
  {
    key: "read_context",
    label: "读取授权上下文",
    status: "running",
    detail: "正在读取画像、组合、新闻、学习和训练状态。",
  },
  {
    key: "build_brief",
    label: "整理判断和安全边界",
    status: "queued",
    detail: "接下来会生成可解释结论，不生成交易动作。",
  },
  {
    key: "prepare_writeback",
    label: "准备待确认建议",
    status: "queued",
    detail: "需要保存到长期资料的内容会先进入待确认区。",
  },
  {
    key: "notify_user",
    label: "保存通知和检查记录",
    status: "queued",
    detail: "完成后会在本页留下通知和结果。",
  },
];

function runStepLabel(status: AutomationRunResult["steps"][number]["status"]): string {
  if (status === "running") {
    return "进行中";
  }
  if (status === "queued") {
    return "等待";
  }
  if (status === "failed") {
    return "失败";
  }
  return "完成";
}

function getPayloadText(payload: AutomationRunResult["outputPayload"], key: string): string | null {
  const value = payload[key];
  return typeof value === "string" && value.trim() ? value : null;
}

function getPrimaryActionLabel(payload: AutomationRunResult["outputPayload"]): string | null {
  const value = payload.primary_action;
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const label = (value as Record<string, unknown>).label;
  return typeof label === "string" && label.trim() ? label : null;
}

function shortNotificationMessage(message: string): string {
  return message.length > 48 ? `${message.slice(0, 48)}...` : message;
}

function AutomationRunPanel({
  result,
  pending,
}: {
  result: AutomationRunResult | null;
  pending: boolean;
}) {
  const steps = pending ? pendingRunSteps : (result?.steps ?? []);
  if (!pending && !result) {
    return null;
  }
  const headline = result ? getPayloadText(result.outputPayload, "headline") : null;
  const safety = result ? getPayloadText(result.outputPayload, "safety_boundary") : null;
  const primaryAction = result ? getPrimaryActionLabel(result.outputPayload) : null;

  return (
    <div
      data-testid={`automation-run-timeline-${result?.automationKey ?? "pending"}`}
      className="automation-run-compact"
    >
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <p className="section-kicker">检查过程</p>
          <h3 className="mt-1 text-base font-semibold text-[color:var(--ink-strong)]">
            {pending ? "正在执行本轮自动任务" : result?.summary ?? "本轮自动任务已完成"}
          </h3>
        </div>
        <StatusPill tone={result?.status === "failed" ? "warning" : "positive"}>
          {pending ? "运行中" : result?.status === "failed" ? "失败" : "已完成"}
        </StatusPill>
      </div>
      <ol className="automation-run-steps">
        {steps.map((step, index) => (
          <li
            key={step.key}
            data-testid="automation-run-step"
          >
            <span>
              {index + 1}
            </span>
            <div>
              <div className="flex flex-wrap items-center gap-1.5">
                <p>
                  {step.label}
                </p>
                <em>
                  {runStepLabel(step.status)}
                </em>
              </div>
              <small>
                {step.detail}
              </small>
            </div>
          </li>
        ))}
      </ol>
      {result ? (
        <div
          data-testid={`automation-run-result-${result.automationKey}`}
          className="automation-run-result"
        >
          {headline ? <p><strong>本轮判断</strong>{headline}</p> : null}
          {primaryAction ? <p>安全下一步：{primaryAction}</p> : null}
          <p>待确认建议：{result.createdPendingProposalIds.length} 条</p>
          {safety ? <p>安全边界：{safety}</p> : null}
          {result.errorMessage ? (
            <p className="font-semibold text-red-700">错误：{result.errorMessage}</p>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

export function AutomationsWorkspace() {
  const queryClient = useQueryClient();
  const [operationError, setOperationError] = useState<string | null>(null);
  const [runResults, setRunResults] = useState<
    Partial<Record<AutomationKey, AutomationRunResult>>
  >({});

  const automationsQuery = useQuery({
    queryKey: ["automations-state"],
    queryFn: getAutomationsState,
    retry: false,
  });

  const updateMutation = useMutation({
    mutationFn: updateAutomationSetting,
    onMutate: () => setOperationError(null),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["automations-state"] });
      await queryClient.invalidateQueries({ queryKey: ["profile-context"] });
    },
    onError: (error) => {
      setOperationError(
        isApiError(error) ? error.message : "自动任务设置更新失败。",
      );
    },
  });

  const runMutation = useMutation({
    mutationFn: runAutomationNow,
    onMutate: (automationKey) => {
      setOperationError(null);
      setRunResults((current) => {
        const next = { ...current };
        delete next[automationKey];
        return next;
      });
    },
    onSuccess: async (result) => {
      setRunResults((current) => ({
        ...current,
        [result.automationKey]: result,
      }));
      await queryClient.invalidateQueries({ queryKey: ["automations-state"] });
      await queryClient.invalidateQueries({ queryKey: ["profile-context"] });
      await queryClient.invalidateQueries({
        queryKey: ["profile-pending-proposals"],
      });
    },
    onError: (error) => {
      setOperationError(
        isApiError(error) ? error.message : "自动任务暂时无法运行。",
      );
    },
  });

  const state = automationsQuery.data;
  const isLoading = automationsQuery.isLoading;
  const error = automationsQuery.error;
  const dailyBrief = state?.dailyBriefSummary ?? null;

  const toggleAutomation = (item: AutomationItem) => {
    updateMutation.mutate({
      automationKey: item.key,
      enabled: !item.enabled,
    });
  };

  const updateCadence = (item: AutomationItem, cadenceKey: string) => {
    updateMutation.mutate({
      automationKey: item.key,
      cadenceKey,
    });
  };

  return (
    <div className="automation-command-page command-single-page">
      <div className="command-page-heading">
        <div className="min-w-0">
          <p className="section-kicker">自动任务</p>
          <h1>控制定期检查什么</h1>
          <p>只做读取、整理和待确认建议；关键变化由你决定是否保存。</p>
        </div>
        <StatusPill tone="accent">安全边界：不会连接券商</StatusPill>
      </div>

      {isLoading ? (
        <LoadingState>正在读取自动任务授权...</LoadingState>
      ) : error ? (
        <ErrorState>
          {isApiError(error) && error.status === 401
            ? "请先登录后再管理自动任务。"
            : "自动任务状态暂时无法读取。"}
        </ErrorState>
      ) : state ? (
        <div className="automation-command-grid">
          <Panel variant="strong" className="automation-summary-panel">
            <div className="min-w-0">
              <StatusPill tone="positive">默认今日简报</StatusPill>
              <h2>{dailyBrief?.headline ?? "每日简报会先替你完成第一轮判断。"}</h2>
              <p>
                {dailyBrief?.beginnerExplanation ??
                  "当资料足够时，系统会把组合、资讯、行为和学习状态压缩成一个安全下一步。"}
              </p>
              {operationError ? (
                <p className="command-inline-error">{operationError}</p>
              ) : null}
            </div>
            <div className="automation-summary-metrics">
              <div>
                <span>已开启</span>
                <strong>{state.activeCount}/{state.totalCount}</strong>
              </div>
              <div>
                <span>上次简报</span>
                <strong>{formatDateTime(dailyBrief?.asOf ?? null)}</strong>
              </div>
              <div>
                <span>覆盖来源</span>
                <strong>{getBriefCoverageCount(state)}/7</strong>
              </div>
            </div>
          </Panel>

          <Panel className="automation-list-panel">
            <div className="command-panel-heading">
              <div>
                <p className="section-kicker">授权任务</p>
                <h2>四个后台检查</h2>
              </div>
              <StatusPill tone="neutral">分析优先</StatusPill>
            </div>
            <div className="automation-task-list">
          {state.automations.map((item) => {
            const Icon = automationIcons[item.key];
            const isUpdating =
              updateMutation.isPending &&
              updateMutation.variables?.automationKey === item.key;
            const isRunning =
              runMutation.isPending && runMutation.variables === item.key;

            return (
              <article
                key={item.key}
                  className={`automation-task-row ${item.enabled ? "automation-task-row-active" : ""}`}
              >
                  <div className="automation-task-title">
                    <span className="automation-task-icon">
                    <Icon
                      aria-hidden="true"
                      className="size-5 text-[color:var(--accent-teal)]"
                    />
                  </span>
                    <div className="min-w-0">
                      <div className="flex min-w-0 flex-wrap items-center gap-2">
                        <h3>
                    {item.title}
                        </h3>
                  <StatusPill tone={statusTone(item)}>{statusLabel(item)}</StatusPill>
                      </div>
                      <p>{item.summary}</p>
                    </div>
                </div>
                  <label className="automation-cadence-control">
                    <span>运行频率</span>
                  <select
                    value={item.cadenceKey}
                    onChange={(event) => updateCadence(item, event.target.value)}
                    disabled={!item.enabled || isUpdating}
                      className="field-input command-select disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {item.cadenceOptions.map((option) => (
                      <option key={option.key} value={option.key}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
                  <div className="automation-chip-block automation-read-scope">
                    <span>会读取</span>
                    <div>
                      {item.readScope.slice(0, 3).map((entry) => (
                        <StatusPill key={entry} tone="neutral">{entry}</StatusPill>
                      ))}
                      {item.readScope.length > 3 ? (
                        <StatusPill tone="neutral">+{item.readScope.length - 3}</StatusPill>
                      ) : null}
                  </div>
                  </div>
                  <div className="automation-chip-block automation-output-scope">
                    <span>会生成</span>
                    <div>
                      {item.outputScope.slice(0, 3).map((entry) => (
                        <StatusPill key={entry} tone="accent">{entry}</StatusPill>
                      ))}
                      {item.outputScope.length > 3 ? (
                        <StatusPill tone="accent">+{item.outputScope.length - 3}</StatusPill>
                      ) : null}
                  </div>
                  </div>
                  <div className="automation-row-actions">
                    <button
                      type="button"
                      role="switch"
                      aria-checked={item.enabled}
                      data-testid={`automation-toggle-${item.key}`}
                      disabled={isUpdating}
                      onClick={() => toggleAutomation(item)}
                      className="command-switch"
                    >
                      <span>{item.enabled ? "已开启" : "未开启"}</span>
                      <i aria-hidden="true" />
                    </button>
                    <Button
                      type="button"
                      variant="secondary"
                      className="automation-run-button"
                      data-testid={`automation-run-button-${item.key}`}
                      disabled={!item.canRunNow || isRunning}
                      onClick={() => runMutation.mutate(item.key)}
                    >
                      {isRunning ? "运行中" : "立即运行"}
                    </Button>
                </div>
                  <details className="automation-boundary-details">
                    <summary>边界</summary>
                    <p>确认边界：{item.confirmationBoundary}</p>
                    <p>安全边界：{item.safetyBoundary}</p>
                    {item.lastRun ? (
                      <p>最近运行：{item.lastRun.summary ?? "已记录运行结果。"}</p>
                    ) : null}
                    {item.disabledReason ? <p>{item.disabledReason}</p> : null}
                  </details>
                  {isRunning || runResults[item.key] ? (
                    <AutomationRunPanel
                      pending={isRunning}
                      result={runResults[item.key] ?? null}
                    />
                  ) : null}
              </article>
            );
          })}
            </div>
          </Panel>

          <Panel className="automation-side-panel">
            <div className="command-panel-heading">
              <div>
                <p className="section-kicker">运行队列</p>
                <h2>下一次检查</h2>
              </div>
              <Clock3 aria-hidden="true" className="size-5 text-[color:var(--accent-teal)]" />
            </div>
            <ol className="command-side-list">
              {state.nextQueue.slice(0, 3).map((item, index) => (
                <li key={item.automationKey}>
                  <span>{index + 1}</span>
                  <div>
                    <strong>{item.title}</strong>
                    <p>{formatDateTime(item.nextRunAt)}，{item.cadenceLabel}</p>
                  </div>
                </li>
              ))}
              {state.nextQueue.length === 0 ? (
                <li className="command-empty-line">暂无已授权的下一次自动任务。</li>
              ) : null}
            </ol>

            <div className="automation-notification-block">
              <div className="command-panel-heading">
                <div>
                  <p className="section-kicker">最近通知</p>
                  <h2>{state.recentNotifications.length} 条</h2>
                </div>
                <BellRing aria-hidden="true" className="size-5 text-[color:var(--accent-teal)]" />
              </div>
              <div className="command-side-list">
                {state.recentNotifications.slice(0, 2).map((notification) => (
                  <div key={notification.id} className="command-notice">
                    <strong>{notification.title}</strong>
                    <p>{shortNotificationMessage(notification.message)}</p>
                    {notification.actionRoute && notification.actionLabel ? (
                      <Link href={notification.actionRoute}>{notification.actionLabel}</Link>
                    ) : null}
                  </div>
                ))}
                {state.recentNotifications.length === 0 ? (
                  <p className="command-empty-line">
                    自动任务完成后会在这里留下通知和确认入口。
                  </p>
                ) : null}
              </div>
            </div>

            <div className="automation-safety-card">
              <div className="flex items-center gap-2">
                <ListChecks aria-hidden="true" className="size-4 text-[color:var(--accent-teal)]" />
                <strong>安全边界</strong>
              </div>
              <p>
                自动任务只负责读取、分析、提醒和准备待确认建议。FundGene 不会连接券商、不执行买卖、不生成仓位指令，也不会在你未确认前改写关键画像。
              </p>
              <Link href="/profile">查看授权记录</Link>
            </div>
          </Panel>
        </div>
      ) : null}
    </div>
  );
}
