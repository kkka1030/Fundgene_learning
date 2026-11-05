"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import {
  Activity,
  ArrowRight,
  Bot,
  CircleStop,
  History,
  ListChecks,
  SendHorizontal,
  ShieldCheck,
  Sparkles,
} from "lucide-react";

import {
  ApiError,
  cancelAgentRun,
  discardSessionQueuedFollowUp,
  getActiveAgentRuns,
  getAgentRunEvents,
  getAgentRunStatus,
  getAgentRunTrace,
  getAssistantConversation,
  getAssistantSession,
  getAssistantSessions,
  getBehaviorProfile,
  getDashboardState,
  getSessionQueuedFollowUp,
  getSessionUser,
  markSessionQueuedFollowUpSubmitted,
  queueAgentRunFollowUp,
  streamAssistantMessage,
  type AdvisorActionTarget,
  type ActiveAgentRuns,
  type AgentRunEvent,
  type AgentRunEvents,
  type AgentRunStatus,
  type AgentRunTrace,
  type AssistantConversationMessage,
  type AssistantConversationState,
  type AssistantSessionSummary,
  type AdvisorStructuredResponse,
  type DashboardDailyBrief,
  type DashboardState,
  type QueuedFollowUp,
  type SafeNextAction,
} from "@/lib/api";
import { formatProductCopy, formatUserVisibleCopy } from "@/lib/display-labels";
import { MetricCard } from "./metric-card";
import { SectionBlock } from "./section-block";
import { cn, StatusBadge } from "./ui/primitives";

const starterPrompts = [
  "我刚开始买基金，怎么理解风险等级和回撤？",
  "如果我总想追涨，FundGene 应该怎么帮我拆解这个问题？",
  "帮我检查这份组合里最需要关注的风险来源。",
];

function formatIntent(intent: string): string {
  if (intent === "learning") {
    return "基金基础";
  }
  if (intent === "behavior") {
    return "行为偏差";
  }
  if (intent === "portfolio") {
    return "组合解释";
  }
  if (intent === "simulation") {
    return "情境演练";
  }
  if (intent === "news") {
    return "新闻解读";
  }
  return intent;
}

function formatTimestamp(value: string | null): string {
  if (!value) {
    return "刚刚";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "刚刚";
  }

  return new Intl.DateTimeFormat("zh-CN", {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function findPreviousUserQuestion(
  messages: AssistantConversationMessage[],
  currentIndex: number,
): string {
  for (let index = currentIndex - 1; index >= 0; index -= 1) {
    const item = messages[index];
    if (item.role === "user") {
      return item.content;
    }
  }

  return "本轮问题";
}

function isApiError(error: unknown): error is ApiError {
  return error instanceof ApiError;
}

function buildUserFollowUpPrompts(
  response: AdvisorStructuredResponse,
  question: string,
): string[] {
  const context = `${question} ${response.answer}`.toLowerCase();

  if (response.intent === "portfolio") {
    return [
      "帮我检查组合里最需要先看的风险。",
      "帮我判断我的持仓是不是太集中。",
    ];
  }

  if (response.intent === "behavior") {
    return [
      "帮我拆解我追涨时的触发点。",
      "帮我做一个本周可执行的行为复盘。",
    ];
  }

  if (response.intent === "simulation") {
    return [
      "帮我选择一个适合新手的情境训练。",
      "把这次训练复盘成下一次检查清单。",
    ];
  }

  if (response.intent === "news") {
    return [
      "把这条信息拆成事实、影响路径和不确定性。",
      "帮我判断它和我的基金类型有什么关系。",
    ];
  }

  if (context.includes("回撤") || context.includes("drawdown")) {
    return [
      "我想用一个数字例子理解回撤。",
      "帮我判断我能承受多大回撤。",
    ];
  }

  if (context.includes("风险等级") || context.includes("risk")) {
    return [
      "帮我把风险等级翻译成新手能懂的话。",
      "帮我判断这个风险等级和我的承受能力是否匹配。",
    ];
  }

  return [
    "帮我把这个概念讲成一个新手例子。",
    "我学完这个概念后下一步该练什么？",
  ];
}

function inferActionTarget(action: string): AdvisorActionTarget {
  const normalized = action.toLowerCase();
  if (normalized.includes("dashboard") || action.includes("工作台")) {
    return {
      label: "查看今日简报",
      href: "/today",
      intent: "dashboard",
      kind: "internal_link",
    };
  }
  if (
    normalized.includes("learning") ||
    action.includes("学习") ||
    action.includes("课程")
  ) {
    return {
      label: "进入学习中心",
      href: "/learning",
      intent: "learning",
      kind: "internal_link",
    };
  }
  if (
    normalized.includes("portfolio") ||
    action.includes("组合") ||
    action.includes("持仓")
  ) {
    return {
      label: "查看组合体检",
      href: "/portfolio?from=agent&focus=concentration",
      intent: "portfolio",
      kind: "internal_link",
    };
  }
  if (
    normalized.includes("simulation") ||
    action.includes("情境") ||
    action.includes("训练")
  ) {
    return {
      label: "开始情境训练",
      href: "/simulation",
      intent: "simulation",
      kind: "internal_link",
    };
  }
  if (
    normalized.includes("news") ||
    action.includes("资讯") ||
    action.includes("新闻") ||
    action.includes("政策")
  ) {
    return {
      label: "查看资讯解读",
      href: "/news",
      intent: "news",
      kind: "internal_link",
    };
  }
  return {
    label: "继续交给 Agent",
    href: "/agent",
    intent: "coach",
    kind: "internal_link",
  };
}

function getQueryValue(searchParams: URLSearchParams, keys: string[]): string | null {
  for (const key of keys) {
    const value = searchParams.get(key);
    if (value && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function buildContextPrompt(searchParams: URLSearchParams): string | null {
  const focus = getQueryValue(searchParams, ["focus"]);

  if (focus === "news") {
    return "请结合我的组合，解释这条资讯可能影响什么、哪些地方不能当成买卖信号？";
  }

  if (focus === "simulation") {
    return "请帮我复盘这次模拟训练里最容易失守的节点，并告诉我下次行动前要先检查什么。";
  }

  if (focus === "learning") {
    return "请用一个简单例子帮我理解这节内容，并告诉我怎么用到我的基金决策里。";
  }

  if (focus === "portfolio") {
    return "请帮我先检查这份组合里最需要关注的风险来源，并说明下一步只该做什么安全检查。";
  }

  if (focus === "daily-brief") {
    return "请解释今天这条判断为什么和我有关，以及我下一步应该先检查什么。";
  }

  return null;
}

function buildSubmittedMessage({
  draft,
  contextPrompt,
  contextDisplayMessage,
}: {
  draft: string;
  contextPrompt: string;
  contextDisplayMessage: string;
}): string {
  const normalizedDraft = draft.trim();
  if (
    contextDisplayMessage.trim().length >= 4 &&
    contextPrompt.trim().length > 0 &&
    normalizedDraft === contextPrompt.trim()
  ) {
    return contextDisplayMessage.trim();
  }
  return normalizedDraft;
}

function buildSafeActionHref(action: SafeNextAction): string {
  const routeAliases: Record<string, string> = {
    "/dashboard": "/today",
    "/coach": "/agent",
  };
  const rawRoute = action.targetRoute.startsWith("/") ? action.targetRoute : "/today";
  const route = routeAliases[rawRoute] ?? rawRoute;
  const params = new URLSearchParams();

  Object.entries(action.targetParams).forEach(([key, value]) => {
    if (value) {
      params.set(key, value === "dashboard" ? "today" : value);
    }
  });

  const query = params.toString();
  return query ? `${route}?${query}` : route;
}

function buildDailyBriefCoachPrompts(brief: DashboardDailyBrief | null): string[] {
  if (!brief) {
    return starterPrompts;
  }

  const prompts = [
    `请解释今天这个判断：${brief.headline}`,
    "帮我检查这份组合里最需要关注的风险来源。",
    `我执行“${brief.primaryAction.label}”前，应该先检查什么？`,
  ];

  return prompts.map((item) => formatProductCopy(item));
}

function formatWritebackLabel(action: SafeNextAction): string {
  const labels: Record<SafeNextAction["type"], string> = {
    ask_coach: "让教练继续拆解",
    inspect_portfolio: "进入组合体检",
    learn: "进入学习中心",
    read_news_context: "查看资讯影响",
    record_behavior: "记录为待确认线索",
    run_simulation: "开始情境训练",
  };

  return labels[action.type] ?? action.label;
}

type RunStepStatus = "done" | "active" | "waiting";

function getRunProgressSteps({
  trace,
  pending,
}: {
  trace: AgentRunTrace | undefined;
  pending: boolean;
}): Array<{ label: string; detail: string; status: RunStepStatus }> {
  const hasTrace = Boolean(trace);
  const isCompleted = trace?.run.runStatus === "completed";
  const isCancelled = trace?.run.runStatus === "cancelled";
  const hasSources = (trace?.toolCalls.length ?? 0) > 0 || (trace?.evidenceRefs.length ?? 0) > 0;

  return [
    {
      label: "理解任务意图",
      detail: pending || hasTrace ? "已把问题转成工作目标" : "等待你布置任务",
      status: pending || hasTrace ? "done" : "waiting",
    },
    {
      label: "读取授权上下文",
      detail: pending || hasTrace ? "画像、今日简报和最近对话已接入" : "新任务开始后读取",
      status: pending || hasTrace ? "done" : "waiting",
    },
    {
      label: "检查相关来源",
      detail: hasSources
        ? `已整理 ${trace?.evidenceRefs.length ?? 0} 条可解释依据`
        : pending
          ? "正在筛选组合、学习、资讯或训练线索"
          : "按任务需要读取",
      status: hasSources ? "done" : pending ? "active" : "waiting",
    },
    {
      label: "生成安全下一步",
      detail: isCancelled
        ? "本轮已停止，未形成新的行动建议"
        : isCompleted
          ? "已返回解释、边界和行动入口"
          : pending
            ? "正在组织回答"
            : "等待完成前置检查",
      status: isCompleted || isCancelled ? "done" : pending ? "active" : "waiting",
    },
  ];
}

function getLiveRunProgressSteps(
  events: AgentRunEvent[],
): Array<{ label: string; detail: string; status: RunStepStatus }> {
  return events
    .filter((event) =>
      [
        "turn_started",
        "step_started",
        "step_completed",
        "tool_call_started",
        "tool_call_completed",
        "input_queued",
        "input_submitted",
        "input_discarded",
        "turn_cancel_requested",
        "turn_aborted",
        "turn_interrupted",
        "turn_error",
        "agent_message",
        "turn_closed",
        "turn_complete",
      ].includes(event.eventType),
    )
    .slice(-8)
    .map((event) => {
      const isRunning =
        event.status === "running" ||
        event.status === "cancelling" ||
        event.eventType.endsWith("_started");
      return {
        label: event.title,
        detail: formatLiveRunEventDetail(event),
        status: isRunning ? "active" : "done",
      };
    });
}

function mergeAgentRunEvents(
  replayEvents: AgentRunEvent[] | undefined,
  liveEvents: AgentRunEvent[],
  expectedRunId?: string | null,
): AgentRunEvent[] {
  const targetRunId =
    expectedRunId ??
    liveEvents.at(-1)?.runId ??
    replayEvents?.at(-1)?.runId ??
    null;
  const eventsByKey = new Map<string, AgentRunEvent>();

  [...(replayEvents ?? []), ...liveEvents].forEach((event) => {
    if (targetRunId && event.runId !== targetRunId) {
      return;
    }
    const key = event.id || `${event.runId}:${event.sequence}`;
    eventsByKey.set(key, event);
  });

  return Array.from(eventsByKey.values()).sort((left, right) => {
    const sequenceDelta = left.sequence - right.sequence;
    if (sequenceDelta !== 0) {
      return sequenceDelta;
    }
    return (left.at ?? "").localeCompare(right.at ?? "");
  });
}

function formatLiveRunEventDetail(event: AgentRunEvent): string {
  if (event.eventType === "turn_started") {
    return "已接收任务，正在建立本轮上下文";
  }
  if (event.eventType === "tool_call_started") {
    return "正在读取授权范围内的资料";
  }
  if (event.eventType === "tool_call_completed") {
    return event.durationMs !== null
      ? `资料读取完成，用时 ${event.durationMs}ms`
      : "资料读取完成";
  }
  if (event.eventType === "agent_message") {
    return event.status === "cancelled"
      ? "停止说明已经同步到会话"
      : "回答已经生成，正在同步会话";
  }
  if (event.eventType === "input_queued") {
    return "已收到下一句，会等本轮完成后再发送";
  }
  if (event.eventType === "input_submitted") {
    return "排队内容已进入下一轮整理";
  }
  if (event.eventType === "input_discarded") {
    return "排队内容已取消或被新的下一句替换";
  }
  if (event.eventType === "turn_complete") {
    return "本轮任务已完成";
  }
  if (event.eventType === "turn_closed") {
    if (event.status === "interrupted") {
      return "本轮已中断，未作为完成判断处理";
    }
    return "本轮已停止，未作为正常完成处理";
  }
  if (event.eventType === "turn_interrupted") {
    return "上次整理中断了，可以重新发送问题";
  }
  if (event.eventType === "turn_cancel_requested") {
    return "已收到停止请求，正在结束当前步骤";
  }
  if (event.eventType === "turn_aborted") {
    return "本轮整理已停止，没有替你确认长期记录";
  }
  if (event.eventType === "turn_error") {
    return "这次整理没有顺利完成，可以稍后重试";
  }
  if (event.eventType === "step_started") {
    return "正在处理这一步";
  }
  return event.durationMs !== null
    ? `这一步已完成，用时 ${event.durationMs}ms`
    : "这一步已完成";
}

function getRunStatusLabel(status: AgentRunStatus | undefined): string | null {
  if (!status) {
    return null;
  }
  if (status.status === "cancelled") {
    return "已停止";
  }
  if (status.status === "interrupted") {
    return "已中断";
  }
  if (status.status === "failed") {
    return "需要重试";
  }
  if (status.cancelRequested || status.status === "cancelling") {
    return "正在停止";
  }
  if (status.active || status.status === "running") {
    return "正在整理";
  }
  if (status.status === "completed") {
    return "已完成";
  }
  return "已同步";
}

function getRunStatusMessage(status: AgentRunStatus | undefined): string | null {
  if (!status) {
    return null;
  }
  if (status.status === "cancelled") {
    return "本轮整理已停止，没有替你确认长期记录。";
  }
  if (status.status === "interrupted") {
    return "上次整理中断了，没有形成完整结论，可以重新发送。";
  }
  if (status.status === "failed") {
    return "这次整理没有顺利完成，可以调整问题后重试。";
  }
  if (status.cancelRequested || status.status === "cancelling") {
    return "已收到停止请求，会在当前步骤结束后收束。";
  }
  return status.message;
}

function getLatestActiveRun(runs: ActiveAgentRuns | undefined): AgentRunStatus | null {
  if (!runs || runs.runs.length === 0) {
    return null;
  }
  return runs.runs[0] ?? null;
}

function shouldAutoSubmitQueuedFollowUp(
  queuedFollowUp: QueuedFollowUp | null,
  runStatus: AgentRunStatus | undefined,
): boolean {
  return Boolean(
    queuedFollowUp &&
      queuedFollowUp.status === "queued" &&
      runStatus &&
      runStatus.runId === queuedFollowUp.queuedAfterRunId &&
      runStatus.status === "completed" &&
      !runStatus.active &&
      !runStatus.cancelRequested,
  );
}

function keepQueuedFollowUp(item: QueuedFollowUp | null | undefined): QueuedFollowUp | null {
  return item?.status === "queued" ? item : null;
}

function RunStatusDot({ status }: { status: RunStepStatus }) {
  return (
    <span
      className={
        status === "done"
          ? "mt-1 size-2.5 rounded-full bg-[color:var(--accent-teal)]"
          : status === "active"
            ? "mt-1 size-2.5 rounded-full bg-[#ffb340] shadow-[0_0_0_4px_rgba(255,179,64,0.15)]"
            : "mt-1 size-2.5 rounded-full border border-[color:var(--line-soft)] bg-white"
      }
    />
  );
}

function SessionRail({
  dailyBrief,
  conversation,
  sessions,
  activeSessionId,
  pending,
  onNewTask,
  onPromptFill,
  onSelectSession,
}: {
  dailyBrief: DashboardDailyBrief | null;
  conversation: AssistantConversationState;
  sessions: AssistantSessionSummary[];
  activeSessionId: string | null;
  pending: boolean;
  onNewTask: () => void;
  onPromptFill: (prompt: string) => void;
  onSelectSession: (sessionId: string) => void;
}) {
  const taskPrompts = buildDailyBriefCoachPrompts(dailyBrief);
  const visibleSessions =
    sessions.length > 0
      ? sessions
      : conversation.session
        ? [conversation.session]
        : [];

  return (
    <aside
      data-testid="agent-session-rail"
      className="agent-context-strip agent-side-card workbench-panel order-2 flex w-full min-w-0 max-w-[calc(100vw-2rem)] flex-col gap-3 p-4 lg:order-none lg:max-w-none"
    >
      <div className="agent-reference-header">
        <div className="min-w-0">
          <p className="section-kicker">任务来源</p>
          <h2 className="mt-1 text-lg font-semibold">Agent 工作台</h2>
          <p className="mt-1 text-xs leading-5 text-[color:var(--ink-muted)]">
            今日判断、历史问题和任务模板会一起构成本轮上下文。
          </p>
        </div>
      </div>

      <button
        type="button"
        className="agent-new-task-button min-h-[2.75rem] w-full min-w-0 rounded-full border border-[rgba(0,113,227,0.32)] px-4 text-center text-sm font-bold leading-none shadow-[inset_0_1px_0_rgba(255,255,255,0.28),0_14px_28px_rgba(0,113,227,0.18)] transition hover:-translate-y-0.5 disabled:cursor-not-allowed disabled:opacity-60"
        style={{ background: "#0071e3", color: "#ffffff" }}
        onClick={onNewTask}
        disabled={pending}
      >
        新会话
      </button>

      <div className="agent-reference-scroll">
        <section className="agent-context-brief rounded-lg border border-[rgba(0,113,227,0.16)] bg-[rgba(0,113,227,0.06)] px-3 py-3">
          <p className="section-kicker">今日判断</p>
          <p className="mt-2 line-clamp-3 text-sm font-semibold leading-6 text-[color:var(--ink-strong)]">
            {dailyBrief?.headline ?? "先从一个具体问题开始，教练会整理依据和边界。"}
          </p>
          <div className="mt-3 flex flex-wrap gap-1.5">
            <StatusBadge tone={dailyBrief?.sourceCoverage.portfolio ? "positive" : "warning"}>
              {dailyBrief?.sourceCoverage.portfolio ? "组合已检查" : "组合待补"}
            </StatusBadge>
            <StatusBadge tone={dailyBrief?.sourceCoverage.profile ? "positive" : "neutral"}>
              画像已建立
            </StatusBadge>
            <StatusBadge tone={dailyBrief?.sourceCoverage.behavior ? "accent" : "neutral"}>
              行为待观察
            </StatusBadge>
          </div>
        </section>

        <section className="agent-rail-section">
          <div className="mb-2 flex items-center gap-2">
            <History aria-hidden="true" className="size-4 text-[color:var(--accent-teal)]" />
            <p className="section-kicker">最近对话</p>
          </div>
          <div className="grid gap-2" aria-label="历史会话列表">
          {visibleSessions.length > 0 ? (
            visibleSessions.map((session) => {
              const active = activeSessionId === session.id;
              return (
                <button
                  key={session.id}
                  type="button"
                  className={
                    active
                      ? "rounded-lg border border-[rgba(0,113,227,0.24)] bg-[rgba(0,113,227,0.08)] px-3 py-3 text-left"
                      : "rounded-lg border border-[color:var(--line-soft)] bg-white/62 px-3 py-3 text-left transition hover:border-[rgba(0,113,227,0.2)] hover:bg-white"
                  }
                  onClick={() => onSelectSession(session.id)}
                  disabled={pending}
                  aria-current={active ? "true" : undefined}
                >
                  <span className="flex items-center justify-between gap-2">
                    <span className="min-w-0 truncate text-sm font-semibold text-[color:var(--ink-strong)]">
                      {session.topic}
                    </span>
                    <span className="shrink-0 text-[0.68rem] font-bold text-[color:var(--accent-teal)]">
                      {session.messageCount > 0
                        ? `${session.messageCount} 条`
                        : formatTimestamp(session.updatedAt)}
                    </span>
                  </span>
                  <span className="mt-1 line-clamp-2 block text-xs leading-5 text-[color:var(--ink-muted)]">
                    {session.lastQuestion ??
                      session.lastAnswerPreview ??
                      "打开这段对话继续。"}
                  </span>
                </button>
              );
            })
          ) : (
            <div className="rounded-lg border border-dashed border-[color:var(--line-soft)] px-3 py-4 text-xs leading-5 text-[color:var(--ink-muted)]">
              还没有历史会话。发送第一条问题后，这里会保存记录。
            </div>
          )}
          </div>
        </section>

        <section className="agent-rail-section">
          <div className="mb-2 flex items-center gap-2">
            <ListChecks aria-hidden="true" className="size-4 text-[color:var(--accent-teal)]" />
            <p className="section-kicker">任务模板</p>
          </div>
          <div className="flex min-w-0 max-w-full gap-2 overflow-x-auto pb-1 lg:grid lg:overflow-visible lg:pb-0">
            {taskPrompts.slice(0, 3).map((prompt) => (
              <button
                key={prompt}
                type="button"
                className="w-[13rem] max-w-[75vw] flex-none rounded-full border border-[color:var(--line-soft)] bg-white/72 px-3 py-2 text-left text-xs font-semibold leading-5 text-[color:var(--ink-soft)] transition hover:bg-white lg:w-auto lg:max-w-none lg:flex-auto lg:rounded-lg"
                onClick={() => onPromptFill(prompt)}
                disabled={pending}
              >
                {prompt}
              </button>
            ))}
          </div>
        </section>
      </div>
    </aside>
  );
}

function AgentRunStatusPanel({
  trace,
  liveEvents,
  replayEvents,
  runId,
  runStatus,
  loading,
  pending,
  dashboard,
  dailyBrief,
}: {
  trace: AgentRunTrace | undefined;
  liveEvents: AgentRunEvent[];
  replayEvents: AgentRunEvents | undefined;
  runId: string | null;
  runStatus: AgentRunStatus | undefined;
  loading: boolean;
  pending: boolean;
  dashboard: DashboardState;
  dailyBrief: DashboardDailyBrief | null;
}) {
  const displayEvents = mergeAgentRunEvents(replayEvents?.events, liveEvents, runId);
  const statusLabel = getRunStatusLabel(runStatus);
  const statusMessage = getRunStatusMessage(runStatus);
  const steps =
    displayEvents.length > 0
      ? getLiveRunProgressSteps(displayEvents)
      : getRunProgressSteps({ trace, pending });
  const coverageItems = [
    {
      label: "今日判断",
      detail: dailyBrief ? "已形成今日判断" : "等待画像和资料补齐",
      active: Boolean(dailyBrief),
    },
    {
      label: "组合巡检",
      detail: dashboard.portfolioStatus?.hasReport ? "最近报告可用于追问" : "可从任务中发起",
      active: Boolean(dashboard.portfolioStatus?.hasReport),
    },
    {
      label: "学习/训练",
      detail: dashboard.learningStatus?.recommendedCourseTitle ?? dashboard.simulationStatus?.recommendedScenarioTitle ?? "可生成练习建议",
      active: Boolean(
        dashboard.learningStatus?.recommendedCourseTitle ||
          dashboard.simulationStatus?.recommendedScenarioTitle,
      ),
    },
    {
      label: "资讯影响",
      detail: dashboard.newsStatus?.latestTitle ?? "有问题时再读取相关资讯",
      active: Boolean(dashboard.newsStatus?.hasAnalysis),
    },
  ];

  return (
    <aside
      data-testid="agent-run-status"
      className="agent-process-panel agent-side-card workbench-panel order-3 grid w-full min-w-0 max-w-[calc(100vw-2rem)] grid-cols-[minmax(0,1fr)] gap-4 overflow-hidden p-4 lg:order-none lg:max-w-none"
    >
      <div>
        <div className="flex flex-wrap items-center gap-2">
          <div className="flex flex-1 flex-wrap items-center gap-2">
            <StatusBadge tone="accent">整理状态</StatusBadge>
            <StatusBadge
              tone={
                pending || loading
                  ? "warning"
                  : trace?.run.runStatus === "completed"
                    ? "positive"
                    : "neutral"
              }
            >
              {statusLabel ??
                (pending
                  ? "正在整理"
                  : loading
                    ? "同步中"
                    : trace?.run.runStatus === "cancelled"
                      ? "已停止"
                      : trace
                        ? "已完成"
                        : "等待问题")}
            </StatusBadge>
          </div>
        </div>
        <h2 className="mt-3 text-lg font-semibold">整理进度</h2>
        <p className="mt-1 text-xs leading-5 text-[color:var(--ink-muted)]">
          {statusMessage ?? "默认显示自然语言步骤；更细的执行依据放在分层入口里。"}
        </p>
      </div>

      <div className="flow-line space-y-4">
        {steps.map((step, index) => (
          <div key={`${step.label}-${index}`} className="grid grid-cols-[1rem_minmax(0,1fr)] gap-3">
            <RunStatusDot status={step.status} />
            <div>
              <p className="text-sm font-semibold text-[color:var(--ink-strong)]">
                {step.label}
              </p>
              <p className="mt-1 text-xs leading-5 text-[color:var(--ink-muted)]">
                {step.detail}
              </p>
            </div>
          </div>
        ))}
      </div>

      <section className="rounded-lg border border-[color:var(--line-soft)] bg-white/62 p-3">
        <div className="flex items-center gap-2">
          <Activity aria-hidden="true" className="size-4 text-[color:var(--accent-teal)]" />
          <p className="section-kicker">自动检查</p>
        </div>
        <div className="mt-3 grid gap-2">
          {coverageItems.map((item) => (
            <div
              key={item.label}
              className="rounded-lg border border-[color:var(--line-soft)] bg-white/72 px-3 py-2"
            >
              <div className="flex items-center justify-between gap-2">
                <p className="text-xs font-bold text-[color:var(--ink-strong)]">
                  {item.label}
                </p>
                <span
                  className={
                    item.active
                      ? "size-2 rounded-full bg-[color:var(--accent-teal)]"
                      : "size-2 rounded-full bg-[color:var(--line-soft)]"
                  }
                />
              </div>
              <p className="mt-1 text-[0.7rem] leading-5 text-[color:var(--ink-muted)]">
                {item.detail}
              </p>
            </div>
          ))}
        </div>
        <Link
          href="/automations"
          className="mt-3 inline-flex items-center gap-1 text-xs font-bold text-[color:var(--accent-teal)]"
        >
          管理自动任务
          <ArrowRight aria-hidden="true" className="size-3.5" />
        </Link>
      </section>

      <details className="rounded-lg border border-[color:var(--line-soft)] bg-white/62 p-3">
        <summary className="cursor-pointer text-xs font-bold tracking-[0.08em] text-[color:var(--accent-teal)]">
          查看整理依据
        </summary>
        <div className="mt-3 space-y-2 text-xs leading-5 text-[color:var(--ink-muted)]">
          <p>这里会说明本次回答参考了哪些方向，以及为什么没有变成账户操作建议。</p>
          <p>更完整的内部记录只保留给授权审计，不默认展示给新手用户。</p>
        </div>
      </details>
    </aside>
  );
}

function DailyBriefCoachHeader({
  brief,
  latestIntent,
  onPromptFill,
}: {
  brief: DashboardDailyBrief | null;
  latestIntent: string | null;
  onPromptFill: (prompt: string) => void;
}) {
  const prompts = buildDailyBriefCoachPrompts(brief);

  if (!brief) {
    return (
      <div className="border-b border-[color:var(--line-soft)] px-5 py-4 sm:px-6">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <StatusBadge tone="accent">教练工作台</StatusBadge>
              {latestIntent ? (
                <StatusBadge tone="positive">{formatIntent(latestIntent)}</StatusBadge>
              ) : null}
            </div>
            <h3 className="mt-3 text-2xl font-semibold leading-tight">
              布置一个任务，Agent 会先拆解再回答。
            </h3>
          </div>
          <div className="text-xs leading-5 text-[color:var(--ink-muted)] sm:max-w-[15rem] sm:text-right">
            {latestIntent
              ? `最近主题：${formatIntent(latestIntent)}`
              : "还没有历史回答，先从一个具体问题开始。"}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="border-b border-[color:var(--line-soft)] bg-white/72 px-5 py-3 sm:px-6">
      <details>
        <summary className="flex cursor-pointer list-none flex-wrap items-center justify-between gap-3 text-sm font-semibold text-[color:var(--ink-strong)]">
          <span className="min-w-0 truncate">
            今日判断会作为本轮回答参考：{brief.headline}
          </span>
          <span className="shrink-0 text-xs font-bold text-[color:var(--accent-teal)]">
            为什么这么建议
          </span>
        </summary>
        <div className="mt-3 grid gap-3 lg:grid-cols-[minmax(0,1fr)_16rem]">
          <div>
            <p className="text-sm leading-6 text-[color:var(--ink-soft)]">
              {brief.beginnerExplanation}
            </p>
            <div className="mt-3 grid gap-2 md:grid-cols-3">
              {prompts.map((prompt) => (
                <button
                  key={prompt}
                  type="button"
                  className="rounded-lg border border-[rgba(0,113,227,0.16)] bg-[rgba(0,113,227,0.07)] px-3 py-2 text-left text-xs font-semibold leading-5 text-[color:var(--ink-soft)] transition hover:border-[rgba(0,113,227,0.28)] hover:bg-white"
                  onClick={() => onPromptFill(prompt)}
                >
                  {prompt}
                </button>
              ))}
            </div>
          </div>
          <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/70 px-3 py-3">
            <p className="section-kicker">安全边界</p>
            <p className="mt-2 text-xs leading-5 text-[color:var(--ink-muted)]">
              {brief.doNotDo}
            </p>
            <Link
              href={buildSafeActionHref(brief.primaryAction)}
              className="mt-3 inline-flex text-xs font-bold text-[color:var(--accent-teal)]"
            >
              {formatWritebackLabel(brief.primaryAction)}
            </Link>
          </div>
        </div>
      </details>
    </div>
  );
}

function StructuredAnswerCanvas({
  response,
  question,
  onPromptFill,
  showcaseReveal = false,
}: {
  response: AdvisorStructuredResponse;
  question: string;
  onPromptFill: (prompt: string) => void;
  showcaseReveal?: boolean;
}) {
  const actionItems =
    response.recommendedActions.length > 0
      ? response.recommendedActions.map((item) => formatProductCopy(item))
      : ["把这次回答带回学习、组合或情境训练中继续验证。"];
  const actionTargets = response.recommendedActionTargets;
  const followUps =
    buildUserFollowUpPrompts(response, question).map((item) =>
      formatProductCopy(item),
    );

  return (
    <div
      className={`assistant-answer-card overflow-hidden ${
        showcaseReveal ? "assistant-answer-card-showcase" : ""
      }`}
    >
      <div className="border-b border-[color:var(--line-soft)] px-4 py-4">
        <div className="flex flex-wrap items-center gap-2">
          <StatusBadge tone="accent">{formatIntent(response.intent)}</StatusBadge>
          <span className="text-xs font-bold text-[color:var(--ink-muted)]">
            针对：{question}
          </span>
        </div>
        <p className="agent-answer-summary mt-3 text-sm leading-7 text-[color:var(--ink-soft)]">
          {formatUserVisibleCopy(response.answer)}
        </p>
      </div>

      <div className="grid gap-0 md:grid-cols-2">
        <div className="border-b border-[color:var(--line-soft)] px-4 py-4 md:border-b-0 md:border-r">
          <div className="flex items-center gap-2">
            <ShieldCheck aria-hidden="true" className="size-4 text-[color:var(--accent-teal)]" />
            <p className="section-kicker">边界提醒</p>
          </div>
          <p className="mt-2 text-xs leading-6 text-[color:var(--ink-muted)]">
            {formatUserVisibleCopy(response.riskNotice)}
          </p>
        </div>
        <div className="px-4 py-4">
          <div className="flex items-center gap-2">
            <ListChecks aria-hidden="true" className="size-4 text-[color:var(--accent-teal)]" />
            <p className="section-kicker">下一步</p>
          </div>
          <div className="mt-3 space-y-2">
            {actionItems.slice(0, 2).map((item, index) => {
              const target = actionTargets[index] ?? inferActionTarget(item);
              return (
                <Link
                  key={`${item}-${index}`}
                  href={target.href}
                  className="coach-action-link group"
                >
                  <span className="grid min-w-0 gap-1">
                    <span className="text-xs leading-5 text-[color:var(--ink-soft)]">
                      <span className="font-semibold text-[color:var(--ink-strong)]">
                        {index + 1}.{" "}
                      </span>
                      {item}
                    </span>
                    <span className="text-[0.7rem] font-bold text-[color:var(--accent-teal)]">
                      {target.label}
                    </span>
                  </span>
                  <ArrowRight
                    aria-hidden="true"
                    className="size-4 flex-none text-[color:var(--accent-teal)] transition-transform group-hover:translate-x-0.5"
                  />
                </Link>
              );
            })}
          </div>
        </div>
      </div>

      <div className="border-t border-[color:var(--line-soft)] px-4 py-4">
        <div className="flex items-center gap-2">
          <ArrowRight aria-hidden="true" className="size-4 text-[color:var(--accent-teal)]" />
          <p className="section-kicker">下一句可以问</p>
        </div>

        <div className="mt-3 grid gap-2">
          {followUps.slice(0, 2).map((item) => (
            <button
              key={item}
              type="button"
              className="coach-followup-chip group"
              onClick={() => onPromptFill(item)}
            >
              <span className="grid min-w-0 gap-1">
                <span>{item}</span>
                <span className="text-[0.68rem] font-bold text-[color:var(--accent-teal)]">
                  填入输入框
                </span>
              </span>
              <ArrowRight
                aria-hidden="true"
                className="size-4 flex-none text-[color:var(--accent-teal)] transition-transform group-hover:translate-x-0.5"
              />
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

function AgentWorkspaceTopbar({
  contextPanelCollapsed,
  runPanelCollapsed,
  pending,
  onToggleContextPanel,
  onToggleRunPanel,
  onNewTask,
}: {
  contextPanelCollapsed: boolean;
  runPanelCollapsed: boolean;
  pending: boolean;
  onToggleContextPanel: () => void;
  onToggleRunPanel: () => void;
  onNewTask: () => void;
}) {
  const layoutSummary =
    contextPanelCollapsed && runPanelCollapsed
      ? "专注模式"
    : contextPanelCollapsed
        ? "任务参考已收起"
        : runPanelCollapsed
          ? "整理依据已收起"
          : "完整工作区";

  return (
    <div
      data-testid="agent-workspace-topbar"
      className="agent-workspace-strip"
    >
      <div className="flex min-w-0 items-center gap-2">
        <Bot aria-hidden="true" className="size-4 shrink-0 text-[color:var(--accent-teal)]" />
        <span className="truncate text-sm font-semibold text-[color:var(--ink-strong)]">
          Agent 工作区
        </span>
        <span className="hidden text-xs font-medium text-[color:var(--ink-muted)] sm:inline">
          {layoutSummary}
        </span>
        <span className="agent-topbar-chip hidden sm:inline-flex">解释</span>
        <span className="agent-topbar-chip hidden sm:inline-flex">训练</span>
        <span className="agent-topbar-chip hidden sm:inline-flex">确认</span>
      </div>
      <div className="agent-layout-text-controls" aria-label="教练工作区布局">
        <button
          type="button"
          className="agent-layout-text-button"
          onClick={onNewTask}
          disabled={pending}
        >
          新会话
        </button>
        <button
          type="button"
          className="agent-layout-text-button"
          aria-label={contextPanelCollapsed ? "展开任务参考" : "收起任务参考"}
          aria-pressed={contextPanelCollapsed}
          onClick={onToggleContextPanel}
        >
          {contextPanelCollapsed ? "显示参考" : "隐藏参考"}
        </button>
        <button
          type="button"
          className="agent-layout-text-button"
          aria-label={runPanelCollapsed ? "展开整理一览" : "收起整理一览"}
          aria-pressed={runPanelCollapsed}
          onClick={onToggleRunPanel}
        >
          {runPanelCollapsed ? "显示依据" : "隐藏依据"}
        </button>
      </div>
    </div>
  );
}

export function CoachWorkspace() {
  const queryClient = useQueryClient();
  const searchParams = useSearchParams();
  const showcaseMode = ["1", "true", "yes"].includes(
    (getQueryValue(searchParams, ["showcase"]) ?? "").toLowerCase(),
  );
  const contextPrompt =
    getQueryValue(searchParams, ["prompt", "q"]) ??
    buildContextPrompt(searchParams) ??
    "";
  const contextDisplayMessage =
    getQueryValue(searchParams, ["display_message", "message"]) ?? "";
  const startsWithFreshTask =
    Boolean(contextPrompt) ||
    ["1", "true", "yes"].includes(
      (getQueryValue(searchParams, ["new", "fresh", "new_session"]) ?? "").toLowerCase(),
    );
  const [draft, setDraft] = useState(() => contextPrompt);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [pendingQuestion, setPendingQuestion] = useState<string | null>(null);
  const [liveRunEvents, setLiveRunEvents] = useState<AgentRunEvent[]>([]);
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [cancelNotice, setCancelNotice] = useState<string | null>(null);
  const [localQueuedFollowUp, setLocalQueuedFollowUp] =
    useState<QueuedFollowUp | null>(null);
  const [freshTaskMode, setFreshTaskMode] = useState(() => startsWithFreshTask);
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null);
  const [isCompactWorkspace, setIsCompactWorkspace] = useState(
    () => typeof window !== "undefined" && window.matchMedia("(max-width: 1399px)").matches,
  );
  const [contextPanelCollapsed, setContextPanelCollapsed] = useState(
    () => typeof window !== "undefined" && window.matchMedia("(max-width: 1399px)").matches,
  );
  const [runPanelCollapsed, setRunPanelCollapsed] = useState(
    () => typeof window !== "undefined" && window.matchMedia("(max-width: 1023px)").matches,
  );
  const messageScrollerRef = useRef<HTMLDivElement | null>(null);
  const composerTextareaRef = useRef<HTMLTextAreaElement | null>(null);
  const appliedContextPromptRef = useRef(contextPrompt);
  const autoSubmittedQueuedFollowUpIdRef = useRef<string | null>(null);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const compactQuery = window.matchMedia("(max-width: 1399px)");
    const handleCompactChange = (event: MediaQueryListEvent | MediaQueryList) => {
      setIsCompactWorkspace(event.matches);
      if (event.matches) {
        setContextPanelCollapsed(true);
      }
    };

    handleCompactChange(compactQuery);
    compactQuery.addEventListener("change", handleCompactChange);

    return () => {
      compactQuery.removeEventListener("change", handleCompactChange);
    };
  }, []);

  useEffect(() => {
    if (!contextPrompt) {
      if (startsWithFreshTask) {
        setFreshTaskMode(true);
        setSelectedSessionId(null);
        window.requestAnimationFrame(() => {
          composerTextareaRef.current?.focus();
        });
      }
      return;
    }

    if (contextPrompt !== appliedContextPromptRef.current) {
      setDraft((currentDraft) => {
        if (
          currentDraft.trim().length > 0 &&
          currentDraft !== appliedContextPromptRef.current
        ) {
          return currentDraft;
        }
        return contextPrompt;
      });
      appliedContextPromptRef.current = contextPrompt;
      setFreshTaskMode(true);
      setSelectedSessionId(null);
      setSubmitError(null);
    }

    window.requestAnimationFrame(() => {
      composerTextareaRef.current?.focus();
    });
  }, [contextPrompt, startsWithFreshTask]);

  const sessionQuery = useQuery({
    queryKey: ["session-user"],
    queryFn: getSessionUser,
    retry: false,
  });

  const isOnboarded = Boolean(sessionQuery.data?.onboardingCompleted);

  const behaviorQuery = useQuery({
    queryKey: ["behavior-profile"],
    queryFn: getBehaviorProfile,
    enabled: isOnboarded,
    retry: false,
  });

  const dashboardQuery = useQuery({
    queryKey: ["dashboard"],
    queryFn: getDashboardState,
    enabled: isOnboarded,
    retry: false,
  });

  const coachQuery = useQuery({
    queryKey: ["coach-session", selectedSessionId],
    queryFn: () =>
      selectedSessionId
        ? getAssistantConversation(selectedSessionId)
        : getAssistantSession(),
    enabled: isOnboarded,
    retry: false,
  });
  const coachSessionsQuery = useQuery({
    queryKey: ["coach-sessions"],
    queryFn: getAssistantSessions,
    enabled: isOnboarded,
    retry: false,
  });
  const activeRunSessionId = coachQuery.data?.session?.id ?? selectedSessionId;
  const activeRunsQuery = useQuery({
    queryKey: ["agent-active-runs", activeRunSessionId],
    queryFn: () => getActiveAgentRuns(activeRunSessionId),
    enabled: isOnboarded,
    refetchInterval: (query) => {
      const activeRuns = query.state.data?.runs ?? [];
      return activeRuns.some((run) => run.active) ? 1200 : false;
    },
    retry: false,
  });
  const queuedFollowUpQuery = useQuery({
    queryKey: ["assistant-queued-follow-up", activeRunSessionId],
    queryFn: () => getSessionQueuedFollowUp(activeRunSessionId ?? ""),
    enabled: Boolean(isOnboarded && activeRunSessionId),
    retry: false,
  });
  const discoveredActiveRun = getLatestActiveRun(activeRunsQuery.data);
  const latestTraceRunId =
    coachQuery.data?.messages
      .slice()
      .reverse()
      .find((item) => item.role === "assistant" && item.agentRunId)?.agentRunId ??
    null;
  const observedRunId =
    activeRunId ?? discoveredActiveRun?.runId ?? (pendingQuestion ? null : latestTraceRunId);
  const traceQuery = useQuery({
    queryKey: ["agent-run-trace", observedRunId],
    queryFn: () => getAgentRunTrace(observedRunId ?? ""),
    enabled: Boolean(isOnboarded && observedRunId),
    retry: false,
  });
  const runEventsQuery = useQuery({
    queryKey: ["agent-run-events", observedRunId],
    queryFn: () => getAgentRunEvents(observedRunId ?? ""),
    enabled: Boolean(isOnboarded && observedRunId),
    retry: false,
  });
  const runStatusQuery = useQuery({
    queryKey: ["agent-run-status", observedRunId],
    queryFn: () => getAgentRunStatus(observedRunId ?? ""),
    enabled: Boolean(isOnboarded && observedRunId),
    refetchInterval: (query) => {
      const status = query.state.data;
      return status?.active || status?.status === "cancelling" ? 1200 : false;
    },
    retry: false,
  });
  const queriedRunStatus =
    runStatusQuery.data?.runId === observedRunId ? runStatusQuery.data : undefined;
  const observedRunStatus =
    queriedRunStatus ??
    (discoveredActiveRun?.runId === observedRunId ? discoveredActiveRun : undefined);
  const observedTrace =
    traceQuery.data?.run.id === observedRunId ? traceQuery.data : undefined;
  const observedReplayEvents =
    runEventsQuery.data?.runId === observedRunId ? runEventsQuery.data : undefined;
  const queuedFollowUp =
    keepQueuedFollowUp(localQueuedFollowUp) ??
    keepQueuedFollowUp(observedRunStatus?.queuedFollowUp) ??
    keepQueuedFollowUp(queuedFollowUpQuery.data?.queuedFollowUp) ??
    null;

  const cancelRunMutation = useMutation({
    mutationFn: async (runId: string) => cancelAgentRun(runId),
    onSuccess: async (result) => {
      const discardedQueuedFollowUp = queuedFollowUp;
      if (result.cancelRequested) {
        setLocalQueuedFollowUp(null);
      }
      setCancelNotice(result.message);
      if (discardedQueuedFollowUp) {
        await queryClient.invalidateQueries({
          queryKey: ["assistant-queued-follow-up", discardedQueuedFollowUp.sessionId],
        });
        await queryClient.invalidateQueries({
          queryKey: ["agent-run-events", discardedQueuedFollowUp.queuedAfterRunId],
        });
      }
      await runStatusQuery.refetch();
    },
    onError: (error) => {
      setCancelNotice(
        error instanceof Error ? error.message : "停止请求没有发送成功，请稍后重试。",
      );
    },
  });

  const queueFollowUpMutation = useMutation({
    mutationFn: async (input: { runId: string; message: string }) =>
      queueAgentRunFollowUp(input.runId, input.message),
    onSuccess: async (result) => {
      setLocalQueuedFollowUp(keepQueuedFollowUp(result.queuedFollowUp));
      setDraft("");
      setSubmitError(null);
      setCancelNotice(result.message);
      await queryClient.invalidateQueries({
        queryKey: ["assistant-queued-follow-up"],
      });
      if (result.queuedFollowUp) {
        await queryClient.invalidateQueries({
          queryKey: ["agent-run-events", result.queuedFollowUp.queuedAfterRunId],
        });
      }
      await runStatusQuery.refetch();
    },
    onError: (error) => {
      setSubmitError(
        error instanceof Error
          ? error.message
          : "下一句没有排队成功，请等当前回答结束后再发送。",
      );
    },
  });

  const discardQueuedFollowUpMutation = useMutation({
    mutationFn: async (sessionId: string) => discardSessionQueuedFollowUp(sessionId),
    onSuccess: async (result) => {
      setLocalQueuedFollowUp(keepQueuedFollowUp(result.queuedFollowUp));
      setCancelNotice(result.message);
      await queryClient.invalidateQueries({
        queryKey: ["assistant-queued-follow-up"],
      });
      if (result.queuedFollowUp) {
        await queryClient.invalidateQueries({
          queryKey: ["agent-run-events", result.queuedFollowUp.queuedAfterRunId],
        });
      }
      await runStatusQuery.refetch();
    },
  });

  const coachMutation = useMutation({
    mutationFn: async (message: string) => {
      const trimmedMessage = message.trim();
      if (trimmedMessage.length < 4) {
        throw new Error("请先写下一个至少 4 个字符的问题。");
      }

      return streamAssistantMessage(
        {
          message: trimmedMessage,
          sessionId: freshTaskMode ? null : coachQuery.data?.session?.id ?? null,
          startNewSession: freshTaskMode,
          context: {
            fromRoute: getQueryValue(searchParams, ["from", "from_route"]),
            focus: getQueryValue(searchParams, ["focus"]),
            dailyBriefId:
              getQueryValue(searchParams, ["daily_brief_id", "dailyBriefId"]) ??
              dashboardQuery.data?.dailyBrief?.briefId ??
              null,
            sourceIds: {
              portfolio_analysis_id:
                getQueryValue(searchParams, ["portfolio_analysis_id"]) ?? "",
              portfolio_snapshot_id:
                getQueryValue(searchParams, ["portfolio_snapshot_id"]) ?? "",
              news_analysis_id: getQueryValue(searchParams, ["news_analysis_id"]) ?? "",
              news_item_id: getQueryValue(searchParams, ["news_item_id"]) ?? "",
              course_slug: getQueryValue(searchParams, ["course", "course_slug"]) ?? "",
              section_slug: getQueryValue(searchParams, ["section", "section_slug"]) ?? "",
              simulation_session_id:
                getQueryValue(searchParams, ["simulation_session_id"]) ?? "",
              scenario_id: getQueryValue(searchParams, ["scenario_id"]) ?? "",
            },
          },
        },
        {
          onEvent: (event) => {
            if (event.runId) {
              setActiveRunId(event.runId);
            }
            if (event.eventType === "turn_cancel_requested") {
              setCancelNotice("正在停止本次整理。");
            }
            if (event.eventType === "turn_aborted") {
              setCancelNotice("已停止这次整理，没有写入新的长期记录。");
            }
            if (event.eventType === "turn_closed" && event.status === "cancelled") {
              setCancelNotice("已停止这次整理，没有写入新的长期记录。");
            }
            setLiveRunEvents((current) => {
              if (current.some((item) => item.id === event.id)) {
                return current;
              }
              return [...current, event].slice(-14);
            });
          },
        },
      );
    },
    onMutate: () => {
      setLiveRunEvents([]);
      setActiveRunId(null);
      setCancelNotice(null);
    },
    onSuccess: async (conversation, submittedMessage) => {
      setDraft((currentDraft) =>
        currentDraft.trim() === submittedMessage.trim() ? "" : currentDraft,
      );
      setPendingQuestion(null);
      setFreshTaskMode(false);
      setSubmitError(null);
      setActiveRunId(null);
      setSelectedSessionId(conversation.session?.id ?? null);
      queryClient.setQueryData(
        ["coach-session", conversation.session?.id ?? null],
        conversation,
      );
      queryClient.setQueryData(["coach-session", null], conversation);
      await queryClient.invalidateQueries({ queryKey: ["agent-active-runs"] });
      await queryClient.invalidateQueries({ queryKey: ["coach-sessions"] });
      await queryClient.invalidateQueries({ queryKey: ["dashboard"] });
    },
    onError: (error, submittedMessage) => {
      const runStillActive = isApiError(error) && error.status === 202;
      if (runStillActive) {
        if (error.runId) {
          setActiveRunId(error.runId);
        }
        setCancelNotice(error.message);
        setSubmitError(null);
        return;
      }

      setPendingQuestion(null);
      setActiveRunId(null);
      setDraft((currentDraft) => currentDraft || submittedMessage);
      setSubmitError(error instanceof Error ? error.message : "发送失败，请稍后重试。");
    },
  });
  const runInProgress = coachMutation.isPending || Boolean(observedRunStatus?.active);
  const stoppableRunId =
    activeRunId ?? (observedRunStatus?.active ? observedRunStatus.runId : null);

  useEffect(() => {
    if (
      !queuedFollowUp ||
      runInProgress ||
      coachMutation.isPending ||
      autoSubmittedQueuedFollowUpIdRef.current === queuedFollowUp.id ||
      !shouldAutoSubmitQueuedFollowUp(queuedFollowUp, observedRunStatus)
    ) {
      return;
    }

    autoSubmittedQueuedFollowUpIdRef.current = queuedFollowUp.id;
    const followUpMessage = queuedFollowUp.message;
    const timer = window.setTimeout(() => {
      setPendingQuestion(followUpMessage);
      setDraft("");
      setSubmitError(null);
      setCancelNotice("上一条已完成，正在发送排队的下一句。");
      coachMutation.mutate(followUpMessage, {
        onSuccess: async () => {
          try {
            await markSessionQueuedFollowUpSubmitted(
              queuedFollowUp.sessionId,
              queuedFollowUp.id,
            );
            setLocalQueuedFollowUp(null);
            setCancelNotice(null);
          } catch {
            setLocalQueuedFollowUp(null);
            setCancelNotice("排队的下一句已发送，但发送状态同步失败；刷新后可再次确认。");
          } finally {
            await queryClient.invalidateQueries({
              queryKey: ["assistant-queued-follow-up"],
            });
            await queryClient.invalidateQueries({
              queryKey: ["agent-run-events", queuedFollowUp.queuedAfterRunId],
            });
          }
        },
        onError: () => {
          autoSubmittedQueuedFollowUpIdRef.current = null;
        },
      });
    }, 0);

    return () => {
      window.clearTimeout(timer);
    };
  }, [
    coachMutation,
    observedRunStatus,
    queryClient,
    queuedFollowUp,
    runInProgress,
  ]);

  useEffect(() => {
    const scroller = messageScrollerRef.current;
    if (!scroller) {
      return;
    }

    const scrollToLatest = () => {
      scroller.scrollTop = scroller.scrollHeight;
    };

    const frame = window.requestAnimationFrame(scrollToLatest);
    const timer = window.setTimeout(scrollToLatest, 80);

    return () => {
      window.cancelAnimationFrame(frame);
      window.clearTimeout(timer);
    };
  }, [coachQuery.data?.messages.length, pendingQuestion, freshTaskMode]);

  if (sessionQuery.isLoading) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/60 px-5 py-4 text-sm text-[color:var(--ink-soft)]">
        正在初始化教练会话上下文...
      </div>
    );
  }

  if (isApiError(sessionQuery.error) && sessionQuery.error.status === 401) {
    return (
      <div className="space-y-6">
        <SectionBlock
          eyebrow="教练"
          title="请先登录，再进入个人教练工作区。"
          description="登录后才能保存提问、回答和后续训练动作。"
        >
          <div className="paper-panel-strong rounded-lg px-5 py-6">
            <div className="mt-5 flex flex-wrap gap-3">
              <Link href="/start" className="action-button">
                去登录 / 注册
              </Link>
              <Link href="/" className="action-button-secondary">
                回到总览
              </Link>
            </div>
          </div>
        </SectionBlock>
      </div>
    );
  }

  if (sessionQuery.error || !sessionQuery.data) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
        当前教练会话不可用：{sessionQuery.error?.message ?? "未知错误"}
      </div>
    );
  }

  const sessionUser = sessionQuery.data;

  if (!sessionUser.onboardingCompleted) {
    return (
      <div className="space-y-8">
        <SectionBlock
          eyebrow="教练"
          title="先完成建档，再让教练围绕你的真实画像工作。"
          description="先完成基础资料和问卷，教练才能围绕你的风险等级和行为焦点解释问题。"
        >
          <div className="grid gap-4 lg:grid-cols-3">
            <MetricCard
              label="当前用户"
              value={sessionUser.displayName ?? "已登录用户"}
              detail={sessionUser.email ?? sessionUser.id}
              accent="moss"
            />
            <MetricCard
              label="建档"
              value="待完成"
              detail="请先补齐基础画像和风险问卷。"
              accent="gold"
            />
            <MetricCard
              label="当前目标"
              value={sessionUser.primaryGoal ?? "待填写"}
              detail="目标会影响教练如何解释你的问题。"
              accent="clay"
            />
          </div>
          <div className="flex flex-wrap gap-3">
            <Link href="/onboarding" className="action-button">
              去完成建档
            </Link>
            <Link href="/today" className="action-button-secondary">
              先看今日
            </Link>
          </div>
        </SectionBlock>
      </div>
    );
  }

  if (behaviorQuery.isLoading || dashboardQuery.isLoading || coachQuery.isLoading) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/60 px-5 py-4 text-sm text-[color:var(--ink-soft)]">
        正在同步教练工作区、行为画像和最近交互...
      </div>
    );
  }

  if (
    behaviorQuery.error ||
    dashboardQuery.error ||
    coachQuery.error ||
    coachSessionsQuery.error
  ) {
    const error =
      behaviorQuery.error?.message ??
      dashboardQuery.error?.message ??
      coachQuery.error?.message ??
      coachSessionsQuery.error?.message ??
      "请求失败";
    return (
      <div className="space-y-4 rounded-lg border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
        <p>加载教练工作区失败：{error}</p>
        <p className="text-xs">请确认服务已运行，且当前登录会话有效。</p>
      </div>
    );
  }

  const behavior = behaviorQuery.data;
  const dashboard = dashboardQuery.data;
  const conversation: AssistantConversationState = coachQuery.data ?? {
    session: null,
    messages: [],
  };
  const sessionHistory = coachSessionsQuery.data ?? [];
  const activeSessionId = selectedSessionId ?? conversation.session?.id ?? null;
  const visibleMessages = freshTaskMode ? [] : conversation.messages;
  const hiddenMessageCount = Math.max(
    conversation.messages.length - visibleMessages.length,
    0,
  );
  const visibleStartIndex = conversation.messages.length - visibleMessages.length;
  const latestCoachActivity = dashboard?.latestCoachActivity ?? null;
  const dailyBrief = dashboard?.dailyBrief ?? null;
  const latestIntent = latestCoachActivity?.intent ?? null;
  const dailyBriefPrompts = buildDailyBriefCoachPrompts(dailyBrief);
  const workspaceGridClassName = cn(
    "agent-workspace-grid agent-command-grid grid max-w-full gap-4 overflow-hidden",
    !contextPanelCollapsed &&
      !runPanelCollapsed &&
      "agent-workspace-grid-three",
    contextPanelCollapsed &&
      !runPanelCollapsed &&
      "agent-workspace-grid-no-context",
    !contextPanelCollapsed &&
      runPanelCollapsed &&
      "agent-workspace-grid-no-overview",
    contextPanelCollapsed &&
      runPanelCollapsed &&
      "agent-workspace-grid-solo",
  );

  if (!behavior || !dashboard) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/60 px-5 py-4 text-sm text-[color:var(--ink-soft)]">
        暂无可展示的教练数据，请稍后重试。
      </div>
    );
  }

  function handlePromptFill(prompt: string) {
    setDraft(prompt);
    setSubmitError(null);
  }

  function handleNewTask() {
    setDraft("");
    setSubmitError(null);
    setPendingQuestion(null);
    setLocalQueuedFollowUp(null);
    setFreshTaskMode(true);
    setSelectedSessionId(null);
    window.requestAnimationFrame(() => {
      composerTextareaRef.current?.focus();
    });
  }

  function handleSelectSession(sessionId: string) {
    setSelectedSessionId(sessionId);
    setFreshTaskMode(false);
    setDraft("");
    setSubmitError(null);
    setPendingQuestion(null);
    setLocalQueuedFollowUp(null);
  }

  function handleToggleContextPanel() {
    setContextPanelCollapsed((collapsed) => {
      const nextCollapsed = !collapsed;
      if (isCompactWorkspace && !nextCollapsed) {
        setRunPanelCollapsed(true);
      }
      return nextCollapsed;
    });
  }

  function handleToggleRunPanel() {
    setRunPanelCollapsed((collapsed) => {
      const nextCollapsed = !collapsed;
      if (isCompactWorkspace && !nextCollapsed) {
        setContextPanelCollapsed(true);
      }
      return nextCollapsed;
    });
  }

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitError(null);

    const message = buildSubmittedMessage({
      draft,
      contextPrompt,
      contextDisplayMessage,
    });
    if (message.length < 4) {
      setSubmitError("请先写下一个至少 4 个字符的问题。");
      return;
    }

    if (runInProgress) {
      if (!stoppableRunId) {
        setSubmitError("上一条还在启动整理，等进度出现后再排队下一句。");
        return;
      }
      queueFollowUpMutation.mutate({ runId: stoppableRunId, message });
      return;
    }

    setPendingQuestion(message);
    setDraft("");
    coachMutation.mutate(message);
  }

  const isHandoffDraft =
    contextPrompt.trim().length > 0 && draft.trim() === contextPrompt.trim();

  return (
    <div className="agent-command-dashboard grid gap-3">
      <AgentWorkspaceTopbar
        contextPanelCollapsed={contextPanelCollapsed}
        runPanelCollapsed={runPanelCollapsed}
        pending={runInProgress}
        onToggleContextPanel={handleToggleContextPanel}
        onToggleRunPanel={handleToggleRunPanel}
        onNewTask={handleNewTask}
      />

      <div className={workspaceGridClassName}>
        {!contextPanelCollapsed ? (
          <SessionRail
            dailyBrief={dailyBrief}
            conversation={conversation}
            sessions={sessionHistory}
            activeSessionId={activeSessionId}
            pending={runInProgress}
            onNewTask={handleNewTask}
            onPromptFill={handlePromptFill}
            onSelectSession={handleSelectSession}
          />
        ) : null}

        <section
          data-testid="agent-active-session"
          className="workbench-panel coach-chat-panel agent-active-panel agent-chat-shell order-1 min-w-0 max-w-[calc(100vw-2rem)] overflow-hidden lg:order-none lg:max-w-none"
        >
          <DailyBriefCoachHeader
            brief={dailyBrief}
            latestIntent={latestIntent}
            onPromptFill={handlePromptFill}
          />

        <div
          ref={messageScrollerRef}
          role="log"
          aria-live="polite"
          aria-busy={runInProgress}
          className="coach-message-log space-y-4 scroll-smooth px-4 py-5 sm:px-6"
        >
          {visibleMessages.length > 0 || pendingQuestion || queuedFollowUp ? (
            <>
              {hiddenMessageCount > 0 ? (
                <div className="coach-history-pill mx-auto w-fit rounded-full border border-[color:var(--line-soft)] bg-white/[0.04] px-3 py-1 text-xs font-bold text-[color:var(--ink-muted)]">
                  已显示最近 {visibleMessages.length} 条消息，历史对话已保存。
                </div>
              ) : null}

              {visibleMessages.map((item, index) => (
                <article
                  key={item.id}
                  className={
                    item.role === "user"
                      ? "coach-message coach-message-user"
                      : "coach-message coach-message-assistant"
                  }
                >
                  {item.role === "assistant" && item.advisorResponse ? (
                    <>
                      <div className="coach-assistant-meta">
                        <p className="text-xs font-semibold text-[color:var(--ink-muted)]">
                          FundGene 教练
                        </p>
                        <span className="text-xs text-[color:var(--ink-muted)]">
                          {formatTimestamp(item.createdAt)}
                        </span>
                      </div>
                      <StructuredAnswerCanvas
                        response={item.advisorResponse}
                        question={findPreviousUserQuestion(
                          conversation.messages,
                          visibleStartIndex + index,
                        )}
                        onPromptFill={handlePromptFill}
                        showcaseReveal={showcaseMode}
                      />
                    </>
                  ) : (
                    <>
                      <div className="flex items-center justify-between gap-3">
                        <p className="text-xs font-semibold text-[color:var(--ink-muted)]">
                          {item.role === "user" ? "你" : "FundGene 教练"}
                        </p>
                        <span className="text-xs text-[color:var(--ink-muted)]">
                          {formatTimestamp(item.createdAt)}
                        </span>
                      </div>
                      <p className="mt-2 whitespace-pre-wrap text-sm leading-7 text-[color:var(--ink-soft)]">
                        {item.content}
                      </p>
                    </>
                  )}
                </article>
              ))}

              {pendingQuestion ? (
                <>
                  <article className="coach-message coach-message-user">
                    <div className="flex items-center justify-between gap-3">
                      <p className="text-xs font-semibold text-[color:var(--ink-muted)]">
                        你
                      </p>
                      <span className="text-xs text-[color:var(--ink-muted)]">
                        正在发送
                      </span>
                    </div>
                    <p className="mt-2 whitespace-pre-wrap text-sm leading-7 text-[color:var(--ink-soft)]">
                      {pendingQuestion}
                    </p>
                  </article>

                  <article
                    className="coach-message coach-message-assistant"
                    aria-label="FundGene 教练正在生成回答"
                  >
                    <div className="coach-assistant-meta">
                      <p className="text-xs font-semibold text-[color:var(--ink-muted)]">
                        FundGene 教练
                      </p>
                      <span className="text-xs text-[color:var(--ink-muted)]">
                        正在生成
                      </span>
                    </div>
                    <div className="assistant-answer-card px-4 py-4">
                      <div className="flex items-center gap-3">
                        <span
                          aria-hidden="true"
                          className="size-2.5 rounded-full bg-[color:var(--accent-teal)] shadow-[0_0_18px_rgba(70,208,172,0.55)] motion-safe:animate-pulse"
                        />
                        <p className="text-sm font-bold text-[color:var(--ink-soft)]">
                          正在结合你的画像和这轮问题组织回答...
                        </p>
                      </div>
                      <p className="mt-2 text-xs leading-6 text-[color:var(--ink-muted)]">
                        完成后会显示解释、风险边界和下一步动作。
                      </p>
                    </div>
                  </article>
                </>
              ) : null}

              {queuedFollowUp &&
              !(coachMutation.isPending && pendingQuestion === queuedFollowUp.message) ? (
                <article className="coach-message coach-message-user">
                  <div className="flex items-center justify-between gap-3">
                    <p className="text-xs font-semibold text-[color:var(--ink-muted)]">
                      你
                    </p>
                    <span className="text-xs text-[color:var(--ink-muted)]">
                      已排队
                    </span>
                  </div>
                  <p className="mt-2 whitespace-pre-wrap text-sm leading-7 text-[color:var(--ink-soft)]">
                    {queuedFollowUp.message}
                  </p>
                  <p className="mt-2 text-xs leading-5 text-[color:var(--ink-muted)]">
                    上一条完成后发送；如果上一条被停止或失败，会先保留给你确认。
                  </p>
                </article>
              ) : null}
            </>
          ) : (
            <div className="grid h-full min-h-[20rem] place-items-center">
              <div className="max-w-2xl text-center">
                <Bot
                  aria-hidden="true"
                  className="mx-auto size-10 text-[color:var(--accent-teal)]"
                />
                <h3 className="mt-4 text-2xl font-semibold">今天想先问清楚什么？</h3>
                <div className="mt-5 grid gap-2 text-left">
                  {dailyBriefPrompts.map((prompt) => (
                    <button
                      key={prompt}
                      type="button"
                      className="group flex items-center justify-between gap-3 rounded-lg border border-[color:var(--line-soft)] bg-white/[0.04] px-3 py-3 text-left text-sm font-bold text-[color:var(--ink-soft)] transition-colors hover:bg-white/[0.08]"
                      onClick={() => handlePromptFill(prompt)}
                      disabled={runInProgress}
                    >
                      <span>{prompt}</span>
                      <ArrowRight
                        aria-hidden="true"
                        className="size-4 flex-none text-[color:var(--accent-teal)] transition-transform group-hover:translate-x-0.5"
                      />
                    </button>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>

        <form
          className="coach-composer shrink-0 border-t border-[color:var(--line-soft)] px-4 py-3 sm:px-6"
          onSubmit={handleSubmit}
        >
          {visibleMessages.length > 0 ? (
            <div className="agent-composer-suggestions mb-2 flex items-center gap-2 overflow-x-auto pb-1">
              <span className="shrink-0 text-xs font-semibold text-[color:var(--ink-muted)]">
                接着问
              </span>
              {dailyBriefPrompts.slice(0, 3).map((prompt) => (
                <button
                  key={prompt}
                  type="button"
                  className="shrink-0 rounded-full border border-[color:var(--line-soft)] bg-white/70 px-3 py-1 text-xs font-semibold text-[color:var(--ink-soft)] transition-colors hover:bg-white"
                  onClick={() => handlePromptFill(prompt)}
                >
                  {prompt}
                </button>
              ))}
            </div>
          ) : null}

          <label className="block rounded-[20px] border border-[rgba(0,113,227,0.16)] bg-white/82 p-3 shadow-[0_14px_34px_rgba(15,23,42,0.06)]">
            <span className="mb-2 flex items-center gap-2 text-xs font-bold uppercase tracking-[0.12em] text-[color:var(--accent-teal)]">
              <Sparkles aria-hidden="true" className="size-3.5" />
              任务输入
            </span>
            {isHandoffDraft ? (
              <span className="mb-2 block text-xs font-semibold normal-case tracking-normal text-[color:var(--ink-muted)]">
                已带入上一页的问题，确认无误后点击发送。
              </span>
            ) : null}
            <textarea
              ref={composerTextareaRef}
              className="min-h-[72px] w-full resize-y border-0 bg-transparent text-base leading-7 text-[color:var(--ink-strong)] outline-none placeholder:text-[color:var(--ink-muted)]"
              value={draft}
              onChange={(event) => setDraft(event.target.value)}
              placeholder={
                dailyBrief
                  ? `例如：请解释今天这个判断：${dailyBrief.headline}`
                  : "例如：我刚开始买基金，怎么理解风险等级和回撤？"
              }
            />
          </label>

          <div className="agent-composer-actions mt-2.5 flex flex-col gap-2.5 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex flex-wrap items-center gap-3">
              <button
                type="submit"
                className="action-button min-w-[7.5rem] disabled:cursor-not-allowed disabled:opacity-60"
                disabled={
                  runInProgress
                    ? draft.trim().length < 4 ||
                      queueFollowUpMutation.isPending ||
                      !stoppableRunId
                    : coachMutation.isPending
                }
              >
                <SendHorizontal aria-hidden="true" className="size-4" />
                {runInProgress
                  ? draft.trim().length >= 4
                    ? queueFollowUpMutation.isPending
                      ? "排队中"
                      : "排队发送"
                    : "生成中"
                  : isHandoffDraft
                    ? "确认发送"
                    : "发送"}
              </button>
              {stoppableRunId ? (
                <button
                  type="button"
                  className="action-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                  onClick={() => cancelRunMutation.mutate(stoppableRunId)}
                  disabled={cancelRunMutation.isPending}
                >
                  <CircleStop aria-hidden="true" className="size-4" />
                  {cancelRunMutation.isPending ? "停止中" : "停止"}
                </button>
              ) : null}
              <button
                type="button"
                className="action-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                onClick={() => setDraft("")}
                disabled={draft.length === 0}
              >
                清空
              </button>
              {queuedFollowUp ? (
                <button
                  type="button"
                  className="action-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                  onClick={() =>
                    discardQueuedFollowUpMutation.mutate(queuedFollowUp.sessionId)
                  }
                  disabled={discardQueuedFollowUpMutation.isPending}
                >
                  取消排队
                </button>
              ) : null}
            </div>
            <span className="agent-composer-hint text-xs leading-5 text-[color:var(--ink-muted)] sm:max-w-[21rem] sm:text-right">
              {queuedFollowUp
                ? "下一句已排队；上一条正常完成后会自动发送。"
                : runInProgress
                  ? cancelNotice ?? "正在生成上一条回答；你可以先排队下一句。"
                : isHandoffDraft
                  ? "从其他页面带来的问题不会自动发送，避免替你确认。"
                  : "回答会自动保留风险边界，并把建议转成下一步动作。"}
            </span>
          </div>

          {submitError ? (
            <div className="mt-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              {submitError}
            </div>
          ) : null}
          </form>
        </section>

        {!runPanelCollapsed ? (
          <AgentRunStatusPanel
            trace={observedTrace}
            liveEvents={liveRunEvents}
            replayEvents={observedReplayEvents}
            runId={observedRunId}
            runStatus={observedRunStatus}
            loading={traceQuery.isLoading || runEventsQuery.isLoading || runStatusQuery.isLoading}
            pending={runInProgress}
            dashboard={dashboard}
            dailyBrief={dailyBrief}
          />
        ) : null}
      </div>
    </div>
  );
}
