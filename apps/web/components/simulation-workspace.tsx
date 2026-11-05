"use client";

import Link from "next/link";
import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle,
  BarChart3,
  CheckCircle2,
  ClipboardCheck,
  Info,
  LineChart,
  ShieldCheck,
  TrendingDown,
  TrendingUp,
} from "lucide-react";

import {
  ApiError,
  getBehaviorProfile,
  getSessionUser,
  getSimulationSession,
  getSimulationReview,
  getSimulationScenarios,
  startSimulationSession,
  submitSimulationAction,
  type BehaviorProfile,
  type SimulationFeedback,
  type SimulationReview,
  type SimulationScenario,
  type SimulationSession,
} from "@/lib/api";
import {
  formatBiasTag,
  formatBiasTags,
  formatProductCopy,
  formatRiskLevel,
} from "@/lib/display-labels";
import { buildAgentPromptHref } from "@/lib/navigation";
import { MetricCard } from "./metric-card";
import { SectionBlock } from "./section-block";

function joinClasses(...parts: Array<string | false | null | undefined>) {
  return parts.filter(Boolean).join(" ");
}

function isApiError(error: unknown): error is ApiError {
  return error instanceof ApiError;
}

function formatTimestamp(value: string | null): string {
  if (!value) {
    return "待生成";
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

function splitInsightText(value: string): string[] {
  const parts = value.match(/[^。！？；]+[。！？；]?/g) ?? [value];
  return parts.map((part) => part.trim()).filter(Boolean);
}

function isSessionCompleted(session: SimulationSession | null): boolean {
  if (!session) {
    return false;
  }

  return session.status === "completed" || session.completedAt !== null;
}

function getProgressValue(session: SimulationSession | null): number {
  if (!session) {
    return 0;
  }

  if (!session.totalSteps || session.totalSteps <= 0) {
    return isSessionCompleted(session) ? 100 : 0;
  }

  const cappedStep = Math.min(session.currentStep, session.totalSteps);
  return Math.max(0, Math.min(100, Math.round((cappedStep / session.totalSteps) * 100)));
}

function scenarioIcon(index: number) {
  if (index === 0) {
    return TrendingDown;
  }
  if (index === 1) {
    return TrendingUp;
  }
  return ClipboardCheck;
}

function scenarioStatusLabel(
  scenario: SimulationScenario,
  selectedScenario: SimulationScenario | null,
  activeSession: SimulationSession | null,
) {
  if (activeSession?.scenarioId === scenario.id && !isSessionCompleted(activeSession)) {
    return "当前进行中";
  }
  if (scenario.id === selectedScenario?.id) {
    return "已选中";
  }
  if (scenario.completed) {
    return "已完成";
  }
  return "未开始";
}

function ScenarioMiniChart() {
  return (
    <svg
      className="simulation-market-chart"
      viewBox="0 0 620 250"
      role="img"
      aria-label="历史市场回放曲线"
    >
      {[0, 1, 2, 3].map((line) => (
        <line
          key={line}
          x1="28"
          x2="592"
          y1={46 + line * 48}
          y2={46 + line * 48}
          className="simulation-chart-grid"
        />
      ))}
      <polyline
        className="simulation-chart-line"
        points="30,108 70,92 112,116 150,101 190,88 230,72 270,104 310,122 350,160 390,188 430,206 470,152 510,132 552,142 590,118"
      />
      <line x1="350" x2="350" y1="36" y2="220" className="simulation-chart-now" />
      <circle cx="350" cy="160" r="8" className="simulation-chart-dot" />
      <text x="310" y="28" className="simulation-chart-label">
        当前时间点
      </text>
      <text x="28" y="236" className="simulation-chart-axis">
        03-01
      </text>
      <text x="290" y="236" className="simulation-chart-axis">
        03-17
      </text>
      <text x="552" y="236" className="simulation-chart-axis">
        03-29
      </text>
    </svg>
  );
}

function StageTimeline({
  session,
  scenario,
}: {
  session: SimulationSession | null;
  scenario: SimulationScenario | null;
}) {
  const total = session?.totalSteps && session.totalSteps > 0 ? session.totalSteps : 4;
  const current = session ? Math.min(Math.max(session.currentStep, 1), total) : 0;
  const labels =
    scenario?.timelinePreview && scenario.timelinePreview.length >= total
      ? scenario.timelinePreview.slice(0, total)
      : ["事件出现", "市场发酵", "恐慌扩散", "逐步企稳"].slice(0, total);

  return (
    <ol className="simulation-stage-line" aria-label="情境进程">
      {labels.map((label, index) => {
        const step = index + 1;
        const done = current > 0 && step < current;
        const active = current > 0 && step === current;
        return (
          <li key={`${label}-${step}`} className={active ? "simulation-stage-active" : ""}>
            <span>{done ? <CheckCircle2 aria-hidden="true" className="size-4" /> : step}</span>
            <strong>阶段 {step}</strong>
            <p>{label}</p>
          </li>
        );
      })}
    </ol>
  );
}

function getTrainingBiasLabels(
  scenario: SimulationScenario | null,
  behavior: BehaviorProfile,
): string[] {
  const focus =
    scenario?.biasFocus && scenario.biasFocus.length > 0
      ? scenario.biasFocus
      : behavior.biasTags;
  const labels = formatBiasTags(focus).filter(Boolean);
  return labels.length > 0 ? labels.slice(0, 3) : ["情绪反应", "风险纪律"];
}

function formatReviewEvidence(value: string): string {
  const [rawBias, ...rest] = value.split("：");
  if (rest.length > 0) {
    return `${formatBiasTag(rawBias)}：${formatProductCopy(rest.join("："))}`;
  }

  return formatProductCopy(value);
}

function buildReviewFallbackAction(review: SimulationReview): string {
  const firstBias = review.biasSignals[0] ? formatBiasTag(review.biasSignals[0]) : "冲动触发点";
  return `下次遇到类似场景时，先停下来检查一次${firstBias}，再决定是否继续动作。`;
}

function SimulationReviewRoom({
  review,
  reviewError,
  isLoading,
  session,
  onBack,
  onRestart,
}: {
  review: SimulationReview | null;
  reviewError: Error | null;
  isLoading: boolean;
  session: SimulationSession;
  onBack: () => void;
  onRestart: () => void;
}) {
  if (isLoading) {
    return (
      <div className="simulation-review-room">
        <div className="simulation-inline-state">正在读取最终复盘...</div>
      </div>
    );
  }

  if (reviewError) {
    return (
      <div className="simulation-review-room">
        <div className="simulation-inline-state simulation-inline-error">
          复盘读取失败：{reviewError.message}
        </div>
        <button type="button" className="action-button-secondary" onClick={onBack}>
          返回训练室
        </button>
      </div>
    );
  }

  if (!review) {
    return (
      <div className="simulation-review-room">
        <div className="simulation-inline-state">当前训练已完成，但复盘结果尚未可用。</div>
        <button type="button" className="action-button-secondary" onClick={onBack}>
          返回训练室
        </button>
      </div>
    );
  }

  const focusItems =
    review.improvementAreas.length > 0
      ? review.improvementAreas
      : review.biasSignals.map((item) => `${formatBiasTag(item)}需要继续观察。`);
  const nextActions =
    review.recommendedNextActions.length > 0
      ? review.recommendedNextActions.map((item) => formatProductCopy(item))
      : [buildReviewFallbackAction(review)];
  const evidenceItems = review.behaviorEvidenceCandidates.map(formatReviewEvidence);
  const reflectionItems =
    review.reflectionQuestions.length > 0
      ? review.reflectionQuestions
      : ["这次最容易让你失守的是哪个信息点？", "下一次行动前，你要先检查哪条计划边界？"];

  return (
    <div className="simulation-review-room" data-testid="simulation-review-room">
      <div className="simulation-review-summary">
        <div>
          <p className="section-kicker">最终复盘</p>
          <h2>这次训练先记住一件事</h2>
          <p>{formatProductCopy(review.overallAssessment)}</p>
        </div>
        <dl>
          <div>
            <dt>情境</dt>
            <dd>{review.scenarioTitle ?? session.scenarioTitle ?? "历史情境训练"}</dd>
          </div>
          <div>
            <dt>完成时间</dt>
            <dd>{formatTimestamp(review.completedAt)}</dd>
          </div>
        </dl>
      </div>

      <div className="simulation-review-grid">
        <section>
          <p className="section-kicker">暴露的触发点</p>
          <ul>
            {(focusItems.length > 0 ? focusItems : ["这次没有形成明确触发点，建议回到教练继续拆解。"])
              .slice(0, 3)
              .map((item) => (
                <li key={item}>{formatProductCopy(item)}</li>
              ))}
          </ul>
        </section>
        <section>
          <p className="section-kicker">下一次先做什么</p>
          <ul>
            {nextActions.slice(0, 3).map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </section>
        <section>
          <p className="section-kicker">待确认线索</p>
          <ul>
            {(evidenceItems.length > 0 ? evidenceItems : ["这次只作为训练记录，不自动改写你的行为画像。"])
              .slice(0, 2)
              .map((item) => (
                <li key={item}>{item}</li>
              ))}
          </ul>
          <small>这些只是候选线索，需要你确认或用后续训练继续验证。</small>
        </section>
      </div>

      <div className="simulation-review-footer">
        <div>
          <p className="section-kicker">可以带回教练的问题</p>
          <p>{formatProductCopy(reflectionItems[0] ?? "这次训练里我最该复盘哪一步？")}</p>
        </div>
        <div>
          <button type="button" className="action-button-secondary" onClick={onBack}>
            返回训练室
          </button>
          <Link
            href={buildAgentPromptHref({
              focus: "simulation",
              from: "simulation",
              prompt:
                "请帮我复盘这次模拟训练里最容易失守的节点，并告诉我下次行动前要先检查什么。",
              sourceIds: {
                simulation_session_id: session.id,
                scenario_id: review.scenarioId ?? session.scenarioId,
              },
            })}
            className="action-button-secondary"
          >
            去教练继续追问
          </Link>
          <button type="button" className="action-button" onClick={onRestart}>
            开始下一次训练
          </button>
        </div>
      </div>
    </div>
  );
}

export function SimulationWorkspace() {
  const queryClient = useQueryClient();
  const [selectedScenarioId, setSelectedScenarioId] = useState<string | null>(null);
  const [activeSession, setActiveSession] = useState<SimulationSession | null>(null);
  const [latestFeedback, setLatestFeedback] = useState<SimulationFeedback | null>(null);
  const [selectedActionId, setSelectedActionId] = useState<string | null>(null);
  const [draftRationale, setDraftRationale] = useState("");
  const [draftWorry, setDraftWorry] = useState("");
  const [draftImpulsePlan, setDraftImpulsePlan] = useState("");
  const [startError, setStartError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [reviewRequested, setReviewRequested] = useState(false);
  const [showTrainingGuide, setShowTrainingGuide] = useState(false);

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

  const scenariosQuery = useQuery({
    queryKey: ["simulation-scenarios"],
    queryFn: getSimulationScenarios,
    enabled: isOnboarded,
    retry: false,
  });

  const scenarios = scenariosQuery.data?.scenarios ?? [];
  const resolvedScenarioId =
    selectedScenarioId && scenarios.some((item) => item.id === selectedScenarioId)
      ? selectedScenarioId
      : scenariosQuery.data?.recommendedScenarioId ?? scenarios[0]?.id ?? null;
  const selectedScenario =
    scenarios.find((item) => item.id === resolvedScenarioId) ?? null;
  const pendingActiveSessionId = scenariosQuery.data?.activeSessionId ?? null;

  const activeSessionQuery = useQuery({
    queryKey: ["simulation-session", pendingActiveSessionId],
    queryFn: () => getSimulationSession(pendingActiveSessionId ?? ""),
    enabled: Boolean(isOnboarded && pendingActiveSessionId && !activeSession),
    retry: false,
  });
  const displayedActiveSession = activeSession ?? activeSessionQuery.data ?? null;
  const displayedActionId = selectedActionId;

  const reviewQuery = useQuery({
    queryKey: ["simulation-review", displayedActiveSession?.id],
    queryFn: () => getSimulationReview(displayedActiveSession?.id ?? ""),
    enabled: Boolean(displayedActiveSession?.id && reviewRequested),
    retry: false,
  });

  const startMutation = useMutation({
    mutationFn: async (scenarioId: string) =>
      startSimulationSession({ scenarioId }),
    onSuccess: async (nextSession) => {
      setActiveSession(nextSession);
      setLatestFeedback(null);
      setSelectedActionId(null);
      setDraftRationale("");
      setDraftWorry("");
      setDraftImpulsePlan("");
      setReviewRequested(false);
      setStartError(null);
      if (nextSession.scenarioId) {
        setSelectedScenarioId(nextSession.scenarioId);
      }
      await queryClient.invalidateQueries({ queryKey: ["simulation-scenarios"] });
    },
    onError: (error) => {
      setStartError(error instanceof Error ? error.message : "无法启动情境，请稍后重试。");
    },
  });

  const resumeMutation = useMutation({
    mutationFn: (sessionId: string) => getSimulationSession(sessionId),
    onSuccess: (session) => {
      setActiveSession(session);
      setLatestFeedback(null);
      setSelectedActionId(null);
      setDraftRationale("");
      setDraftWorry("");
      setDraftImpulsePlan("");
      setReviewRequested(isSessionCompleted(session));
      setStartError(null);
      if (session.scenarioId) {
        setSelectedScenarioId(session.scenarioId);
      }
    },
    onError: (error) => {
      setStartError(error instanceof Error ? error.message : "无法恢复上次训练，请稍后重试。");
    },
  });

  const actionMutation = useMutation({
    mutationFn: async () => {
      const session = displayedActiveSession;
      if (!session) {
        throw new Error("请先启动一个情境训练。");
      }

      if (!displayedActionId) {
        throw new Error("请先选择本轮要执行的动作。");
      }

      const trimmedRationale = draftRationale.trim();
      if (!trimmedRationale) {
        throw new Error("请至少写一句判断理由；如果不确定，可以写“我暂不确定，想先观察风险”。");
      }

      return submitSimulationAction({
        sessionId: session.id,
        eventId: session.activeEvent?.id ?? null,
        actionId: displayedActionId,
        rationale: trimmedRationale,
        worry: draftWorry.trim(),
        impulseControlPlan: draftImpulsePlan.trim(),
      });
    },
    onSuccess: async (result) => {
      setActionError(null);
      setActiveSession(result.session);
      setLatestFeedback(result.feedback);
      setSelectedActionId(null);
      setDraftRationale("");
      setDraftWorry("");
      setDraftImpulsePlan("");
      setReviewRequested(result.reviewReady);
      await queryClient.invalidateQueries({ queryKey: ["simulation-scenarios"] });
      if (result.reviewReady) {
        await Promise.all([
          queryClient.invalidateQueries({ queryKey: ["dashboard"] }),
          queryClient.invalidateQueries({ queryKey: ["behavior-profile"] }),
        ]);
      }
    },
    onError: (error) => {
      setActionError(error instanceof Error ? error.message : "无法提交当前动作。");
    },
  });

  if (sessionQuery.isLoading) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/60 px-5 py-4 text-sm text-[color:var(--ink-soft)]">
        正在初始化历史情境训练上下文...
      </div>
    );
  }

  if (isApiError(sessionQuery.error) && sessionQuery.error.status === 401) {
    return (
      <SectionBlock
        eyebrow="情境训练"
        title="历史情境训练需要先登录。"
        description="登录后才能保存场景选择、动作记录和最终复盘。"
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
        当前情境训练工作区不可用：{sessionQuery.error?.message ?? "未知错误"}
      </div>
    );
  }

  const sessionUser = sessionQuery.data;

  if (!sessionUser.onboardingCompleted) {
    return (
      <SectionBlock
        eyebrow="情境训练"
        title="先完成建档，再进入历史情境训练。"
        description="情境训练会结合你的风险基线和行为偏差。"
      >
        <div className="grid gap-4 lg:grid-cols-3">
          <MetricCard
            label="当前用户"
            value={sessionUser.displayName ?? "已登录用户"}
            detail={sessionUser.email ?? sessionUser.id}
            accent="moss"
          />
          <MetricCard
            label="训练前提"
            value="待完成"
            detail="先保存基础画像与问卷，再开始历史情境。"
            accent="gold"
          />
            <MetricCard
              label="训练方式"
              value="先解释"
              detail="每一步都要求写下理由，再进入动作反馈和最终复盘。"
              accent="clay"
          />
        </div>
        <div className="flex flex-wrap gap-3">
          <Link href="/onboarding" className="action-button">
            去完成建档
          </Link>
          <Link href="/dashboard" className="action-button-secondary">
            返回工作台
          </Link>
        </div>
      </SectionBlock>
    );
  }

  if (behaviorQuery.isLoading) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/60 px-5 py-4 text-sm text-[color:var(--ink-soft)]">
        正在同步行为画像与历史情境训练入口...
      </div>
    );
  }

  if (behaviorQuery.error || !behaviorQuery.data) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
        无法读取行为画像：{behaviorQuery.error?.message ?? "未知错误"}
      </div>
    );
  }

  const behavior = behaviorQuery.data;
  const review = reviewQuery.data ?? null;
  const activeSessionCompleted = isSessionCompleted(displayedActiveSession);
  const activeEvent = displayedActiveSession?.activeEvent ?? null;
  const progressValue = getProgressValue(displayedActiveSession);
  const trainingBiasLabels = getTrainingBiasLabels(selectedScenario, behavior);

  function handleStartScenario() {
    if (!selectedScenario) {
      setStartError("请先从左侧选择一个情境。");
      return;
    }

    setStartError(null);
    setActionError(null);
    startMutation.mutate(selectedScenario.id);
  }

  function handleResumeSession() {
    const sessionId = scenariosQuery.data?.activeSessionId;
    if (!sessionId) {
      setStartError("当前没有可恢复的训练。");
      return;
    }

    setStartError(null);
    resumeMutation.mutate(sessionId);
  }

  function handleSubmitAction(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setActionError(null);
    actionMutation.mutate();
  }

  function handleRequestReview() {
    if (!displayedActiveSession?.id) {
      return;
    }

    setReviewRequested(true);
  }

  return (
    <div className="simulation-command-page space-y-6">
      <section className="simulation-training-room">
        <header className="simulation-room-heading">
          <div>
            <p className="section-kicker">模拟训练 / 历史情境</p>
            <h1>用历史情境练一次不冲动的判断</h1>
            <p>
              基于真实市场事件的阶段推演，复盘当时可见信息，训练理性决策与情绪掌控。
            </p>
          </div>
          <button
            type="button"
            className="simulation-help-pill"
            aria-expanded={showTrainingGuide}
            aria-controls="simulation-training-guide"
            onClick={() => setShowTrainingGuide((value) => !value)}
          >
            <Info aria-hidden="true" className="size-4" />
            训练说明
          </button>
        </header>

        {showTrainingGuide ? (
          <div
            className="simulation-help-popover"
            id="simulation-training-guide"
            role="region"
            aria-label="训练说明"
          >
            <div>
              <strong>怎么完成一次训练</strong>
              <p>先选择历史情境，再在每个阶段基于当时可见信息做一个动作选择。</p>
            </div>
            <div>
              <strong>为什么必须写理由</strong>
              <p>理由用于复盘你的判断链路，只生成候选观察，确认后才会写入画像。</p>
            </div>
            <div>
              <strong>安全边界</strong>
              <p>这里是训练，不是交易建议；不会下单，也不会承诺收益。</p>
            </div>
          </div>
        ) : null}

        {reviewRequested && displayedActiveSession ? (
          <SimulationReviewRoom
            review={review}
            reviewError={reviewQuery.error ?? null}
            isLoading={reviewQuery.isLoading}
            session={displayedActiveSession}
            onBack={() => setReviewRequested(false)}
            onRestart={() => {
              setActiveSession(null);
              setLatestFeedback(null);
              setSelectedActionId(null);
              setDraftRationale("");
              setDraftWorry("");
              setDraftImpulsePlan("");
              setReviewRequested(false);
            }}
          />
        ) : (
        <div className="simulation-room-grid">
            <div className="simulation-room-main">
            <div className="simulation-scenario-tabs" id="simulation-scenario-list">
              {scenariosQuery.isLoading ? (
                <div className="simulation-inline-state">正在加载历史情境...</div>
              ) : scenariosQuery.error ? (
                <div className="simulation-inline-state simulation-inline-error">
                  情境列表暂时不可用：{scenariosQuery.error.message}
                </div>
              ) : scenarios.length === 0 ? (
                <div className="simulation-inline-state">当前还没有可训练的情境。</div>
              ) : null}
              {(scenarios.length > 0 ? scenarios.slice(0, 3) : []).map((scenario, index) => {
                const Icon = scenarioIcon(index);
                const active = scenario.id === selectedScenario?.id;
                return (
                  <button
                    key={scenario.id}
                    type="button"
                    className={joinClasses("simulation-scenario-tab", active && "simulation-scenario-tab-active")}
                    onClick={() => setSelectedScenarioId(scenario.id)}
                    aria-pressed={active}
                  >
                    <span className="simulation-scenario-icon">
                      <Icon aria-hidden="true" className="size-7" />
                    </span>
                    <span>
                      <strong>{scenario.title}</strong>
                      <em>{scenario.synopsis}</em>
                      <small>{scenarioStatusLabel(scenario, selectedScenario, displayedActiveSession)}</small>
                    </span>
                  </button>
                );
              })}
            </div>

            <div className="simulation-stage-panel">
              <div className="simulation-panel-title">
                <div>
                  <p className="section-kicker">情境进程</p>
                  <h2>
                    {displayedActiveSession
                      ? `当前阶段 ${displayedActiveSession.currentStep}/${displayedActiveSession.totalSteps}`
                      : "情境预览"}
                  </h2>
                </div>
                <span>
                  <Info aria-hidden="true" className="size-4" />
                  {displayedActiveSession ? "本阶段基于当时可见信息" : "开始后逐步开放阶段信息"}
                </span>
              </div>
              <StageTimeline session={displayedActiveSession} scenario={selectedScenario} />
              <div className="simulation-chart-grid-layout">
                <div>
                  <p className="section-kicker">市场回放</p>
                  <ScenarioMiniChart />
                  <p className="mt-3 text-xs leading-5 text-[color:var(--ink-muted)]">
                    数据为历史回放，仅供模拟训练使用，不代表未来走势与实际收益。
                  </p>
                </div>
                <div className="simulation-event-brief">
                  <p className="section-kicker">当时的关键事件</p>
                  <ul>
                    {(activeEvent?.marketContext
                      ? splitInsightText(activeEvent.marketContext)
                      : selectedScenario?.timelinePreview ?? [
                          "市场快速下跌，情绪信号变强。",
                          "部分行业出现短期压力。",
                          "需要先核对风险承受能力和原计划。",
                        ]
                    )
                      .slice(0, 3)
                      .map((item) => (
                        <li key={item}>{item}</li>
                      ))}
                  </ul>
                  <div>
                    <span>
                      <BarChart3 aria-hidden="true" className="size-4" />
                      指数走势
                    </span>
                    <span>
                      <LineChart aria-hidden="true" className="size-4" />
                      资金流向
                    </span>
                    <span>
                      <ShieldCheck aria-hidden="true" className="size-4" />
                      新闻资讯
                    </span>
                  </div>
                </div>
              </div>
            </div>

            <div className="simulation-pending-review">
              <div>
                <p className="section-kicker">待回顾</p>
                <h2>你的行为证据预览</h2>
                <p>
                  提交后只生成候选观察，确认后才会保存到行为画像。
                </p>
              </div>
              <div className="simulation-review-card">
                {displayedActiveSession?.actions[0] ? (
                  <>
                    <strong>
                      你选择了：{displayedActiveSession.actions[0].choiceLabel}
                    </strong>
                    <p>{displayedActiveSession.actions[0].reflection ?? "尚未填写理由。"}</p>
                  </>
                ) : (
                  <>
                    <strong>还没有提交本轮动作</strong>
                    <p>选择动作并写下理由后，这里会预览待确认记录。</p>
                  </>
                )}
              </div>
              <div className="simulation-save-card">
                <AlertTriangle aria-hidden="true" className="size-5" />
                <strong>确认后才保存</strong>
                <p>写入你的行为画像前，用于后续洞察与训练优化。</p>
              </div>
            </div>
          </div>

          <aside className="simulation-decision-card" id="simulation-decision-room">
            <div className="simulation-decision-head">
              <p className="section-kicker">训练任务单</p>
              <h2>{activeEvent?.prompt ?? "基于当前阶段的信息，做出你的决策并说明理由。"}</h2>
            </div>

            {displayedActiveSession && !activeSessionCompleted && activeEvent ? (
              <form onSubmit={handleSubmitAction}>
                <div className="simulation-form-scroll" data-testid="simulation-task-scroll">
                  <fieldset>
                    <legend>选择你的行动</legend>
                    <div className="simulation-action-list">
                      {activeEvent.availableActions.map((choice) => (
                        <label
                          key={choice.id}
                          className={joinClasses(
                            "simulation-action-option",
                            displayedActionId === choice.id && "simulation-action-option-active",
                          )}
                        >
                          <input
                            type="radio"
                            name="simulation-action"
                            checked={displayedActionId === choice.id}
                            onChange={() => setSelectedActionId(choice.id)}
                          />
                          <span>
                            <strong>{choice.label}</strong>
                            <em>{choice.description ?? "记录这个动作的理由。"}</em>
                          </span>
                        </label>
                      ))}
                    </div>
                    {!displayedActionId ? (
                      <p className="mt-2 text-xs font-semibold text-[color:var(--ink-muted)]">
                        请主动选择一个行动；系统不会替你默认选中任何动作。
                      </p>
                    ) : null}
                  </fieldset>

                  <label className="simulation-form-field">
                    <span>请先说明你的理由（必填）</span>
                    <textarea
                      value={draftRationale}
                      onChange={(event) => setDraftRationale(event.target.value)}
                      placeholder={
                        displayedActiveSession.reflectionPrompt ??
                        "请结合当前信息，说明你的判断依据和考虑..."
                      }
                    />
                    <small>{draftRationale.length} / 300</small>
                  </label>

                  <label className="simulation-form-field">
                    <span>此时你的担忧主要来自？</span>
                    <select
                      className="field-input"
                      value={draftWorry}
                      onChange={(event) => setDraftWorry(event.target.value)}
                    >
                      <option value="">请选择最主要的担忧</option>
                      <option value="担心继续下跌">担心继续下跌</option>
                      <option value="担心错过反弹">担心错过反弹</option>
                      <option value="担心和原计划冲突">担心和原计划冲突</option>
                      <option value="暂时说不清">暂时说不清</option>
                    </select>
                  </label>

                  <div className="simulation-control-check">
                    <p>冲动控制清单（对自己打个分）</p>
                    {["信息是否充分？", "情绪是否平稳？", "计划是否清晰？", "仓位是否合理？"].map((item) => (
                      <div key={item}>
                        <span>{item}</span>
                        <label><input type="radio" name={item} />低</label>
                        <label><input type="radio" name={item} />中</label>
                        <label><input type="radio" name={item} />高</label>
                      </div>
                    ))}
                  </div>
                </div>

                <div className="simulation-form-actions">
                  <button
                    type="submit"
                    className="action-button w-full justify-center"
                    disabled={
                      actionMutation.isPending ||
                      activeEvent.availableActions.length === 0 ||
                      !displayedActionId ||
                      draftRationale.trim().length === 0
                    }
                  >
                    {actionMutation.isPending ? "提交动作中..." : "提交决策并继续"}
                  </button>
                  <p className="simulation-submit-note">
                    提交后进入下一阶段，稍后可在“待回顾”中确认保存。
                  </p>
                  {actionError ? (
                    <div className="command-inline-error" role="alert">
                      {actionError}
                    </div>
                  ) : null}
                </div>
              </form>
            ) : (
              <div className="simulation-start-card">
                <p>
                  {activeSessionCompleted
                    ? "本次训练已完成，可以先查看复盘，再决定是否开始下一次训练。"
                    : "先启动所选情境，FundGene 会给出当前阶段信息和可选动作。"}
                </p>
                {activeSessionCompleted ? (
                  <button
                    type="button"
                    className="action-button w-full justify-center"
                    onClick={handleRequestReview}
                    disabled={reviewQuery.isLoading}
                  >
                    {reviewQuery.isLoading ? "正在读取复盘..." : "查看最终复盘"}
                  </button>
                ) : (
                  <button
                    type="button"
                    className="action-button w-full justify-center"
                    onClick={handleStartScenario}
                    disabled={
                      startMutation.isPending ||
                      scenariosQuery.isLoading ||
                      Boolean(pendingActiveSessionId && !displayedActiveSession)
                    }
                  >
                    {startMutation.isPending
                      ? "启动中..."
                      : pendingActiveSessionId && !displayedActiveSession
                        ? "先继续上次训练"
                        : "开始训练"}
                  </button>
                )}
                {pendingActiveSessionId && !displayedActiveSession ? (
                  <button
                    type="button"
                    className="action-button-secondary w-full justify-center"
                    onClick={handleResumeSession}
                    disabled={resumeMutation.isPending}
                  >
                    {resumeMutation.isPending ? "恢复中..." : "继续上次"}
                  </button>
                ) : null}
                {startError ? (
                  <div className="command-inline-error" role="alert">
                    {startError}
                  </div>
                ) : null}
              </div>
            )}

            <div className="simulation-side-facts">
              <span>风险等级：{formatRiskLevel(behavior.riskLevel)}</span>
              <span>训练偏差：{trainingBiasLabels.join(" / ")}</span>
              <span>
                训练状态：{displayedActiveSession ? (activeSessionCompleted ? "待复盘" : "进行中") : "未开始"}
              </span>
            </div>
          </aside>
        </div>
        )}
      </section>
      <section className="simulation-after-panel">
        <div>
          <p className="section-kicker">训练进度</p>
          <div
            className="simulation-progress-bar"
            role="progressbar"
            aria-label="训练进度"
            aria-valuemin={0}
            aria-valuemax={100}
            aria-valuenow={progressValue}
          >
            <span style={{ width: `${progressValue}%` }} />
          </div>
          <p>
            {displayedActiveSession
              ? `${activeSessionCompleted ? "已完成" : "进行中"} · ${progressValue}% · ${formatTimestamp(displayedActiveSession.startedAt)}`
              : "启动后会显示阶段进度、即时反馈和复盘入口。"}
          </p>
        </div>
        <div>
          <p className="section-kicker">即时反馈</p>
          <p>
            {latestFeedback
              ? formatProductCopy(latestFeedback.summary)
              : "每提交一次动作，这里会显示本轮反馈。"}
          </p>
        </div>
        <div>
          <p className="section-kicker">完成后</p>
          {displayedActiveSession ? (
            <div className="simulation-after-actions">
              <button
                type="button"
                className={activeSessionCompleted ? "action-button" : "action-button-secondary"}
                onClick={handleRequestReview}
                disabled={!activeSessionCompleted || reviewQuery.isLoading}
              >
                {reviewQuery.isLoading ? "正在读取复盘..." : "查看最终复盘"}
              </button>
              <Link href="/dashboard" className="action-button-secondary">
                回工作台
              </Link>
            </div>
          ) : (
            <p>先开始一个情境，完成所有阶段后这里会出现复盘入口。</p>
          )}
        </div>
      </section>

    </div>
  );
}
