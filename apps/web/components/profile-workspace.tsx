"use client";

import Link from "next/link";
import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  BookOpenCheck,
  BriefcaseBusiness,
  CheckCircle2,
  ClipboardCheck,
  Clock3,
  KeyRound,
  UserRoundCog,
} from "lucide-react";

import {
  acceptProfilePendingProposal,
  ApiError,
  getProfileContext,
  getProfileLlmSettings,
  getProfilePendingProposals,
  rejectProfilePendingProposal,
  updateProfileLlmSettings,
  type ProfilePendingProposal,
} from "@/lib/api";
import { formatBiasTags, formatProductCopy, formatRiskLevel } from "@/lib/display-labels";
import {
  Button,
  ErrorState,
  Field,
  LoadingState,
  Panel,
  ProgressBar,
  StatusPill,
} from "./ui/primitives";

function isApiError(error: unknown): error is ApiError {
  return error instanceof ApiError;
}

function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "待录入";
  }

  return new Intl.NumberFormat("zh-CN", {
    style: "currency",
    currency: "CNY",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatDate(value: string | null | undefined): string {
  if (!value) {
    return "暂无记录";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "short",
    day: "numeric",
  }).format(date);
}

function formatUpdateTime(value: string | null | undefined): string {
  if (!value) {
    return "待更新";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("zh-CN", {
    month: "short",
    day: "numeric",
  }).format(date);
}

function proposalOperationLabel(
  action: "accept" | "reject",
  proposal: ProfilePendingProposal | undefined,
) {
  if (!proposal) {
    return null;
  }
  return action === "accept" ? `正在确认 ${proposal.title}` : `正在拒绝 ${proposal.title}`;
}

function formatAutomationLabel(key: string): string {
  if (key === "daily_brief") {
    return "每日简报";
  }
  if (key === "weekly_portfolio") {
    return "组合巡检";
  }
  if (key === "news_watch") {
    return "资讯观察";
  }
  if (key === "behavior_observation") {
    return "行为观察";
  }
  return key;
}

export function ProfileWorkspace() {
  const queryClient = useQueryClient();
  const [operationError, setOperationError] = useState<string | null>(null);
  const [llmOperationError, setLlmOperationError] = useState<string | null>(null);
  const [llmModelName, setLlmModelName] = useState<string | null>(null);
  const [llmApiKey, setLlmApiKey] = useState("");
  const [llmEnabled, setLlmEnabled] = useState<boolean | null>(null);

  const contextQuery = useQuery({
    queryKey: ["profile-context"],
    queryFn: getProfileContext,
    retry: false,
  });

  const proposalsQuery = useQuery({
    queryKey: ["profile-pending-proposals"],
    queryFn: getProfilePendingProposals,
    retry: false,
  });

  const llmSettingsQuery = useQuery({
    queryKey: ["profile-llm-settings"],
    queryFn: getProfileLlmSettings,
    retry: false,
  });

  const acceptMutation = useMutation({
    mutationFn: acceptProfilePendingProposal,
    onMutate: () => setOperationError(null),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["profile-context"] }),
        queryClient.invalidateQueries({ queryKey: ["profile-pending-proposals"] }),
        queryClient.invalidateQueries({ queryKey: ["behavior-profile"] }),
        queryClient.invalidateQueries({ queryKey: ["dashboard-state"] }),
      ]);
    },
    onError: (error) => {
      setOperationError(
        isApiError(error) ? error.message : "待确认资料暂时无法确认。",
      );
    },
  });

  const rejectMutation = useMutation({
    mutationFn: rejectProfilePendingProposal,
    onMutate: () => setOperationError(null),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["profile-context"] }),
        queryClient.invalidateQueries({ queryKey: ["profile-pending-proposals"] }),
      ]);
    },
    onError: (error) => {
      setOperationError(
        isApiError(error) ? error.message : "待确认资料暂时无法拒绝。",
      );
    },
  });

  const llmSettingsMutation = useMutation({
    mutationFn: updateProfileLlmSettings,
    onMutate: () => setLlmOperationError(null),
    onSuccess: async () => {
      setLlmApiKey("");
      await queryClient.invalidateQueries({ queryKey: ["profile-llm-settings"] });
    },
    onError: (error) => {
      setLlmOperationError(
        isApiError(error) ? error.message : "模型设置暂时无法保存。",
      );
    },
  });

  const context = contextQuery.data;
  const proposals = proposalsQuery.data;
  const llmSettings = llmSettingsQuery.data;
  const pendingProposals = proposals?.proposals ?? [];
  const readiness = context?.contextReadiness;
  const isLoading =
    contextQuery.isLoading || proposalsQuery.isLoading || llmSettingsQuery.isLoading;
  const error = contextQuery.error ?? proposalsQuery.error ?? llmSettingsQuery.error;
  const activeAcceptTitle = proposalOperationLabel(
    "accept",
    pendingProposals.find(
      (proposal) => proposal.id === acceptMutation.variables?.proposalId,
    ),
  );
  const activeRejectTitle = proposalOperationLabel(
    "reject",
    pendingProposals.find(
      (proposal) => proposal.id === rejectMutation.variables?.proposalId,
    ),
  );
  const llmStatusTone = llmSettings?.configured ? "positive" : "warning";
  const llmStatusLabel =
    llmSettings?.source === "user"
      ? "用户 Key 已配置"
      : llmSettings?.source === "workspace"
        ? "工作区 Key 已配置"
        : "未配置";
  const effectiveLlmModelName =
    llmModelName ?? llmSettings?.modelName ?? "deepseek:deepseek-v4-pro";
  const effectiveLlmEnabled = llmEnabled ?? llmSettings?.enabled ?? true;

  const saveLlmSettings = () => {
    llmSettingsMutation.mutate({
      modelName: effectiveLlmModelName,
      apiKey: llmApiKey,
      enabled: effectiveLlmEnabled,
    });
  };

  return (
    <div className="profile-command-page command-single-page">
      <div className="command-page-heading">
        <div className="min-w-0">
          <p className="section-kicker">上下文中心</p>
          <h1>这些资料决定 FundGene 怎么判断“和我有关”。</h1>
          <p>关键状态先进入待确认，不会被系统静默保存。</p>
        </div>
        <StatusPill tone="accent">资料</StatusPill>
      </div>

      {isLoading ? (
        <LoadingState>正在读取个人上下文...</LoadingState>
      ) : error ? (
        <ErrorState>
          {isApiError(error) && error.status === 401
            ? "请先登录后再查看资料中心。"
            : "资料中心暂时无法读取。"}
        </ErrorState>
      ) : context ? (
        <div className="profile-command-grid">
          <Panel variant="strong" className="profile-summary-panel">
            <div className="profile-summary-metric">
              <span>上下文完整度</span>
              <strong>{readiness?.readyCount ?? 0}/{readiness?.totalCount ?? 0}</strong>
              <ProgressBar
                value={
                  readiness && readiness.totalCount > 0
                    ? Math.round((readiness.readyCount / readiness.totalCount) * 100)
                    : 0
                }
                label="资料覆盖度"
              />
              <p>资料越完整，判断越贴近你的情况。</p>
            </div>
            <div className="profile-summary-metric">
              <span>待确认资料</span>
              <strong>{proposals?.pendingCount ?? 0} 条</strong>
              <p>已处理 {proposals?.resolvedCount ?? 0} 条</p>
              {operationError ? <p className="command-inline-error">{operationError}</p> : null}
              {activeAcceptTitle || activeRejectTitle ? (
                <p className="command-operation-note">
                  {activeAcceptTitle ?? activeRejectTitle}
                </p>
              ) : null}
            </div>
            <div className="profile-summary-metric">
              <span>资料更新时间</span>
              <strong>{formatUpdateTime(context.portfolioContext.latestSnapshotDate)}</strong>
              <p>大部分资料为最新或已确认状态。</p>
            </div>
          </Panel>

          <div className="profile-main-column">
            <div className="profile-context-cards">
              <Panel className="profile-context-card">
                <div className="profile-card-heading">
                  <ClipboardCheck aria-hidden="true" className="size-5 text-[color:var(--accent-teal)]" />
                  <h2>风险画像</h2>
                </div>
                <dl>
                  <div>
                    <dt>当前等级</dt>
                    <dd>{formatRiskLevel(context.riskProfile.riskLevel)}</dd>
                  </div>
                  <div>
                    <dt>风险分数</dt>
                    <dd>{context.riskProfile.latestRiskScore ?? "待完成问卷"}</dd>
                  </div>
                  <div>
                    <dt>行为焦点</dt>
                    <dd className="profile-chip-line">
                      {formatBiasTags(context.behaviorProfile.biasTags).map((tag) => (
                        <StatusPill key={tag} tone="accent">
                          {tag}
                        </StatusPill>
                      ))}
                      {context.behaviorProfile.biasTags.length === 0 ? (
                        <StatusPill tone="neutral">待观察</StatusPill>
                      ) : null}
                    </dd>
                  </div>
                </dl>
                <Link href="/onboarding" className="command-link-button">更新画像</Link>
              </Panel>

              <Panel className="profile-context-card">
                <div className="profile-card-heading">
                  <BriefcaseBusiness aria-hidden="true" className="size-5 text-[color:var(--accent-teal)]" />
                  <h2>组合上下文</h2>
                </div>
                <dl>
                  <div>
                    <dt>最近快照</dt>
                    <dd>{formatDate(context.portfolioContext.latestSnapshotDate)}</dd>
                  </div>
                  <div>
                    <dt>总资产</dt>
                    <dd>{formatCurrency(context.portfolioContext.totalValue)}</dd>
                  </div>
                </dl>
                <p>
                  {context.portfolioContext.summary ??
                    "录入组合后，FundGene 才能判断新闻和风险是否真正与你有关。"}
                </p>
                <Link href="/portfolio?from=profile" className="command-link-button">查看组合</Link>
              </Panel>

              <Panel className="profile-context-card">
                <div className="profile-card-heading">
                  <BookOpenCheck aria-hidden="true" className="size-5 text-[color:var(--accent-teal)]" />
                  <h2>学习与训练</h2>
                </div>
                <dl>
                  <div>
                    <dt>学习进度</dt>
                    <dd>{context.learningContext.overallProgressPercentage}%</dd>
                  </div>
                  <div>
                    <dt>推荐课程</dt>
                    <dd>
                      {context.learningContext.recommendedCourseTitle
                        ? formatProductCopy(context.learningContext.recommendedCourseTitle)
                        : "待生成"}
                    </dd>
                  </div>
                </dl>
                <p>
                  {context.simulationContext.latestReviewSummary ??
                    "训练复盘会帮助 FundGene 判断你是否需要先练习纪律，而不是继续看更多信息。"}
                </p>
                <Link href="/learning?from=profile" className="command-link-button">查看学习状态</Link>
              </Panel>
            </div>

            <Panel className="profile-agent-map-panel">
              <div className="profile-card-heading">
                <UserRoundCog aria-hidden="true" className="size-5 text-[color:var(--accent-teal)]" />
                <h2>Agent 使用这些资料时会做什么</h2>
              </div>
              <div className="profile-agent-map-grid">
                <div>
                  <span>1</span>
                  <strong>先读授权资料</strong>
                  <p>只读取画像、组合、学习训练和已同步资讯，不越过授权边界。</p>
                </div>
                <div>
                  <span>2</span>
                  <strong>再判断相关性</strong>
                  <p>把新闻或训练结果映射到你的持仓结构、风险等级和行为焦点。</p>
                </div>
                <div>
                  <span>3</span>
                  <strong>最后等待确认</strong>
                  <p>高影响内容先进入待确认资料，不会自动改写长期画像。</p>
                </div>
              </div>
              <p className="profile-agent-map-note">
                这就是答辩中要强调的边界：FundGene 做解释、整理和候选建议，不做交易执行。
              </p>
            </Panel>
          </div>

          <Panel className="profile-decision-panel">
            <div className="command-panel-heading">
              <div>
                <p className="section-kicker">待确认资料</p>
                <h2>
                  {(proposals?.pendingCount ?? 0) > 0
                    ? `${proposals?.pendingCount ?? 0} 条等待你决定`
                    : "暂无待确认项目"}
                </h2>
              </div>
              <UserRoundCog aria-hidden="true" className="size-5 text-[color:var(--accent-teal)]" />
            </div>

            {pendingProposals.length > 0 ? (
              <div className="profile-proposal-list">
                  {pendingProposals.map((proposal) => {
                    const isAccepting =
                      acceptMutation.isPending &&
                      acceptMutation.variables?.proposalId === proposal.id;
                    const isRejecting =
                      rejectMutation.isPending &&
                      rejectMutation.variables?.proposalId === proposal.id;

                    return (
                      <article
                        key={proposal.id}
                        className="profile-proposal-card"
                      >
                        <div className="flex flex-wrap items-start justify-between gap-2">
                          <div className="min-w-0">
                            <StatusPill tone="warning">
                              {proposal.writebackLabel}
                            </StatusPill>
                            <h3>
                              {proposal.title}
                            </h3>
                          </div>
                          <div className="flex gap-2">
                            <Button
                              type="button"
                              variant="secondary"
                              disabled={isRejecting || isAccepting}
                              onClick={() =>
                                rejectMutation.mutate({
                                  proposalId: proposal.id,
                                  reason: "用户在资料中心选择暂不写入。",
                                })
                              }
                            >
                              {isRejecting ? "处理中" : "暂不写入"}
                            </Button>
                            <Button
                              type="button"
                              disabled={isAccepting || isRejecting}
                              onClick={() =>
                                acceptMutation.mutate({
                                  proposalId: proposal.id,
                                  reason: "用户在资料中心确认记录。",
                                })
                              }
                            >
                              {isAccepting ? "确认中" : "确认记录"}
                            </Button>
                          </div>
                        </div>
                        <dl>
                          <div>
                            <dt>来源</dt>
                            <dd>{proposal.sourceLabel}</dd>
                          </div>
                          <div>
                            <dt>证据</dt>
                            <dd>{proposal.evidenceSummary}</dd>
                          </div>
                          <div>
                            <dt>原因</dt>
                            <dd>{proposal.reason}</dd>
                          </div>
                          <div>
                            <dt>安全边界</dt>
                            <dd>{proposal.safetyNote}</dd>
                          </div>
                        </dl>
                      </article>
                    );
                  })}
              </div>
            ) : (
              <div className="profile-empty-proposal">
                <CheckCircle2 aria-hidden="true" className="size-5 text-[color:var(--accent-teal)]" />
                <p>当前没有等待确认的资料。</p>
                <span>已处理 {proposals?.resolvedCount ?? 0} 条</span>
              </div>
            )}

          </Panel>

          <div className="profile-settings-column">
            <Panel className="profile-auth-panel">
              <div className="profile-auth-block">
                <div className="profile-card-heading">
                  <Clock3 aria-hidden="true" className="size-5 text-[color:var(--accent-teal)]" />
                  <h2>授权读取范围</h2>
                </div>
                <div className="profile-auth-list">
                  {context.authorizationScope.map((item) => (
                    <div
                      key={item.key}
                      className="profile-auth-row"
                    >
                      <span>{item.label}</span>
                      <StatusPill tone={item.readable ? "positive" : "warning"}>
                        {item.readable ? "已可读取" : "待补齐"}
                      </StatusPill>
                    </div>
                  ))}
                </div>
                <div className="profile-auth-list profile-automation-auth">
                  <p>自动任务授权</p>
                  {context.automationAuthorizations.map((item) => (
                    <div
                      key={item.automationKey}
                      className="profile-auth-row"
                    >
                      <span>{formatAutomationLabel(item.automationKey)}</span>
                      <StatusPill tone={item.enabled ? "positive" : "warning"}>
                        {item.enabled ? item.cadenceLabel : "未开启"}
                      </StatusPill>
                    </div>
                  ))}
                </div>
                <p className="profile-auth-note">
                  自动任务只能读取这里明确存在的资料。高影响保存仍需确认。
                </p>
              </div>
            </Panel>

            <Panel className="profile-auth-panel profile-llm-panel">
              <div className="profile-auth-block profile-llm-block" id="llm-settings">
                <div className="profile-card-heading">
                  <KeyRound aria-hidden="true" className="size-5 text-[color:var(--accent-teal)]" />
                  <h2>模型设置</h2>
                </div>
                <div className="profile-auth-list">
                  <div className="profile-auth-row">
                    <span>LLM 连接状态</span>
                    <StatusPill tone={llmStatusTone}>{llmStatusLabel}</StatusPill>
                  </div>
                  <div className="profile-auth-row">
                    <span>当前模型</span>
                    <span>{llmSettings?.modelName ?? effectiveLlmModelName}</span>
                  </div>
                  <div className="profile-auth-row">
                    <span>Key 预览</span>
                    <span>{llmSettings?.maskedApiKey ?? "未保存"}</span>
                  </div>
                </div>
                {llmSettings?.warning ? (
                  <p className="command-inline-error">{llmSettings.warning}</p>
                ) : null}
                {llmOperationError ? (
                  <p className="command-inline-error">{llmOperationError}</p>
                ) : null}
                <div className="profile-llm-form">
                  <Field
                    label="DeepSeek API Key"
                    type="password"
                    autoComplete="off"
                    value={llmApiKey}
                    placeholder="sk-..."
                    onChange={(event) => setLlmApiKey(event.target.value)}
                  />
                  <Field
                    label="模型名称"
                    value={effectiveLlmModelName}
                    onChange={(event) => setLlmModelName(event.target.value)}
                  />
                  <label className="profile-llm-toggle">
                    <input
                      type="checkbox"
                      checked={effectiveLlmEnabled}
                      onChange={(event) => setLlmEnabled(event.target.checked)}
                    />
                    启用我的模型 Key
                  </label>
                  <Button
                    type="button"
                    disabled={llmSettingsMutation.isPending}
                    onClick={saveLlmSettings}
                  >
                    {llmSettingsMutation.isPending ? "保存中" : "保存模型设置"}
                  </Button>
                </div>
                <p className="profile-auth-note">
                  保存后只用于教练回答生成；页面只显示脱敏预览。
                </p>
              </div>
            </Panel>
          </div>
        </div>
      ) : null}
    </div>
  );
}
