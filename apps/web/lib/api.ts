type RequestOptions = {
  method?: "DELETE" | "GET" | "POST" | "PATCH" | "PUT";
  body?: unknown;
  timeoutMs?: number;
};

export type JsonObject = Record<string, unknown>;

export class ApiError extends Error {
  status: number;
  runId: string | null;

  constructor(status: number, message: string, options: { runId?: string | null } = {}) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.runId = options.runId ?? null;
  }
}

export type RiskLevel = "conservative" | "balanced" | "growth" | null;
export type LearningProgressStatus =
  | "not_started"
  | "in_progress"
  | "completed";

export type SessionUser = {
  id: string;
  email: string | null;
  profileId: string | null;
  onboardingCompleted: boolean;
  displayName: string | null;
  investingExperience: string | null;
  monthlyContributionBand: string | null;
  primaryGoal: string | null;
};

export type UserSummary = {
  id: string;
  displayName: string;
  investingExperience: string;
  monthlyContributionBand: string | null;
  primaryGoal: string | null;
  onboardingCompleted: boolean;
};

export type BehaviorProfile = {
  userId: string;
  riskLevel: RiskLevel;
  biasTags: string[];
  evidence: string[];
  updatedAt: string | null;
};

export type DashboardSummaryCard = {
  label: string;
  value: string;
  detail: string;
};

export type DashboardCoachActivity = {
  sessionId: string;
  topic: string;
  intent: string | null;
  question: string;
  answerFocus: string;
  recommendedAction: string | null;
  updatedAt: string | null;
};

export type DashboardLearningStatus = {
  overallProgressPercentage: number;
  completedCoursesCount: number;
  totalCourses: number;
  recommendedCourseSlug: string | null;
  recommendedCourseTitle: string | null;
};

export type DashboardPortfolioStatus = {
  hasReport: boolean;
  latestSnapshotId: string | null;
  latestSnapshotDate: string | null;
  totalValue: number | null;
  summary: string | null;
};

export type DashboardSimulationStatus = {
  completedSessionsCount: number;
  recommendedScenarioSlug: string | null;
  recommendedScenarioTitle: string | null;
  latestSessionId: string | null;
  latestScenarioSlug: string | null;
  latestScenarioTitle: string | null;
  latestReviewSummary: string | null;
  latestCompletedAt: string | null;
};

export type DashboardNewsStatus = {
  hasAnalysis: boolean;
  latestAnalysisId: string | null;
  latestItemId: string | null;
  latestItemType: string | null;
  latestTitle: string | null;
  latestSummary: string | null;
  sourceName: string | null;
  beginnerTranslation: string | null;
  recommendedAction: string | null;
  generatedAt: string | null;
};

export type DashboardEvidenceSource =
  | "profile"
  | "portfolio"
  | "news_policy"
  | "learning"
  | "simulation"
  | "behavior"
  | "coach_history";

export type DashboardEvidence = {
  id: string;
  sourceType: DashboardEvidenceSource;
  sourceId: string | null;
  claim: string;
  beginnerTranslation: string;
  supportLevel: "strong" | "medium" | "weak";
  freshnessLabel: string;
  riskBoundary: string;
};

export type SafeNextAction = {
  id: string;
  type:
    | "learn"
    | "inspect_portfolio"
    | "run_simulation"
    | "ask_coach"
    | "record_behavior"
    | "read_news_context";
  label: string;
  reason: string;
  targetRoute: string;
  targetParams: Record<string, string>;
  expectedWriteback: string;
  safetyNote: string;
};

export type DashboardDailyBrief = {
  briefId: string;
  asOf: string | null;
  status:
    | "ready"
    | "starter"
    | "missing_profile"
    | "missing_portfolio"
    | "fallback";
  priorityLevel: "urgent" | "attention" | "learning" | "stable";
  headline: string;
  beginnerExplanation: string;
  evidence: DashboardEvidence[];
  primaryAction: SafeNextAction;
  secondaryActions: SafeNextAction[];
  doNotDo: string;
  sourceCoverage: Record<DashboardEvidenceSource, boolean>;
  traceId: string | null;
};

export type DashboardState = {
  userId: string;
  onboardingCompleted: boolean;
  riskLevel: RiskLevel;
  biasTags: string[];
  latestRiskScore: number | null;
  dailyBrief: DashboardDailyBrief | null;
  nextActions: string[];
  summaryCards: DashboardSummaryCard[];
  learningStatus: DashboardLearningStatus | null;
  portfolioStatus: DashboardPortfolioStatus | null;
  simulationStatus: DashboardSimulationStatus | null;
  newsStatus: DashboardNewsStatus | null;
  latestCoachActivity: DashboardCoachActivity | null;
};

export type LearningCourseSummary = {
  slug: string;
  title: string;
  focus: string;
  description: string;
  estimatedDurationMinutes: number;
  sectionCount: number;
  completedSectionCount: number;
  progressPercentage: number;
  status: LearningProgressStatus;
  nextSectionSlug: string | null;
  nextSectionTitle: string | null;
};

export type LearningPathState = {
  pathSlug: string;
  title: string;
  description: string;
  overallProgressPercentage: number;
  completedCoursesCount: number;
  totalCourses: number;
  recommendedCourseSlug: string | null;
  recommendedCourseTitle: string | null;
  courses: LearningCourseSummary[];
};

export type LearningSectionDetail = {
  slug: string;
  title: string;
  summary: string;
  estimatedDurationMinutes: number;
  position: number;
  completed: boolean;
};

export type LearningCourseDetail = {
  pathSlug: string;
  pathTitle: string;
  courseSlug: string;
  title: string;
  focus: string;
  description: string;
  estimatedDurationMinutes: number;
  sectionCount: number;
  completedSectionCount: number;
  progressPercentage: number;
  status: LearningProgressStatus;
  nextSectionSlug: string | null;
  nextSectionTitle: string | null;
  sections: LearningSectionDetail[];
};

export type LearningProgressInput = {
  courseSlug: string;
  sectionSlug: string;
};

export type LearningProgressUpdate = {
  courseSlug: string;
  progressPercentage: number;
  completedSectionCount: number;
  sectionCount: number;
  status: LearningProgressStatus;
  nextSectionSlug: string | null;
  nextSectionTitle: string | null;
};

export type OnboardingProfileInput = {
  displayName: string;
  investingExperience: string;
  monthlyContributionBand: string;
  primaryGoal: string;
};

export type QuestionnaireInput = {
  answers: Record<string, number>;
};

export type AuthCredentialsInput = {
  email: string;
  password: string;
};

export type AdvisorStructuredResponse = {
  answer: string;
  intent: string;
  citations: string[];
  riskNotice: string;
  recommendedActions: string[];
  recommendedActionTargets: AdvisorActionTarget[];
  followUpQuestions: string[];
};

export type AdvisorActionTarget = {
  label: string;
  href: string;
  intent: string;
  kind: "internal_link";
};

export type AssistantSessionSummary = {
  id: string;
  topic: string;
  contextType: string;
  latestIntent: string | null;
  lastQuestion: string | null;
  lastAnswerPreview: string | null;
  lastRecommendedAction: string | null;
  messageCount: number;
  createdAt: string | null;
  updatedAt: string | null;
};

export type AssistantConversationMessage = {
  id: string;
  role: "user" | "assistant";
  content: string;
  messageType: string;
  createdAt: string | null;
  agentRunId: string | null;
  advisorResponse: AdvisorStructuredResponse | null;
};

export type AssistantConversationState = {
  session: AssistantSessionSummary | null;
  messages: AssistantConversationMessage[];
};

export type AssistantMessageInput = {
  message: string;
  sessionId?: string | null;
  startNewSession?: boolean;
  context?: {
    fromRoute?: string | null;
    focus?: string | null;
    sourceIds?: Record<string, string>;
    dailyBriefId?: string | null;
  };
};

export type AgentRunTraceRun = {
  id: string;
  intent: string | null;
  runStatus: string | null;
  policyStatus: string | null;
  orchestratorVersion: string | null;
  startedAt: string | null;
  completedAt: string | null;
  latencyMs: number | null;
  fallbackReason: string | null;
  toolTrace: JsonObject;
};

export type AgentRunTraceStep = {
  id: string;
  stepName: string;
  sequence: number | null;
  status: string | null;
  latencyMs: number | null;
  inputPayload: JsonObject;
  outputPayload: JsonObject;
  error: string | null;
};

export type AgentRunTraceToolCall = {
  id: string;
  stepId: string | null;
  toolName: string;
  permissionLevel: string | null;
  status: string | null;
  latencyMs: number | null;
  inputPayload: JsonObject;
  outputPayload: JsonObject;
  error: string | null;
};

export type AgentRunTraceEvidenceRef = {
  id: string;
  stepId: string | null;
  workerName: string | null;
  sourceType: string | null;
  sourceId: string | null;
  sourceVersion: string | null;
  quoteOrSummary: string | null;
  claim: string | null;
  supportSummary: string | null;
};

export type AgentRunTrace = {
  run: AgentRunTraceRun;
  contextSnapshot: JsonObject;
  steps: AgentRunTraceStep[];
  toolCalls: AgentRunTraceToolCall[];
  evidenceRefs: AgentRunTraceEvidenceRef[];
  stateUpdateProposals: JsonObject[];
};

export type AgentRunEvent = {
  id: string;
  runId: string;
  sequence: number;
  eventType: string;
  phase: string;
  title: string;
  status: string;
  at: string | null;
  durationMs: number | null;
  payload: JsonObject;
};

export type AgentRunEvents = {
  schemaVersion: string;
  runId: string;
  events: AgentRunEvent[];
};

export type QueuedFollowUp = {
  id: string;
  sessionId: string;
  queuedAfterRunId: string;
  message: string;
  status: string;
  createdAt: string;
  submittedAt: string | null;
  discardedAt: string | null;
};

export type QueuedFollowUpResponse = {
  schemaVersion: string;
  queuedFollowUp: QueuedFollowUp | null;
  message: string;
};

export type AgentRunStatus = {
  schemaVersion: string;
  runId: string;
  sessionId: string | null;
  status: string;
  runStatus: string | null;
  active: boolean;
  cancelRequested: boolean;
  startedAt: string | null;
  completedAt: string | null;
  latencyMs: number | null;
  message: string;
  queuedFollowUp: QueuedFollowUp | null;
};

export type ActiveAgentRuns = {
  schemaVersion: string;
  runs: AgentRunStatus[];
};

export type AssistantMessageStreamHandlers = {
  onEvent?: (event: AgentRunEvent) => void;
};

export type AgentRunCancelResult = {
  runId: string;
  status: string;
  cancelRequested: boolean;
  message: string;
};

export type PortfolioHoldingInput = {
  fundCode: string;
  fundName: string;
  fundType: string;
  marketValue: number;
};

export type PortfolioSnapshotInput = {
  snapshotDate: string;
  cashValue: number;
  holdings: PortfolioHoldingInput[];
};

export type PortfolioHolding = {
  fundCode: string;
  fundName: string;
  fundType: string;
  marketValue: number;
  weight: number;
};

export type PortfolioReport = {
  snapshotId: string;
  snapshotDate: string;
  totalValue: number;
  cashValue: number;
  summary: string;
  riskExposure: string[];
  concentrationFlags: string[];
  allocationBalance: string[];
  recommendedNextActions: string[];
  holdings: PortfolioHolding[];
  generatedAt: string | null;
};

export type PortfolioLatestState = {
  report: PortfolioReport | null;
};

export type PortfolioHistoryItem = {
  snapshotId: string;
  snapshotDate: string;
  totalValue: number;
  summary: string;
  generatedAt: string | null;
};

export type PortfolioHistoryState = {
  items: PortfolioHistoryItem[];
};

export type SimulationScenario = {
  id: string;
  slug: string | null;
  title: string;
  synopsis: string;
  marketPhase: string | null;
  difficulty: string | null;
  estimatedDurationMinutes: number | null;
  decisionCount: number | null;
  startingYear: number | null;
  headline: string | null;
  description: string | null;
  setup: string | null;
  objective: string | null;
  biasFocus: string[];
  tags: string[];
  timelinePreview: string[];
  recommended: boolean;
  completed: boolean;
};

export type SimulationScenarioCatalog = {
  scenarios: SimulationScenario[];
  recommendedScenarioId: string | null;
  activeSessionId: string | null;
};

export type SimulationSessionActionChoice = {
  id: string;
  label: string;
  description: string | null;
  biasSignal: string | null;
};

export type SimulationActionSummary = {
  eventId: string;
  stepIndex: number;
  choiceKey: string;
  choiceLabel: string;
  reflection: string | null;
  isRecommended: boolean;
  createdAt: string | null;
};

export type SimulationSessionEvent = {
  id: string;
  index: number;
  title: string;
  dateLabel: string | null;
  marketContext: string | null;
  prompt: string | null;
  decisionFocus: string[];
  availableActions: SimulationSessionActionChoice[];
};

export type SimulationSession = {
  id: string;
  scenarioId: string | null;
  scenarioTitle: string | null;
  status: string;
  stageLabel: string | null;
  currentStep: number;
  totalSteps: number | null;
  startedAt: string | null;
  completedAt: string | null;
  openingBrief: string | null;
  reflectionPrompt: string | null;
  activeEvent: SimulationSessionEvent | null;
  actions: SimulationActionSummary[];
};

export type SimulationFeedback = {
  summary: string;
  impact: string | null;
  disciplineSignals: string[];
  nextPrompt: string | null;
};

export type SimulationActionResult = {
  session: SimulationSession;
  feedback: SimulationFeedback | null;
  reviewReady: boolean;
};

export type SimulationReview = {
  sessionId: string;
  scenarioId: string | null;
  scenarioTitle: string | null;
  completedAt: string | null;
  overallAssessment: string;
  outcomeSummary: string | null;
  finalDisposition: string | null;
  biasSignals: string[];
  strengths: string[];
  improvementAreas: string[];
  recommendedNextActions: string[];
  reflectionQuestions: string[];
  behaviorEvidenceCandidates: string[];
  pendingStateProposal: string | null;
  scoreLabel: string | null;
};

export type SimulationSessionStartInput = {
  scenarioId: string;
};

export type SimulationActionInput = {
  sessionId: string;
  eventId?: string | null;
  actionId: string;
  rationale?: string;
  worry?: string;
  impulseControlPlan?: string;
};

export type NewsItem = {
  id: string;
  itemType: "news" | "policy" | null;
  title: string;
  source: string | null;
  category: string | null;
  summary: string;
  url: string | null;
  publishedAt: string | null;
  impactAreas: string[];
  tags: string[];
  relevanceScore: number | null;
  latestAnalysisId: string | null;
};

export type NewsCatalogState = {
  items: NewsItem[];
  policyItems: NewsItem[];
  generatedAt: string | null;
};

export type NewsAnalysisInput = {
  itemId?: string | null;
  headline?: string;
  body?: string;
};

export type NewsAnalysis = {
  id: string | null;
  itemId: string | null;
  item: NewsItem | null;
  headline: string;
  factSummary: string;
  impactPath: string[];
  uncertainty: string[];
  riskNotice: string;
  recommendedActions: string[];
  relatedLearning: string[];
  citations: string[];
  modelStatus: "enhanced" | "fallback" | "skipped" | "legacy";
  modelProvider: string | null;
  modelName: string | null;
  fallbackReason: string | null;
  agentProcess: {
    key: string;
    label: string;
    status: "completed" | "warning" | "failed";
    detail: string;
  }[];
  generatedAt: string | null;
};

export type AutomationKey =
  | "daily_brief"
  | "weekly_portfolio"
  | "news_watch"
  | "behavior_observation";

export type AutomationStatus = "ready" | "disabled" | "needs_profile" | "blocked";
export type AutomationRunStatus = "queued" | "running" | "succeeded" | "failed";

export type AutomationCadenceOption = {
  key: string;
  label: string;
};

export type AutomationRunSummary = {
  runId: string;
  agentRunId: string | null;
  status: AutomationRunStatus;
  startedAt: string | null;
  completedAt: string | null;
  summary: string | null;
  outputRef: string | null;
};

export type AutomationItem = {
  key: AutomationKey;
  title: string;
  summary: string;
  enabled: boolean;
  defaultEnabled: boolean;
  cadenceKey: string;
  cadenceLabel: string;
  cadenceOptions: AutomationCadenceOption[];
  readScope: string[];
  outputScope: string[];
  confirmationBoundary: string;
  safetyBoundary: string;
  status: AutomationStatus;
  disabledReason: string | null;
  lastRun: AutomationRunSummary | null;
  nextRunAt: string | null;
  canRunNow: boolean;
};

export type AutomationQueueItem = {
  automationKey: AutomationKey;
  title: string;
  nextRunAt: string | null;
  cadenceLabel: string;
};

export type AutomationDailyBriefSummary = {
  briefId: string;
  headline: string;
  beginnerExplanation: string;
  asOf: string | null;
  sourceCoverage: Record<string, boolean>;
};

export type AutomationNotification = {
  id: string;
  automationKey: AutomationKey;
  title: string;
  message: string;
  actionLabel: string | null;
  actionRoute: string | null;
  createdAt: string | null;
  readAt: string | null;
};

export type AutomationListState = {
  userId: string;
  updatedAt: string | null;
  activeCount: number;
  totalCount: number;
  automations: AutomationItem[];
  nextQueue: AutomationQueueItem[];
  dailyBriefSummary: AutomationDailyBriefSummary | null;
  recentNotifications: AutomationNotification[];
};

export type AutomationUpdateInput = {
  automationKey: AutomationKey;
  enabled?: boolean;
  cadenceKey?: string;
};

export type AutomationRunResult = {
  runId: string;
  agentRunId: string | null;
  automationKey: AutomationKey;
  status: AutomationRunStatus;
  triggerType: "manual" | "scheduled";
  dueAt: string | null;
  startedAt: string | null;
  completedAt: string | null;
  summary: string | null;
  outputRef: string | null;
  errorMessage: string | null;
  createdPendingProposalIds: string[];
  steps: {
    key: string;
    label: string;
    status: "queued" | "running" | "completed" | "failed";
    detail: string;
  }[];
  outputPayload: JsonObject;
};

export type ProfileReadinessItem = {
  key: string;
  label: string;
  ready: boolean;
  lastUpdatedAt: string | null;
  missingActionRoute: string | null;
};

export type ProfileContextState = {
  userId: string;
  displayName: string | null;
  contextReadiness: {
    readyCount: number;
    totalCount: number;
    items: ProfileReadinessItem[];
  };
  riskProfile: {
    riskLevel: RiskLevel;
    latestRiskScore: number | null;
    updatedAt: string | null;
  };
  behaviorProfile: {
    biasTags: string[];
    evidence: string[];
    updatedAt: string | null;
  };
  portfolioContext: {
    hasReport: boolean;
    latestSnapshotDate: string | null;
    totalValue: number | null;
    summary: string | null;
  };
  learningContext: {
    overallProgressPercentage: number;
    recommendedCourseTitle: string | null;
  };
  simulationContext: {
    latestReviewSummary: string | null;
    latestCompletedAt: string | null;
  };
  automationAuthorizations: {
    automationKey: string;
    enabled: boolean;
    cadenceLabel: string;
  }[];
  authorizationScope: {
    key: string;
    label: string;
    readable: boolean;
  }[];
  pendingProposalCount: number;
};

export type ProfilePendingProposal = {
  id: string;
  title: string;
  sourceLabel: string;
  evidenceSummary: string;
  writebackLabel: string;
  targetType: string;
  targetId: string | null;
  patchPreview: JsonObject;
  reason: string;
  validatorStatus: string;
  validatorMessage: string | null;
  status: "pending" | "accepted" | "rejected" | "applied";
  safetyNote: string;
  createdAt: string | null;
  runId: string | null;
};

export type ProfilePendingProposalsState = {
  pendingCount: number;
  resolvedCount: number;
  proposals: ProfilePendingProposal[];
};

export type ProfileProposalDecisionInput = {
  proposalId: string;
  reason?: string;
};

export type ProfileProposalDecisionResult = {
  proposal: ProfilePendingProposal;
  appliedWriteback: boolean;
};

export type ProfileLlmSettings = {
  provider: "deepseek";
  modelName: string;
  configured: boolean;
  enabled: boolean;
  source: "user" | "workspace" | "none";
  maskedApiKey: string | null;
  updatedAt: string | null;
  warning: string | null;
};

export type ProfileLlmSettingsInput = {
  provider?: "deepseek";
  modelName: string;
  apiKey?: string;
  enabled: boolean;
};

function resolveApiBase() {
  const configuredBase = process.env.NEXT_PUBLIC_FUNDGENE_API_URL;
  const rawBase =
    configuredBase ??
    (typeof window !== "undefined" && window.location.hostname === "localhost"
      ? "http://localhost:8000"
      : "http://127.0.0.1:8000");
  return rawBase.endsWith("/") ? rawBase.slice(0, -1) : rawBase;
}

function asObject(value: unknown): JsonObject {
  return typeof value === "object" && value !== null ? (value as JsonObject) : {};
}

function asString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function formatErrorDetail(value: unknown): string | null {
  const direct = asString(value);
  if (direct !== null) {
    return direct;
  }

  if (Array.isArray(value)) {
    const messages = value
      .map((item) => asString(asObject(item).msg))
      .filter((message): message is string => message !== null);
    return messages.length > 0 ? messages.join("；") : null;
  }

  return null;
}

function asStringLike(value: unknown): string | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }

  return asString(value);
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter((item): item is string => typeof item === "string");
}

function asBoolean(value: unknown): boolean {
  return value === true;
}

function asNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function pickString(source: JsonObject, keys: string[]): string | null {
  for (const key of keys) {
    const value = asStringLike(source[key]);
    if (value) {
      return value;
    }
  }

  return null;
}

function pickNumber(source: JsonObject, keys: string[]): number | null {
  for (const key of keys) {
    const value = asNumber(source[key]);
    if (value !== null) {
      return value;
    }
  }

  return null;
}

function pickStringArray(source: JsonObject, keys: string[]): string[] {
  for (const key of keys) {
    const value = asStringArray(source[key]);
    if (value.length > 0) {
      return value;
    }

    const singleValue = asStringLike(source[key]);
    if (singleValue) {
      return [singleValue];
    }
  }

  return [];
}

function pickObject(source: JsonObject, keys: string[]): JsonObject {
  for (const key of keys) {
    const value = asObject(source[key]);
    if (Object.keys(value).length > 0) {
      return value;
    }
  }

  return {};
}

function pickArray(source: JsonObject, keys: string[]): unknown[] {
  for (const key of keys) {
    const value = source[key];
    if (Array.isArray(value)) {
      return value;
    }
  }

  return [];
}

function asRiskLevel(value: unknown): RiskLevel {
  if (
    value === "conservative" ||
    value === "balanced" ||
    value === "growth"
  ) {
    return value;
  }

  return null;
}

function asLearningStatus(value: unknown): LearningProgressStatus | null {
  if (
    value === "not_started" ||
    value === "in_progress" ||
    value === "completed"
  ) {
    return value;
  }

  return null;
}

async function request(path: string, options: RequestOptions): Promise<unknown> {
  const url = `${resolveApiBase()}${path.startsWith("/api") ? path : `/api${path}`}`;
  const headers: Record<string, string> = {};
  if (options.body !== undefined) {
    headers["Content-Type"] = "application/json";
  }

  const controller = new AbortController();
  const timeoutMs = options.timeoutMs ?? 45_000;
  const timeout = globalThis.setTimeout(() => controller.abort(), timeoutMs);

  let response: Response;
  try {
    response = await fetch(url, {
      method: options.method ?? "GET",
      headers,
      body: options.body ? JSON.stringify(options.body) : undefined,
      cache: "no-store",
      credentials: "include",
      signal: controller.signal,
    });
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new ApiError(
        408,
        "服务正在启动，通常需要 20-60 秒。请稍等一下，系统会继续重试。",
      );
    }
    throw error;
  } finally {
    globalThis.clearTimeout(timeout);
  }

  if (!response.ok) {
    let message = "request failed";

    try {
      const payload = asObject(await response.json());
      message = formatErrorDetail(payload.detail) ?? message;
    } catch {
      const text = await response.text();
      message = text || message;
    }

    throw new ApiError(response.status, message);
  }

  if (response.status === 204) {
    return null;
  }

  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    return response.json();
  }

  return response.text();
}

function pickSessionSource(input: unknown): JsonObject {
  const root = asObject(input);
  const nested = asObject(root.user);
  return Object.keys(nested).length > 0 ? nested : root;
}

function parseSessionUser(input: unknown): SessionUser {
  const source = pickSessionSource(input);
  const profile = asObject(source.profile);

  const id = asString(source.id) ?? asString(profile.id) ?? "unknown";
  const onboardingCompleted =
    asBoolean(source.onboarding_completed) ||
    asBoolean(profile.onboarding_completed);

  return {
    id,
    email: asString(source.email),
    profileId: asString(source.profile_id),
    onboardingCompleted,
    displayName: asString(source.display_name) ?? asString(profile.display_name),
    investingExperience:
      asString(source.investing_experience) ??
      asString(profile.investing_experience),
    monthlyContributionBand:
      asString(source.monthly_contribution_band) ??
      asString(profile.monthly_contribution_band),
    primaryGoal: asString(source.primary_goal) ?? asString(profile.primary_goal),
  };
}

function parseUserSummary(input: unknown): UserSummary {
  const source = asObject(input);
  return {
    id: asString(source.id) ?? "unknown",
    displayName: asString(source.display_name) ?? "新用户",
    investingExperience: asString(source.investing_experience) ?? "beginner",
    monthlyContributionBand: asString(source.monthly_contribution_band),
    primaryGoal: asString(source.primary_goal),
    onboardingCompleted: asBoolean(source.onboarding_completed),
  };
}

function parseBehaviorProfile(input: unknown): BehaviorProfile {
  const source = asObject(input);
  return {
    userId: asString(source.user_id) ?? "unknown",
    riskLevel: asRiskLevel(source.risk_level),
    biasTags: asStringArray(source.bias_tags),
    evidence: asStringArray(source.evidence),
    updatedAt: asString(source.updated_at),
  };
}

function parseDashboardSummaryCards(input: unknown): DashboardSummaryCard[] {
  if (!Array.isArray(input)) {
    return [];
  }

  return input
    .map((item) => {
      const card = asObject(item);
      const label = asString(card.label);
      const value = asString(card.value);
      const detail = asString(card.detail);

      if (!label || !value || !detail) {
        return null;
      }

      return { label, value, detail };
    })
    .filter((item): item is DashboardSummaryCard => item !== null);
}

function parseDashboardCoachActivity(input: unknown): DashboardCoachActivity | null {
  const source = asObject(input);
  const sessionId = asString(source.session_id);
  const topic = asString(source.topic);
  const question = asString(source.question);
  const answerFocus = asString(source.answer_focus);

  if (!sessionId || !topic || !question || !answerFocus) {
    return null;
  }

  return {
    sessionId,
    topic,
    intent: asString(source.intent),
    question,
    answerFocus,
    recommendedAction: asString(source.recommended_action),
    updatedAt: asString(source.updated_at),
  };
}

function parseDashboardLearningStatus(
  input: unknown,
): DashboardLearningStatus | null {
  const source = asObject(input);
  const overallProgressPercentage = asNumber(source.overall_progress_percentage);
  const completedCoursesCount = asNumber(source.completed_courses_count);
  const totalCourses = asNumber(source.total_courses);

  if (
    overallProgressPercentage === null ||
    completedCoursesCount === null ||
    totalCourses === null
  ) {
    return null;
  }

  return {
    overallProgressPercentage,
    completedCoursesCount,
    totalCourses,
    recommendedCourseSlug: asString(source.recommended_course_slug),
    recommendedCourseTitle: asString(source.recommended_course_title),
  };
}

function parseDashboardPortfolioStatus(
  input: unknown,
): DashboardPortfolioStatus | null {
  const source = asObject(input);
  const hasReport = asBoolean(source.has_report);
  const hasAnyField =
    hasReport ||
    asString(source.latest_snapshot_id) !== null ||
    asString(source.latest_snapshot_date) !== null ||
    asNumber(source.total_value) !== null ||
    asString(source.summary) !== null;

  if (!hasAnyField) {
    return null;
  }

  return {
    hasReport,
    latestSnapshotId: asString(source.latest_snapshot_id),
    latestSnapshotDate: asString(source.latest_snapshot_date),
    totalValue: asNumber(source.total_value),
    summary: asString(source.summary),
  };
}

function parseDashboardSimulationStatus(
  input: unknown,
): DashboardSimulationStatus | null {
  const source = asObject(input);
  const completedSessionsCount = pickNumber(source, [
    "completed_sessions_count",
    "completedSessionsCount",
  ]);
  const hasAnyField =
    completedSessionsCount !== null ||
    pickString(source, ["recommended_scenario_slug", "recommendedScenarioSlug"]) !== null ||
    pickString(source, ["latest_session_id", "latestSessionId"]) !== null ||
    pickString(source, ["latest_review_summary", "latestReviewSummary"]) !== null;

  if (!hasAnyField) {
    return null;
  }

  return {
    completedSessionsCount: completedSessionsCount ?? 0,
    recommendedScenarioSlug: pickString(source, [
      "recommended_scenario_slug",
      "recommendedScenarioSlug",
    ]),
    recommendedScenarioTitle: pickString(source, [
      "recommended_scenario_title",
      "recommendedScenarioTitle",
    ]),
    latestSessionId: pickString(source, ["latest_session_id", "latestSessionId"]),
    latestScenarioSlug: pickString(source, [
      "latest_scenario_slug",
      "latestScenarioSlug",
    ]),
    latestScenarioTitle: pickString(source, [
      "latest_scenario_title",
      "latestScenarioTitle",
    ]),
    latestReviewSummary: pickString(source, [
      "latest_review_summary",
      "latestReviewSummary",
    ]),
    latestCompletedAt: pickString(source, [
      "latest_completed_at",
      "latestCompletedAt",
    ]),
  };
}

function parseDashboardNewsStatus(input: unknown): DashboardNewsStatus | null {
  const source = asObject(input);
  const hasAnalysis =
    asBoolean(source.has_analysis) || asBoolean(source.hasAnalysis);
  const hasAnyField =
    hasAnalysis ||
    pickString(source, ["latest_analysis_id", "latestAnalysisId"]) !== null ||
    pickString(source, ["latest_title", "latestTitle"]) !== null ||
    pickString(source, ["latest_summary", "latestSummary"]) !== null ||
    pickString(source, ["beginner_translation", "beginnerTranslation"]) !== null;

  if (!hasAnyField) {
    return null;
  }

  return {
    hasAnalysis,
    latestAnalysisId: pickString(source, [
      "latest_analysis_id",
      "latestAnalysisId",
    ]),
    latestItemId: pickString(source, ["latest_item_id", "latestItemId"]),
    latestItemType: pickString(source, ["latest_item_type", "latestItemType"]),
    latestTitle: pickString(source, ["latest_title", "latestTitle"]),
    latestSummary: pickString(source, ["latest_summary", "latestSummary"]),
    sourceName: pickString(source, ["source_name", "sourceName"]),
    beginnerTranslation: pickString(source, [
      "beginner_translation",
      "beginnerTranslation",
    ]),
    recommendedAction: pickString(source, [
      "recommended_action",
      "recommendedAction",
    ]),
    generatedAt: pickString(source, ["generated_at", "generatedAt"]),
  };
}

function asDashboardEvidenceSource(value: unknown): DashboardEvidenceSource | null {
  if (
    value === "profile" ||
    value === "portfolio" ||
    value === "news_policy" ||
    value === "learning" ||
    value === "simulation" ||
    value === "behavior" ||
    value === "coach_history"
  ) {
    return value;
  }
  return null;
}

function asSafeActionType(value: unknown): SafeNextAction["type"] | null {
  if (
    value === "learn" ||
    value === "inspect_portfolio" ||
    value === "run_simulation" ||
    value === "ask_coach" ||
    value === "record_behavior" ||
    value === "read_news_context"
  ) {
    return value;
  }
  return null;
}

function parseTargetParams(input: unknown): Record<string, string> {
  const source = asObject(input);
  return Object.fromEntries(
    Object.entries(source).filter(
      (entry): entry is [string, string] => typeof entry[1] === "string",
    ),
  );
}

function parseDashboardEvidence(input: unknown): DashboardEvidence | null {
  const source = asObject(input);
  const id = asString(source.id);
  const sourceType = asDashboardEvidenceSource(source.source_type ?? source.sourceType);
  const claim = asString(source.claim);
  const beginnerTranslation = pickString(source, [
    "beginner_translation",
    "beginnerTranslation",
  ]);
  const supportLevel = asString(source.support_level ?? source.supportLevel);
  const freshnessLabel = pickString(source, ["freshness_label", "freshnessLabel"]);
  const riskBoundary = pickString(source, ["risk_boundary", "riskBoundary"]);

  if (
    !id ||
    !sourceType ||
    !claim ||
    !beginnerTranslation ||
    !freshnessLabel ||
    !riskBoundary ||
    (supportLevel !== "strong" && supportLevel !== "medium" && supportLevel !== "weak")
  ) {
    return null;
  }

  return {
    id,
    sourceType,
    sourceId: pickString(source, ["source_id", "sourceId"]),
    claim,
    beginnerTranslation,
    supportLevel,
    freshnessLabel,
    riskBoundary,
  };
}

function parseSafeNextAction(input: unknown): SafeNextAction | null {
  const source = asObject(input);
  const id = asString(source.id);
  const type = asSafeActionType(source.type);
  const label = asString(source.label);
  const reason = asString(source.reason);
  const targetRoute = pickString(source, ["target_route", "targetRoute"]);
  const expectedWriteback = pickString(source, [
    "expected_writeback",
    "expectedWriteback",
  ]);
  const safetyNote = pickString(source, ["safety_note", "safetyNote"]);

  if (
    !id ||
    !type ||
    !label ||
    !reason ||
    !targetRoute ||
    !targetRoute.startsWith("/") ||
    !expectedWriteback ||
    !safetyNote
  ) {
    return null;
  }

  return {
    id,
    type,
    label,
    reason,
    targetRoute,
    targetParams: parseTargetParams(source.target_params ?? source.targetParams),
    expectedWriteback,
    safetyNote,
  };
}

function parseDashboardDailyBrief(input: unknown): DashboardDailyBrief | null {
  const source = asObject(input);
  const briefId = pickString(source, ["brief_id", "briefId"]);
  const status = asString(source.status);
  const priorityLevel = pickString(source, ["priority_level", "priorityLevel"]);
  const headline = asString(source.headline);
  const beginnerExplanation = pickString(source, [
    "beginner_explanation",
    "beginnerExplanation",
  ]);
  const primaryAction = parseSafeNextAction(
    source.primary_action ?? source.primaryAction,
  );
  const doNotDo = pickString(source, ["do_not_do", "doNotDo"]);

  if (
    !briefId ||
    !headline ||
    !beginnerExplanation ||
    !primaryAction ||
    !doNotDo ||
    (status !== "ready" &&
      status !== "starter" &&
      status !== "missing_profile" &&
      status !== "missing_portfolio" &&
      status !== "fallback") ||
    (priorityLevel !== "urgent" &&
      priorityLevel !== "attention" &&
      priorityLevel !== "learning" &&
      priorityLevel !== "stable")
  ) {
    return null;
  }

  const sourceCoverage = asObject(
    source.source_coverage ?? source.sourceCoverage,
  ) as Record<DashboardEvidenceSource, unknown>;
  const coverageEntries: DashboardEvidenceSource[] = [
    "profile",
    "portfolio",
    "news_policy",
    "learning",
    "simulation",
    "behavior",
    "coach_history",
  ];

  return {
    briefId,
    asOf: pickString(source, ["as_of", "asOf"]),
    status,
    priorityLevel,
    headline,
    beginnerExplanation,
    evidence: pickArray(source, ["evidence"])
      .map((item) => parseDashboardEvidence(item))
      .filter((item): item is DashboardEvidence => item !== null)
      .slice(0, 3),
    primaryAction,
    secondaryActions: pickArray(source, ["secondary_actions", "secondaryActions"])
      .map((item) => parseSafeNextAction(item))
      .filter((item): item is SafeNextAction => item !== null),
    doNotDo,
    sourceCoverage: Object.fromEntries(
      coverageEntries.map((key) => [key, sourceCoverage[key] === true]),
    ) as Record<DashboardEvidenceSource, boolean>,
    traceId: pickString(source, ["trace_id", "traceId"]),
  };
}

function parseDashboard(input: unknown): DashboardState {
  const source = asObject(input);
  return {
    userId: asString(source.user_id) ?? "unknown",
    onboardingCompleted: asBoolean(source.onboarding_completed),
    riskLevel: asRiskLevel(source.risk_level),
    biasTags: asStringArray(source.bias_tags),
    latestRiskScore: asNumber(source.latest_risk_score),
    dailyBrief: parseDashboardDailyBrief(source.daily_brief ?? source.dailyBrief),
    nextActions: asStringArray(source.next_actions),
    summaryCards: parseDashboardSummaryCards(source.summary_cards),
    learningStatus: parseDashboardLearningStatus(source.learning_status),
    portfolioStatus: parseDashboardPortfolioStatus(source.portfolio_status),
    simulationStatus: parseDashboardSimulationStatus(
      source.simulation_status ?? source.simulationStatus,
    ),
    newsStatus: parseDashboardNewsStatus(source.news_status ?? source.newsStatus),
    latestCoachActivity: parseDashboardCoachActivity(source.latest_coach_activity),
  };
}

function parseAdvisorStructuredResponse(
  input: unknown,
): AdvisorStructuredResponse | null {
  const source = asObject(input);
  const answer = asString(source.answer);
  const intent = asString(source.intent);
  const riskNotice = asString(source.risk_notice);

  if (!answer || !intent || !riskNotice) {
    return null;
  }

  return {
    answer,
    intent,
    citations: asStringArray(source.citations),
    riskNotice,
    recommendedActions: asStringArray(source.recommended_actions),
    recommendedActionTargets: parseAdvisorActionTargets(
      source.recommended_action_targets ?? source.recommendedActionTargets,
    ),
    followUpQuestions: asStringArray(source.follow_up_questions),
  };
}

function parseAdvisorActionTargets(input: unknown): AdvisorActionTarget[] {
  return pickArray({ items: input }, ["items"])
    .map((item) => {
      const source = asObject(item);
      const label = asString(source.label);
      const href = asString(source.href);
      const intent = asString(source.intent);
      const kind = asString(source.kind);
      if (
        !label ||
        !href ||
        !intent ||
        kind !== "internal_link" ||
        !href.startsWith("/")
      ) {
        return null;
      }
      return { label, href, intent, kind };
    })
    .filter((item): item is AdvisorActionTarget => item !== null);
}

function parseAssistantSessionSummary(input: unknown): AssistantSessionSummary | null {
  const source = asObject(input);
  const id = asString(source.id);
  const topic = asString(source.topic);
  const contextType = asString(source.context_type);

  if (!id || !topic || !contextType) {
    return null;
  }

  return {
    id,
    topic,
    contextType,
    latestIntent: asString(source.latest_intent),
    lastQuestion: asString(source.last_question),
    lastAnswerPreview: asString(source.last_answer_preview),
    lastRecommendedAction: asString(source.last_recommended_action),
    messageCount: asNumber(source.message_count) ?? 0,
    createdAt: asString(source.created_at),
    updatedAt: asString(source.updated_at),
  };
}

function parseAssistantConversationMessage(
  input: unknown,
): AssistantConversationMessage | null {
  const source = asObject(input);
  const id = asString(source.id);
  const role = asString(source.role);
  const content = asString(source.content);
  const messageType = asString(source.message_type);

  if (!id || (role !== "user" && role !== "assistant") || !content || !messageType) {
    return null;
  }

  return {
    id,
    role,
    content,
    messageType,
    createdAt: asString(source.created_at),
    agentRunId: asString(source.agent_run_id),
    advisorResponse: parseAdvisorStructuredResponse(source.advisor_response),
  };
}

function parseAssistantConversation(input: unknown): AssistantConversationState {
  const source = asObject(input);
  const rawMessages = Array.isArray(source.messages) ? source.messages : [];

  return {
    session: parseAssistantSessionSummary(source.session),
    messages: rawMessages
      .map((item) => parseAssistantConversationMessage(item))
      .filter((item): item is AssistantConversationMessage => item !== null),
  };
}

function parseAgentRunTraceRun(input: unknown, fallbackRunId: string): AgentRunTraceRun {
  const source = asObject(input);

  return {
    id: pickString(source, ["id", "run_id", "runId"]) ?? fallbackRunId,
    intent: pickString(source, ["intent"]),
    runStatus: pickString(source, ["runStatus", "run_status", "status"]),
    policyStatus: pickString(source, ["policyStatus", "policy_status"]),
    orchestratorVersion: pickString(source, [
      "orchestratorVersion",
      "orchestrator_version",
    ]),
    startedAt: pickString(source, ["startedAt", "started_at"]),
    completedAt: pickString(source, ["completedAt", "completed_at"]),
    latencyMs: pickNumber(source, ["latencyMs", "latency_ms"]),
    fallbackReason: pickString(source, ["fallbackReason", "fallback_reason"]),
    toolTrace: pickObject(source, ["toolTrace", "tool_trace"]),
  };
}

function parseAgentRunTraceStep(input: unknown, index: number): AgentRunTraceStep {
  const source = asObject(input);
  const sequence = pickNumber(source, ["sequence"]);

  return {
    id:
      pickString(source, ["id", "step_id", "stepId"]) ??
      `step-${sequence ?? index + 1}`,
    stepName:
      pickString(source, ["stepName", "step_name", "name"]) ??
      `步骤 ${sequence ?? index + 1}`,
    sequence,
    status: pickString(source, ["status"]),
    latencyMs: pickNumber(source, ["latencyMs", "latency_ms"]),
    inputPayload: pickObject(source, ["inputPayload", "input_payload"]),
    outputPayload: pickObject(source, ["outputPayload", "output_payload"]),
    error: pickString(source, ["error"]),
  };
}

function parseAgentRunTraceToolCall(
  input: unknown,
  index: number,
): AgentRunTraceToolCall {
  const source = asObject(input);

  return {
    id:
      pickString(source, ["id", "tool_call_id", "toolCallId"]) ??
      `tool-${index + 1}`,
    stepId: pickString(source, ["stepId", "step_id"]),
    toolName:
      pickString(source, ["toolName", "tool_name", "name"]) ??
      `工具 ${index + 1}`,
    permissionLevel: pickString(source, [
      "permissionLevel",
      "permission_level",
    ]),
    status: pickString(source, ["status"]),
    latencyMs: pickNumber(source, ["latencyMs", "latency_ms"]),
    inputPayload: pickObject(source, ["inputPayload", "input_payload"]),
    outputPayload: pickObject(source, ["outputPayload", "output_payload"]),
    error: pickString(source, ["error"]),
  };
}

function parseAgentRunTraceEvidenceRef(
  input: unknown,
  index: number,
): AgentRunTraceEvidenceRef {
  const source = asObject(input);

  return {
    id:
      pickString(source, ["id", "evidence_ref_id", "evidenceRefId"]) ??
      `evidence-${index + 1}`,
    stepId: pickString(source, ["stepId", "step_id"]),
    workerName: pickString(source, ["workerName", "worker_name"]),
    sourceType: pickString(source, ["sourceType", "source_type"]),
    sourceId: pickString(source, ["sourceId", "source_id"]),
    sourceVersion: pickString(source, ["sourceVersion", "source_version"]),
    quoteOrSummary: pickString(source, ["quoteOrSummary", "quote_or_summary"]),
    claim: pickString(source, ["claim"]),
    supportSummary: pickString(source, ["supportSummary", "support_summary"]),
  };
}

function parseAgentRunTrace(
  input: unknown,
  fallbackRunId: string,
): AgentRunTrace {
  const source = asObject(input);

  return {
    run: parseAgentRunTraceRun(source.run, fallbackRunId),
    contextSnapshot: pickObject(source, ["contextSnapshot", "context_snapshot"]),
    steps: pickArray(source, ["steps"]).map((item, index) =>
      parseAgentRunTraceStep(item, index),
    ),
    toolCalls: pickArray(source, ["toolCalls", "tool_calls"]).map((item, index) =>
      parseAgentRunTraceToolCall(item, index),
    ),
    evidenceRefs: pickArray(source, ["evidenceRefs", "evidence_refs"]).map(
      (item, index) => parseAgentRunTraceEvidenceRef(item, index),
    ),
    stateUpdateProposals: pickArray(source, [
      "stateUpdateProposals",
      "state_update_proposals",
    ]).map((item) => asObject(item)),
  };
}

function parseAgentRunEvent(
  input: unknown,
  fallbackEventId?: string | null,
): AgentRunEvent {
  const source = asObject(input);
  const sequence = pickNumber(source, ["sequence"]) ?? 0;

  return {
    id:
      pickString(source, ["id", "event_id", "eventId"]) ??
      fallbackEventId ??
      `agent-event-${sequence}`,
    runId: pickString(source, ["runId", "run_id"]) ?? "",
    sequence,
    eventType:
      pickString(source, ["eventType", "event_type", "type"]) ?? "runtime_event",
    phase: pickString(source, ["phase"]) ?? "runtime",
    title: pickString(source, ["title"]) ?? "整理任务",
    status: pickString(source, ["status"]) ?? "completed",
    at: pickString(source, ["at"]),
    durationMs: pickNumber(source, ["durationMs", "duration_ms"]),
    payload: pickObject(source, ["payload"]),
  };
}

function parseAgentRunEvents(input: unknown, fallbackRunId: string): AgentRunEvents {
  const source = asObject(input);
  const runId = pickString(source, ["runId", "run_id"]) ?? fallbackRunId;
  return {
    schemaVersion:
      pickString(source, ["schemaVersion", "schema_version"]) ??
      "agent_run_events_v1",
    runId,
    events: pickArray(source, ["events"]).map((event) => parseAgentRunEvent(event)),
  };
}

function parseQueuedFollowUp(input: unknown): QueuedFollowUp | null {
  const source = asObject(input);
  const id = pickString(source, ["id"]);
  const sessionId = pickString(source, ["sessionId", "session_id"]);
  const queuedAfterRunId = pickString(source, [
    "queuedAfterRunId",
    "queued_after_run_id",
  ]);
  const message = pickString(source, ["message"]);
  const createdAt = pickString(source, ["createdAt", "created_at"]);

  if (!id || !sessionId || !queuedAfterRunId || !message || !createdAt) {
    return null;
  }

  return {
    id,
    sessionId,
    queuedAfterRunId,
    message,
    status: pickString(source, ["status"]) ?? "queued",
    createdAt,
    submittedAt: pickString(source, ["submittedAt", "submitted_at"]),
    discardedAt: pickString(source, ["discardedAt", "discarded_at"]),
  };
}

function parseQueuedFollowUpResponse(input: unknown): QueuedFollowUpResponse {
  const source = asObject(input);
  return {
    schemaVersion:
      pickString(source, ["schemaVersion", "schema_version"]) ??
      "queued_follow_up_v1",
    queuedFollowUp: parseQueuedFollowUp(
      pickObject(source, ["queuedFollowUp", "queued_follow_up"]),
    ),
    message: pickString(source, ["message"]) ?? "排队状态已同步。",
  };
}

function parseAgentRunStatus(input: unknown, fallbackRunId: string): AgentRunStatus {
  const source = asObject(input);
  return {
    schemaVersion:
      pickString(source, ["schemaVersion", "schema_version"]) ??
      "agent_run_status_v1",
    runId: pickString(source, ["runId", "run_id"]) ?? fallbackRunId,
    sessionId: pickString(source, ["sessionId", "session_id"]),
    status: pickString(source, ["status"]) ?? "unknown",
    runStatus: pickString(source, ["runStatus", "run_status"]),
    active: asBoolean(source.active),
    cancelRequested:
      asBoolean(source.cancelRequested) || asBoolean(source.cancel_requested),
    startedAt: pickString(source, ["startedAt", "started_at"]),
    completedAt: pickString(source, ["completedAt", "completed_at"]),
    latencyMs: pickNumber(source, ["latencyMs", "latency_ms"]),
    message: pickString(source, ["message"]) ?? "整理状态已同步。",
    queuedFollowUp: parseQueuedFollowUp(
      pickObject(source, ["queuedFollowUp", "queued_follow_up"]),
    ),
  };
}

function parseActiveAgentRuns(input: unknown): ActiveAgentRuns {
  const source = asObject(input);
  return {
    schemaVersion:
      pickString(source, ["schemaVersion", "schema_version"]) ??
      "active_agent_runs_v1",
    runs: pickArray(source, ["runs"]).map((item) =>
      parseAgentRunStatus(item, ""),
    ),
  };
}

function parseAgentRunCancelResult(input: unknown, runId: string): AgentRunCancelResult {
  const source = asObject(input);
  return {
    runId: pickString(source, ["runId", "run_id"]) ?? runId,
    status: pickString(source, ["status"]) ?? "unknown",
    cancelRequested:
      asBoolean(source.cancelRequested) || asBoolean(source.cancel_requested),
    message: pickString(source, ["message"]) ?? "已请求停止本次整理。",
  };
}

function parseLearningCourseSummary(input: unknown): LearningCourseSummary | null {
  const source = asObject(input);
  const slug = asString(source.slug);
  const title = asString(source.title);
  const focus = asString(source.focus);
  const description = asString(source.description);
  const estimatedDurationMinutes = asNumber(source.estimated_duration_minutes);
  const sectionCount = asNumber(source.section_count);
  const completedSectionCount = asNumber(source.completed_section_count);
  const progressPercentage = asNumber(source.progress_percentage);
  const status = asLearningStatus(source.status);

  if (
    !slug ||
    !title ||
    !focus ||
    !description ||
    estimatedDurationMinutes === null ||
    sectionCount === null ||
    completedSectionCount === null ||
    progressPercentage === null ||
    status === null
  ) {
    return null;
  }

  return {
    slug,
    title,
    focus,
    description,
    estimatedDurationMinutes,
    sectionCount,
    completedSectionCount,
    progressPercentage,
    status,
    nextSectionSlug: asString(source.next_section_slug),
    nextSectionTitle: asString(source.next_section_title),
  };
}

function parseLearningPath(input: unknown): LearningPathState {
  const source = asObject(input);
  const rawCourses = Array.isArray(source.courses) ? source.courses : [];

  return {
    pathSlug: asString(source.path_slug) ?? "beginner-core-path",
    title: asString(source.title) ?? "Learning path",
    description: asString(source.description) ?? "",
    overallProgressPercentage: asNumber(source.overall_progress_percentage) ?? 0,
    completedCoursesCount: asNumber(source.completed_courses_count) ?? 0,
    totalCourses: asNumber(source.total_courses) ?? 0,
    recommendedCourseSlug: asString(source.recommended_course_slug),
    recommendedCourseTitle: asString(source.recommended_course_title),
    courses: rawCourses
      .map((item) => parseLearningCourseSummary(item))
      .filter((item): item is LearningCourseSummary => item !== null),
  };
}

function parseLearningSectionDetail(input: unknown): LearningSectionDetail | null {
  const source = asObject(input);
  const slug = asString(source.slug);
  const title = asString(source.title);
  const summary = asString(source.summary);
  const estimatedDurationMinutes = asNumber(source.estimated_duration_minutes);
  const position = asNumber(source.position);

  if (
    !slug ||
    !title ||
    !summary ||
    estimatedDurationMinutes === null ||
    position === null
  ) {
    return null;
  }

  return {
    slug,
    title,
    summary,
    estimatedDurationMinutes,
    position,
    completed: asBoolean(source.completed),
  };
}

function parseLearningCourseDetail(input: unknown): LearningCourseDetail {
  const source = asObject(input);
  const rawSections = Array.isArray(source.sections) ? source.sections : [];

  return {
    pathSlug: asString(source.path_slug) ?? "beginner-core-path",
    pathTitle: asString(source.path_title) ?? "Learning path",
    courseSlug: asString(source.course_slug) ?? "unknown-course",
    title: asString(source.title) ?? "Untitled course",
    focus: asString(source.focus) ?? "",
    description: asString(source.description) ?? "",
    estimatedDurationMinutes: asNumber(source.estimated_duration_minutes) ?? 0,
    sectionCount: asNumber(source.section_count) ?? 0,
    completedSectionCount: asNumber(source.completed_section_count) ?? 0,
    progressPercentage: asNumber(source.progress_percentage) ?? 0,
    status: asLearningStatus(source.status) ?? "not_started",
    nextSectionSlug: asString(source.next_section_slug),
    nextSectionTitle: asString(source.next_section_title),
    sections: rawSections
      .map((item) => parseLearningSectionDetail(item))
      .filter((item): item is LearningSectionDetail => item !== null),
  };
}

function parseLearningProgressUpdate(input: unknown): LearningProgressUpdate {
  const source = asObject(input);
  return {
    courseSlug: asString(source.course_slug) ?? "unknown-course",
    progressPercentage: asNumber(source.progress_percentage) ?? 0,
    completedSectionCount: asNumber(source.completed_section_count) ?? 0,
    sectionCount: asNumber(source.section_count) ?? 0,
    status: asLearningStatus(source.status) ?? "not_started",
    nextSectionSlug: asString(source.next_section_slug),
    nextSectionTitle: asString(source.next_section_title),
  };
}

function parsePortfolioHolding(input: unknown): PortfolioHolding | null {
  const source = asObject(input);
  const fundCode = asString(source.fund_code);
  const fundName = asString(source.fund_name);
  const fundType = asString(source.fund_type);
  const marketValue = asNumber(source.market_value);
  const weight = asNumber(source.weight);

  if (!fundCode || !fundName || !fundType || marketValue === null || weight === null) {
    return null;
  }

  return {
    fundCode,
    fundName,
    fundType,
    marketValue,
    weight,
  };
}

function parsePortfolioReport(input: unknown): PortfolioReport | null {
  const source = asObject(input);
  const snapshotId = asString(source.snapshot_id);
  const snapshotDate = asString(source.snapshot_date);
  const totalValue = asNumber(source.total_value);
  const cashValue = asNumber(source.cash_value);
  const summary = asString(source.summary);
  const rawHoldings = Array.isArray(source.holdings) ? source.holdings : [];

  if (
    !snapshotId ||
    !snapshotDate ||
    totalValue === null ||
    cashValue === null ||
    !summary
  ) {
    return null;
  }

  return {
    snapshotId,
    snapshotDate,
    totalValue,
    cashValue,
    summary,
    riskExposure: asStringArray(source.risk_exposure),
    concentrationFlags: asStringArray(source.concentration_flags),
    allocationBalance: asStringArray(source.allocation_balance),
    recommendedNextActions: asStringArray(source.recommended_next_actions),
    holdings: rawHoldings
      .map((item) => parsePortfolioHolding(item))
      .filter((item): item is PortfolioHolding => item !== null),
    generatedAt: asString(source.generated_at),
  };
}

function parsePortfolioLatest(input: unknown): PortfolioLatestState {
  const source = asObject(input);
  return {
    report: parsePortfolioReport(source.report),
  };
}

function parsePortfolioHistoryItem(input: unknown): PortfolioHistoryItem | null {
  const source = asObject(input);
  const snapshotId = asString(source.snapshot_id);
  const snapshotDate = asString(source.snapshot_date);
  const totalValue = asNumber(source.total_value);
  const summary = asString(source.summary);

  if (!snapshotId || !snapshotDate || totalValue === null || !summary) {
    return null;
  }

  return {
    snapshotId,
    snapshotDate,
    totalValue,
    summary,
    generatedAt: asString(source.generated_at),
  };
}

function parsePortfolioHistory(input: unknown): PortfolioHistoryState {
  const source = asObject(input);
  const rawItems = Array.isArray(source.items) ? source.items : [];

  return {
    items: rawItems
      .map((item) => parsePortfolioHistoryItem(item))
      .filter((item): item is PortfolioHistoryItem => item !== null),
  };
}

function parseSimulationScenario(input: unknown): SimulationScenario | null {
  const source = asObject(input);
  const id = pickString(source, ["id", "scenario_id", "slug"]);
  const title = pickString(source, ["title", "name"]);
  const synopsis = pickString(source, ["synopsis", "summary", "description"]);

  if (!id || !title || !synopsis) {
    return null;
  }

  const timelinePreview = pickStringArray(source, [
    "timeline_preview",
    "key_events",
    "step_titles",
  ]);

  return {
    id,
    slug: pickString(source, ["slug"]),
    title,
    synopsis,
    marketPhase: pickString(source, ["market_phase", "phase_label", "theme"]),
    difficulty: pickString(source, ["difficulty", "difficulty_label", "level"]),
    estimatedDurationMinutes: pickNumber(source, [
      "estimated_duration_minutes",
      "duration_minutes",
    ]),
    decisionCount:
      pickNumber(source, [
        "decision_count",
        "events_count",
        "steps_count",
        "event_count",
      ]) ??
      (timelinePreview.length > 0 ? timelinePreview.length : null),
    startingYear: pickNumber(source, ["starting_year", "year"]),
    headline: pickString(source, ["headline", "hero_headline"]),
    description: pickString(source, ["description", "summary", "synopsis"]),
    setup: pickString(source, ["setup", "background", "context"]),
    objective: pickString(source, ["objective", "goal", "training_goal"]),
    biasFocus: pickStringArray(source, [
      "bias_focus",
      "behavior_focus",
      "bias_tags",
    ]),
    tags: pickStringArray(source, ["tags", "focus_tags"]),
    timelinePreview,
    recommended:
      asBoolean(source.recommended) || asBoolean(source.is_recommended),
    completed: asBoolean(source.completed) || asBoolean(source.is_completed),
  };
}

function parseSimulationScenarioCatalog(
  input: unknown,
): SimulationScenarioCatalog {
  if (Array.isArray(input)) {
    return {
      scenarios: input
        .map((item) => parseSimulationScenario(item))
        .filter((item): item is SimulationScenario => item !== null),
      recommendedScenarioId: null,
      activeSessionId: null,
    };
  }

  const source = asObject(input);
  const rawScenarios = pickArray(source, ["scenarios", "items"]);
  const recommendedScenarioId = pickString(source, [
    "recommended_scenario_id",
    "recommended_scenario_slug",
  ]);
  const activeSessionId =
    pickString(source, ["active_session_id"]) ??
    rawScenarios
      .map((item) => asObject(item))
      .map((item) => pickString(item, ["active_session_id"]))
      .find((value) => Boolean(value)) ??
    null;

  return {
    scenarios: rawScenarios
      .map((item) => {
        const parsed = parseSimulationScenario(item);
        if (!parsed) {
          return null;
        }

        const rawItem = asObject(item);
        const status = pickString(rawItem, ["status"]);
        const scenarioId = parsed.slug ?? parsed.id;

        return {
          ...parsed,
          recommended:
            parsed.recommended ||
            (recommendedScenarioId !== null && scenarioId === recommendedScenarioId),
          completed: parsed.completed || status === "completed",
        };
      })
      .filter((item): item is SimulationScenario => item !== null),
    recommendedScenarioId,
    activeSessionId,
  };
}

function parseSimulationSessionActionChoice(
  input: unknown,
): SimulationSessionActionChoice | null {
  const source = asObject(input);
  const id = pickString(source, ["id", "action_id", "slug", "key", "choice_key"]);
  const label = pickString(source, ["label", "title", "action"]);

  if (!id || !label) {
    return null;
  }

  return {
    id,
    label,
    description: pickString(source, [
      "description",
      "rationale_hint",
      "explanation",
    ]),
    biasSignal:
      pickString(source, ["bias_signal", "behavior_signal"]) ??
      asStringArray(source.signals)[0] ??
      null,
  };
}

function parseSimulationSessionEvent(
  input: unknown,
): SimulationSessionEvent | null {
  const source = asObject(input);
  const index =
    pickNumber(source, ["step_index", "position", "index", "sequence"]) ?? 1;
  const title =
    pickString(source, ["title", "headline", "label"]) ?? `关键节点 ${index}`;
  const availableActions = pickArray(source, [
    "available_actions",
    "actions",
    "choices",
  ])
    .map((item) => parseSimulationSessionActionChoice(item))
    .filter((item): item is SimulationSessionActionChoice => item !== null);
  const id =
    pickString(source, ["id", "event_id", "step_id"]) ?? `event-${index}`;

  return {
    id,
    index,
    title,
    dateLabel: pickString(source, ["date_label", "as_of_label", "date"]),
    marketContext: pickString(source, [
      "market_context",
      "context",
      "brief",
      "summary",
      "narrative",
    ]),
    prompt: pickString(source, ["prompt", "question", "decision_prompt"]),
    decisionFocus: pickStringArray(source, [
      "decision_focus",
      "focus_tags",
      "bias_focus",
    ]),
    availableActions,
  };
}

function parseSimulationActionSummary(input: unknown): SimulationActionSummary | null {
  const source = asObject(input);
  const stepIndex = pickNumber(source, ["step_index", "stepIndex", "index"]) ?? 0;
  const choiceKey = pickString(source, ["choice_key", "choiceKey", "action_id", "actionId"]);
  const choiceLabel = pickString(source, [
    "choice_label",
    "choiceLabel",
    "label",
    "action",
  ]);

  if (!choiceKey || !choiceLabel) {
    return null;
  }

  return {
    eventId:
      pickString(source, ["event_id", "eventId"]) ??
      `event-${stepIndex || "unknown"}`,
    stepIndex,
    choiceKey,
    choiceLabel,
    reflection: pickString(source, ["reflection", "rationale", "reason"]),
    isRecommended: asBoolean(source.is_recommended) || asBoolean(source.isRecommended),
    createdAt: pickString(source, ["created_at", "createdAt"]),
  };
}

function parseSimulationSession(input: unknown): SimulationSession | null {
  const source = asObject(input);
  const nestedScenario = pickObject(source, ["scenario"]);
  const id = pickString(source, ["id", "session_id"]);

  if (!id) {
    return null;
  }

  const activeEvent = parseSimulationSessionEvent(
    source.active_event ?? source.current_event ?? source.event,
  );
  const actions = pickArray(source, ["actions", "submitted_actions", "history"])
    .map((item) => parseSimulationActionSummary(item))
    .filter((item): item is SimulationActionSummary => item !== null);
  const completedAt = pickString(source, ["completed_at"]);
  const status =
    pickString(source, ["status", "state"]) ??
    (completedAt || !activeEvent ? "completed" : "in_progress");

  return {
    id,
    scenarioId:
      pickString(source, ["scenario_id", "scenario_slug"]) ??
      pickString(nestedScenario, ["id", "scenario_id", "slug"]),
    scenarioTitle:
      pickString(source, ["scenario_title"]) ??
      pickString(nestedScenario, ["title", "name"]),
    status,
    stageLabel: pickString(source, ["stage_label", "phase_label"]),
    currentStep:
      pickNumber(source, ["current_step", "current_event_index"]) ??
      activeEvent?.index ??
      1,
    totalSteps:
      pickNumber(source, ["total_steps", "total_events"]) ??
      pickNumber(nestedScenario, ["decision_count", "events_count", "steps_count"]),
    startedAt: pickString(source, ["started_at", "created_at"]),
    completedAt,
    openingBrief:
      pickString(source, ["opening_brief", "introduction", "setup", "background"]) ??
      activeEvent?.marketContext ??
      null,
    reflectionPrompt: pickString(source, [
      "reflection_prompt",
      "rationale_prompt",
      "next_prompt",
    ]) ??
      activeEvent?.prompt ??
      null,
    activeEvent,
    actions,
  };
}

function parseSimulationFeedback(input: unknown): SimulationFeedback | null {
  const source = asObject(input);
  const summary = pickString(source, ["summary", "feedback", "message"]);
  const impact = pickString(source, ["impact", "consequence"]);
  const nextPrompt = pickString(source, ["next_prompt", "reflection_prompt"]);
  const disciplineSignals = pickStringArray(source, [
    "discipline_signals",
    "behavior_signals",
    "bias_signals",
  ]);

  if (!summary && !impact && !nextPrompt && disciplineSignals.length === 0) {
    return null;
  }

  return {
    summary: summary ?? impact ?? nextPrompt ?? "已记录本次动作。",
    impact,
    disciplineSignals,
    nextPrompt,
  };
}

function buildSimulationFeedbackFromAction(
  session: SimulationSession,
): SimulationFeedback | null {
  const latestAction = session.actions[session.actions.length - 1];

  if (!latestAction) {
    return null;
  }

  const choice = `「${latestAction.choiceLabel}」`;
  const summary = latestAction.isRecommended
    ? `已记录本轮动作 ${choice}。这一步和情境训练的纪律目标一致，重点是把“先检查计划”变成可重复动作。`
    : `已记录本轮动作 ${choice}。这一步值得复盘：先看它是在回应计划，还是在缓解当下情绪。`;
  const impact = latestAction.reflection
    ? `你的判断理由：${latestAction.reflection}`
    : "这次没有写下判断理由。下次建议至少补一句“我为什么现在这样选”，复盘才有抓手。";
  const nextPrompt =
    session.status === "completed"
      ? "动作链已经完成，可以读取最终复盘。"
      : session.activeEvent?.prompt ?? null;

  return {
    summary,
    impact,
    disciplineSignals: latestAction.isRecommended ? ["no_major_bias_detected"] : [],
    nextPrompt,
  };
}

function parseSimulationActionResult(input: unknown): SimulationActionResult {
  const source = asObject(input);
  const session =
    parseSimulationSession(source.session) ?? parseSimulationSession(source);

  if (!session) {
    throw new ApiError(500, "Simulation session payload is invalid.");
  }

  const feedback = parseSimulationFeedback(
    source.feedback ?? source.decision_feedback ?? source.result,
  ) ?? buildSimulationFeedbackFromAction(session);

  return {
    session,
    feedback,
    reviewReady:
      asBoolean(source.review_ready) ||
      asBoolean(source.has_review) ||
      session.status === "completed" ||
      (session.activeEvent === null && session.completedAt !== null),
  };
}

function parseSimulationReview(
  input: unknown,
  fallbackSessionId: string,
): SimulationReview {
  const source = asObject(input);
  const nestedReview = pickObject(source, ["review"]);
  const reviewSource =
    Object.keys(nestedReview).length > 0 ? nestedReview : source;
  const directStrengths = pickStringArray(reviewSource, [
    "strengths",
    "what_went_well",
    "positive_signals",
  ]);
  const directImprovements = pickStringArray(reviewSource, [
    "improvement_areas",
    "what_to_improve",
    "missed_opportunities",
  ]);
  const legacyObservations = pickStringArray(reviewSource, [
    "bias_observations",
  ]);
  const looksLikePositiveObservation =
    directStrengths.length === 0 &&
    directImprovements.length === 0 &&
    legacyObservations.some((item) =>
      item.includes("先做了计划检查") ||
      item.includes("稳住") ||
      item.includes("纪律"),
    );

  return {
    sessionId:
      pickString(reviewSource, ["session_id"]) ??
      pickString(source, ["session_id"]) ??
      fallbackSessionId,
    scenarioId:
      pickString(reviewSource, ["scenario_id", "scenario_slug"]) ??
      pickString(source, ["scenario_id", "scenario_slug"]),
    scenarioTitle:
      pickString(reviewSource, ["scenario_title"]) ??
      pickString(source, ["scenario_title"]),
    completedAt:
      pickString(reviewSource, ["completed_at", "generated_at"]) ??
      pickString(source, ["completed_at", "generated_at"]),
    overallAssessment:
      pickString(reviewSource, [
        "overall_assessment",
        "summary",
        "overall_summary",
        "review_summary",
        "decision_summary",
      ]) ?? "复盘已生成。",
    outcomeSummary: pickString(reviewSource, [
      "outcome_summary",
      "what_happened",
      "market_outcome",
      "coach_feedback",
    ]),
    finalDisposition: pickString(reviewSource, [
      "final_disposition",
      "behavior_verdict",
      "training_result",
    ]),
    biasSignals: (() => {
      const directSignals = pickStringArray(reviewSource, [
        "bias_signals",
        "behavior_signals",
        "exposed_biases",
      ]);
      if (directSignals.length > 0) {
        return directSignals;
      }

      const biasFocus = pickString(reviewSource, ["bias_focus"]);
      return biasFocus ? [biasFocus] : [];
    })(),
    strengths:
      directStrengths.length > 0
        ? directStrengths
        : looksLikePositiveObservation
          ? legacyObservations
          : [],
    improvementAreas:
      directImprovements.length > 0
        ? directImprovements
        : directStrengths.length > 0
          ? []
        : looksLikePositiveObservation
          ? []
          : legacyObservations,
    recommendedNextActions: pickStringArray(reviewSource, [
      "recommended_next_actions",
      "next_actions",
    ]),
    reflectionQuestions: pickStringArray(reviewSource, [
      "reflection_questions",
      "follow_up_questions",
    ]),
    behaviorEvidenceCandidates: pickArray(reviewSource, [
      "behavior_evidence_candidates",
      "behaviorEvidenceCandidates",
    ])
      .map((item) => {
        const candidate = asObject(item);
        const signal = pickString(candidate, ["observed_signal", "observedSignal"]);
        const biasType = pickString(candidate, ["bias_type", "biasType"]);
        if (!signal && !biasType) {
          return null;
        }
        return biasType ? `${biasType}：${signal ?? "行为证据候选"}` : signal;
      })
      .filter((item): item is string => item !== null),
    pendingStateProposal: (() => {
      const proposal = pickObject(reviewSource, [
        "pending_state_proposal",
        "pendingStateProposal",
      ]);
      return pickString(proposal, ["reason", "summary", "status"]);
    })(),
    scoreLabel: pickString(reviewSource, [
      "score_label",
      "discipline_score_label",
      "score",
    ]),
  };
}

function parseNewsItem(input: unknown): NewsItem | null {
  const source = asObject(input);
  const id = pickString(source, ["id", "news_id", "item_id", "slug", "url"]);
  const title = pickString(source, ["title", "headline", "name"]);
  const summary = pickString(source, [
    "summary",
    "description",
    "excerpt",
    "brief",
    "body",
  ]);

  if (!id || !title || !summary) {
    return null;
  }

  return {
    id,
    itemType:
      pickString(source, ["item_type", "itemType", "type"]) === "policy"
        ? "policy"
        : pickString(source, ["item_type", "itemType", "type"]) === "news"
          ? "news"
          : null,
    title,
    source: pickString(source, [
      "source",
      "source_name",
      "publisher",
      "provider",
    ]),
    category: pickString(source, [
      "category",
      "item_type",
      "type",
      "topic",
      "policy_area",
    ]),
    summary,
    url: pickString(source, ["url", "link"]),
    publishedAt: pickString(source, [
      "published_at",
      "publishedAt",
      "date",
      "created_at",
    ]),
    impactAreas: pickStringArray(source, [
      "impact_areas",
      "impactAreas",
      "affected_areas",
      "themes",
    ]),
    tags: pickStringArray(source, ["tags", "labels", "keywords"]),
    relevanceScore: pickNumber(source, [
      "relevance_score",
      "relevanceScore",
      "score",
    ]),
    latestAnalysisId: pickString(source, [
      "latest_analysis_id",
      "latestAnalysisId",
    ]),
  };
}

function parseNewsCatalog(input: unknown): NewsCatalogState {
  if (Array.isArray(input)) {
    return {
      items: input
        .map((item) => parseNewsItem(item))
        .filter((item): item is NewsItem => item !== null),
      policyItems: [],
      generatedAt: null,
    };
  }

  const source = asObject(input);
  const rawItems = pickArray(source, ["items", "news", "news_items", "articles"]);
  const rawPolicyItems = pickArray(source, [
    "policy_items",
    "policy",
    "policyItems",
  ]);

  return {
    items: rawItems
      .map((item) => parseNewsItem(item))
      .filter((item): item is NewsItem => item !== null),
    policyItems: rawPolicyItems
      .map((item) => parseNewsItem(item))
      .filter((item): item is NewsItem => item !== null),
    generatedAt: pickString(source, ["generated_at", "updated_at", "as_of"]),
  };
}

function parseNewsAnalysis(input: unknown): NewsAnalysis {
  const source = asObject(input);
  const nested = pickObject(source, ["analysis", "result"]);
  const analysisSource = Object.keys(nested).length > 0 ? nested : source;
  const itemSource = pickObject(analysisSource, ["item", "news_item", "policy_item"]);
  const parsedItem = parseNewsItem(itemSource);
  const facts = pickStringArray(analysisSource, ["facts", "fact_list"]);
  const citationStrings = pickStringArray(analysisSource, [
    "citations",
    "sources",
  ]);
  const citationObjects = pickArray(analysisSource, ["citations", "sources"])
    .map((item) => {
      const citation = asObject(item);
      const title = pickString(citation, ["title", "source_name", "url"]);
      const url = pickString(citation, ["url", "link"]);
      if (title && url) {
        return `${title} (${url})`;
      }
      return title ?? url;
    })
    .filter((item): item is string => item !== null);
  const headline =
    pickString(analysisSource, ["headline", "title", "news_title"]) ??
    pickString(itemSource, ["title", "headline", "name"]) ??
    pickString(source, ["headline", "title"]) ??
    "新闻解读";
  const factSummary =
    pickString(analysisSource, [
      "fact_summary",
      "summary",
      "beginner_translation",
      "what_happened",
      "answer",
    ]) ??
    (facts.length > 0
      ? facts.join(" ")
      : "后端已返回解读结果，但没有提供事实摘要字段。");
  const riskNotice =
    pickString(analysisSource, ["risk_notice", "riskNotice", "disclaimer"]) ??
    "资讯解读只帮助理解事实与影响路径，不构成买卖建议。";

  return {
    id: pickString(analysisSource, ["id", "analysis_id"]),
    itemId:
      pickString(analysisSource, ["item_id", "news_id"]) ??
      pickString(itemSource, ["id", "item_id", "news_id"]) ??
      pickString(source, ["item_id", "news_id"]),
    item: parsedItem,
    headline,
    factSummary,
    impactPath: pickStringArray(analysisSource, [
      "impact_paths",
      "impact_path",
      "impactPath",
      "impact_analysis",
      "influence_chain",
    ]),
    uncertainty: pickStringArray(analysisSource, [
      "uncertainty_notes",
      "uncertainty",
      "uncertainties",
      "unknowns",
    ]),
    riskNotice,
    recommendedActions: pickStringArray(analysisSource, [
      "recommended_next_actions",
      "recommended_actions",
      "next_actions",
      "recommendedNextActions",
    ]),
    relatedLearning: pickStringArray(analysisSource, [
      "related_learning_topics",
      "related_learning",
      "learning_links",
      "relatedLearning",
    ]),
    citations:
      citationStrings.length > 0 ? citationStrings : citationObjects,
    modelStatus: (() => {
      const value = pickString(analysisSource, ["model_status", "modelStatus"]);
      if (
        value === "enhanced" ||
        value === "fallback" ||
        value === "skipped" ||
        value === "legacy"
      ) {
        return value;
      }
      return "legacy";
    })(),
    modelProvider: pickString(analysisSource, ["model_provider", "modelProvider"]),
    modelName: pickString(analysisSource, ["model_name", "modelName"]),
    fallbackReason: pickString(analysisSource, ["fallback_reason", "fallbackReason"]),
    agentProcess: pickArray(analysisSource, ["agent_process", "agentProcess"])
      .map((item) => {
        const step = asObject(item);
        const key = asString(step.key);
        const label = asString(step.label);
        const detail = asString(step.detail);
        const rawStatus = asString(step.status);
        const status =
          rawStatus === "completed" ||
          rawStatus === "warning" ||
          rawStatus === "failed"
            ? rawStatus
            : "warning";
        return key && label && detail ? { key, label, status, detail } : null;
      })
      .filter((item): item is NewsAnalysis["agentProcess"][number] => item !== null),
    generatedAt:
      pickString(analysisSource, ["generated_at", "created_at"]) ??
      pickString(source, ["generated_at", "created_at"]),
  };
}

function asAutomationKey(value: unknown): AutomationKey | null {
  if (
    value === "daily_brief" ||
    value === "weekly_portfolio" ||
    value === "news_watch" ||
    value === "behavior_observation"
  ) {
    return value;
  }
  return null;
}

function asAutomationStatus(value: unknown): AutomationStatus {
  if (
    value === "ready" ||
    value === "disabled" ||
    value === "needs_profile" ||
    value === "blocked"
  ) {
    return value;
  }
  return "blocked";
}

function asAutomationRunStatus(value: unknown): AutomationRunStatus {
  if (
    value === "queued" ||
    value === "running" ||
    value === "succeeded" ||
    value === "failed"
  ) {
    return value;
  }
  return "failed";
}

function parseAutomationRunSummary(input: unknown): AutomationRunSummary | null {
  const source = asObject(input);
  const runId = pickString(source, ["run_id", "runId"]);
  if (!runId) {
    return null;
  }
  return {
    runId,
    agentRunId: pickString(source, ["agent_run_id", "agentRunId"]),
    status: asAutomationRunStatus(source.status),
    startedAt: pickString(source, ["started_at", "startedAt"]),
    completedAt: pickString(source, ["completed_at", "completedAt"]),
    summary: asString(source.summary),
    outputRef: pickString(source, ["output_ref", "outputRef"]),
  };
}

function parseAutomationItem(input: unknown): AutomationItem | null {
  const source = asObject(input);
  const key = asAutomationKey(source.key);
  const title = asString(source.title);
  const summary = asString(source.summary);
  const cadenceKey = pickString(source, ["cadence_key", "cadenceKey"]);
  const cadenceLabel = pickString(source, ["cadence_label", "cadenceLabel"]);
  const confirmationBoundary = pickString(source, [
    "confirmation_boundary",
    "confirmationBoundary",
  ]);
  const safetyBoundary = pickString(source, ["safety_boundary", "safetyBoundary"]);

  if (
    !key ||
    !title ||
    !summary ||
    !cadenceKey ||
    !cadenceLabel ||
    !confirmationBoundary ||
    !safetyBoundary
  ) {
    return null;
  }

  return {
    key,
    title,
    summary,
    enabled: asBoolean(source.enabled),
    defaultEnabled: asBoolean(source.default_enabled ?? source.defaultEnabled),
    cadenceKey,
    cadenceLabel,
    cadenceOptions: pickArray(source, ["cadence_options", "cadenceOptions"])
      .map((item) => {
        const option = asObject(item);
        const optionKey = asString(option.key);
        const label = asString(option.label);
        return optionKey && label ? { key: optionKey, label } : null;
      })
      .filter((item): item is AutomationCadenceOption => item !== null),
    readScope: pickStringArray(source, ["read_scope", "readScope"]),
    outputScope: pickStringArray(source, ["output_scope", "outputScope"]),
    confirmationBoundary,
    safetyBoundary,
    status: asAutomationStatus(source.status),
    disabledReason: pickString(source, ["disabled_reason", "disabledReason"]),
    lastRun: parseAutomationRunSummary(source.last_run ?? source.lastRun),
    nextRunAt: pickString(source, ["next_run_at", "nextRunAt"]),
    canRunNow: asBoolean(source.can_run_now ?? source.canRunNow),
  };
}

function parseAutomationNotification(input: unknown): AutomationNotification | null {
  const source = asObject(input);
  const id = asString(source.id);
  const automationKey = asAutomationKey(
    source.automation_key ?? source.automationKey,
  );
  const title = asString(source.title);
  const message = asString(source.message);
  if (!id || !automationKey || !title || !message) {
    return null;
  }
  return {
    id,
    automationKey,
    title,
    message,
    actionLabel: pickString(source, ["action_label", "actionLabel"]),
    actionRoute: pickString(source, ["action_route", "actionRoute"]),
    createdAt: pickString(source, ["created_at", "createdAt"]),
    readAt: pickString(source, ["read_at", "readAt"]),
  };
}

function parseAutomationList(input: unknown): AutomationListState {
  const source = asObject(input);
  return {
    userId: pickString(source, ["user_id", "userId"]) ?? "unknown",
    updatedAt: pickString(source, ["updated_at", "updatedAt"]),
    activeCount: pickNumber(source, ["active_count", "activeCount"]) ?? 0,
    totalCount: pickNumber(source, ["total_count", "totalCount"]) ?? 0,
    automations: pickArray(source, ["automations"])
      .map((item) => parseAutomationItem(item))
      .filter((item): item is AutomationItem => item !== null),
    nextQueue: pickArray(source, ["next_queue", "nextQueue"])
      .map((item) => {
        const queueItem = asObject(item);
        const automationKey = asAutomationKey(
          queueItem.automation_key ?? queueItem.automationKey,
        );
        const title = asString(queueItem.title);
        const cadenceLabel = pickString(queueItem, [
          "cadence_label",
          "cadenceLabel",
        ]);
        if (!automationKey || !title || !cadenceLabel) {
          return null;
        }
        return {
          automationKey,
          title,
          nextRunAt: pickString(queueItem, ["next_run_at", "nextRunAt"]),
          cadenceLabel,
        };
      })
      .filter((item): item is AutomationQueueItem => item !== null),
    dailyBriefSummary: (() => {
      const brief = asObject(
        source.daily_brief_summary ?? source.dailyBriefSummary,
      );
      const briefId = pickString(brief, ["brief_id", "briefId"]);
      const headline = asString(brief.headline);
      const beginnerExplanation = pickString(brief, [
        "beginner_explanation",
        "beginnerExplanation",
      ]);
      if (!briefId || !headline || !beginnerExplanation) {
        return null;
      }
      return {
        briefId,
        headline,
        beginnerExplanation,
        asOf: pickString(brief, ["as_of", "asOf"]),
        sourceCoverage: Object.fromEntries(
          Object.entries(asObject(brief.source_coverage ?? brief.sourceCoverage))
            .filter((entry): entry is [string, boolean] => entry[1] === true || entry[1] === false),
        ),
      };
    })(),
    recentNotifications: pickArray(source, [
      "recent_notifications",
      "recentNotifications",
    ])
      .map((item) => parseAutomationNotification(item))
      .filter((item): item is AutomationNotification => item !== null),
  };
}

function parseAutomationRunResult(input: unknown): AutomationRunResult {
  const source = asObject(input);
  const runId = pickString(source, ["run_id", "runId"]);
  const automationKey = asAutomationKey(
    source.automation_key ?? source.automationKey,
  );
  if (!runId || !automationKey) {
    throw new ApiError(500, "Automation run payload is invalid.");
  }
  return {
    runId,
    agentRunId: pickString(source, ["agent_run_id", "agentRunId"]),
    automationKey,
    status: asAutomationRunStatus(source.status),
    triggerType:
      source.trigger_type === "scheduled" || source.triggerType === "scheduled"
        ? "scheduled"
        : "manual",
    dueAt: pickString(source, ["due_at", "dueAt"]),
    startedAt: pickString(source, ["started_at", "startedAt"]),
    completedAt: pickString(source, ["completed_at", "completedAt"]),
    summary: asString(source.summary),
    outputRef: pickString(source, ["output_ref", "outputRef"]),
    errorMessage: pickString(source, ["error_message", "errorMessage"]),
    createdPendingProposalIds: asStringArray(
      source.created_pending_proposal_ids ?? source.createdPendingProposalIds,
    ),
    steps: pickArray(source, ["steps"])
      .map((item) => {
        const step = asObject(item);
        const key = asString(step.key);
        const label = asString(step.label);
        const detail = asString(step.detail);
        const rawStatus = asString(step.status);
        const status =
          rawStatus === "queued" ||
          rawStatus === "running" ||
          rawStatus === "completed" ||
          rawStatus === "failed"
            ? rawStatus
            : "completed";
        return key && label && detail ? { key, label, detail, status } : null;
      })
      .filter((item): item is AutomationRunResult["steps"][number] => item !== null),
    outputPayload: asObject(source.output_payload ?? source.outputPayload),
  };
}

function parseProfilePendingProposal(input: unknown): ProfilePendingProposal | null {
  const source = asObject(input);
  const id = asString(source.id);
  const title = asString(source.title);
  const sourceLabel = pickString(source, ["source_label", "sourceLabel"]);
  const evidenceSummary = pickString(source, [
    "evidence_summary",
    "evidenceSummary",
  ]);
  const writebackLabel = pickString(source, [
    "writeback_label",
    "writebackLabel",
  ]);
  const reason = asString(source.reason);
  const validatorStatus = pickString(source, [
    "validator_status",
    "validatorStatus",
  ]);
  const safetyNote = pickString(source, ["safety_note", "safetyNote"]);
  const status = asString(source.status);

  if (
    !id ||
    !title ||
    !sourceLabel ||
    !evidenceSummary ||
    !writebackLabel ||
    !reason ||
    !validatorStatus ||
    !safetyNote ||
    (status !== "pending" &&
      status !== "accepted" &&
      status !== "rejected" &&
      status !== "applied")
  ) {
    return null;
  }

  return {
    id,
    title,
    sourceLabel,
    evidenceSummary,
    writebackLabel,
    targetType: pickString(source, ["target_type", "targetType"]) ?? "unknown",
    targetId: pickString(source, ["target_id", "targetId"]),
    patchPreview: asObject(source.patch_preview ?? source.patchPreview),
    reason,
    validatorStatus,
    validatorMessage: pickString(source, [
      "validator_message",
      "validatorMessage",
    ]),
    status,
    safetyNote,
    createdAt: pickString(source, ["created_at", "createdAt"]),
    runId: pickString(source, ["run_id", "runId"]),
  };
}

function parseProfilePendingProposals(
  input: unknown,
): ProfilePendingProposalsState {
  const source = asObject(input);
  return {
    pendingCount: pickNumber(source, ["pending_count", "pendingCount"]) ?? 0,
    resolvedCount: pickNumber(source, ["resolved_count", "resolvedCount"]) ?? 0,
    proposals: pickArray(source, ["proposals"])
      .map((item) => parseProfilePendingProposal(item))
      .filter((item): item is ProfilePendingProposal => item !== null),
  };
}

function parseProfileContext(input: unknown): ProfileContextState {
  const source = asObject(input);
  const readiness = asObject(
    source.context_readiness ?? source.contextReadiness,
  );
  const risk = asObject(source.risk_profile ?? source.riskProfile);
  const behavior = asObject(source.behavior_profile ?? source.behaviorProfile);
  const portfolio = asObject(
    source.portfolio_context ?? source.portfolioContext,
  );
  const learning = asObject(source.learning_context ?? source.learningContext);
  const simulation = asObject(
    source.simulation_context ?? source.simulationContext,
  );

  return {
    userId: pickString(source, ["user_id", "userId"]) ?? "unknown",
    displayName: pickString(source, ["display_name", "displayName"]),
    contextReadiness: {
      readyCount: pickNumber(readiness, ["ready_count", "readyCount"]) ?? 0,
      totalCount: pickNumber(readiness, ["total_count", "totalCount"]) ?? 0,
      items: pickArray(readiness, ["items"])
        .map((item) => {
          const readinessItem = asObject(item);
          const key = asString(readinessItem.key);
          const label = asString(readinessItem.label);
          if (!key || !label) {
            return null;
          }
          return {
            key,
            label,
            ready: asBoolean(readinessItem.ready),
            lastUpdatedAt: pickString(readinessItem, [
              "last_updated_at",
              "lastUpdatedAt",
            ]),
            missingActionRoute: pickString(readinessItem, [
              "missing_action_route",
              "missingActionRoute",
            ]),
          };
        })
        .filter((item): item is ProfileReadinessItem => item !== null),
    },
    riskProfile: {
      riskLevel: asRiskLevel(risk.risk_level ?? risk.riskLevel),
      latestRiskScore: pickNumber(risk, ["latest_risk_score", "latestRiskScore"]),
      updatedAt: pickString(risk, ["updated_at", "updatedAt"]),
    },
    behaviorProfile: {
      biasTags: pickStringArray(behavior, ["bias_tags", "biasTags"]),
      evidence: asStringArray(behavior.evidence),
      updatedAt: pickString(behavior, ["updated_at", "updatedAt"]),
    },
    portfolioContext: {
      hasReport: asBoolean(portfolio.has_report ?? portfolio.hasReport),
      latestSnapshotDate: pickString(portfolio, [
        "latest_snapshot_date",
        "latestSnapshotDate",
      ]),
      totalValue: asNumber(portfolio.total_value ?? portfolio.totalValue),
      summary: asString(portfolio.summary),
    },
    learningContext: {
      overallProgressPercentage:
        pickNumber(learning, [
          "overall_progress_percentage",
          "overallProgressPercentage",
        ]) ?? 0,
      recommendedCourseTitle: pickString(learning, [
        "recommended_course_title",
        "recommendedCourseTitle",
      ]),
    },
    simulationContext: {
      latestReviewSummary: pickString(simulation, [
        "latest_review_summary",
        "latestReviewSummary",
      ]),
      latestCompletedAt: pickString(simulation, [
        "latest_completed_at",
        "latestCompletedAt",
      ]),
    },
    automationAuthorizations: pickArray(source, [
      "automation_authorizations",
      "automationAuthorizations",
    ])
      .map((item) => {
        const authorization = asObject(item);
        const automationKey = pickString(authorization, [
          "automation_key",
          "automationKey",
        ]);
        const cadenceLabel = pickString(authorization, [
          "cadence_label",
          "cadenceLabel",
        ]);
        if (!automationKey || !cadenceLabel) {
          return null;
        }
        return {
          automationKey,
          enabled: asBoolean(authorization.enabled),
          cadenceLabel,
        };
      })
      .filter((item): item is ProfileContextState["automationAuthorizations"][number] => item !== null),
    authorizationScope: pickArray(source, [
      "authorization_scope",
      "authorizationScope",
    ])
      .map((item) => {
        const scope = asObject(item);
        const key = asString(scope.key);
        const label = asString(scope.label);
        if (!key || !label) {
          return null;
        }
        return { key, label, readable: asBoolean(scope.readable) };
      })
      .filter((item): item is ProfileContextState["authorizationScope"][number] => item !== null),
    pendingProposalCount:
      pickNumber(source, ["pending_proposal_count", "pendingProposalCount"]) ?? 0,
  };
}

function parseProfileLlmSettings(input: unknown): ProfileLlmSettings {
  const source = asObject(input);
  const provider = asString(source.provider) === "deepseek" ? "deepseek" : "deepseek";
  const status = asString(source.source);
  return {
    provider,
    modelName:
      pickString(source, ["model_name", "modelName"]) ?? "deepseek:deepseek-v4-pro",
    configured: asBoolean(source.configured),
    enabled: asBoolean(source.enabled),
    source:
      status === "user" || status === "workspace" || status === "none"
        ? status
        : "none",
    maskedApiKey: pickString(source, ["masked_api_key", "maskedApiKey"]),
    updatedAt: pickString(source, ["updated_at", "updatedAt"]),
    warning: asString(source.warning),
  };
}

export async function registerAuthAccount(
  input: AuthCredentialsInput,
): Promise<void> {
  await request("/api/auth/register", {
    method: "POST",
    body: {
      email: input.email.trim(),
      password: input.password,
    },
  });
}

export async function loginAuthAccount(
  input: AuthCredentialsInput,
): Promise<void> {
  await request("/api/auth/login", {
    method: "POST",
    body: {
      email: input.email.trim(),
      password: input.password,
    },
  });
}

export async function logoutAuthSession(): Promise<void> {
  await request("/api/auth/logout", { method: "POST" });
}

export async function getSessionUser(): Promise<SessionUser> {
  const payload = await request("/api/auth/session", { timeoutMs: 60_000 });
  return parseSessionUser(payload);
}

export async function getCurrentUser(): Promise<UserSummary> {
  const payload = await request("/api/users/me", {});
  return parseUserSummary(payload);
}

export async function upsertOnboardingProfile(
  input: OnboardingProfileInput,
): Promise<UserSummary> {
  const payload = await request("/api/onboarding/profile", {
    method: "POST",
    body: {
      display_name: input.displayName,
      investing_experience: input.investingExperience,
      monthly_contribution_band: input.monthlyContributionBand,
      primary_goal: input.primaryGoal,
    },
  });

  const source = asObject(payload);
  return parseUserSummary(source.user);
}

export async function submitRiskQuestionnaire(
  input: QuestionnaireInput,
): Promise<BehaviorProfile> {
  const payload = await request("/api/behavior/questionnaires", {
    method: "POST",
    body: {
      questionnaire_version: "v1",
      answers: input.answers,
    },
  });

  return parseBehaviorProfile(payload);
}

export async function getBehaviorProfile(): Promise<BehaviorProfile> {
  const payload = await request("/api/behavior/profile", {});
  return parseBehaviorProfile(payload);
}

export async function getDashboardState(): Promise<DashboardState> {
  const payload = await request("/api/dashboard", {});
  return parseDashboard(payload);
}

export async function getLearningPath(): Promise<LearningPathState> {
  const payload = await request("/api/learning/path", {});
  return parseLearningPath(payload);
}

export async function getLearningCourse(
  courseSlug: string,
): Promise<LearningCourseDetail> {
  const payload = await request(`/api/learning/courses/${courseSlug}`, {});
  return parseLearningCourseDetail(payload);
}

export async function updateLearningProgress(
  input: LearningProgressInput,
): Promise<LearningProgressUpdate> {
  const payload = await request("/api/learning/progress", {
    method: "POST",
    body: {
      course_slug: input.courseSlug,
      section_slug: input.sectionSlug,
      status: "completed",
    },
  });

  return parseLearningProgressUpdate(payload);
}

export async function getAssistantSession(): Promise<AssistantConversationState> {
  const payload = await request("/api/assistant/session", {});
  return parseAssistantConversation(payload);
}

export async function getAssistantSessions(): Promise<AssistantSessionSummary[]> {
  const payload = await request("/api/assistant/sessions", {});
  return pickArray(asObject(payload), ["sessions"])
    .map((item) => parseAssistantSessionSummary(item))
    .filter((item): item is AssistantSessionSummary => item !== null);
}

export async function getAssistantConversation(
  sessionId: string,
): Promise<AssistantConversationState> {
  const payload = await request(
    `/api/assistant/sessions/${encodeURIComponent(sessionId)}`,
    {},
  );
  return parseAssistantConversation(payload);
}

export async function sendAssistantMessage(
  input: AssistantMessageInput,
): Promise<AssistantConversationState> {
  const body = buildAssistantMessageBody(input);
  const payload = await request("/api/assistant/messages", {
    method: "POST",
    body,
  });

  return parseAssistantConversation(payload);
}

export async function streamAssistantMessage(
  input: AssistantMessageInput,
  handlers: AssistantMessageStreamHandlers = {},
): Promise<AssistantConversationState> {
  const url = `${resolveApiBase()}/api/assistant/messages/stream`;
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(buildAssistantMessageBody(input)),
    cache: "no-store",
    credentials: "include",
  });

  if (!response.ok) {
    let message = "request failed";
    try {
      const payload = asObject(await response.json());
      message = formatErrorDetail(payload.detail) ?? message;
    } catch {
      const text = await response.text();
      message = text || message;
    }
    throw new ApiError(response.status, message);
  }

  if (!response.body) {
    return sendAssistantMessage(input);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let conversation: AssistantConversationState | null = null;
  let latestRunId: string | null = null;
  let latestRunSequence: number | null = null;
  let streamErrorMessage: string | null = null;

  const handleBlock = (block: string) => {
    const lines = block.split(/\r?\n/);
    const eventName =
      lines
        .find((line) => line.startsWith("event:"))
        ?.slice("event:".length)
        .trim() ?? "message";
    const eventId =
      lines
        .find((line) => line.startsWith("id:"))
        ?.slice("id:".length)
        .trim() ?? null;
    const data = lines
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice("data:".length).trim())
      .join("\n");

    if (!data) {
      return;
    }

    const payload = JSON.parse(data) as unknown;
    if (eventName === "agent_event") {
      const event = parseAgentRunEvent(payload, eventId);
      if (event.runId) {
        if (event.runId !== latestRunId) {
          latestRunSequence = null;
        }
        latestRunId = event.runId;
        latestRunSequence = Math.max(latestRunSequence ?? 0, event.sequence);
      }
      handlers.onEvent?.(event);
      return;
    }
    if (eventName === "conversation") {
      conversation = parseAssistantConversation(payload);
      return;
    }
    if (eventName === "error") {
      const source = asObject(payload);
      const errorRunId = pickString(source, ["runId", "run_id"]);
      if (errorRunId) {
        latestRunId = errorRunId;
      }
      streamErrorMessage =
        pickString(source, ["message"]) ?? "发送失败，请稍后重试。";
    }
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    buffer += decoder.decode(value, { stream: true });
    let boundary = findSseBlockBoundary(buffer);
    while (boundary) {
      const block = buffer.slice(0, boundary.index).trim();
      buffer = buffer.slice(boundary.index + boundary.length);
      if (block) {
        handleBlock(block);
      }
      boundary = findSseBlockBoundary(buffer);
    }
  }

  buffer += decoder.decode();
  if (buffer.trim()) {
    handleBlock(buffer.trim());
  }

  if (conversation) {
    return conversation;
  }
  if (latestRunId) {
    return recoverAssistantConversationFromRun(latestRunId, {
      afterSequence: latestRunSequence,
      onEvent: handlers.onEvent,
    });
  }
  if (streamErrorMessage) {
    throw new ApiError(500, streamErrorMessage);
  }

  throw new ApiError(500, "Agent 已开始处理，但没有返回最终会话。");
}

function findSseBlockBoundary(
  buffer: string,
): { index: number; length: number } | null {
  const match = /\r?\n\r?\n/.exec(buffer);
  if (!match || typeof match.index !== "number") {
    return null;
  }
  return { index: match.index, length: match[0].length };
}

async function recoverAssistantConversationFromRun(
  runId: string,
  options: {
    afterSequence?: number | null;
    onEvent?: (event: AgentRunEvent) => void;
  } = {},
): Promise<AssistantConversationState> {
  let runStatus: AgentRunStatus | null = null;
  try {
    runStatus = await getAgentRunStatus(runId);
  } catch {
    runStatus = null;
  }

  const sessionIdFromStatus = runStatus?.sessionId;
  if (runStatus?.active) {
    if (typeof options.afterSequence === "number") {
      try {
        const incrementalEvents = await getAgentRunEvents(
          runId,
          options.afterSequence,
        );
        incrementalEvents.events.forEach((event) => options.onEvent?.(event));
      } catch {
        // Status is authoritative for active recovery; event replay is best-effort.
      }
    }
    throw new ApiError(202, "整理还在进行中，稍后会自动同步最新状态。", {
      runId,
    });
  }

  if (sessionIdFromStatus) {
    return getAssistantConversation(sessionIdFromStatus);
  }

  let runEvents: AgentRunEvents | null = null;
  try {
    runEvents = await getAgentRunEvents(runId);
  } catch {
    runEvents = null;
  }
  const sessionIdFromEvents = (runEvents?.events ?? [])
    .map((event) => pickString(event.payload, ["session_id", "sessionId"]))
    .find((sessionId): sessionId is string => Boolean(sessionId));
  if (sessionIdFromEvents) {
    return getAssistantConversation(sessionIdFromEvents);
  }

  throw new ApiError(
    500,
    runStatus?.message ?? "Agent 已开始处理，但没有返回最终会话。",
  );
}

function buildAssistantMessageBody(input: AssistantMessageInput): JsonObject {
  return {
    message: input.message.trim(),
    session_id: input.sessionId ?? undefined,
    start_new_session: input.startNewSession ?? false,
    context: input.context
      ? {
          from_route: input.context.fromRoute ?? undefined,
          focus: input.context.focus ?? undefined,
          source_ids: input.context.sourceIds ?? undefined,
          daily_brief_id: input.context.dailyBriefId ?? undefined,
        }
      : undefined,
  };
}

export async function getAgentRunTrace(runId: string): Promise<AgentRunTrace> {
  const payload = await request(`/api/assistant/runs/${runId}/trace`, {});
  return parseAgentRunTrace(payload, runId);
}

export async function getAgentRunEvents(
  runId: string,
  afterSequence?: number,
): Promise<AgentRunEvents> {
  const query =
    typeof afterSequence === "number"
      ? `?after_sequence=${encodeURIComponent(String(afterSequence))}`
      : "";
  const payload = await request(`/api/assistant/runs/${runId}/events${query}`, {});
  return parseAgentRunEvents(payload, runId);
}

export async function getAgentRunStatus(runId: string): Promise<AgentRunStatus> {
  const payload = await request(`/api/assistant/runs/${runId}/status`, {});
  return parseAgentRunStatus(payload, runId);
}

export async function getActiveAgentRuns(
  sessionId?: string | null,
): Promise<ActiveAgentRuns> {
  const query = sessionId ? `?session_id=${encodeURIComponent(sessionId)}` : "";
  const payload = await request(`/api/assistant/runs/active${query}`, {});
  return parseActiveAgentRuns(payload);
}

export async function queueAgentRunFollowUp(
  runId: string,
  message: string,
): Promise<QueuedFollowUpResponse> {
  const payload = await request(`/api/assistant/runs/${runId}/queued-follow-up`, {
    method: "POST",
    body: { message },
  });
  return parseQueuedFollowUpResponse(payload);
}

export async function getSessionQueuedFollowUp(
  sessionId: string,
): Promise<QueuedFollowUpResponse> {
  const payload = await request(
    `/api/assistant/sessions/${sessionId}/queued-follow-up`,
    {},
  );
  return parseQueuedFollowUpResponse(payload);
}

export async function markSessionQueuedFollowUpSubmitted(
  sessionId: string,
  queuedFollowUpId: string,
): Promise<QueuedFollowUpResponse> {
  const payload = await request(
    `/api/assistant/sessions/${sessionId}/queued-follow-up/submitted`,
    { method: "POST", body: { queued_follow_up_id: queuedFollowUpId } },
  );
  return parseQueuedFollowUpResponse(payload);
}

export async function discardSessionQueuedFollowUp(
  sessionId: string,
): Promise<QueuedFollowUpResponse> {
  const payload = await request(
    `/api/assistant/sessions/${sessionId}/queued-follow-up`,
    { method: "DELETE" },
  );
  return parseQueuedFollowUpResponse(payload);
}

export async function cancelAgentRun(runId: string): Promise<AgentRunCancelResult> {
  const payload = await request(`/api/assistant/runs/${runId}/cancel`, {
    method: "POST",
  });
  return parseAgentRunCancelResult(payload, runId);
}

export async function createPortfolioSnapshot(
  input: PortfolioSnapshotInput,
): Promise<PortfolioReport> {
  const payload = await request("/api/portfolio/snapshots", {
    method: "POST",
    body: {
      snapshot_date: input.snapshotDate,
      cash_value: input.cashValue,
      holdings: input.holdings.map((holding) => ({
        fund_code: holding.fundCode.trim(),
        fund_name: holding.fundName.trim(),
        fund_type: holding.fundType.trim(),
        market_value: holding.marketValue,
      })),
    },
  });

  const report = parsePortfolioReport(payload);
  if (!report) {
    throw new ApiError(500, "Portfolio report payload is invalid.");
  }
  return report;
}

export async function getLatestPortfolioReport(): Promise<PortfolioLatestState> {
  const payload = await request("/api/portfolio/latest", {});
  return parsePortfolioLatest(payload);
}

export async function getPortfolioHistory(): Promise<PortfolioHistoryState> {
  const payload = await request("/api/portfolio/history", {});
  return parsePortfolioHistory(payload);
}

export async function getSimulationScenarios(): Promise<SimulationScenarioCatalog> {
  const payload = await request("/api/simulations/scenarios", {});
  return parseSimulationScenarioCatalog(payload);
}

export async function startSimulationSession(
  input: SimulationSessionStartInput,
): Promise<SimulationSession> {
  const payload = await request("/api/simulations/sessions", {
    method: "POST",
    body: {
      scenario_slug: input.scenarioId,
    },
  });

  const session = parseSimulationSession(payload);
  if (!session) {
    throw new ApiError(500, "Simulation session payload is invalid.");
  }

  return session;
}

export async function getSimulationSession(
  sessionId: string,
): Promise<SimulationSession> {
  const payload = await request(`/api/simulations/sessions/${sessionId}`, {});
  const session = parseSimulationSession(payload);
  if (!session) {
    throw new ApiError(500, "Simulation session payload is invalid.");
  }

  return session;
}

export async function submitSimulationAction(
  input: SimulationActionInput,
): Promise<SimulationActionResult> {
  const payload = await request("/api/simulations/actions", {
    method: "POST",
    body: {
      session_id: input.sessionId,
      event_id: input.eventId ?? undefined,
      choice_key: input.actionId,
      rationale: input.rationale?.trim() || undefined,
      worry: input.worry?.trim() || undefined,
      impulse_control_plan: input.impulseControlPlan?.trim() || undefined,
    },
  });

  return parseSimulationActionResult(payload);
}

export async function getSimulationReview(
  sessionId: string,
): Promise<SimulationReview> {
  const payload = await request(`/api/simulations/review/${sessionId}`, {});
  return parseSimulationReview(payload, sessionId);
}

export async function getNewsCatalog(
  options: { refresh?: boolean } = { refresh: false },
): Promise<NewsCatalogState> {
  const query = options.refresh === true ? "?refresh=true&limit=20" : "";
  const payload = await request(`/api/news${query}`, {});
  return parseNewsCatalog(payload);
}

export async function analyzeNews(
  input: NewsAnalysisInput,
): Promise<NewsAnalysis> {
  const payload = await request("/api/news/analyze", {
    method: "POST",
    body: {
      item_id: input.itemId ?? undefined,
      headline: input.headline?.trim() || undefined,
      body: input.body?.trim() || undefined,
    },
  });

  return parseNewsAnalysis(payload);
}

export async function getNewsAnalysis(analysisId: string): Promise<NewsAnalysis> {
  const payload = await request(`/api/news/analyses/${analysisId}`, {});
  return parseNewsAnalysis(payload);
}

export async function getAutomationsState(): Promise<AutomationListState> {
  const payload = await request("/api/automations", {});
  return parseAutomationList(payload);
}

export async function updateAutomationSetting(
  input: AutomationUpdateInput,
): Promise<AutomationItem> {
  const payload = await request(`/api/automations/${input.automationKey}`, {
    method: "PATCH",
    body: {
      enabled: input.enabled,
      cadence_key: input.cadenceKey,
    },
  });
  const item = parseAutomationItem(payload);
  if (!item) {
    throw new ApiError(500, "Automation payload is invalid.");
  }
  return item;
}

export async function runAutomationNow(
  automationKey: AutomationKey,
): Promise<AutomationRunResult> {
  const payload = await request(`/api/automations/${automationKey}/run`, {
    method: "POST",
  });
  return parseAutomationRunResult(payload);
}

export async function getProfileContext(): Promise<ProfileContextState> {
  const payload = await request("/api/profile/context", {});
  return parseProfileContext(payload);
}

export async function getProfilePendingProposals(): Promise<ProfilePendingProposalsState> {
  const payload = await request("/api/profile/pending-proposals", {});
  return parseProfilePendingProposals(payload);
}

export async function getProfileLlmSettings(): Promise<ProfileLlmSettings> {
  const payload = await request("/api/profile/llm-settings", {});
  return parseProfileLlmSettings(payload);
}

export async function updateProfileLlmSettings(
  input: ProfileLlmSettingsInput,
): Promise<ProfileLlmSettings> {
  const payload = await request("/api/profile/llm-settings", {
    method: "PUT",
    body: {
      provider: input.provider ?? "deepseek",
      model_name: input.modelName.trim(),
      api_key: input.apiKey?.trim() || undefined,
      enabled: input.enabled,
    },
  });
  return parseProfileLlmSettings(asObject(payload).settings);
}

export async function acceptProfilePendingProposal(
  input: ProfileProposalDecisionInput,
): Promise<ProfileProposalDecisionResult> {
  const payload = await request(
    `/api/profile/pending-proposals/${input.proposalId}/accept`,
    {
      method: "POST",
      body: { reason: input.reason },
    },
  );
  const source = asObject(payload);
  const proposal = parseProfilePendingProposal(source.proposal);
  if (!proposal) {
    throw new ApiError(500, "Profile proposal payload is invalid.");
  }
  return {
    proposal,
    appliedWriteback: asBoolean(
      source.applied_writeback ?? source.appliedWriteback,
    ),
  };
}

export async function rejectProfilePendingProposal(
  input: ProfileProposalDecisionInput,
): Promise<ProfileProposalDecisionResult> {
  const payload = await request(
    `/api/profile/pending-proposals/${input.proposalId}/reject`,
    {
      method: "POST",
      body: { reason: input.reason },
    },
  );
  const source = asObject(payload);
  const proposal = parseProfilePendingProposal(source.proposal);
  if (!proposal) {
    throw new ApiError(500, "Profile proposal payload is invalid.");
  }
  return {
    proposal,
    appliedWriteback: asBoolean(
      source.applied_writeback ?? source.appliedWriteback,
    ),
  };
}
