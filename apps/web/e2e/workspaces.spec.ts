import { expect, test } from "@playwright/test";

import { mockFundGeneApi } from "./fixtures";

test.describe.configure({ mode: "serial" });
test.setTimeout(60_000);

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    window.localStorage.removeItem("fundgene-sidebar-collapsed");
  });
  await mockFundGeneApi(page);
});

test("renders the primary workspace routes with mocked API state", async ({
  page,
}) => {
  const routes = [
    { path: "/today", text: "今日简报" },
    { path: "/agent", text: "Agent 工作区" },
    { path: "/automations", text: "自动任务" },
    { path: "/profile", text: "待确认资料" },
    { path: "/portfolio", text: "组合画像结论" },
    { path: "/onboarding", text: "开始建档" },
    { path: "/learning", text: "今日训练任务" },
    { path: "/simulation", text: "训练任务单" },
    { path: "/news", text: "影响路径图" },
  ];

  for (const route of routes) {
    await page.goto(route.path, { waitUntil: "domcontentloaded" });
    await expect(page.getByText(route.text).first()).toBeVisible({
      timeout: 15_000,
    });
  }
});

test("simulation exposes training guidance and keeps the submit action visible", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1365, height: 768 });
  await page.goto("/simulation", { waitUntil: "domcontentloaded" });

  const guideButton = page.getByRole("button", { name: "训练说明" });
  await expect(guideButton).toBeVisible({ timeout: 15_000 });
  await guideButton.click();
  await expect(page.getByRole("region", { name: "训练说明" })).toBeVisible();
  await expect(guideButton).toHaveAttribute("aria-expanded", "true");

  const submitButton = page.getByRole("button", { name: "提交决策并继续" });
  await expect(submitButton).toBeVisible();
  await expect(submitButton).toBeInViewport();
  await expect(submitButton).toBeDisabled();
  await expect(page.getByRole("radio", { checked: true })).toHaveCount(0);

  const taskScroll = page.getByTestId("simulation-task-scroll");
  await expect(taskScroll).toHaveCSS("overflow-y", "auto");
  const scrollMetrics = await taskScroll.evaluate((element) => ({
    clientHeight: element.clientHeight,
    scrollHeight: element.scrollHeight,
  }));
  expect(scrollMetrics.clientHeight).toBeGreaterThan(120);
  expect(scrollMetrics.scrollHeight).toBeGreaterThanOrEqual(
    scrollMetrics.clientHeight,
  );
});

test("simulation opens the final review after a completed training action", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1365, height: 768 });
  await page.goto("/simulation", { waitUntil: "domcontentloaded" });

  await page
    .getByPlaceholder("请结合当前信息，说明你的判断依据和考虑。")
    .fill("我先保留仓位，等风险信息更清楚再决定。");
  await page.getByRole("radio").first().check();
  await page.getByRole("button", { name: "提交决策并继续" }).click();
  await expect(page.getByText("这次训练先记住一件事")).toBeVisible({
    timeout: 15_000,
  });
  await expect(page.getByText("你在波动阶段选择先观察")).toBeVisible();
  await expect(page.getByText("暴露的触发点")).toBeVisible();
  await expect(page.getByText("performance_chasing_risk")).toHaveCount(0);

  await page.getByRole("link", { name: "去教练继续追问" }).click();
  await expect(page).toHaveURL(/\/agent\?/);
  await expect(page.locator("textarea")).toHaveValue(
    "请帮我复盘这次模拟训练里最容易失守的节点，并告诉我下次行动前要先检查什么。",
  );
});

test("coach keeps internal trace details out of the user-facing answer", async ({
  page,
}) => {
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  await expect(page.getByText("边界提醒").first()).toBeVisible({
    timeout: 20_000,
  });
  await expect(page.getByRole("link", { name: /进入学习中心/ })).toHaveAttribute(
    "href",
    "/learning",
  );
  await expect(page.getByText("下一步").first()).toBeVisible();
  await expect(page.getByTestId("agent-run-status")).toContainText(
    "生成给用户的回答",
  );
  await expect(page.getByTestId("agent-run-status")).toContainText("本次整理已完成。");
  await expect(page.getByText("下一句可以问").first()).not.toBeVisible();
  await expect(page.getByText("证据链路")).toHaveCount(0);
  await expect(page.getByText("Run ID: run_e2e")).toHaveCount(0);
  await expect(page.getByText("learning_knowledge_lookup")).toHaveCount(0);
  await expect(page.getByText("portfolio_snapshots")).toHaveCount(0);
  await expect(page.getByText("Session rail")).toHaveCount(0);
  await expect(page.getByText("Run status")).toHaveCount(0);
});

test("agent mobile keeps the active task surface before supporting panels", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 900 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  const activeSessionBox = await page
    .getByTestId("agent-active-session")
    .boundingBox();

  expect(activeSessionBox).not.toBeNull();
  await expect(page.getByTestId("agent-session-rail")).toHaveCount(0);
  await expect(page.getByTestId("agent-run-status")).toHaveCount(0);
  const mobileNavigation = page.getByRole("navigation", {
    name: "FundGene 工作区导航",
  });
  await expect(mobileNavigation.getByText("今日")).toBeVisible();
  await expect(mobileNavigation.getByText("Agent")).toBeVisible();
  await expect(mobileNavigation.getByText("自动任务")).toBeVisible();
  await expect(mobileNavigation.getByText("资料")).toBeVisible();
  await expect(mobileNavigation.getByText("学习训练")).not.toBeVisible();
  await expect(mobileNavigation.getByText("资讯解读")).not.toBeVisible();
  await expect(mobileNavigation.getByText("模拟训练")).not.toBeVisible();
});

test("agent new session opens a blank task surface", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 720 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  await expect(page.getByText("今天想先问清楚什么？")).toHaveCount(0);
  await page.getByRole("button", { name: "新会话" }).click();
  await expect(page.getByText("今天想先问清楚什么？")).toBeVisible();
  await expect(page.getByRole("log").locator(".coach-message")).toHaveCount(0);
  await expect(page.locator("textarea")).toBeFocused();
});

test("agent example task jumps to the portfolio analysis view", async ({ page }) => {
  await page.setViewportSize({ width: 1365, height: 768 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  await page.getByRole("button", { name: "新会话" }).click();
  await page
    .locator("textarea")
    .fill("帮我检查这份组合里最需要关注的风险来源。");
  await page.getByRole("button", { name: "发送" }).click();

  const portfolioAction = page.getByRole("link", { name: /查看持仓分析/ }).first();
  await expect(portfolioAction).toBeVisible({ timeout: 15_000 });
  await expect(portfolioAction).toHaveAttribute(
    "href",
    "/portfolio?from=agent&focus=concentration",
  );

  await portfolioAction.click();
  await expect(page).toHaveURL(/\/portfolio\?from=agent&focus=concentration/);
  await expect(page.getByText("来自 Agent 的持仓分析")).toBeVisible();
  await expect(page.getByText("先看第一大持仓、资产分布和集中度。")).toBeVisible();
  await expect(page.getByText("资产分布", { exact: true })).toBeVisible();
  await expect(page.getByText("持仓权重", { exact: true })).toBeVisible();
  await expect(page.getByText("第一大持仓").first()).toBeVisible();
});

test("agent history reloads a saved conversation and continues it", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1280, height: 720 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  await page.getByRole("button", { name: "展开任务参考" }).click();
  await page.getByRole("button", { name: /热门基金要不要追/ }).click();

  const messageLog = page.getByRole("log");
  await expect(
    messageLog.getByText("我总想追最近涨得多的基金，怎么办？", { exact: true }),
  ).toBeVisible();
  await expect(
    messageLog.getByText("那我看到热门基金连续上涨时应该怎么处理？", { exact: true }),
  ).toBeVisible();
  await expect(
    messageLog.getByText("先把它拆成三部分：事实是它为什么上涨").first(),
  ).toBeVisible();
  await expect(messageLog.locator(".coach-message")).toHaveCount(4);

  await page.locator("textarea").fill("继续这段历史对话。");
  await page.getByRole("button", { name: "发送" }).click();

  await expect(messageLog.getByText("继续这段历史对话。", { exact: true })).toBeVisible();
  await expect(
    messageLog.getByText("可以，我们接着用同一套三步检查来看这次冲动。").first(),
  ).toBeVisible();
  await expect(messageLog.locator(".coach-message")).toHaveCount(6);
});

test("agent recovers the conversation when the stream misses the final payload", async ({
  page,
}) => {
  const recoveryRequests: string[] = [];
  page.on("request", (request) => {
    const path = new URL(request.url()).pathname;
    if (
      path === "/api/assistant/runs/run_e2e/status" ||
      path === "/api/assistant/sessions/session_e2e"
    ) {
      recoveryRequests.push(path);
    }
  });

  await page.setViewportSize({ width: 1280, height: 720 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });
  await page.getByRole("button", { name: "新会话" }).click();
  await expect(
    page.getByText("回撤可以理解为基金净值从阶段高点跌到低点的幅度。"),
  ).toHaveCount(0);
  await page.locator("textarea").fill("触发流式恢复测试");
  await page.getByRole("button", { name: "发送" }).click();

  await expect(
    page.getByText("回撤可以理解为基金净值从阶段高点跌到低点的幅度。"),
  ).toBeVisible({ timeout: 15_000 });
  await expect(page.getByTestId("agent-run-status")).toContainText(
    "任务处理完成",
  );
  await expect(page.getByText("Agent 已开始处理，但没有返回最终会话。")).toHaveCount(0);
  expect(recoveryRequests).toContain("/api/assistant/sessions/session_e2e");
});

test("agent recovers the conversation when the stream ends with a recoverable error", async ({
  page,
}) => {
  const recoveryRequests: string[] = [];
  page.on("request", (request) => {
    const path = new URL(request.url()).pathname;
    if (
      path === "/api/assistant/runs/run_stream_error_recover_e2e/status" ||
      path === "/api/assistant/sessions/session_e2e"
    ) {
      recoveryRequests.push(path);
    }
  });

  await page.setViewportSize({ width: 1280, height: 720 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });
  await page.getByRole("button", { name: "新会话" }).click();
  await page.locator("textarea").fill("触发错误恢复测试");
  await page.getByRole("button", { name: "发送" }).click();

  await expect(
    page.getByText("回撤可以理解为基金净值从阶段高点跌到低点的幅度。"),
  ).toBeVisible({ timeout: 15_000 });
  await expect(page.getByText("流式连接在最终会话返回前断开。")).toHaveCount(0);
  expect(recoveryRequests).toContain(
    "/api/assistant/runs/run_stream_error_recover_e2e/status",
  );
  expect(recoveryRequests).toContain("/api/assistant/sessions/session_e2e");
});

test("agent keeps the run active when the stream disconnects before terminal state", async ({
  page,
}) => {
  const recoveryRequests: string[] = [];
  const eventReplayCursors: string[] = [];
  page.on("request", (request) => {
    const url = new URL(request.url());
    const path = url.pathname;
    if (
      path === "/api/assistant/runs/run_active_disconnect_e2e/status" ||
      path === "/api/assistant/sessions/session_e2e"
    ) {
      recoveryRequests.push(path);
    }
    if (path === "/api/assistant/runs/run_active_disconnect_e2e/events") {
      eventReplayCursors.push(url.searchParams.get("after_sequence") ?? "");
    }
  });
  await page.route(
    "**/api/assistant/runs/run_active_disconnect_e2e/events",
    async (route) => {
      const afterSequence = Number(
        new URL(route.request().url()).searchParams.get("after_sequence") ?? 0,
      );
      const events = [
        {
          id: "run_active_disconnect_e2e:0001",
          run_id: "run_active_disconnect_e2e",
          sequence: 1,
          event_type: "turn_started",
          phase: "turn",
          title: "开始处理任务",
          status: "running",
          at: "2026-04-26T09:35:00Z",
          duration_ms: null,
          payload: {},
        },
        {
          id: "run_active_disconnect_e2e:0002",
          run_id: "run_active_disconnect_e2e",
          sequence: 2,
          event_type: "step_started",
          phase: "context",
          title: "读取授权上下文",
          status: "running",
          at: "2026-04-26T09:35:01Z",
          duration_ms: null,
          payload: {},
        },
        {
          id: "run_active_disconnect_e2e:0003",
          run_id: "run_active_disconnect_e2e",
          sequence: 3,
          event_type: "input_queued",
          phase: "input",
          title: "下一句已排队",
          status: "queued",
          at: "2026-04-26T09:35:02Z",
          duration_ms: null,
          payload: { message_preview: "这条 replay 事件不应被 live 事件吞掉。" },
        },
      ];
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          schema_version: "agent_run_events_v1",
          run_id: "run_active_disconnect_e2e",
          events: events.filter((event) => event.sequence > afterSequence),
        }),
      });
    },
  );

  await page.setViewportSize({ width: 1280, height: 720 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });
  await page.getByRole("button", { name: "新会话" }).click();
  await page.locator("textarea").fill("触发活跃流断开测试");
  await page.getByRole("button", { name: "发送" }).click();

  await expect(
    page.getByText("触发活跃流断开测试", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("正在结合你的画像和这轮问题组织回答..."),
  ).toBeVisible();
  await expect(page.getByTestId("agent-run-status")).toContainText("正在整理");
  await expect(page.getByTestId("agent-run-status")).toContainText(
    "本次整理正在当前进程中运行。",
  );
  await expect(page.getByTestId("agent-run-status")).toContainText("下一句已排队");
  await expect(page.getByTestId("agent-run-status")).toContainText(
    "已收到下一句，会等本轮完成后再发送",
  );
  await expect(
    page.getByText("回撤可以理解为基金净值从阶段高点跌到低点的幅度。"),
  ).toHaveCount(0);
  expect(recoveryRequests).toContain(
    "/api/assistant/runs/run_active_disconnect_e2e/status",
  );
  expect(eventReplayCursors).toContain("2");
  expect(recoveryRequests).not.toContain("/api/assistant/sessions/session_e2e");
});

test("agent discovers an active run after reload and keeps stop available", async ({
  page,
}) => {
  const requests: string[] = [];
  let queuedFollowUpMessage: string | null = null;
  let queuedFollowUpDiscarded = false;
  let runCancelled = false;
  page.on("request", (request) => {
    const path = new URL(request.url()).pathname;
    if (
      path === "/api/assistant/runs/active" ||
      path === "/api/assistant/runs/run_active_e2e/status" ||
      path === "/api/assistant/runs/run_active_e2e/events" ||
      path === "/api/assistant/runs/run_active_e2e/queued-follow-up" ||
      path === "/api/assistant/runs/run_active_e2e/cancel"
    ) {
      requests.push(path);
    }
  });

  await page.route("**/api/assistant/runs/active**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: "active_agent_runs_v1",
        runs: runCancelled
          ? []
          : [
              {
                schema_version: "agent_run_status_v1",
                run_id: "run_active_e2e",
                session_id: "session_e2e",
                status: "running",
                run_status: "running",
                active: true,
                cancel_requested: false,
                started_at: "2026-04-26T09:45:00Z",
                completed_at: null,
                latency_ms: null,
                message: "本次整理正在当前进程中运行。",
              },
            ],
      }),
    });
  });
  await page.route("**/api/assistant/runs/run_active_e2e/status", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: "agent_run_status_v1",
        run_id: "run_active_e2e",
        session_id: "session_e2e",
        status: runCancelled ? "cancelled" : "running",
        run_status: runCancelled ? "cancelled" : "running",
        active: !runCancelled,
        cancel_requested: runCancelled,
        started_at: "2026-04-26T09:45:00Z",
        completed_at: runCancelled ? "2026-04-26T09:45:16Z" : null,
        latency_ms: runCancelled ? 16_000 : null,
        message: runCancelled ? "本次整理已停止，没有替你确认长期记录。" : "本次整理正在当前进程中运行。",
      }),
    });
  });
  await page.route(
    "**/api/assistant/runs/run_active_e2e/queued-follow-up",
    async (route) => {
      const payload = route.request().postDataJSON() as { message?: string };
      queuedFollowUpMessage = payload.message ?? "";
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          schema_version: "queued_follow_up_v1",
          queued_follow_up: {
            id: "queued_active_e2e",
            session_id: "session_e2e",
            queued_after_run_id: "run_active_e2e",
            message: queuedFollowUpMessage,
            status: "queued",
            created_at: "2026-04-26T09:45:10Z",
            submitted_at: null,
            discarded_at: null,
          },
          message: "已排队，上一条完成后发送。",
        }),
      });
    },
  );
  await page.route("**/api/assistant/runs/run_active_e2e/events", async (route) => {
    const events = [
      {
        id: "run_active_e2e:0001",
        run_id: "run_active_e2e",
        sequence: 1,
        event_type: "turn_started",
        phase: "turn",
        title: "开始处理任务",
        status: "running",
        at: "2026-04-26T09:45:00Z",
        duration_ms: null,
        payload: {},
      },
    ];
    if (queuedFollowUpMessage) {
      events.push({
        id: "run_active_e2e:0002",
        run_id: "run_active_e2e",
        sequence: 2,
        event_type: "input_queued",
        phase: "input",
        title: "下一句已排队",
        status: "queued",
        at: "2026-04-26T09:45:10Z",
        duration_ms: null,
        payload: { message_preview: queuedFollowUpMessage },
      });
    }
    if (queuedFollowUpDiscarded) {
      events.push({
        id: "run_active_e2e:0003",
        run_id: "run_active_e2e",
        sequence: 3,
        event_type: "input_discarded",
        phase: "input",
        title: "停止后已取消排队下一句",
        status: "discarded",
        at: "2026-04-26T09:45:15Z",
        duration_ms: null,
        payload: { message_preview: queuedFollowUpMessage ?? "" },
      });
    }
    if (runCancelled) {
      events.push(
        {
          id: "run_active_e2e:0004",
          run_id: "run_active_e2e",
          sequence: 4,
          event_type: "agent_message",
          phase: "message",
          title: "同步停止说明",
          status: "cancelled",
          at: "2026-04-26T09:45:16Z",
          duration_ms: null,
          payload: {},
        },
        {
          id: "run_active_e2e:0005",
          run_id: "run_active_e2e",
          sequence: 5,
          event_type: "turn_closed",
          phase: "turn",
          title: "本轮已停止",
          status: "cancelled",
          at: "2026-04-26T09:45:16Z",
          duration_ms: 16_000,
          payload: {},
        },
      );
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: "agent_run_events_v1",
        run_id: "run_active_e2e",
        events,
      }),
    });
  });
  await page.route("**/api/assistant/runs/run_active_e2e/cancel", async (route) => {
    queuedFollowUpDiscarded = Boolean(queuedFollowUpMessage);
    runCancelled = true;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        run_id: "run_active_e2e",
        status: "cancelling",
        cancel_requested: true,
        message: "已请求停止本次整理，排队的下一句不会自动发送。",
      }),
    });
  });

  await page.setViewportSize({ width: 1280, height: 720 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  await expect(page.getByTestId("agent-run-status")).toContainText("正在整理");
  await expect(page.getByRole("button", { name: "停止" })).toBeVisible();
  await expect(page.getByRole("button", { name: "生成中" })).toBeDisabled();
  await page.locator("textarea").fill("再帮我接着看新闻影响。");
  const queueRequest = page.waitForRequest((request) => {
    const path = new URL(request.url()).pathname;
    return path === "/api/assistant/runs/run_active_e2e/queued-follow-up";
  });
  await page.getByRole("button", { name: "排队发送" }).click();
  await queueRequest;
  await expect(
    page.getByText("再帮我接着看新闻影响。", { exact: true }),
  ).toBeVisible();
  await expect(page.getByText("上一条完成后发送；如果上一条被停止或失败")).toBeVisible();
  await expect(page.getByTestId("agent-run-status")).toContainText("下一句已排队");
  await expect(page.getByTestId("agent-run-status")).toContainText(
    "已收到下一句，会等本轮完成后再发送",
  );
  const cancelRequest = page.waitForRequest((request) => {
    const path = new URL(request.url()).pathname;
    return path === "/api/assistant/runs/run_active_e2e/cancel";
  });
  await page.getByRole("button", { name: "停止" }).click();
  await cancelRequest;
  await expect(
    page.getByText("再帮我接着看新闻影响。", { exact: true }),
  ).toHaveCount(0);
  await expect(page.getByTestId("agent-run-status")).toContainText("已停止");
  await expect(page.getByTestId("agent-run-status")).toContainText("本轮已停止");
  await expect(page.getByTestId("agent-run-status")).toContainText(
    "本轮已停止，未作为正常完成处理",
  );
  await expect(page.getByTestId("agent-run-status")).not.toContainText("本轮任务已完成");
  expect(requests).toContain("/api/assistant/runs/active");
  expect(requests).toContain("/api/assistant/runs/run_active_e2e/status");
  expect(requests).toContain("/api/assistant/runs/run_active_e2e/events");
  expect(requests).toContain("/api/assistant/runs/run_active_e2e/queued-follow-up");
  expect(requests).toContain("/api/assistant/runs/run_active_e2e/cancel");
});

test("agent keeps stale queued follow-up pending after a cancelled source run", async ({
  page,
}) => {
  let submittedRequests = 0;
  let streamRequests = 0;
  const staleQueuedFollowUp = {
    id: "queued_cancelled_e2e",
    session_id: "session_e2e",
    queued_after_run_id: "run_e2e",
    message: "上一条停止后，这句不能自动发送。",
    status: "queued",
    created_at: "2026-04-26T09:50:10Z",
    submitted_at: null,
    discarded_at: null,
  };

  await page.route("**/api/assistant/runs/run_e2e/status", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: "agent_run_status_v1",
        run_id: "run_e2e",
        session_id: "session_e2e",
        status: "cancelled",
        run_status: "cancelled",
        active: false,
        cancel_requested: true,
        started_at: "2026-04-26T09:49:00Z",
        completed_at: "2026-04-26T09:50:00Z",
        latency_ms: 60_000,
        message: "本次整理已停止，没有替你确认长期记录。",
        queued_follow_up: staleQueuedFollowUp,
      }),
    });
  });
  await page.route("**/api/assistant/runs/run_e2e/events", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: "agent_run_events_v1",
        run_id: "run_e2e",
        events: [
          {
            id: "run_e2e:0001",
            run_id: "run_e2e",
            sequence: 1,
            event_type: "turn_started",
            phase: "turn",
            title: "开始处理任务",
            status: "running",
            at: "2026-04-26T09:49:00Z",
            duration_ms: null,
            payload: {},
          },
          {
            id: "run_e2e:0002",
            run_id: "run_e2e",
            sequence: 2,
            event_type: "turn_closed",
            phase: "turn",
            title: "本轮已停止",
            status: "cancelled",
            at: "2026-04-26T09:50:00Z",
            duration_ms: 60_000,
            payload: {},
          },
        ],
      }),
    });
  });
  await page.route(
    "**/api/assistant/sessions/session_e2e/queued-follow-up",
    async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          schema_version: "queued_follow_up_v1",
          queued_follow_up: staleQueuedFollowUp,
          message: "已找到排队的下一句。",
        }),
      });
    },
  );
  await page.route(
    "**/api/assistant/sessions/session_e2e/queued-follow-up/submitted",
    async (route) => {
      submittedRequests += 1;
      await route.fulfill({
        status: 409,
        contentType: "application/json",
        body: JSON.stringify({
          detail:
            "Queued follow-up can only be marked submitted after the previous run completes successfully.",
        }),
      });
    },
  );
  await page.route("**/api/assistant/messages/stream", async (route) => {
    streamRequests += 1;
    await route.fulfill({
      status: 200,
      contentType: "text/event-stream",
      body: "",
    });
  });

  await page.setViewportSize({ width: 1280, height: 720 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  await expect(page.getByTestId("agent-run-status")).toContainText("已停止");
  await expect(
    page.getByText("上一条停止后，这句不能自动发送。", { exact: true }),
  ).toBeVisible();
  await expect(page.getByText("上一条完成后发送；如果上一条被停止或失败")).toBeVisible();
  await page.waitForTimeout(500);
  expect(streamRequests).toBe(0);
  expect(submittedRequests).toBe(0);
});

test("agent shows inactive running run as interrupted instead of completed", async ({
  page,
}) => {
  await page.route("**/api/assistant/runs/run_e2e/status", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: "agent_run_status_v1",
        run_id: "run_e2e",
        session_id: "session_e2e",
        status: "interrupted",
        run_status: "interrupted",
        active: false,
        cancel_requested: false,
        started_at: "2026-04-26T09:49:00Z",
        completed_at: "2026-04-26T09:50:00Z",
        latency_ms: 60_000,
        message: "上次整理中断了，没有形成完整结论，可以重新发送。",
      }),
    });
  });
  await page.route("**/api/assistant/runs/run_e2e/events", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: "agent_run_events_v1",
        run_id: "run_e2e",
        events: [
          {
            id: "run_e2e:0001",
            run_id: "run_e2e",
            sequence: 1,
            event_type: "turn_interrupted",
            phase: "turn",
            title: "上次整理已中断",
            status: "interrupted",
            at: "2026-04-26T09:50:00Z",
            duration_ms: null,
            payload: {
              reason: "inactive_running_run",
              message: "运行记录中断，未产生完整终态。",
            },
          },
          {
            id: "run_e2e:0002",
            run_id: "run_e2e",
            sequence: 2,
            event_type: "turn_closed",
            phase: "turn",
            title: "本轮已中断",
            status: "interrupted",
            at: "2026-04-26T09:50:00Z",
            duration_ms: 60_000,
            payload: {
              reason: "inactive_running_run",
              message: "运行记录中断，未产生完整终态。",
            },
          },
        ],
      }),
    });
  });

  await page.setViewportSize({ width: 1280, height: 720 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  await expect(page.getByTestId("agent-run-status")).toContainText("已中断");
  await expect(page.getByTestId("agent-run-status")).toContainText(
    "本轮已中断，未作为完成判断处理",
  );
  await expect(page.getByTestId("agent-run-status")).not.toContainText("已完成");
  await expect(page.getByTestId("agent-run-status")).not.toContainText("本轮任务已完成");
});

test("agent marks the exact queued follow-up item after auto submit", async ({
  page,
}) => {
  const queuedFollowUp = {
    id: "queued_exact_e2e",
    session_id: "session_e2e",
    queued_after_run_id: "run_e2e",
    message: "上一条完成后，自动发送这一句。",
    status: "queued",
    created_at: "2026-04-26T09:55:10Z",
    submitted_at: null,
    discarded_at: null,
  };
  let streamRequests = 0;
  let submittedPayload: Record<string, unknown> | null = null;

  await page.route("**/api/assistant/runs/run_e2e/status", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: "agent_run_status_v1",
        run_id: "run_e2e",
        session_id: "session_e2e",
        status: "completed",
        run_status: "completed",
        active: false,
        cancel_requested: false,
        started_at: "2026-04-26T09:54:00Z",
        completed_at: "2026-04-26T09:55:00Z",
        latency_ms: 60_000,
        message: "本次整理已完成。",
        queued_follow_up: queuedFollowUp,
      }),
    });
  });
  await page.route(
    "**/api/assistant/sessions/session_e2e/queued-follow-up",
    async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          schema_version: "queued_follow_up_v1",
          queued_follow_up: queuedFollowUp,
          message: "已找到排队的下一句。",
        }),
      });
    },
  );
  await page.route(
    "**/api/assistant/sessions/session_e2e/queued-follow-up/submitted",
    async (route) => {
      submittedPayload = route.request().postDataJSON() as Record<string, unknown>;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          schema_version: "queued_follow_up_v1",
          queued_follow_up: {
            ...queuedFollowUp,
            status: "submitted",
            submitted_at: "2026-04-26T09:56:00Z",
          },
          message: "排队内容已发送。",
        }),
      });
    },
  );
  await page.route("**/api/assistant/messages/stream", async (route) => {
    streamRequests += 1;
    await route.fulfill({
      status: 200,
      contentType: "text/event-stream",
      body: [
        "event: conversation",
        `data: ${JSON.stringify({
          session: {
            id: "session_e2e",
            topic: "自动发送排队输入",
            context_type: "coach",
            latest_intent: "portfolio",
            last_question: queuedFollowUp.message,
            last_answer_preview: "已继续整理。",
            last_recommended_action: "保持确认。",
            message_count: 4,
            created_at: "2026-04-26T09:00:00Z",
            updated_at: "2026-04-26T09:56:00Z",
          },
          messages: [
            {
              id: "message_auto_user",
              role: "user",
              content: queuedFollowUp.message,
              message_type: "user_prompt",
              created_at: "2026-04-26T09:55:30Z",
            },
            {
              id: "message_auto_assistant",
              role: "assistant",
              content: "已继续整理这句排队输入。",
              message_type: "advisor_response",
              created_at: "2026-04-26T09:56:00Z",
              agent_run_id: "run_auto_e2e",
              advisor_response: {
                answer: "已继续整理这句排队输入。",
                intent: "portfolio",
                citations: [],
                risk_notice: "不会替你买卖或下单。",
                recommended_actions: [],
                recommended_action_targets: [],
                follow_up_questions: [],
              },
            },
          ],
        })}`,
        "",
        "",
      ].join("\n"),
    });
  });

  await page.setViewportSize({ width: 1280, height: 720 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  await expect(page.getByText("已继续整理这句排队输入。")).toBeVisible({
    timeout: 15_000,
  });
  await expect
    .poll(() => submittedPayload, { timeout: 15_000 })
    .toEqual({ queued_follow_up_id: "queued_exact_e2e" });
  expect(streamRequests).toBe(1);
});

test("agent compact desktop keeps reference and process panels mutually exclusive", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1280, height: 720 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  await expect(page.getByTestId("agent-session-rail")).toHaveCount(0);
  await expect(page.getByTestId("agent-run-status")).toBeVisible();

  await page.getByRole("button", { name: "展开任务参考" }).click();
  await expect(page.getByTestId("agent-session-rail")).toBeVisible();
  await expect(page.getByTestId("agent-run-status")).toHaveCount(0);
  await page.waitForTimeout(250);

  const referenceBox = await page.getByTestId("agent-session-rail").boundingBox();
  const activeBox = await page.getByTestId("agent-active-session").boundingBox();
  const pageScrollHeight = await page.evaluate(
    () => document.documentElement.scrollHeight,
  );
  const pageClientHeight = await page.evaluate(
    () => document.documentElement.clientHeight,
  );

  expect(referenceBox).not.toBeNull();
  expect(activeBox).not.toBeNull();
  expect(referenceBox!.width).toBeGreaterThan(230);
  expect(activeBox!.width).toBeGreaterThan(680);
  expect(pageScrollHeight).toBe(pageClientHeight);

  await page.getByRole("button", { name: "展开整理一览" }).click();
  await expect(page.getByTestId("agent-session-rail")).toHaveCount(0);
  await expect(page.getByTestId("agent-run-status")).toBeVisible();
});

test("workspace sidebar collapses and lets the main surface expand", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 980 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  const sidebarBefore = await page.getByTestId("workspace-sidebar").boundingBox();
  const mainBefore = await page.getByTestId("workspace-main").boundingBox();

  await page.getByRole("button", { name: "收起侧边栏" }).click();
  await expect(page.getByRole("button", { name: "展开侧边栏" })).toBeVisible();
  await page.waitForTimeout(250);

  const sidebarAfter = await page.getByTestId("workspace-sidebar").boundingBox();
  const mainAfter = await page.getByTestId("workspace-main").boundingBox();

  expect(sidebarBefore).not.toBeNull();
  expect(mainBefore).not.toBeNull();
  expect(sidebarAfter).not.toBeNull();
  expect(mainAfter).not.toBeNull();
  expect(sidebarAfter!.width).toBeLessThan(sidebarBefore!.width);
  expect(mainAfter!.width).toBeGreaterThan(mainBefore!.width);

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("agent-collapsed-sidebar-desktop.png"),
  });
});

test("agent support panels can collapse to expand the active session", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 980 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  await expect(page.getByTestId("agent-session-rail")).toBeVisible();
  await expect(page.getByTestId("agent-run-status")).toBeVisible();
  const activeBefore = await page.getByTestId("agent-active-session").boundingBox();

  await page.getByRole("button", { name: "收起任务参考" }).click();
  await page.getByRole("button", { name: "收起整理一览" }).click();
  await expect(page.getByRole("button", { name: "展开任务参考" })).toHaveText(
    "显示参考",
  );
  await expect(page.getByRole("button", { name: "展开整理一览" })).toHaveText(
    "显示依据",
  );
  await page.waitForTimeout(250);

  const activeAfter = await page.getByTestId("agent-active-session").boundingBox();

  expect(activeBefore).not.toBeNull();
  expect(activeAfter).not.toBeNull();
  await expect(page.getByTestId("agent-session-rail")).toHaveCount(0);
  await expect(page.getByTestId("agent-run-status")).toHaveCount(0);
  expect(activeBefore!.width).toBeGreaterThan(500);
  expect(activeAfter!.width).toBeGreaterThan(activeBefore!.width);

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("agent-collapsed-support-panels-desktop.png"),
  });

  await page.setViewportSize({ width: 1600, height: 980 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  const wideActiveBefore = await page
    .getByTestId("agent-active-session")
    .boundingBox();
  await page.getByRole("button", { name: "收起任务参考" }).click();
  await page.getByRole("button", { name: "收起整理一览" }).click();
  await page.waitForTimeout(250);
  const wideActiveAfter = await page
    .getByTestId("agent-active-session")
    .boundingBox();

  expect(wideActiveBefore).not.toBeNull();
  expect(wideActiveAfter).not.toBeNull();
  expect(wideActiveAfter!.width).toBeGreaterThan(wideActiveBefore!.width);
});

test("agent page keeps long conversation inside the message log at MacBook width", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1365, height: 768 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });

  const messageLog = page.getByRole("log");
  await expect(messageLog).toBeVisible({ timeout: 15_000 });
  await expect(messageLog).toHaveCSS("overflow-y", "auto");

  const logBox = await messageLog.boundingBox();
  expect(logBox).not.toBeNull();

  await messageLog.evaluate((element) => {
    element.scrollTop = 0;
  });
  await page.mouse.move(logBox!.x + logBox!.width / 2, logBox!.y + 80);
  await page.mouse.wheel(0, 700);
  await page.waitForTimeout(150);

  const pageScrollY = await page.evaluate(() => window.scrollY);
  const logState = await messageLog.evaluate((element) => ({
    scrollTop: element.scrollTop,
    scrollHeight: element.scrollHeight,
    clientHeight: element.clientHeight,
  }));

  expect(pageScrollY).toBe(0);
  if (logState.scrollHeight > logState.clientHeight) {
    expect(logState.scrollTop).toBeGreaterThan(0);
  }

  await page.setViewportSize({ width: 1600, height: 900 });
  await page.goto("/agent", { waitUntil: "domcontentloaded" });
  await expect(page.getByRole("log")).toHaveCSS("overflow-y", "auto");
});

test("dashboard daily brief shows evidence, safe action, and module status", async ({
  page,
}) => {
  await page.goto("/today", { waitUntil: "domcontentloaded" });

  await expect(page.getByText("今日简报", { exact: true }).first()).toBeVisible({
    timeout: 15_000,
  });
  await expect(
    page.getByRole("heading", {
      name: "今天先检查组合集中度，不急着响应单条新闻。",
    }),
  ).toBeVisible();
  await expect(page.getByText("为什么")).toBeVisible();
  await expect(page.getByText("今天不要做什么")).toHaveCount(0);
  await expect(page.getByText("今天只做这一步")).toBeVisible();
  await expect(page.getByLabel("今日判断依据")).toBeVisible();
  await expect(page.getByText("第一大持仓约 34%").first()).toBeVisible();
  await expect(page.getByText("已进入判断的资料")).toBeVisible();
  await expect(page.getByText("长期资金入市政策更像长期市场结构信号。")).toBeVisible();
  const coverageList = page.locator(".today-coverage-list");
  await expect(coverageList.getByText("画像")).toBeVisible();
  await expect(coverageList.getByText("组合")).toBeVisible();
  await expect(coverageList.getByText("资讯")).toBeVisible();
  await expect(page.getByText("今日信号面板")).toHaveCount(0);
  await expect(page.getByText("新闻标题：")).toHaveCount(0);
  await expect(page.getByText("新闻概括：")).toHaveCount(0);
  await expect(page.getByText("简要解读：")).toHaveCount(0);
  await expect(page.getByText("把这条政策先翻译成一句新手能执行的话")).toHaveCount(0);
  await expect(page.getByText("直接操作账户")).toHaveCount(0);
  await expect(page.getByRole("link", { name: /去完成下一步/ })).toHaveAttribute(
    "href",
    "/portfolio?from=today&focus=concentration",
  );
  await expect(page.getByText("run_e2e")).toHaveCount(0);
});

test("automations page exposes controllable safe automation settings", async ({
  page,
}) => {
  await page.goto("/automations", { waitUntil: "domcontentloaded" });

  await expect(page.getByText("自动任务").first()).toBeVisible({
    timeout: 15_000,
  });
  await expect(page.getByText("已开启").first()).toBeVisible();
  await expect(page.getByText("运行队列")).toBeVisible();
  await expect(
    page.locator(".automation-safety-card").getByText("FundGene 不会连接券商"),
  ).toBeVisible();

  const weeklySwitch = page.getByRole("switch").nth(1);
  await expect(weeklySwitch).toHaveAttribute("aria-checked", "false");
  await weeklySwitch.click();
  await expect(weeklySwitch).toHaveAttribute("aria-checked", "true");
  await expect(page.getByText("每周组合巡检").first()).toBeVisible();

  await page.getByTestId("automation-run-button-daily_brief").click();
  await expect(page.getByTestId("automation-run-timeline-daily_brief")).toBeVisible();
  await expect(page.getByText("检查过程").first()).toBeVisible();
  await expect(page.getByText("读取授权上下文").first()).toBeVisible();
});

test("profile page keeps context and writebacks confirmable", async ({
  page,
}) => {
  await page.goto("/profile", { waitUntil: "domcontentloaded" });

  await expect(page.getByText("上下文完整度").first()).toBeVisible({
    timeout: 15_000,
  });
  await expect(page.getByText("资料更新时间")).toBeVisible();
  await expect(page.getByText("待确认资料").first()).toBeVisible();
  await expect(page.getByText("把“追热点倾向”加入待观察行为证据")).toBeVisible();

  await page.getByRole("button", { name: "确认记录" }).first().click();
  await expect(
    page.getByText("把“追热点倾向”加入待观察行为证据"),
  ).toHaveCount(0);
  await expect(page.getByText("已处理 1 条").first()).toBeVisible();
});

test("portfolio catches duplicate fund codes before saving", async ({ page }) => {
  await page.goto("/portfolio", { waitUntil: "domcontentloaded" });

  await page.getByRole("button", { name: "用示例覆盖当前输入" }).click();
  await page.getByRole("button", { name: "添加一行持仓" }).click();
  await page.locator('input[name="holdings.3.fundCode"]').fill("161725");
  await page.locator('input[name="holdings.3.fundName"]').fill("重复白酒基金");
  await page.locator('input[name="holdings.3.marketValue"]').fill("5000");
  await page.getByRole("button", { name: "检查草稿权重" }).click();

  await expect(page.getByText("基金代码 161725 已重复").first()).toBeVisible();
  await expect(page.getByText("请先合并或删除重复基金代码").first()).toBeVisible();
});

test("portfolio reviews a draft before saving a snapshot", async ({ page }) => {
  await page.goto("/portfolio", { waitUntil: "domcontentloaded" });

  await page.getByRole("button", { name: "用示例覆盖当前输入" }).click();
  await expect(page.getByText("草稿总资产").first()).toBeVisible();
  await expect(page.getByText("¥52,000").first()).toBeVisible();

  await page.getByRole("button", { name: "检查草稿权重" }).click();
  await expect(page.getByText("草稿已检查").first()).toBeVisible();

  await page.getByRole("button", { name: "确认并保存快照" }).click();
  await expect(page.getByText("已生成 2026/4/26 的组合报告").first()).toBeVisible();
});

test("news workspace can request a structured analysis", async ({ page }) => {
  const initialCatalogResponse = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return url.pathname === "/api/news" && !url.search.includes("refresh=true");
  });
  await page.goto("/news", { waitUntil: "domcontentloaded" });
  await initialCatalogResponse;

  await page
    .getByRole("button", { name: "查看资讯：北向资金净流入超百亿元 科技板块获加仓" })
    .click();
  await expect(
    page
      .getByLabel("资讯解读画布")
      .getByRole("heading", { name: "北向资金净流入超百亿元 科技板块获加仓" }),
  ).toBeVisible();

  const refreshCatalogResponse = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return url.pathname === "/api/news" && url.search.includes("refresh=true");
  });
  await page.getByRole("button", { name: "同步真实资讯" }).click();
  await refreshCatalogResponse;

  await page.getByRole("button", { name: "生成解读" }).first().click();

  await expect(page.getByText("影响路径图").first()).toBeVisible();
  await expect(page.getByTestId("news-selected-source")).toBeVisible();
  await expect(page.getByTestId("news-analysis-process")).toBeVisible();
  await expect(page.getByText("已生成解读").first()).toBeVisible();
  await expect(page.getByRole("button", { name: "重新解读" }).first()).toBeVisible();
  await expect(page.getByText("支持性的货币政策立场").first()).toBeVisible();
  await expect(page.getByText("资讯解读不构成买卖建议").first()).toBeVisible();
});

test("news agent link prefills the composer with source context", async ({ page }) => {
  await page.goto("/news", { waitUntil: "domcontentloaded" });

  await page.getByRole("link", { name: "让 Agent 结合我的组合解释" }).click();
  await expect(page).toHaveURL(/\/agent\?/);
  await expect(page.locator("textarea")).toHaveValue(
    "请结合我的组合，解释《央行：下一阶段将坚持支持性的货币政策立场》可能影响什么、哪些地方不能当成买卖信号？",
  );
  await page.getByRole("button", { name: "确认发送" }).click();
  await expect(
    page.getByText("结合你的画像、组合报告和最近同步的《央行：下一阶段将坚持支持性的货币政策立场》"),
  ).toBeVisible();
  await expect(page.getByTestId("agent-run-status")).toContainText(
    "本次整理已完成",
    { timeout: 15_000 },
  );
});
