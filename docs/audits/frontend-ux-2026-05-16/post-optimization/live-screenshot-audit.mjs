import { createRequire } from "node:module";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(new URL("../../../../apps/web/package.json", import.meta.url));
const { chromium } = require("@playwright/test");

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const screenshotsDir = path.join(scriptDir, "screenshots");
const base = process.env.FUNDGENE_BASE_URL ?? "http://127.0.0.1:3000";
const demoEmail = process.env.FUNDGENE_DEMO_EMAIL ?? "demo@fundgene.local";
const demoPassword = process.env.FUNDGENE_DEMO_PASSWORD ?? "fundgene-demo-2026";

const viewports = {
  desktop: { width: 1440, height: 1100 },
  mobile: { width: 390, height: 900 },
};

const publicRoutes = [
  { route: "/", name: "overview" },
  { route: "/start", name: "start" },
];

const workspaceRoutes = [
  { route: "/dashboard", name: "dashboard" },
  { route: "/onboarding", name: "onboarding" },
  { route: "/coach", name: "coach" },
  { route: "/learning", name: "learning" },
  { route: "/learning/fund-basics", name: "course-fund-basics" },
  { route: "/portfolio", name: "portfolio" },
  { route: "/simulation", name: "simulation" },
  { route: "/news", name: "news" },
];

const internalTerms = [
  "Advisor orchestrator",
  "Agent route",
  "agent_run",
  "run_id",
  "worker_output",
  "Scenario dossier",
  "Training objective",
  "Bias focus",
  "Timeline preview",
  "Decision room",
  "Choose an action",
  "Instant feedback",
  "After this session",
  "Final review",
  "Outcome summary",
  "Behavior readout",
  "Review checkpoint",
  "Structured readout",
  "Market signal",
  "Manual snapshot",
  "Latest evidence",
  "Reflection prompts",
  "Continue the loop",
  "What went well",
  "Watch next time",
  "FundGene Coach",
  "Simulation",
  "Learning",
  "Portfolio",
  "Dashboard",
  "Guardrail",
  "BEGINNER-FIRST",
  "Hi, learner",
  "Hi,",
  " min",
  "Section ",
  "dev fixture",
  "fixture://",
  "mock",
  "POLICY",
  "Beginner Core Path",
];

const routeScreens = [];
const interactions = [];
const internalTermFailures = [];
const errors = [];

async function settle(page) {
  await page.waitForLoadState("domcontentloaded");
  await page.waitForLoadState("networkidle", { timeout: 8_000 }).catch(() => {});
  await page.waitForTimeout(350);
}

async function login(page) {
  await page.goto(`${base}/start`);
  await settle(page);

  if (/\/(dashboard|onboarding)/.test(new URL(page.url()).pathname)) {
    return;
  }

  await page.getByRole("button", { name: "登录" }).first().click().catch(() => {});
  await page.getByLabel("邮箱").fill(demoEmail);
  await page.getByLabel("密码").fill(demoPassword);
  await page.getByRole("button", { name: /登录并进入工作台/ }).click();
  await page.waitForURL(/\/(dashboard|onboarding)/, { timeout: 15_000 });
  await settle(page);
}

async function inspectPage(page, route, viewport, screenshotName) {
  await page.goto(`${base}${route}`);
  await settle(page);

  const screenshotPath = path.join(screenshotsDir, screenshotName);
  await page.screenshot({ path: screenshotPath, fullPage: true });

  const result = await page.evaluate((terms) => {
    const text = document.body.innerText;
    const doc = document.documentElement;
    const body = document.body;
    const overflow =
      Math.max(doc.scrollWidth, body.scrollWidth) >
      Math.max(doc.clientWidth, window.innerWidth) + 1;
    const hits = terms.filter((term) => text.includes(term));
    return { textLength: text.length, overflow, hits };
  }, internalTerms);

  const entry = {
    route,
    viewport,
    screenshot: `screenshots/${screenshotName}`,
    overflow: result.overflow,
    internalTerms: result.hits,
    textLength: result.textLength,
  };
  routeScreens.push(entry);

  if (result.overflow) {
    errors.push({ type: "horizontal_overflow", route, viewport });
  }
  if (result.hits.length > 0) {
    internalTermFailures.push({ route, viewport, terms: result.hits });
  }
}

async function screenshotInteraction(page, name, filename) {
  await settle(page);
  await page.screenshot({ path: path.join(screenshotsDir, filename), fullPage: true });
  interactions.push({ name, ok: true, screenshot: `screenshots/${filename}` });
}

async function runInteraction(name, fn) {
  try {
    await fn();
  } catch (error) {
    interactions.push({
      name,
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    });
    errors.push({
      type: "interaction_failed",
      name,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

await mkdir(screenshotsDir, { recursive: true });

const browser = await chromium.launch();

try {
  for (const [viewportName, viewport] of Object.entries(viewports)) {
    const publicContext = await browser.newContext({ viewport });
    const publicPage = await publicContext.newPage();
    for (const item of publicRoutes) {
      await inspectPage(publicPage, item.route, viewportName, `${item.name}-${viewportName}.png`);
    }
    await publicContext.close();

    const authContext = await browser.newContext({ viewport });
    const authPage = await authContext.newPage();
    await login(authPage);
    for (const item of workspaceRoutes) {
      await inspectPage(authPage, item.route, viewportName, `${item.name}-${viewportName}.png`);
    }
    await authContext.close();
  }

  const interactionContext = await browser.newContext({ viewport: viewports.desktop });
  const page = await interactionContext.newPage();
  await login(page);

  await runInteraction("coach_send_message", async () => {
    await page.goto(`${base}/coach`);
    await settle(page);
    await page.locator("textarea").fill("我现在只买一只热门基金，会有什么风险？");
    await page.getByRole("button", { name: /^发送$/ }).click();
    await page.getByText("风险边界").first().waitFor({ timeout: 20_000 }).catch(() => {});
    await screenshotInteraction(page, "coach_send_message", "interaction-coach-after-message.png");
  });

  await runInteraction("learning_mark_section", async () => {
    await page.goto(`${base}/learning/fund-basics`);
    await settle(page);
    const button = page.getByRole("button", { name: "标记为已完成" }).first();
    if (await button.isVisible().catch(() => false)) {
      await button.click();
      await page.getByText("已记录完成").first().waitFor({ timeout: 10_000 }).catch(() => {});
    }
    await screenshotInteraction(page, "learning_mark_section", "interaction-learning-course.png");
  });

  await runInteraction("portfolio_invalid_submit", async () => {
    await page.goto(`${base}/portfolio`);
    await settle(page);
    await page.getByRole("button", { name: "添加一行持仓" }).click();
    await page.getByRole("button", { name: "生成组合报告" }).click();
    await page.getByText("请先补齐标红的持仓字段").waitFor({ timeout: 10_000 });
    await screenshotInteraction(
      page,
      "portfolio_invalid_submit",
      "interaction-portfolio-field-errors.png",
    );
  });

  await runInteraction("portfolio_valid_submit", async () => {
    await page.getByRole("button", { name: "填入示例快照" }).click();
    await page.getByRole("button", { name: "生成组合报告" }).click();
    await page.getByText("最近报告").first().waitFor({ timeout: 20_000 }).catch(() => {});
    await screenshotInteraction(
      page,
      "portfolio_valid_submit",
      "interaction-portfolio-valid-submit.png",
    );
  });

  await runInteraction("simulation_start", async () => {
    await page.goto(`${base}/simulation`);
    await settle(page);
    const resume = page.getByRole("button", { name: "继续上次训练" }).first();
    if (await resume.isVisible().catch(() => false)) {
      await resume.click();
    } else {
      await page.getByRole("button", { name: "开始这个情境" }).first().click();
    }
    await page.getByText("选择本轮动作").first().waitFor({ timeout: 20_000 });
    await screenshotInteraction(page, "simulation_start", "interaction-simulation-started.png");
  });

  await runInteraction("simulation_submit_action", async () => {
    await page.locator("button").filter({ hasText: "可选动作" }).first().click();
    await page.locator("textarea").fill("先控制仓位，避免把短期波动当成长期判断。");
    await page.getByRole("button", { name: "提交本轮动作" }).click();
    await page.getByText("即时反馈").first().waitFor({ timeout: 20_000 }).catch(() => {});
    await screenshotInteraction(
      page,
      "simulation_submit_action",
      "interaction-simulation-resume-action.png",
    );
  });

  await runInteraction("news_analyze", async () => {
    await page.goto(`${base}/news`);
    await settle(page);
    await page.getByRole("button", { name: "生成解读" }).first().click();
    await page.getByText("刚生成的资讯解读").waitFor({ timeout: 20_000 });
    await screenshotInteraction(page, "news_analyze", "interaction-news-after-analysis.png");
  });

  await interactionContext.close();
} finally {
  await browser.close();
}

const report = {
  meta: {
    base,
    createdAt: new Date().toISOString(),
  },
  routeScreens,
  interactions,
  internalTermFailures,
  errors,
};

await writeFile(
  path.join(scriptDir, "post-optimization-live-audit.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);

console.log(
  JSON.stringify(
    {
      routeScreens: routeScreens.length,
      interactions: interactions.length,
      errors: errors.length,
      internalTermFailures: internalTermFailures.length,
    },
    null,
    2,
  ),
);

if (errors.length > 0 || internalTermFailures.length > 0) {
  process.exitCode = 1;
}
