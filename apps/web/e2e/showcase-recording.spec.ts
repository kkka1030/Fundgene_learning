import { expect, test, type Locator, type Page } from "@playwright/test";

import { mockFundGeneApi } from "./fixtures";

test.describe.configure({ mode: "serial" });
test.setTimeout(120_000);
test.use({
  viewport: { width: 1440, height: 980 },
  video: { mode: "on", size: { width: 1440, height: 980 } },
  launchOptions: { slowMo: 80 },
});

function withShowcase(path: string) {
  const joiner = path.includes("?") ? "&" : "?";
  return `${path}${joiner}showcase=1`;
}

async function installShowcaseCursor(page: Page) {
  await page.addInitScript(() => {
    function installCursor() {
      if ((window as Window & { __fundgeneShowcaseCursor?: unknown })
        .__fundgeneShowcaseCursor) {
        return;
      }

      const style = document.createElement("style");
      style.textContent = `
        .fundgene-showcase-cursor {
          position: fixed;
          left: 0;
          top: 0;
          z-index: 2147483647;
          width: 26px;
          height: 33px;
          pointer-events: none;
          opacity: 0;
          transform: translate3d(720px, 490px, 0);
          transition: opacity 260ms ease;
          will-change: transform, opacity;
        }

        .fundgene-showcase-cursor svg {
          display: block;
          width: 100%;
          height: 100%;
          filter: drop-shadow(0 8px 15px rgba(15, 23, 42, 0.18));
          transform-origin: 6px 4px;
          transition: transform 120ms ease;
        }

        .fundgene-showcase-cursor.is-clicking svg {
          transform: scale(0.94);
          filter: drop-shadow(0 8px 16px rgba(0, 113, 227, 0.24));
        }

        .fundgene-showcase-ripple {
          position: fixed;
          z-index: 2147483646;
          width: 14px;
          height: 14px;
          border: 1.5px solid rgba(0, 113, 227, 0.34);
          border-radius: 999px;
          pointer-events: none;
          transform: translate(-50%, -50%) scale(0.6);
          animation: fundgene-showcase-ripple 520ms ease-out forwards;
          box-shadow: 0 0 22px rgba(0, 113, 227, 0.14);
        }

        @keyframes fundgene-showcase-ripple {
          from {
            opacity: 0.9;
            transform: translate(-50%, -50%) scale(0.55);
          }
          to {
            opacity: 0;
            transform: translate(-50%, -50%) scale(2.4);
          }
        }
      `;
      document.head.appendChild(style);

      const cursor = document.createElement("div");
      cursor.className = "fundgene-showcase-cursor";
      cursor.innerHTML = `
        <svg viewBox="0 0 32 40" aria-hidden="true">
          <path
            d="M6 4L6 31.5L13.2 24.1L18.6 35.6L24.3 32.9L18.9 21.6H29.2L6 4Z"
            fill="rgba(255,255,255,0.92)"
            stroke="rgba(0,113,227,0.70)"
            stroke-width="1.8"
            stroke-linejoin="round"
          />
        </svg>
      `;
      document.body.appendChild(cursor);

      let cursorX = 720;
      let cursorY = 490;
      let animationFrame = 0;
      let fadeTimer = 0;

      function scheduleFade() {
        window.clearTimeout(fadeTimer);
        fadeTimer = window.setTimeout(() => {
          cursor.style.opacity = "0.16";
        }, 760);
      }

      function move(x: number, y: number) {
        cursorX = x;
        cursorY = y;
        cursor.style.opacity = "0.72";
        cursor.style.transform = `translate3d(${x - 6}px, ${y - 4}px, 0)`;
        scheduleFade();
      }

      function animateTo(x: number, y: number, durationMs = 420) {
        window.cancelAnimationFrame(animationFrame);
        const startX = cursorX;
        const startY = cursorY;
        const startedAt = window.performance.now();
        const ease = (t: number) => 1 - Math.pow(1 - t, 3);

        return new Promise<void>((resolve) => {
          function frame(now: number) {
            const progress = Math.min(1, (now - startedAt) / durationMs);
            const eased = ease(progress);
            move(startX + (x - startX) * eased, startY + (y - startY) * eased);
            if (progress < 1) {
              animationFrame = window.requestAnimationFrame(frame);
            } else {
              resolve();
            }
          }

          animationFrame = window.requestAnimationFrame(frame);
        });
      }

      function click(x: number, y: number) {
        move(x, y);
        cursor.style.opacity = "0.88";
        cursor.classList.add("is-clicking");
        window.setTimeout(() => cursor.classList.remove("is-clicking"), 180);
        scheduleFade();

        const ripple = document.createElement("span");
        ripple.className = "fundgene-showcase-ripple";
        ripple.style.left = `${x}px`;
        ripple.style.top = `${y}px`;
        document.body.appendChild(ripple);
        window.setTimeout(() => ripple.remove(), 700);
      }

      document.addEventListener(
        "mousemove",
        (event) => move(event.clientX, event.clientY),
        { passive: true },
      );
      document.addEventListener(
        "mousedown",
        (event) => click(event.clientX, event.clientY),
        true,
      );

      (window as Window & {
        __fundgeneShowcaseCursor?: {
          move: (x: number, y: number) => void;
          animateTo: (x: number, y: number, durationMs?: number) => Promise<void>;
          click: (x: number, y: number) => void;
        };
      }).__fundgeneShowcaseCursor = { move, animateTo, click };
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", installCursor, { once: true });
    } else {
      installCursor();
    }
  });
}

async function waitForWorkspaceReady(page: Page) {
  await page.waitForFunction(
    () => {
      const text = document.body.innerText;
      const blockingLoadingCopy = [
        "正在验证登录会话",
        "正在读取当前账号状态",
        "正在初始化教练会话上下文",
        "正在同步教练工作区",
      ];
      return blockingLoadingCopy.every((copy) => !text.includes(copy));
    },
    undefined,
    { timeout: 15_000 },
  );
}

async function settle(page: Page, ms = 900) {
  await page.waitForTimeout(ms);
}

async function showcaseMove(
  page: Page,
  x: number,
  y: number,
  options: { durationMs?: number; settleMs?: number; steps?: number } = {},
) {
  await page.evaluate(
    async ({ targetX, targetY, durationMs }) => {
      await (window as Window & {
        __fundgeneShowcaseCursor?: {
          animateTo: (x: number, y: number, durationMs?: number) => Promise<void>;
        };
      }).__fundgeneShowcaseCursor?.animateTo(targetX, targetY, durationMs);
    },
    {
      targetX: x,
      targetY: y,
      durationMs: options.durationMs ?? 360,
    },
  );
  await page.mouse.move(x, y);
  await settle(page, options.settleMs ?? 120);
}

async function locatorCenter(locator: Locator) {
  await expect(locator).toBeVisible();
  const box = await locator.boundingBox();
  if (!box) {
    throw new Error("Cannot locate showcase target on the current page.");
  }
  return {
    x: box.x + box.width / 2,
    y: box.y + box.height / 2,
  };
}

async function showcaseHover(
  page: Page,
  locator: Locator,
  options: { durationMs?: number; settleMs?: number; steps?: number } = {},
) {
  const point = await locatorCenter(locator);
  await showcaseMove(page, point.x, point.y, options);
}

async function showcaseClick(
  page: Page,
  locator: Locator,
  options: { durationMs?: number; settleMs?: number; steps?: number } = {},
) {
  await expect(locator).toBeEnabled({ timeout: 15_000 });
  const point = await locatorCenter(locator);
  await showcaseMove(page, point.x, point.y, {
    durationMs: options.durationMs ?? 320,
    settleMs: 80,
  });
  await locator.click({ delay: 90 });
  await settle(page, options.settleMs ?? 220);
}

async function prepare(page: Page, path: string) {
  await installShowcaseCursor(page);
  await mockFundGeneApi(page);
  await page.goto(withShowcase(path), { waitUntil: "domcontentloaded" });
  await page.evaluate(async () => {
    if ("fonts" in document) {
      await Promise.race([
        document.fonts.ready,
        new Promise((resolve) => window.setTimeout(resolve, 1500)),
      ]);
    }
  });
  await waitForWorkspaceReady(page);
  await page.waitForFunction(
    () =>
      Boolean(
        (window as Window & { __fundgeneShowcaseCursor?: unknown })
          .__fundgeneShowcaseCursor,
      ),
  );
  await settle(page);
}

test("01-hero-command-center", async ({ page }) => {
  await prepare(page, "/");
  await expect(page.getByText("FundGene Workspace").first()).toBeVisible();
  await showcaseHover(page, page.getByRole("link", { name: /看今日判断/ }).first(), {
    durationMs: 360,
    settleMs: 520,
  });
  await showcaseClick(page, page.getByRole("link", { name: /查看今日简报/ }).first(), {
    durationMs: 320,
    settleMs: 900,
  });
});

test("02-today-daily-brief", async ({ page }) => {
  await prepare(page, "/today");
  await expect(page.getByText("今日简报").first()).toBeVisible();
  await showcaseMove(page, 1060, 390, { durationMs: 420, settleMs: 1400 });
});

test("03-agent-workspace", async ({ page }) => {
  await prepare(page, "/agent?new=1");
  await expect(page.getByText("Agent 工作区").first()).toBeVisible();
  const composer = page.locator("textarea");
  await expect(page.getByText("今天想先问清楚什么？")).toBeVisible();
  await expect(composer).toHaveValue("");
  await showcaseClick(page, composer, { durationMs: 320, settleMs: 160 });
  await page.keyboard.type("今天先检查组合集中度，我具体应该先看哪几项？", {
    delay: 28,
  });
  await settle(page, 520);
  const sendButton = page.getByRole("button", { name: /^确认发送$|^发送$/ });
  await sendButton.scrollIntoViewIfNeeded();
  await showcaseClick(page, sendButton);
  await settle(page, 320);
  if ((await composer.inputValue()).trim().length > 0) {
    await sendButton.click();
  }
  await expect(page.getByText("今天先检查组合集中度").first()).toBeVisible({
    timeout: 10_000,
  });
  await expect(page.getByText("第一大持仓约 34%").first()).toBeVisible({
    timeout: 15_000,
  });
  await settle(page, 1450);

  await showcaseClick(page, composer, { durationMs: 300, settleMs: 160 });
  await page.keyboard.type(
    "那我下一步应该学什么，能帮我把检查顺序写成清单吗？",
    { delay: 24 },
  );
  await settle(page, 420);
  await showcaseClick(page, sendButton, { durationMs: 300, settleMs: 520 });
  await expect(page.getByText("那我下一步应该学什么").first()).toBeVisible({
    timeout: 10_000,
  });
  await expect(page.getByText("学习和检查清单").first()).toBeVisible({
    timeout: 15_000,
  });
  await page.mouse.wheel(0, 620);
  await settle(page, 5200);
});

test("04-news-impact", async ({ page }) => {
  await prepare(page, "/news");
  await expect(page.getByText("影响路径图").first()).toBeVisible();
  await showcaseClick(page, page.getByRole("button", { name: /查看资讯/ }).first(), {
    settleMs: 500,
  });
  await settle(page, 600);
  await showcaseClick(
    page,
    page.getByRole("button", { name: /生成解读|重新解读/ }).first(),
  );
  await expect(page.getByTestId("news-analysis-process")).toBeVisible({
    timeout: 15_000,
  });
  await settle(page, 1800);
  await showcaseClick(
    page,
    page.getByRole("link", { name: /让 Agent 结合我的组合解释/ }),
    { durationMs: 260, settleMs: 520 },
  );
  await expect(page).toHaveURL(/\/agent/);
  await expect(page.locator("textarea")).toHaveValue(/央行：下一阶段将坚持支持性的货币政策立场/);
  const newsPrompt = await page.locator("textarea").inputValue();
  await expect(page.getByText("今天想先问清楚什么？")).toBeVisible();
  await showcaseClick(page, page.getByRole("button", { name: /^确认发送$|^发送$/ }), {
    durationMs: 320,
    settleMs: 620,
  });
  await expect(page.getByText(newsPrompt).first()).toBeVisible({ timeout: 15_000 });
  await expect(page.getByText("结合你的画像").first()).toBeVisible({
    timeout: 15_000,
  });
  await settle(page, 5600);
});

test("05-simulation-training", async ({ page }) => {
  await prepare(page, "/simulation");
  await expect(page.getByText("训练任务单").first()).toBeVisible();
  await showcaseClick(page, page.getByLabel("持有").first(), {
    durationMs: 320,
    settleMs: 320,
  });
  await showcaseClick(page, page.locator("textarea").first(), {
    durationMs: 260,
    settleMs: 120,
  });
  await page.keyboard.type("我先不被短期下跌带着走，先核对持仓比例和原计划。", {
    delay: 16,
  });
  await page.locator("select").selectOption("担心继续下跌");
  await settle(page, 360);
  await showcaseClick(page, page.getByRole("button", { name: /提交决策并继续/ }), {
    durationMs: 300,
    settleMs: 760,
  });
  await expect(page.getByTestId("simulation-review-room")).toBeVisible({
    timeout: 15_000,
  });
  await settle(page, 1700);
});

test("06-automation-profile", async ({ page }) => {
  await prepare(page, "/automations");
  await expect(page.getByText("自动任务").first()).toBeVisible();
  await showcaseClick(page, page.getByTestId("automation-toggle-news_watch"), {
    durationMs: 340,
    settleMs: 700,
  });
  await showcaseClick(page, page.getByTestId("automation-run-button-news_watch"), {
    durationMs: 320,
    settleMs: 700,
  });
  await expect(page.getByTestId("automation-run-result-news_watch")).toBeVisible({
    timeout: 15_000,
  });
  await expect(page.getByText("央行支持性货币政策信息值得观察").first()).toBeVisible();
  await page.getByTestId("automation-run-result-news_watch").scrollIntoViewIfNeeded();
  await settle(page, 1300);

  await page.goto(withShowcase("/profile"), { waitUntil: "domcontentloaded" });
  await expect(page.getByText("待确认资料").first()).toBeVisible();
  await waitForWorkspaceReady(page);
  await expect(page.getByText("央行政策新闻的组合影响观察").first()).toBeVisible({
    timeout: 15_000,
  });
  await settle(page, 1400);
});

test("07-agent-first-core-loop", async ({ page }) => {
  await prepare(page, "/agent?new=1");
  await expect(page.getByText("Agent 工作区").first()).toBeVisible();
  const composer = page.locator("textarea");
  const sendButton = page.getByRole("button", { name: /^确认发送$|^发送$/ });

  await expect(page.getByText("今天想先问清楚什么？")).toBeVisible();
  await expect(composer).toHaveValue("");
  await showcaseClick(page, composer, { durationMs: 320, settleMs: 160 });
  await page.keyboard.type(
    "今天我应该先处理什么？如果有新闻影响，也帮我结合组合解释。",
    { delay: 24 },
  );
  await settle(page, 520);
  await showcaseClick(page, sendButton, { durationMs: 320, settleMs: 760 });
  await expect(page.getByText("我建议今天先处理一件事").first()).toBeVisible({
    timeout: 15_000,
  });
  await settle(page, 3600);

  await showcaseClick(page, composer, { durationMs: 280, settleMs: 160 });
  await page.keyboard.type("为什么这和我的组合有关？", { delay: 26 });
  await settle(page, 420);
  await showcaseClick(page, sendButton, { durationMs: 300, settleMs: 760 });
  await expect(page.getByText("第一大持仓约 34%").first()).toBeVisible({
    timeout: 15_000,
  });
  await settle(page, 4200);

  await showcaseClick(page, composer, { durationMs: 280, settleMs: 160 });
  await page.keyboard.type("那我下一步应该做训练还是改组合？", { delay: 26 });
  await settle(page, 420);
  await showcaseClick(page, sendButton, { durationMs: 300, settleMs: 760 });
  await expect(page.getByText("下一步更适合先做训练").first()).toBeVisible({
    timeout: 15_000,
  });
  await page.mouse.wheel(0, 560);
  await settle(page, 5200);
});

test("08-agent-input-to-portfolio-analysis", async ({ page }) => {
  await prepare(page, "/agent?new=1");
  await expect(page.getByText("Agent 工作区").first()).toBeVisible();

  const composer = page.locator("textarea");
  await expect(page.getByText("今天想先问清楚什么？")).toBeVisible();
  await showcaseClick(page, composer, { durationMs: 320, settleMs: 160 });
  await page.keyboard.type("帮我检查这份组合里最需要关注的风险来源。", {
    delay: 28,
  });
  await settle(page, 520);

  const sendButton = page.getByRole("button", { name: /^确认发送$|^发送$/ });
  await showcaseClick(page, sendButton, { durationMs: 320, settleMs: 520 });
  await expect(page.getByText("查看持仓分析").first()).toBeVisible({
    timeout: 15_000,
  });
  await settle(page, 1050);

  const portfolioAction = page.getByRole("link", { name: /查看持仓分析/ }).first();
  await showcaseClick(page, portfolioAction, { durationMs: 340, settleMs: 900 });
  await expect(page).toHaveURL(/\/portfolio\?from=agent&focus=concentration/);
  await expect(page.getByText("来自 Agent 的持仓分析")).toBeVisible({
    timeout: 15_000,
  });
  await expect(page.getByText("资产分布", { exact: true })).toBeVisible();
  await expect(page.getByText("持仓权重", { exact: true })).toBeVisible();
  await showcaseHover(page, page.getByText("第一大持仓").first(), {
    durationMs: 420,
    settleMs: 3000,
  });
});
