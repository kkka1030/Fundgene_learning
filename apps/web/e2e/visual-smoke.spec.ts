import { expect, test } from "@playwright/test";

import { mockFundGeneApi } from "./fixtures";

test.describe.configure({ mode: "serial" });
test.setTimeout(60_000);

const visualRoutes = [
  { name: "overview", path: "/", marker: "FundGene Workspace" },
  {
    name: "start",
    path: "/start",
    marker: "开始建档",
    authenticated: false,
  },
  { name: "today", path: "/today", marker: "今日简报" },
  { name: "agent", path: "/agent", marker: "Agent 工作区" },
  { name: "automations", path: "/automations", marker: "自动任务" },
  { name: "profile", path: "/profile", marker: "上下文完整度" },
  { name: "onboarding", path: "/onboarding", marker: "开始建档" },
  { name: "learning", path: "/learning", marker: "今日训练任务" },
  {
    name: "course-detail",
    path: "/learning/risk-basics",
    marker: "每一节都要服务一个可执行判断动作。",
  },
  { name: "portfolio", path: "/portfolio", marker: "组合画像结论" },
  { name: "simulation", path: "/simulation", marker: "训练任务单" },
  { name: "news", path: "/news", marker: "影响路径图" },
];

for (const route of visualRoutes) {
  test(`${route.name} visual smoke`, async ({ page }, testInfo) => {
    await mockFundGeneApi(page, {
      authenticated: route.authenticated !== false,
    });

    for (const viewport of [
      { label: "desktop", width: 1440, height: 980 },
      { label: "mobile", width: 390, height: 900 },
    ]) {
      await page.setViewportSize(viewport);
      await page.goto(route.path, { waitUntil: "domcontentloaded" });
      await expect(page.getByText(route.marker).first()).toBeVisible({
        timeout: 15_000,
      });

      const hasHorizontalOverflow = await page.evaluate(
        () => document.documentElement.scrollWidth > window.innerWidth + 1,
      );
      expect(hasHorizontalOverflow).toBe(false);

      if (
        viewport.label === "desktop" &&
        (route.path === "/" ||
          route.path === "/today" ||
          route.path === "/agent" ||
          route.path === "/learning" ||
          route.path === "/news" ||
          route.path === "/simulation")
      ) {
        const hasPageVerticalOverflow = await page.evaluate(
          () => document.documentElement.scrollHeight > window.innerHeight + 1,
        );
        expect(hasPageVerticalOverflow).toBe(false);
      }

      await page.screenshot({
        fullPage: true,
        path: testInfo.outputPath(`${route.name}-${viewport.label}.png`),
      });
    }
  });
}
