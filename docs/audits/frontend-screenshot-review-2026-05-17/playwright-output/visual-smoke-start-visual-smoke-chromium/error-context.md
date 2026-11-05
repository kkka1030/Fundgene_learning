# Instructions

- Following Playwright test failed.
- Explain why, be concise, respect Playwright best practices.
- Provide a snippet of code with the fix, if possible.

# Test info

- Name: visual-smoke.spec.ts >> start visual smoke
- Location: e2e/visual-smoke.spec.ts:30:7

# Error details

```
Error: expect(locator).toBeVisible() failed

Locator: getByText('账号入口').first()
Expected: visible
Timeout: 15000ms
Error: element(s) not found

Call log:
  - Expect "toBeVisible" with timeout 15000ms
  - waiting for getByText('账号入口').first()

```

# Page snapshot

```yaml
- generic [active] [ref=e1]:
  - generic [ref=e4]: 正在加载认证入口...
  - alert [ref=e5]
```

# Test source

```ts
  1  | import { expect, test } from "@playwright/test";
  2  | 
  3  | import { mockFundGeneApi } from "./fixtures";
  4  | 
  5  | test.describe.configure({ mode: "serial" });
  6  | 
  7  | const visualRoutes = [
  8  |   { name: "overview", path: "/", marker: "核心模块矩阵" },
  9  |   {
  10 |     name: "start",
  11 |     path: "/start",
  12 |     marker: "账号入口",
  13 |     authenticated: false,
  14 |   },
  15 |   { name: "dashboard", path: "/dashboard", marker: "今日任务流" },
  16 |   { name: "onboarding", path: "/onboarding", marker: "建档进度" },
  17 |   { name: "coach", path: "/coach", marker: "对话优先" },
  18 |   { name: "learning", path: "/learning", marker: "三门基础课先撑住新手的决策语言" },
  19 |   {
  20 |     name: "course-detail",
  21 |     path: "/learning/risk-basics",
  22 |     marker: "每一节都要能读、能查、能带去提问。",
  23 |   },
  24 |   { name: "portfolio", path: "/portfolio", marker: "最近一份组合体检报告" },
  25 |   { name: "simulation", path: "/simulation", marker: "情境档案" },
  26 |   { name: "news", path: "/news", marker: "选择一条资讯，生成结构化解读" },
  27 | ];
  28 | 
  29 | for (const route of visualRoutes) {
  30 |   test(`${route.name} visual smoke`, async ({ page }, testInfo) => {
  31 |     await mockFundGeneApi(page, {
  32 |       authenticated: route.authenticated !== false,
  33 |     });
  34 | 
  35 |     for (const viewport of [
  36 |       { label: "desktop", width: 1440, height: 980 },
  37 |       { label: "mobile", width: 390, height: 900 },
  38 |     ]) {
  39 |       await page.setViewportSize(viewport);
  40 |       await page.goto(route.path, { waitUntil: "domcontentloaded" });
> 41 |       await expect(page.getByText(route.marker).first()).toBeVisible({
     |                                                          ^ Error: expect(locator).toBeVisible() failed
  42 |         timeout: 15_000,
  43 |       });
  44 | 
  45 |       const hasHorizontalOverflow = await page.evaluate(
  46 |         () => document.documentElement.scrollWidth > window.innerWidth + 1,
  47 |       );
  48 |       expect(hasHorizontalOverflow).toBe(false);
  49 | 
  50 |       await page.screenshot({
  51 |         fullPage: true,
  52 |         path: testInfo.outputPath(`${route.name}-${viewport.label}.png`),
  53 |       });
  54 |     }
  55 |   });
  56 | }
  57 | 
```