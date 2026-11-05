import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import { mockFundGeneApi } from "./fixtures";

const a11yRoutes = [
  "/",
  "/today",
  "/agent",
  "/automations",
  "/profile",
  "/portfolio",
  "/news",
];

test.describe.configure({ mode: "serial" });
test.setTimeout(60_000);

test.beforeEach(async ({ page }) => {
  await mockFundGeneApi(page);
});

for (const path of a11yRoutes) {
  test(`has no critical axe violations on ${path}`, async ({ page }) => {
    await page.goto(path);
    await page.addStyleTag({
      content: `
        *, *::before, *::after {
          animation: none !important;
          transition: none !important;
          scroll-behavior: auto !important;
        }
      `,
    });

    const results = await new AxeBuilder({ page }).analyze();
    const criticalViolations = results.violations.filter((violation) =>
      ["critical", "serious"].includes(violation.impact ?? ""),
    );

    expect(criticalViolations).toEqual([]);
  });
}
