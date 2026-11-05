import { expect, test } from "@playwright/test";

function authJson(status: number, body: unknown) {
  return {
    status,
    contentType: "application/json",
    body: JSON.stringify(body),
  };
}

test("start page keeps account entry usable while session check retries", async ({
  page,
}) => {
  let sessionRequests = 0;

  await page.route("**/api/auth/session", async (route) => {
    sessionRequests += 1;

    if (sessionRequests === 1) {
      return route.fulfill(
        authJson(408, {
          detail: "服务正在启动，通常需要 20-60 秒。请稍等一下，系统会继续重试。",
        }),
      );
    }

    return route.fulfill(
      authJson(401, { detail: "Authentication required." }),
    );
  });

  await page.goto("/start", { waitUntil: "domcontentloaded" });

  await expect(
    page.getByRole("heading", { name: "开始建档", exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("button", { name: /注册并开始建档/ })).toBeVisible();
  await expect
    .poll(() => sessionRequests, { timeout: 10_000 })
    .toBeGreaterThanOrEqual(2);
  await expect(page.getByText("当前认证服务不可用")).toHaveCount(0);
  await expect(page.getByText("页面不会假装生成结果")).toHaveCount(0);
});
