# Instructions

- Following Playwright test failed.
- Explain why, be concise, respect Playwright best practices.
- Provide a snippet of code with the fix, if possible.

# Test info

- Name: workspaces.spec.ts >> portfolio catches duplicate fund codes before saving
- Location: e2e/workspaces.spec.ts:66:5

# Error details

```
Test timeout of 60000ms exceeded.
```

```
Error: locator.click: Test timeout of 60000ms exceeded.
Call log:
  - waiting for getByRole('button', { name: '用示例覆盖当前输入' })

```

# Page snapshot

```yaml
- generic [active] [ref=e1]:
  - button "Open Next.js Dev Tools" [ref=e7] [cursor=pointer]:
    - img [ref=e8]
  - alert [ref=e11]
  - generic [ref=e12]:
    - banner [ref=e13]:
      - link "回到 FundGene 总览" [ref=e14] [cursor=pointer]:
        - /url: /
        - text: FundGene
      - generic [ref=e15]:
        - img [ref=e16]
        - generic [ref=e19]: 主路径：建档、工作台、教练/组合/训练
      - generic [ref=e21]:
        - generic [ref=e22]: 你好，QA 用户
        - generic [ref=e23]: FG
    - generic [ref=e24]:
      - complementary [ref=e25]:
        - navigation "FundGene 工作区导航" [ref=e26]:
          - link "总览" [ref=e27] [cursor=pointer]:
            - /url: /
            - img [ref=e28]
            - generic [ref=e31]: 总览
          - link "工作台" [ref=e32] [cursor=pointer]:
            - /url: /dashboard
            - img [ref=e33]
            - generic [ref=e38]: 工作台
          - link "建档" [ref=e39] [cursor=pointer]:
            - /url: /onboarding
            - img [ref=e40]
            - generic [ref=e44]: 建档
          - link "教练" [ref=e45] [cursor=pointer]:
            - /url: /coach
            - img [ref=e46]
            - generic [ref=e49]: 教练
          - link "学习" [ref=e50] [cursor=pointer]:
            - /url: /learning
            - img [ref=e51]
            - generic [ref=e54]: 学习
          - link "组合" [ref=e55] [cursor=pointer]:
            - /url: /portfolio
            - img [ref=e56]
            - generic [ref=e60]: 组合
          - link "情境" [ref=e61] [cursor=pointer]:
            - /url: /simulation
            - img [ref=e62]
            - generic [ref=e65]: 情境
          - link "资讯" [ref=e66] [cursor=pointer]:
            - /url: /news
            - img [ref=e67]
            - generic [ref=e70]: 资讯
          - button "退出" [ref=e71] [cursor=pointer]:
            - img [ref=e72]
            - generic [ref=e75]: 退出
      - main [ref=e76]:
        - generic [ref=e77]:
          - paragraph [ref=e78]: 体检
          - heading "组合" [level=1] [ref=e79]
          - paragraph [ref=e80]: 持仓分析与报告
        - generic [ref=e82]:
          - generic [ref=e83]:
            - generic [ref=e84]:
              - generic [ref=e85]:
                - generic [ref=e86]: 组合分析
                - heading "先解释持仓结构，再讨论配置原则。" [level=2] [ref=e87]
              - paragraph [ref=e88]: 手动录入快照后，系统会生成结构化报告，并在工作台和教练中继续引用。
            - generic [ref=e89]:
              - generic [ref=e90]:
                - paragraph [ref=e91]: 已有最近报告，可以直接更新快照。
                - paragraph [ref=e92]: 总资产只按你录入的现金和持仓估算；这里不接实时行情，也不输出买卖指令。
              - generic [ref=e93]:
                - link "录入快照" [ref=e94] [cursor=pointer]:
                  - /url: "#portfolio-snapshot-form"
                - link "看最近报告" [ref=e95] [cursor=pointer]:
                  - /url: "#portfolio-latest-report"
          - generic [ref=e96]:
            - generic [ref=e98]:
              - generic [ref=e99]:
                - generic [ref=e100]:
                  - generic [ref=e101]: 手动快照
                  - heading "录入第一份或下一份持仓快照。" [level=2] [ref=e102]
                - paragraph [ref=e103]: 这里记录组合状态，不生成交易指令。
              - generic [ref=e104]:
                - generic [ref=e105]:
                  - generic [ref=e106]:
                    - text: 快照日期
                    - textbox "快照日期 这会成为一份可回看的历史快照日期。" [ref=e107]: 2026-05-17
                    - text: 这会成为一份可回看的历史快照日期。
                  - generic [ref=e108]:
                    - text: 现金金额（元）
                    - textbox "现金金额（元） 没有现金留白填 0；这里记录当前现金，不是收益率。" [ref=e109]:
                      - /placeholder: 0、6,000 或 0.6万
                      - text: "0"
                    - text: 没有现金留白填 0；这里记录当前现金，不是收益率。
                - generic [ref=e111]:
                  - generic [ref=e112]:
                    - paragraph [ref=e113]: 持仓 01
                    - generic [ref=e114]: 至少保留一行
                  - generic [ref=e115]:
                    - generic [ref=e116]:
                      - text: 基金代码
                      - textbox "基金代码" [ref=e117]:
                        - /placeholder: 例如：161725
                    - generic [ref=e118]:
                      - text: 基金名称
                      - textbox "基金名称" [ref=e119]:
                        - /placeholder: 例如：招商中证白酒指数
                    - generic [ref=e120]:
                      - text: 基金类型
                      - combobox "基金类型" [ref=e121]:
                        - option "权益基金" [selected]
                        - option "混合基金"
                        - option "债券基金"
                        - option "货币基金"
                        - option "海外 / QDII"
                        - option "商品 / 黄金"
                    - generic [ref=e122]:
                      - text: 当前市值（元）
                      - textbox "当前市值（元） 填当前市值，不是买入本金，也不是收益率。" [ref=e123]:
                        - /placeholder: 例如：12000、12,000 或 1.2万
                      - text: 填当前市值，不是买入本金，也不是收益率。
                - generic [ref=e125]:
                  - generic [ref=e126]:
                    - generic [ref=e127]:
                      - paragraph [ref=e128]: 提交前预览
                      - heading "先检查权重，再生成报告。" [level=3] [ref=e129]
                      - paragraph [ref=e130]: 这里按当前草稿估算，不接实时行情，也不是最终分析结论。
                    - generic [ref=e131]:
                      - generic [ref=e132]: 草稿总资产
                      - strong [ref=e133]: ¥0
                  - generic [ref=e134]:
                    - generic [ref=e135]:
                      - paragraph [ref=e136]: 现金比例
                      - paragraph [ref=e137]: 0%
                    - generic [ref=e138]:
                      - paragraph [ref=e139]: 持仓数量
                      - paragraph [ref=e140]: 0 只
                    - generic [ref=e141]:
                      - paragraph [ref=e142]: 最大持仓
                      - paragraph [ref=e143]: 待填写
                  - generic [ref=e145]: 先填写至少一只基金和持仓金额，这里会显示草稿权重。
                - generic [ref=e146]:
                  - button "用示例" [ref=e147] [cursor=pointer]
                  - button "添加" [ref=e148] [cursor=pointer]
                  - button "检查草稿权重" [ref=e149] [cursor=pointer]
            - generic [ref=e151]:
              - generic [ref=e152]:
                - generic [ref=e153]:
                  - generic [ref=e154]: 最近报告
                  - heading "最近一份组合体检报告" [level=2] [ref=e155]
                - paragraph [ref=e156]: 报告分为配置分布、持仓权重、风险暴露和下一步动作。
              - generic [ref=e157]:
                - generic [ref=e159]:
                  - generic [ref=e160]:
                    - generic [ref=e161]:
                      - paragraph [ref=e162]: 快照 2026/4/26
                      - heading "待继续观察" [level=3] [ref=e163]
                    - generic [ref=e164]:
                      - paragraph [ref=e165]: 报告生成
                      - paragraph [ref=e166]: 2026/4/26
                  - paragraph [ref=e167]: 这份快照总资产约为 52000 元，权益暴露适中，第一大持仓约 34%。
                - generic [ref=e169]:
                  - generic [ref=e170]:
                    - img "组合配置分布：权益基金 34.6%，债券基金 30.8%，海外 / QDII 23.1%，现金 11.5%" [ref=e171]
                    - generic:
                      - generic: 待继续观察
                  - generic [ref=e174]:
                    - generic [ref=e175]:
                      - generic [ref=e176]:
                        - paragraph [ref=e177]: 总资产
                        - paragraph [ref=e178]: ¥52,000
                      - generic [ref=e179]:
                        - paragraph [ref=e180]: 现金
                        - paragraph [ref=e181]: 11.5%
                      - generic [ref=e182]:
                        - paragraph [ref=e183]: 第一大持仓
                        - paragraph [ref=e184]: 34.6%
                    - generic [ref=e185]:
                      - generic [ref=e187]:
                        - generic [ref=e190]: 权益基金
                        - generic [ref=e191]: 34.6%
                      - generic [ref=e195]:
                        - generic [ref=e198]: 债券基金
                        - generic [ref=e199]: 30.8%
                      - generic [ref=e203]:
                        - generic [ref=e206]: 海外 / QDII
                        - generic [ref=e207]: 23.1%
                      - generic [ref=e211]:
                        - generic [ref=e214]: 现金
                        - generic [ref=e215]: 11.5%
                - generic [ref=e218]:
                  - generic [ref=e219]:
                    - paragraph [ref=e220]: 风险暴露
                    - generic [ref=e222]:
                      - generic [ref=e223]: "1"
                      - generic [ref=e224]: 组合里既有成长暴露，也保留了一定缓冲。
                  - generic [ref=e225]:
                    - paragraph [ref=e226]: 集中度提示
                    - generic [ref=e228]:
                      - generic [ref=e229]: "1"
                      - generic [ref=e230]: 第一大持仓超过 25%，建议检查单一主题风险。
                  - generic [ref=e231]:
                    - paragraph [ref=e232]: 配置平衡
                    - generic [ref=e234]:
                      - generic [ref=e235]: "1"
                      - generic [ref=e236]: 组合覆盖多个类型，但仍要检查主题是否重复。
                  - generic [ref=e237]:
                    - paragraph [ref=e238]: 下一步动作
                    - generic [ref=e240]:
                      - generic [ref=e241]: "1"
                      - generic [ref=e242]: 先检查第一大持仓是否过重。
                - generic [ref=e243]:
                  - generic [ref=e244]:
                    - generic [ref=e245]:
                      - paragraph [ref=e246]: 持仓权重
                      - heading "每一只基金在组合里承担多少波动。" [level=3] [ref=e247]
                    - generic [ref=e248]: 3 只基金
                  - generic [ref=e250]:
                    - generic [ref=e251]:
                      - generic [ref=e252]:
                        - generic [ref=e253]:
                          - paragraph [ref=e254]: 持仓 01
                          - heading "招商中证白酒指数" [level=3] [ref=e255]
                          - paragraph [ref=e256]: 161725 · 权益基金
                        - generic [ref=e257]: ¥18,000
                      - generic [ref=e261]: 34.6%
                    - generic [ref=e262]:
                      - generic [ref=e263]:
                        - generic [ref=e264]:
                          - paragraph [ref=e265]: 持仓 02
                          - heading "易方达安心债券" [level=3] [ref=e266]
                          - paragraph [ref=e267]: 110027 · 债券基金
                        - generic [ref=e268]: ¥16,000
                      - generic [ref=e272]: 30.8%
                    - generic [ref=e273]:
                      - generic [ref=e274]:
                        - generic [ref=e275]:
                          - paragraph [ref=e276]: 持仓 03
                          - heading "华夏恒生 ETF 联接" [level=3] [ref=e277]
                          - paragraph [ref=e278]: 000071 · 海外 / QDII
                        - generic [ref=e279]: ¥12,000
                      - generic [ref=e283]: 23.1%
          - generic [ref=e284]:
            - generic [ref=e287]:
              - generic [ref=e289]: 最近报告
              - generic [ref=e291]:
                - paragraph [ref=e292]: 已生成
                - paragraph [ref=e293]: 每次提交都会生成一份新的持仓快照和分析记录。
            - generic [ref=e296]:
              - generic [ref=e298]: 最近估值
              - generic [ref=e300]:
                - paragraph [ref=e301]: ¥52,000
                - paragraph [ref=e302]: 总资产按现金加持仓快照估算，不接实时行情。
            - generic [ref=e305]:
              - generic [ref=e307]: 最近快照
              - generic [ref=e309]:
                - paragraph [ref=e310]: 2026/4/26
                - paragraph [ref=e311]: 后续教练会优先参考最近一份组合报告。
            - generic [ref=e314]:
              - generic [ref=e316]: 历史记录
              - generic [ref=e318]:
                - paragraph [ref=e319]: 1 份
                - paragraph [ref=e320]: 每次录入都保留历史，不用最新状态覆盖一切。
          - generic [ref=e323]:
            - generic [ref=e324]:
              - generic [ref=e325]:
                - generic [ref=e326]: 历史记录
                - heading "历史快照" [level=2] [ref=e327]
              - paragraph [ref=e328]: 每次录入都会保留一份历史，便于对照结构变化。
            - generic [ref=e331]:
              - generic [ref=e332]:
                - paragraph [ref=e333]: 2026/4/26
                - paragraph [ref=e334]: 这份快照总资产约为 52000 元，权益暴露适中，第一大持仓约 34%。
              - generic [ref=e335]: ¥52,000
```

# Test source

```ts
  1   | import { expect, test } from "@playwright/test";
  2   | 
  3   | import { mockFundGeneApi } from "./fixtures";
  4   | 
  5   | test.describe.configure({ mode: "serial" });
  6   | test.setTimeout(60_000);
  7   | 
  8   | test.beforeEach(async ({ page }) => {
  9   |   await mockFundGeneApi(page);
  10  | });
  11  | 
  12  | test("renders the primary workspace routes with mocked API state", async ({
  13  |   page,
  14  | }) => {
  15  |   const routes = [
  16  |     { path: "/dashboard", text: "今日任务流" },
  17  |     { path: "/portfolio", text: "最近一份组合体检报告" },
  18  |     { path: "/coach", text: "对话优先" },
  19  |     { path: "/onboarding", text: "建档进度" },
  20  |     { path: "/learning", text: "学习中心" },
  21  |     { path: "/simulation", text: "历史情境训练" },
  22  |     { path: "/news", text: "资讯解读" },
  23  |   ];
  24  | 
  25  |   for (const route of routes) {
  26  |     await page.goto(route.path, { waitUntil: "domcontentloaded" });
  27  |     await expect(page.getByText(route.text).first()).toBeVisible({
  28  |       timeout: 15_000,
  29  |     });
  30  |   }
  31  | });
  32  | 
  33  | test("coach keeps internal trace details out of the user-facing answer", async ({
  34  |   page,
  35  | }) => {
  36  |   await page.goto("/coach", { waitUntil: "domcontentloaded" });
  37  | 
  38  |   await expect(page.getByText("边界提醒").first()).toBeVisible({
  39  |     timeout: 20_000,
  40  |   });
  41  |   await expect(page.getByRole("link", { name: /进入学习中心/ })).toHaveAttribute(
  42  |     "href",
  43  |     "/learning",
  44  |   );
  45  |   await expect(page.getByText("下一句可以问").first()).toBeVisible();
  46  |   await expect(page.getByText("我想用一个数字例子理解回撤。")).toBeVisible();
  47  |   await expect(page.getByText("证据链路")).toHaveCount(0);
  48  |   await expect(page.getByText("Run ID: run_e2e")).toHaveCount(0);
  49  |   await expect(page.getByText("learning_knowledge_lookup")).toHaveCount(0);
  50  | });
  51  | 
  52  | test("dashboard command center shows simulation and news status", async ({
  53  |   page,
  54  | }) => {
  55  |   await page.goto("/dashboard", { waitUntil: "domcontentloaded" });
  56  | 
  57  |   await expect(page.getByRole("heading", { name: "今日任务流" })).toBeVisible({
  58  |     timeout: 15_000,
  59  |   });
  60  |   await expect(page.getByText("回撤纪律训练").first()).toBeVisible();
  61  |   await expect(page.getByText("最近复盘显示你能先检查计划").first()).toBeVisible();
  62  |   await expect(page.getByText("长期资金入市政策继续推进").first()).toBeVisible();
  63  |   await expect(page.getByText("这条政策更像长期市场结构信号").first()).toBeVisible();
  64  | });
  65  | 
  66  | test("portfolio catches duplicate fund codes before saving", async ({ page }) => {
  67  |   await page.goto("/portfolio", { waitUntil: "domcontentloaded" });
  68  | 
> 69  |   await page.getByRole("button", { name: "用示例覆盖当前输入" }).click();
      |                                                         ^ Error: locator.click: Test timeout of 60000ms exceeded.
  70  |   await page.getByRole("button", { name: "添加一行持仓" }).click();
  71  |   await page.locator('input[name="holdings.3.fundCode"]').fill("161725");
  72  |   await page.locator('input[name="holdings.3.fundName"]').fill("重复白酒基金");
  73  |   await page.locator('input[name="holdings.3.marketValue"]').fill("5000");
  74  |   await page.getByRole("button", { name: "检查草稿权重" }).click();
  75  | 
  76  |   await expect(page.getByText("基金代码 161725 已重复").first()).toBeVisible();
  77  |   await expect(page.getByText("请先合并或删除重复基金代码").first()).toBeVisible();
  78  | });
  79  | 
  80  | test("portfolio reviews a draft before saving a snapshot", async ({ page }) => {
  81  |   await page.goto("/portfolio", { waitUntil: "domcontentloaded" });
  82  | 
  83  |   await page.getByRole("button", { name: "用示例覆盖当前输入" }).click();
  84  |   await expect(page.getByText("草稿总资产").first()).toBeVisible();
  85  |   await expect(page.getByText("¥52,000").first()).toBeVisible();
  86  | 
  87  |   await page.getByRole("button", { name: "检查草稿权重" }).click();
  88  |   await expect(page.getByText("草稿已检查").first()).toBeVisible();
  89  | 
  90  |   await page.getByRole("button", { name: "确认并保存快照" }).click();
  91  |   await expect(page.getByText("已生成 2026/4/26 的组合报告").first()).toBeVisible();
  92  | });
  93  | 
  94  | test("news workspace can request a structured analysis", async ({ page }) => {
  95  |   await page.goto("/news", { waitUntil: "domcontentloaded" });
  96  | 
  97  |   await page.getByRole("button", { name: "生成解读" }).first().click();
  98  | 
  99  |   await expect(page.getByText("刚生成的资讯解读")).toBeVisible();
  100 |   await expect(page.getByText("政策信息强调长期资金入市")).toBeVisible();
  101 |   await expect(page.getByText("资讯解读不构成买卖建议")).toBeVisible();
  102 | });
  103 | 
```