# Instructions

- Following Playwright test failed.
- Explain why, be concise, respect Playwright best practices.
- Provide a snippet of code with the fix, if possible.

# Test info

- Name: visual-smoke.spec.ts >> simulation visual smoke
- Location: e2e/visual-smoke.spec.ts:30:7

# Error details

```
Error: expect(locator).toBeVisible() failed

Locator:  getByText('情境档案').first()
Expected: visible
Received: hidden
Timeout:  15000ms

Call log:
  - Expect "toBeVisible" with timeout 15000ms
  - waiting for getByText('情境档案').first()
    16 × locator resolved to <p class="section-kicker">情境档案</p>
       - unexpected value "hidden"

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
          - paragraph [ref=e78]: 训练
          - heading "情境" [level=1] [ref=e79]
          - paragraph [ref=e80]: 历史训练与复盘
        - generic [ref=e82]:
          - generic [ref=e84]:
            - generic [ref=e85]:
              - generic [ref=e86]:
                - paragraph [ref=e87]: 历史情境训练
                - heading "把“市场一震就想动手”的瞬间，变成一份可追溯的决策日志。" [level=1] [ref=e88]
                - paragraph [ref=e89]: 挑选历史场景，在关键节点写下动作和理由，再用结构化复盘看清自己的纪律与偏差。 这里不模拟交易执行，只训练判断过程。
              - generic [ref=e90]:
                - generic [ref=e91]: 风险等级：平衡
                - generic [ref=e92]: 当前偏差焦点：追热点倾向
                - generic [ref=e93]: 可选场景 1 个
              - link "选择场景" [ref=e95] [cursor=pointer]:
                - /url: "#simulation-scenario-list"
            - generic [ref=e96]:
              - generic [ref=e98]:
                - generic [ref=e99]: "01"
                - generic [ref=e100]:
                  - paragraph [ref=e101]: 挑选场景
                  - paragraph [ref=e102]: 先理解背景，不让训练变成无上下文点按钮。
              - generic [ref=e104]:
                - generic [ref=e105]: "02"
                - generic [ref=e106]:
                  - paragraph [ref=e107]: 写下理由
                  - paragraph [ref=e108]: 动作之前先写判断依据，留下以后能复盘的证据。
              - generic [ref=e110]:
                - generic [ref=e111]: "03"
                - generic [ref=e112]:
                  - paragraph [ref=e113]: 接收反馈
                  - paragraph [ref=e114]: 每一步读取反馈，不把行为偏差藏在结果后面。
              - generic [ref=e116]:
                - generic [ref=e117]: "04"
                - generic [ref=e118]:
                  - paragraph [ref=e119]: 生成复盘
                  - paragraph [ref=e120]: 最后回看强项、盲点和下周训练动作。
          - generic [ref=e121]:
            - generic [ref=e123]:
              - paragraph [ref=e124]: 行为焦点
              - paragraph [ref=e125]: 追热点倾向
              - paragraph [ref=e126]: 训练优先围绕你最容易被情绪推着走的那一类场景展开。
            - generic [ref=e128]:
              - paragraph [ref=e129]: 推荐场景
              - paragraph [ref=e130]: 回撤纪律训练
              - paragraph [ref=e131]: 当前会优先高亮推荐场景，但仍允许你主动切换。
            - generic [ref=e133]:
              - paragraph [ref=e134]: 训练状态
              - paragraph [ref=e135]: 未开始
              - paragraph [ref=e136]: 启动第一个情境后，这里会显示训练状态。
            - generic [ref=e138]:
              - paragraph [ref=e139]: 训练链路
              - paragraph [ref=e140]: 4 步
              - paragraph [ref=e141]: 选场景、启动训练、提交动作、生成复盘。
          - generic [ref=e142]:
            - generic [ref=e143]:
              - generic [ref=e144]:
                - generic [ref=e145]:
                  - generic [ref=e146]: 场景目录
                  - heading "先从场景池里挑一个值得练的历史节点。" [level=2] [ref=e147]
                - paragraph [ref=e148]: 左侧是可用情境；选择后，右侧会展开背景、训练目标和时间线预览。
              - button "震荡回撤 推荐 回撤纪律训练 初级 在连续下跌中练习先复盘再行动。 回撤 纪律 恐慌卖出 预计时长 12 分钟 决策节点 2 起始背景 2018" [pressed] [ref=e150] [cursor=pointer]:
                - generic [ref=e151]:
                  - generic [ref=e152]:
                    - generic [ref=e153]:
                      - generic [ref=e154]: 震荡回撤
                      - generic [ref=e155]: 推荐
                    - heading "回撤纪律训练" [level=2] [ref=e156]
                  - generic [ref=e157]: 初级
                - paragraph [ref=e158]: 在连续下跌中练习先复盘再行动。
                - generic [ref=e159]:
                  - generic [ref=e160]: 回撤
                  - generic [ref=e161]: 纪律
                  - generic [ref=e162]: 恐慌卖出
                - generic [ref=e163]:
                  - generic [ref=e164]:
                    - text: 预计时长
                    - paragraph [ref=e165]: 12 分钟
                  - generic [ref=e166]:
                    - text: 决策节点
                    - paragraph [ref=e167]: "2"
                  - generic [ref=e168]:
                    - text: 起始背景
                    - paragraph [ref=e169]: "2018"
            - generic [ref=e170]:
              - generic [ref=e171]:
                - generic [ref=e172]:
                  - generic [ref=e173]: 情境档案
                  - heading "当市场连续下跌时，先稳住判断顺序。" [level=2] [ref=e174]
                - paragraph [ref=e175]: 这个场景训练回撤中的行动纪律。
              - generic [ref=e176]:
                - generic [ref=e177]:
                  - generic [ref=e178]:
                    - generic [ref=e179]:
                      - paragraph [ref=e180]: 震荡回撤
                      - heading "回撤纪律训练" [level=2] [ref=e181]
                    - generic [ref=e182]: 初级
                  - paragraph [ref=e183]: 市场连续调整，热门主题基金明显回撤。
                  - generic [ref=e184]:
                    - generic [ref=e185]:
                      - paragraph [ref=e186]: 训练目标
                      - paragraph [ref=e187]: 记录你是否能先检查计划而不是马上卖出。
                    - generic [ref=e188]:
                      - paragraph [ref=e189]: 行为焦点
                      - generic [ref=e191]: 恐慌卖出
                - generic [ref=e192]:
                  - paragraph [ref=e193]: 时间线预览
                  - generic [ref=e194]:
                    - generic [ref=e195]:
                      - generic [ref=e197]: "1"
                      - generic [ref=e199]: 第一轮下跌
                    - generic [ref=e200]:
                      - generic [ref=e202]: "2"
                      - generic [ref=e203]: 反弹诱惑
                - generic [ref=e204]:
                  - button "开始这个情境" [ref=e205] [cursor=pointer]
                  - link "先去教练预热" [ref=e206] [cursor=pointer]:
                    - /url: /coach
          - generic [ref=e207]:
            - generic [ref=e208]:
              - generic [ref=e209]:
                - generic [ref=e210]: 本轮判断
                - heading "启动一个情境后，这里会进入训练会话。" [level=2] [ref=e211]
              - paragraph [ref=e212]: 先从上方卡片选择一个场景并启动训练，再进入关键节点判断。
            - generic [ref=e213]:
              - generic [ref=e215]: 还没有启动中的训练。先从上面的情境档案中点“开始这个情境”，这里才会展开真正的决策节点。
              - generic [ref=e216]:
                - generic [ref=e217]:
                  - generic [ref=e219]:
                    - paragraph [ref=e220]: 训练看板
                    - heading "等待开始" [level=2] [ref=e221]
                  - progressbar "训练进度" [ref=e222]
                  - generic [ref=e224]: 进度 0%
                  - paragraph [ref=e225]: 会话启动后，这里会显示摘要与阶段说明。
                - generic [ref=e226]:
                  - paragraph [ref=e227]: 即时反馈
                  - paragraph [ref=e228]: 你每提交一次动作，这里都会渲染本轮反馈，而不是只在结尾告诉你结果。
                - generic [ref=e229]:
                  - paragraph [ref=e230]: 训练结束后
                  - generic [ref=e231]:
                    - generic [ref=e232]: 1. 读取最终复盘，确认这次训练暴露了哪些行为偏差。
                    - generic [ref=e233]: 2. 回到工作台，看下一步训练动作是否被刷新。
                    - generic [ref=e234]: 3. 去教练追问“为什么我会在这一类波动里犹豫或冲动”。
                  - generic [ref=e235]:
                    - button "查看最终复盘" [disabled] [ref=e236]
                    - link "回工作台" [ref=e237] [cursor=pointer]:
                      - /url: /dashboard
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