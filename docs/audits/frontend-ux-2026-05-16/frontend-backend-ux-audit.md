# FundGene 前端、后端功能与 UX 审计

日期: 2026-05-16  
范围: `apps/web` 当前所有用户入口和工作区页面，重点检查前后端闭环、页面是否适配功能、内部信息是否过度暴露、移动端/桌面端体验。  
约束: 本轮不改产品代码，只生成审计证据、问题汇总和改进方案。

## 一句话结论

当前 FundGene 的主要业务闭环已经不是纯壳子：登录、建档、工作台、教练问答、学习进度、组合报告、情境训练、资讯解读都能通过 live 前端连到 live 后端。但产品 UI 还没有完全从“内部 agent/runtime 控制台”切换成“新手基金教练产品”，最明显的问题是工作台和情境页把 `Advisor orchestrator`、`Agent route`、`Scenario dossier`、`performance_chasing_risk` 等内部词直接展示给用户。

## 当前 agent 通俗总结

现在的 FundGene agent 可以理解成一个“总教练 + 后台工具箱”：

- 用户看到的应该是一个基金学习教练：帮用户解释风险、拆解问题、生成下一步动作、把组合/资讯/训练结果串起来。
- 后端实际运行的是一个单入口 `AdvisorAgent` / `Advisor Orchestrator`：它读取用户画像、学习进度、组合报告、情境训练和资讯解读，再调用内部工具链生成结构化回答。
- 重要边界是：它不替用户交易，不承诺收益，不直接输出买卖指令。
- 目前前端最大的问题是，有些页面把“后台怎么调度 agent”也展示给了普通用户。对开发者这是 trace/context，对新手用户会像看系统日志。

## 审计方法与产物

现场环境:

- API: `http://127.0.0.1:8000`
- Web: `http://127.0.0.1:3000`
- 数据库: `/tmp/fundgene_frontend_audit.db`
- 模式: `FUNDGENE_AGENT_MODE=deterministic`
- Demo 账号: `demo@fundgene.local`

审计产物:

- 路由与交互总结果: [live-audit-results.json](./live-audit-results.json)
- 建档提交补测: [onboarding-live-check.json](./onboarding-live-check.json)
- 教练与资讯补测: [coach-news-live-check.json](./coach-news-live-check.json)
- 组合有效提交补测: [portfolio-valid-submit-check.json](./portfolio-valid-submit-check.json)
- 情境动作提交补测: [simulation-action-live-check.json](./simulation-action-live-check.json)
- 截图目录: [screenshots/](./screenshots/)

参考标准:

- 本仓库 `AGENTS.md` 的 beginner-first、explanation-first、product boundary 要求。
- Vercel Web Interface Guidelines: <https://raw.githubusercontent.com/vercel-labs/web-interface-guidelines/main/command.md>
- 本轮也运行了 `pnpm lint:web`，通过。

## 功能接入现状

| 页面 / 功能 | live 结论 | 证据 | 主要问题 |
|---|---|---|---|
| `/` 总览 | 可访问，产品边界表达清楚，无横向溢出 | `overview-desktop.png`, `overview-mobile.png` | 暂未发现高优先级问题 |
| `/start` 登录/注册 | 未登录可进入账号入口；注册后能进入建档 | `onboarding-live-check.json` | 顶部“搜索”像可用功能但实际不是输入控件 |
| `/dashboard` 工作台 | 能读取用户、行为画像、学习、组合、情境、资讯状态 | `dashboard-desktop.png`, `dashboard-mobile.png` | 暴露 internal agent 术语和 raw bias code |
| `/onboarding` 建档 | 注册、保存基础画像、提交问卷、跳转工作台通过 | `onboarding-live-check.json`, `interaction-onboarding-after-submit.png` | 功能接入正常 |
| `/coach` 教练问答 | live 提问成功；回答隐藏 run id/tool name | `coach-news-live-check.json`, `interaction-coach-after-message.png` | 该页是目前最符合“用户只看解释，不看 trace”的页面 |
| `/learning` 学习中心 | 课程读取和小节完成写回通过 | `interaction-learning-course.png` | 文案中仍有少量英文状态词 |
| `/portfolio` 组合分析 | 有效持仓输入能生成报告；历史和最新报告可见 | `portfolio-valid-submit-check.json`, `interaction-portfolio-valid-submit.png` | 空表单直接提交时只给总错误，字段级引导弱 |
| `/simulation` 情境训练 | 场景读取、启动会话、提交一次动作和即时反馈通过 | `simulation-action-live-check.json`, `interaction-simulation-after-action.png` | 后端返回 active session，但前端不支持恢复 |
| `/news` 资讯解读 | 选择资讯生成结构化解读通过 | `coach-news-live-check.json`, `interaction-news-after-analysis.png` | 列表太长，结果区在列表下方，移动端很难发现 |

## 重点问题

### P1. 工作台和情境页过度暴露内部 agent/runtime 语言

现象:

- 工作台显示 `Advisor orchestrator`、`agent 统一读取`、`进入 Agent 教练`、`Agent route`、`Agent 推荐的下一步`、`Latest evidence`。
- 情境页显示 `Scenario dossier`、`Training objective`、`Bias focus`、`Timeline preview`、`Decision room`、`Choose an action`、`Action`。
- 行为偏差直接显示 raw code，例如 `performance_chasing_risk`、`no_major_bias_detected`。

源码位置:

- `apps/web/components/dashboard-workspace.tsx:328`
- `apps/web/components/dashboard-workspace.tsx:337`
- `apps/web/components/dashboard-workspace.tsx:346`
- `apps/web/components/dashboard-workspace.tsx:428`
- `apps/web/components/dashboard-workspace.tsx:493`
- `apps/web/components/dashboard-workspace.tsx:513`
- `apps/web/components/simulation-workspace.tsx:479`
- `apps/web/components/simulation-workspace.tsx:535`
- `apps/web/components/simulation-workspace.tsx:721`
- `apps/web/components/simulation-workspace.tsx:748`
- `apps/web/components/simulation-workspace.tsx:755`
- `apps/web/components/simulation-workspace.tsx:774`
- `apps/web/components/simulation-workspace.tsx:832`
- `apps/web/components/simulation-workspace.tsx:898`

证据截图:

- [dashboard-desktop.png](./screenshots/dashboard-desktop.png)
- [simulation-desktop.png](./screenshots/simulation-desktop.png)
- [interaction-onboarding-after-submit.png](./screenshots/interaction-onboarding-after-submit.png)

影响:

- 对新手用户来说，这些词像系统实现细节，不像投资教练语言。
- 与 `AGENTS.md` 中“产品不是 agent novelty demo”的方向冲突。
- 容易让页面看起来像开发者控制台，而不是可理解的投资训练产品。

建议:

- 用户侧统一替换为产品语言，例如:
  - `Advisor orchestrator` -> `今日教练建议`
  - `Agent route` -> `今日任务流`
  - `Latest evidence` -> `最新记录`
  - `Scenario dossier` -> `情境档案`
  - `Decision room` -> `本轮判断`
  - `performance_chasing_risk` -> `追涨倾向`
- 后端 raw enum/code 必须经过 display mapping 后再渲染。
- 仅在 dev/audit 模式显示 trace、run id、tool name、worker name、evidence key。

### P1. 情境训练 active session 已有后端信号，但前端不支持恢复

现象:

- 前端 API 已解析 `activeSessionId`。
- UI 检测到 active session 后只提示“当前账号存在一个活跃训练，但此版本暂不支持恢复中途会话。请重新开始一个情境。”

源码位置:

- `apps/web/lib/api.ts:375`
- `apps/web/lib/api.ts:1394`
- `apps/web/lib/api.ts:1424`
- `apps/web/components/simulation-workspace.tsx:797`
- `apps/web/components/simulation-workspace.tsx:799`

证据:

- [interaction-simulation.png](./screenshots/interaction-simulation.png)
- 新用户补测证明启动和提交动作本身可用: [simulation-action-live-check.json](./simulation-action-live-check.json)

影响:

- 用户离开页面后无法继续中途训练，只能重新开始。
- 后端已经暴露了恢复所需的入口信号，前端没有消化掉，属于前后端闭环缺口。
- 容易产生重复 session，训练记录和行为画像会变得难解释。

建议:

- 用 `activeSessionId` 拉取 `GET /api/simulations/sessions/{session_id}` 并 hydrate `activeSession`。
- UI 提供“继续上次训练”和“放弃并重新开始”两个明确动作。
- 如果后端暂不允许放弃 session，先补一个只读恢复路径，至少不要要求用户重开。

### P1. 资讯页列表过长，生成结果离触发动作太远

现象:

- 资讯页一次展示 20 条长卡片，移动端页面非常长。
- 用户点击第一条资讯的“生成解读”后，结构化结果在整段列表和手动表单之后。
- `getNewsCatalog` 默认请求 `/api/news?refresh=true`，普通打开页面也触发 refresh 语义。

源码位置:

- `apps/web/components/news-workspace.tsx:389`
- `apps/web/components/news-workspace.tsx:405`
- `apps/web/components/news-workspace.tsx:422`
- `apps/web/components/news-workspace.tsx:480`
- `apps/web/lib/api.ts:2063`
- `apps/web/lib/api.ts:2064`

证据:

- [news-mobile.png](./screenshots/news-mobile.png)
- [interaction-news-after-analysis.png](./screenshots/interaction-news-after-analysis.png)
- `live-audit-results.json` 中 `/news` 的 DOM 文本长度为 `7334`，控件数为 `53`。

影响:

- 用户点击后不容易发现结果已经生成，尤其是手机端。
- 信息密度高但主次不清，新闻流盖过了解读画布。
- 每次进入都 refresh 资讯，可能带来延迟、网络噪声和不可控内容变化。

建议:

- 默认只展示前 5 条，剩余用分页、筛选或“展开更多”。
- 把解读画布放在右侧 sticky 区域或列表上方；生成后自动滚动/聚焦到结果。
- 普通读取用 `/api/news`，手动刷新单独做按钮或后台任务。
- 英文原始新闻先做中文新手摘要，再展示原文来源。

### P2. 组合表单空提交反馈太粗，和已有报告混在一起

现象:

- 空持仓行直接点“生成组合报告”时，页面只显示总错误：“请为每一行持仓填写基金代码、基金名称和大于 0 的持仓金额。”
- 同一屏里仍然展示已有 `Latest report`，用户容易分不清“这次提交失败”和“历史报告仍可看”。
- 有效输入提交链路是通的。

源码位置:

- `apps/web/components/portfolio-workspace.tsx:557`
- `apps/web/components/portfolio-workspace.tsx:581`
- `apps/web/components/portfolio-workspace.tsx:630`
- `apps/web/components/portfolio-workspace.tsx:667`
- `apps/web/components/portfolio-workspace.tsx:761`

证据:

- 空提交问题: [interaction-portfolio-after-submit.png](./screenshots/interaction-portfolio-after-submit.png)
- 有效提交通过: [portfolio-valid-submit-check.json](./portfolio-valid-submit-check.json), [interaction-portfolio-valid-submit.png](./screenshots/interaction-portfolio-valid-submit.png)

建议:

- 每个字段旁边显示字段级错误，不只显示总错误。
- 对新手提供“填入示例组合”或“用最近一份报告复制为草稿”。
- 表单无效时禁用提交按钮，并说明缺哪几项。
- 把“本次提交状态”和“历史最新报告”视觉上分区，避免混淆。

### P2. 顶部搜索看起来可用，但实际只是静态提示

现象:

- App shell 和 start 页顶部都有搜索图标和“搜索课程、组合报告、情境训练”。
- 这不是 input，也没有命令面板或点击行为。

源码位置:

- `apps/web/components/app-shell.tsx:153`
- `apps/web/components/app-shell.tsx:155`
- `apps/web/components/auth-entry.tsx:116`
- `apps/web/components/auth-entry.tsx:118`

影响:

- 用户会把它理解为搜索框，点击后没有反馈。
- 这类“看似可操作但不可操作”的控件会降低信任感。

建议:

- 短期: 改成静态状态/路径提示，不使用搜索图标和搜索框外观。
- 中期: 做成真正的 command palette，至少支持课程、报告、情境和页面跳转。

### P2. 中英文和 raw enum 混用削弱新手产品感

现象:

- 页面里有 `Learning`、`Portfolio`、`Coach`、`User state`、`Manual snapshot`、`Latest report`、`Holding`、`Snapshot`、`Structured readout` 等混合标签。
- 偏差标签和场景标签直接显示后端 code。

证据:

- [dashboard-desktop.png](./screenshots/dashboard-desktop.png)
- [portfolio-desktop.png](./screenshots/portfolio-desktop.png)
- [simulation-desktop.png](./screenshots/simulation-desktop.png)

建议:

- 建一个统一 `displayLabels` 映射层，覆盖 risk level、bias tag、fund type、scenario status、agent intent、news type。
- 面向用户的主 UI 默认中文；英文可保留在来源、引用或开发模式。

### P2. 当前 Playwright 测试主要是 mock API，不能证明 live 前后端契约

现象:

- `apps/web/e2e/workspaces.spec.ts` 和 `apps/web/e2e/accessibility.spec.ts` 都在 `beforeEach` 使用 `mockFundGeneApi(page)`。
- `visual-smoke.spec.ts` 的 simulation marker 还是 `Scenario dossier`，等于把一个内部词当成了视觉验收条件。

源码位置:

- `apps/web/e2e/workspaces.spec.ts:5`
- `apps/web/e2e/workspaces.spec.ts:8`
- `apps/web/e2e/accessibility.spec.ts:4`
- `apps/web/e2e/accessibility.spec.ts:9`
- `apps/web/e2e/visual-smoke.spec.ts:25`

影响:

- mock 测试能证明组件在固定 fixture 下能渲染，但不能覆盖 live cookie/session、后端字段漂移、refresh 行为、active session 恢复等问题。
- 测试甚至会固化不应该给用户看的 internal label。

建议:

- 保留 mock visual smoke，但新增少量 live contract smoke:
  - register -> onboarding -> dashboard
  - coach question -> persisted assistant answer
  - portfolio valid snapshot -> latest report
  - simulation start -> submit action
  - news item -> analysis result
- visual marker 从内部词改为用户词，例如 `情境档案`。

### P3. 控制台和资源加载有轻微噪声

现象:

- live 审计记录到 3 条 `401 Unauthorized` resource console error，来自初始 session 检查。
- 记录到一次字体资源 `noto-sans-sc...woff2` 的 `net::ERR_ABORTED`。
- 无 `pageErrors`。

证据:

- [live-audit-results.json](./live-audit-results.json)

建议:

- 如果这是预期的未登录探测，前端监控里应降噪或分类。
- 字体 abort 目前不是高优先级，但可以在性能审计时一起看。

## 页面级改进建议

### 工作台

- 把工作台从“agent command center”调整为“今日教练建议 + 进度总览”。
- 所有 agent/runtime 词迁移到 dev trace，不进入默认用户界面。
- 把 raw bias code 显示成用户能懂的中文解释，并附一句“这意味着什么”。

### 教练

- 当前方向正确：回答里展示解释、边界、下一步、追问，不展示 tool/run id。
- 可以继续加强“把本次回答写回哪个产品状态”的可见性，但不要展示内部 trace 名称。

### 学习

- 学习写回可用。
- 建议减少英文状态词，例如 `sections completed`，并把课程完成后对工作台的影响讲得更明确。

### 组合

- 功能链路可用。
- 表单需要字段级校验、示例填充、草稿保存或复制历史报告。
- 新手用户输入基金代码/金额时，最好先解释“不接实时行情，只记录结构”。

### 情境

- 启动和提交动作可用。
- 优先补 active session 恢复。
- 页面文案要从开发者英语切换到训练语言。

### 资讯

- 功能链路可用。
- 优先重排布局：结果画布不能在 20 条新闻之后。
- 增加中文摘要和筛选，减少英文原始新闻对新手的压迫感。

## 建议实施顺序

1. P1 文案和 display mapping: 先把内部 agent 词、raw enum、英文 section kicker 清掉。
2. P1 情境 active session 恢复: 用已存在的 `activeSessionId` 完成继续训练。
3. P1/P2 资讯布局: 限制默认列表数量，把分析结果放到触发点附近。
4. P2 组合表单: 加字段级校验和示例输入。
5. P2 搜索控件: 要么实现 command palette，要么改成静态提示。
6. P2 测试补强: 增加 live contract smoke，避免只靠 mock visual tests。

## 完成度核对

| 原要求 | 完成证据 |
|---|---|
| 全面检查前端 | 覆盖 `/`, `/start`, `/dashboard`, `/onboarding`, `/coach`, `/learning`, `/learning/fund-basics`, `/portfolio`, `/simulation`, `/news` |
| 每个页面截图 | `screenshots/` 下已有桌面和移动端截图 |
| 每个功能是否接后端 | auth、onboarding、coach、learning、portfolio、simulation、news 均有 live 证据或补测 JSON |
| 检查前端页面是否适配功能 | 已列出 dashboard、portfolio、simulation、news 的适配问题 |
| 检查信息是否过度暴露 | P1 明确列出 internal agent/runtime 暴露 |
| 检查用户体验问题 | P1/P2/P3 分级列出，并给出改进顺序 |
| 写成 md 文件 | 本文件 |
| 不改代码 | 仅新增 `docs/audits/frontend-ux-2026-05-16/` 审计文档、JSON 和截图 |

