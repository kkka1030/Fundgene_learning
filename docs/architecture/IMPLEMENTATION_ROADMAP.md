# FundGene 实施路线图

更新日期：2026-05-17
状态：执行基线文档。`apps/web` 与 `apps/api` 已恢复，onboarding / coach / dashboard / learning / portfolio / simulation / news 的真实闭环已经落地，Agent Runtime v2 deterministic spine 与首个 20-case eval harness 已进入主干。当前执行方向已经切换到 Agent Command Center：先重塑 IA 和 agent task loop，再把旧模块收编为工具和详情页。

## 1. 实施原则

- 先修正现实，再扩展能力。
- 先恢复主干，再写功能细节。
- 先做最小可发布主干，再扩展行为画像、模拟和资讯。
- 先锁定 schema、契约和持久化，再扩展模型能力。
- 前端先把信息层级与工作区做对，不在旧 UI 上修修补补。
- 新阶段先做 Agent Command Center，不继续按旧模块顺序扩展。

## 1.1 当前优先路线：Agent Command Center

以下路线是后续实现主线。下面旧 Phase 0-6 保留为“已完成能力资产”和历史基线，不再代表未来产品 IA。

### Command Center Phase 0: 文档与契约收口

目标：

- 防止旧模块导航上下文继续污染实现。
- 统一 `AGENTS.md`、`PROJECT_OVERVIEW.md`、`PRODUCT_BLUEPRINT.md`、UX 和实施计划。

完成标准：

- 后续 agent 能明确知道目标 IA 是 `/today`、`/agent`、`/automations`、`/profile`。
- 旧 `/dashboard`、`/coach`、`/learning`、`/portfolio`、`/simulation`、`/news` 被描述为迁移前身或详情页，而不是一级导航。

### Command Center Phase 1: Route and Navigation Migration

目标：

- `/dashboard -> /today`
- `/coach -> /agent`
- 新增 `/automations`
- 新增或重塑 `/profile`
- 旧模块从主导航移除，保留为 Agent action targets。

验证：

```bash
pnpm lint:web
pnpm build:web
pnpm test:web:e2e
pnpm test:web:a11y
```

必须做桌面和移动端截图检查。

### Command Center Phase 2: Today / Daily Brief

目标：

- 让 Daily Brief 成为默认 home 对象。
- 一屏展示一个判断、最多三条证据、一个安全下一步、一个安全边界和生成时间。

实现方向：

- 可先复用现有 `GET /api/dashboard.daily_brief`。
- 后续可迁移为 `GET /api/today` 或独立 brief endpoint。

### Command Center Phase 3: Agent Workspace Run

目标：

- 把旧 coach chat 改成 task-run 工作台。
- 默认展示自然语言执行步骤。
- 高级模式展示 trace/tool/evidence/policy。
- 关键写回以 confirmation card 处理。

### Command Center Phase 4: L2 Automation MVP

目标：

- 每日简报默认开启但可关闭。
- 组合巡检、资讯影响观察、行为偏差观察以授权卡片呈现。
- 先做规则和运行记录，不急于复杂调度系统。

### Command Center Phase 5: Profile Authorization and Pending Writeback

目标：

- `/profile` 管理风险画像、组合、行为证据、学习状态和自动任务授权。
- 行为画像和高影响状态变化必须 pending confirmation。

### Command Center Phase 6: Module-to-Tool Migration

目标：

- 保留 portfolio/news/learning/simulation 的现有业务能力。
- 它们作为 Agent action 目标、证据详情、自动任务结果和 profile drilldown 存在。
- 不再作为主导航驱动产品。

### Command Center Phase 7: Screenshot and Regression Review

目标：

- 用浏览器截图和测试防止产品回到模块堆叠。

验证：

```bash
pnpm lint:web
pnpm build:web
pnpm test:web:e2e
pnpm test:web:a11y
pnpm test:api
pnpm test:agent-evals
pnpm migrate:api:sql
```

截图至少覆盖 `/today`、`/agent`、`/automations`、`/profile` 的桌面与移动端，以及一次 Agent Workspace task run。

## 2. Phase 0: 仓库现实收口

目标：

- 修正文档与仓库现实不一致的问题
- 统一 `AGENTS.md` 为唯一指定全局上下文
- 明确哪些资产保留、哪些归档、哪些废弃
- 明确 repo boundary 异常是后续实现阶段的硬阻塞项

完成状态：

- 已完成

## 3. Phase 1: 恢复应用主根目录

目标：

- 修复 repo boundary，使 FundGene 成为可独立管理的工程单元
- 恢复 `apps/web` 与 `apps/api`
- 建立真实可运行的 workspace 和后端项目骨架
- 锁定 migration-first、contract-first 的基础链路

当前状态：

- repo boundary 已修复
- `apps/web` 已恢复并通过构建
- `apps/api` 已恢复并通过 HTTP / 测试 / Alembic 离线验证
- migration-first 当前代码基线已推进到 `20260429_0009`
- 先前 live PostgreSQL migration 已验证到 `20260402_0005`
- 新 revisions `20260426_0006`、`20260426_0007`、`20260426_0008` 与 `20260429_0009` 已通过 `pnpm migrate:api:sql`，但 live 本地升级仍需在 Docker daemon 可用时重跑

完成状态：

- 已完成

## 4. Phase 2: MVP 主干 A

目标：

- 打通 onboarding / risk profile / dashboard / coach / learning 的第一条闭环

当前状态：

- `/start` -> onboarding -> questionnaire -> coach -> dashboard 已是稳定真实主路径
- minimal auth/session baseline 已落地
- `/coach` 已完成真实持久化闭环
- `/learning` 已完成真实持久化闭环：
  - 主学习路径可读取
  - 课程详情可读取
  - section 完成状态可回写
  - dashboard 下一步动作会感知学习进度

输出物：

- 基础认证与用户档案
- 风险问卷
- 仪表盘
- 单入口教练工作区
- 学习中心与学习进度

完成标准：

- 用户可完成一次 onboarding
- 可发起一次基金基础问答并看到结构化回答
- 可完成一个学习单元并回写进度
- 仪表盘能展示下一步动作

完成状态：

- 已完成

## 5. Phase 3: MVP 主干 B

目标：

- 打通组合录入、组合体检、解释性建议与报告沉淀

当前状态：

- `/portfolio` 已支持手动录入持仓快照
- 后端会生成解释型报告并持久化到 `portfolio_analyses`
- 最近报告与历史记录都可以回读
- dashboard 会感知最近组合状态
- coach 的 portfolio intent 已可引用最近一份真实组合报告

输出物：

- 组合快照录入
- 风险暴露与集中度分析
- 以原则表达的后续动作建议
- 结构化分析报告与历史记录

完成标准：

- 用户可提交一组持仓并获得可解释的组合报告
- 报告可落库、可回看、可被助手引用

完成状态：

- 已完成

## 6. Phase 4: MVP 主干 C

目标：

- 打通行为画像与微型历史情境训练闭环

当前状态：

- `GET /api/behavior/training-plan` 已可从行为画像与最近复盘导出训练焦点
- `/simulation` 已支持读取情境目录、启动 session、提交节点动作、读取最终复盘
- 后端已持久化 `scenarios`、`scenario_events`、`simulation_sessions`、`simulation_actions`、`simulation_reviews`
- dashboard 已可展示 simulation 状态与下一步训练动作
- coach 的 behavior / simulation intent 已可引用最新训练上下文

输出物：

- 偏差标签与证据结构
- 行为训练计划
- 1 到 2 个高质量微型历史场景
- 训练复盘报告

依赖关系：

- 已完成的 onboarding / dashboard / coach / learning / portfolio 数据基线

风险点：

- 画像标签抽象化、缺少证据
- 模拟功能膨胀成市场终端或玩法系统

完成标准：

- 用户完成一次情境训练后，系统可生成复盘并回流画像
- 仪表盘能展示变化与下一步训练建议

完成状态：

- 已完成

## 7. Phase 5: 扩展与硬化

目标：

- 增加最小新闻解读能力、观测、部署与评测

输出物：

- 最小新闻与政策解读链路（已落地）
- Sentry 接入
- Docker 化运行方案
- Prompt / API / UI 基础回归（API 测试、Web E2E/a11y 已落地）

依赖关系：

- MVP 主干已经可真实使用

风险点：

- 在主干未稳时过早接外部数据源
- 为了“看起来完整”引入过多插件与平台

完成标准：

- 应用具备基础部署能力
- 关键链路有可观测性与回归基线

当前状态：

- 部分完成：新闻/政策解读、readiness endpoint、Web E2E/a11y 与 CI baseline 已落地。
- 未完成：Sentry、OpenTelemetry、生产部署路径、live PostgreSQL 升级到 `20260429_0009`。

## 8. Phase 6: Agent Runtime v2

目标：

- 把当前单入口 advisor runtime 升级为可追踪、可评测、可审计的 agent runtime。
- 增加技术含量，但不把产品变成多 agent 展示项目。

首批输出物：

- `agent_runs` 增强或新增 run / step / tool call / evidence ref / policy decision 表
- `context_snapshot` 结构，记录本次回答使用的用户画像、组合报告、行为状态、模拟复盘和新闻解读版本
- typed worker contracts：learning、portfolio、behavior、news
- 最小 read-only tool registry：schema、权限、timeout、audit
- input guard、worker output validator、final response validator、policy guard
- 20 条以上 regression eval cases，覆盖解释质量、证据引用、安全边界和金融表述克制性
- debug trace API 或内部页面，能回看一次回答为何调用了哪些 worker、依据了哪些证据、被哪些 guard 改写

完成标准：

- `POST /api/assistant/messages` 对外契约保持稳定。
- 每次重要回答都有可回看的 trace、evidence refs 和 policy decision。
- 至少四类 intent 可进入 typed worker 路径：learning、portfolio、behavior、news。
- 失败时有 rule-based 或 template fallback，不产生空白回答。

当前状态：

- 已完成第一条 deterministic advisor spine：`POST /api/assistant/messages` 创建 run，执行 v2 orchestrator，写入 steps/tool calls/evidence/state proposals/policy status/context snapshot，并保存结构化 assistant message。
- trace 持久化已由 `20260429_0009` 提供，`GET /api/assistant/runs/{run_id}/trace` 已支持当前用户授权读取，`/coach` 已挂载 `TraceAuditPanel`。
- typed workers、read-only tool registry、policy guard、evidence refs、composer fallback 和 context snapshot 已进入主干。
- 首个本地 agent eval harness 已落地：`services/evals/agent_eval_cases.json` 包含 20 条 regression cases，根脚本 `pnpm test:agent-evals` 已接入 API CI job。
- 当前已通过 `pnpm test:api`、`pnpm test:agent-evals`、`pnpm migrate:api:sql`，以及相关 web lint/build/E2E 验证。
- 未完成：更丰富的跨领域 worker composition、selective RAG、citation faithfulness、eval-run persistence、Sentry/OpenTelemetry、生产部署硬化，以及 Docker daemon 可用后的 live PostgreSQL migration 到 `20260429_0009`。

## 9. 并行推进规则

- 产品、前端、后端可以并行设计，但实现必须以 API 契约和数据模型为交汇点。
- 前端可在契约锁定后先用 mock 开发工作区骨架，但 mock 结构必须服从真实 schema。
- 后端优先保证结构化输出、持久化与可追溯，再扩展模型能力与检索能力。
