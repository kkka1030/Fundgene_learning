# FundGene 系统架构基线

更新日期：2026-05-17
状态：技术与职责边界基线。主干实现根已恢复，onboarding / coach / learning / portfolio / simulation / news 的最小真实闭环已落地；Agent Runtime v2 deterministic spine 和首个 20-case eval harness 已进入主干。下一阶段架构重点已经转向 Agent Command Center：Today/Daily Brief、Agent Workspace Run、L2 自动任务、Profile Authorization、模块能力工具化，以及分层 trace 展示。

## 1. 设计原则

- 后端拥有业务真相
- 单入口 agent 优先于复杂多 agent 编排
- schema-first 与 traceability-first
- migration-first 与 contract-first
- 前端以产品工作流和解释呈现为中心
- Agent Command Center 优先于模块导航：用户看到一个 agent 帮他完成综合判断，portfolio/news/learning/simulation 是工具与详情页

## 2. 当前仓库现实

已确认：

- `apps/web` 已恢复为真实前端主应用根，并已通过 lint/build/E2E/a11y 验证
- `apps/web` 已落地 `/start`、`/onboarding`、`/dashboard`、`/coach`、`/learning`、`/portfolio`、`/simulation`、`/news` 的真实工作流主路径
- `/coach`、`/learning`、`/portfolio`、`/simulation`、`/news` 都已经从壳页推进到真实工作区，并能把结果回流 dashboard 或 advisor context
- 目标 IA 已变更为 `/today`、`/agent`、`/automations`、`/profile`；当前 `/dashboard` 是 `/today` 的迁移前身，当前 `/coach` 是 `/agent` 的迁移前身，旧模块页应降级为工具详情页
- `apps/api` 已恢复为真实后端主应用根，并已通过测试、HTTP 健康检查、Alembic 离线迁移验证；live PostgreSQL migration 曾验证到 `20260402_0005`
- `apps/api` 已落地 auth、assistant、onboarding、behavior、dashboard、learning、portfolio、simulation、news 的最小真实接口
- `data/dev/fundgene.db` 可作为历史 schema 对照
- `archive/legacy-frontend` 与 `archive/legacy-backend` 仅作参考

因此，当前架构设计必须区分：

- 目标主干
- 已存在的参考资产
- 已经恢复但仍需继续补业务能力的实现主干

## 3. 前端方案

技术：

- Next.js 16
- React 19
- TypeScript
- App Router
- Tailwind CSS 4
- TanStack Query
- Zod
- Apache ECharts

职责：

- 承担产品工作流、信息层级、结构化答案渲染、图表与交互工作区
- 不承载领域判断和分析真相

当前阶段策略：

- 先在 `apps/web` 内聚实现，不急于抽 `packages/ui`
- 先把 Agent Command Center IA、共享 shell 和四个主页面骨架做好，再逐步迁移旧模块入口
- 当前已经接上的真实工作流仍是重要能力资产，但不再代表未来导航：`/start`、onboarding、questionnaire、coach、learning、portfolio、simulation、news、dashboard 都应被收编到 Today / Agent / Automations / Profile 的主路径下
- 不沿用 legacy UI、路由结构和 Ant Design 体系

## 4. 后端方案

技术：

- Python 3.12+
- FastAPI
- PydanticAI
- SQLAlchemy 2.x
- Alembic
- PostgreSQL

可选：

- Redis：缓存、限流、后台任务或协调需要时再启用
- pgvector：检索确有必要时再启用

职责：

- 鉴权、持久化、领域服务、agent runtime、工具执行、结构化输出、审计与风控提示

当前阶段现实：

- 真实入口已存在：`/api/health`、`/api/ready`、auth、assistant、onboarding、behavior、dashboard、learning、portfolio、simulation、news 主路径接口
- Alembic 基线已存在，当前离线验证到 `20260426_0008`
- live migration 已经在本地 Dockerized PostgreSQL 上验证到 `20260402_0005`；最新 revisions 仍需 Docker daemon 可用后重跑

## 5. Agent Runtime 方案

产品默认使用单入口 advisor agent。

新产品架构层：

- `TodayBriefService`：生成或读取 Daily Brief，服务 `/today` 与默认 L2 自动化。
- `AgentWorkspaceRunService`：把用户任务组织成 Agent Workspace run，返回自然语言步骤、结果、CommandAction、pending confirmation 和高级 trace 链接。
- `AutomationService`：管理 Daily Brief、组合巡检、资讯观察、行为偏差观察的授权、频率、运行记录和安全边界。
- `ProfileAuthorizationService`：管理用户画像、组合、行为证据、学习状态和 pending writeback confirmation。
- Domain detail services：portfolio、news、learning、simulation、behavior 继续存在，但主要服务 agent tools、evidence drilldown 和 action targets。

当前基线：

- `AdvisorAgent`
  - 用户主入口
  - 负责意图识别、工具链选择、最终答案组织
- `LearningToolchain`
- `PortfolioToolchain`
- `BehaviorToolchain`
- `SimulationToolchain`
- `NewsToolchain`

所有重要输出都应通过 Pydantic schema 固定为结构化响应。

响应最小字段：

- `answer`
- `intent`
- `citations`
- `risk_notice`
- `recommended_actions`
- `follow_up_questions`

Agent Command Center 需要新增或统一的产品对象：

- `DailyBrief`
- `CommandAction`
- `AgentWorkspaceRun`
- `AgentWorkspaceStep`
- `AutomationRule`
- `AutomationRun`
- `PendingUserConfirmation`
- `ProfileAuthorization`

运行记录最小字段：

- 输入负载
- 输出负载
- schema 版本
- 模型信息
- 工具调用序列
- fallback 原因

下一阶段方向是 `Agent Runtime v2`，但它仍然不是自由聊天式多 agent 系统。

v2 目标形态：

- 一个用户可见的 `Advisor Orchestrator`
- typed domain workers：learning、portfolio、behavior、news，simulation 先作为 behavior context 使用
- 内部 read-only tool registry：schema、权限、timeout、审计记录
- evidence grounding：SQL-first 读取用户画像、组合报告、行为状态、模拟复盘、持久化解读；RAG 仅用于课程、术语、政策解释和新闻来源材料
- policy guard：输出 `allow`、`revise` 或 `block_with_guidance`
- persisted trace：run、step、tool call、evidence ref、policy decision 必须在运行过程中写入
- eval harness：首个 20-case 本地 harness 已覆盖新手解释、组合风险、行为偏差、新闻政策解读和安全边界；后续继续扩展 faithfulness、边界样例和持久化 eval-run 记录

v2 不允许：

- worker 之间自由聊天
- worker 直接修改 canonical user state
- 模型直接执行任意 SQL
- 未经 typed gateway 暴露 MCP 或本地资源能力

UI trace 分层：

- 默认用户层展示自然语言步骤，例如“正在读取你的风险画像”“正在筛选相关资讯”。
- 用户展开层展示证据来源、更新时间、支持强度和安全边界。
- Dev/Audit 层展示 run id、tool calls、worker outputs、policy status 和 latency。

## 6. 数据与存储方案

主库目标：

- PostgreSQL

开发参考：

- `data/dev/fundgene.db`

设计原则：

- 原始输入与系统结论分离
- 报告和 agent run 可版本化、可追溯
- 重要 AI 输出可落库、可回看、可引用

最关键的数据域：

- 用户域
- 学习域
- 组合域
- 模拟域
- 资讯域
- agent 运行域
- command center 域：daily briefs、agent workspace runs、automation settings/runs、pending confirmations、profile authorizations

## 7. API 契约策略

- 以 `docs/api/API_CONTRACT.md` 作为当前契约基线
- 用 FastAPI OpenAPI 输出机器可读契约；`apps/api` 已可作为后续契约生成源
- 前端依契约开发；mock 也要服从真实 schema
- SSE 流式接口只允许受控事件类型，最终事件返回结构化 payload

当前实现说明：

- onboarding/profile、users/me、behavior、dashboard、assistant、learning、portfolio、simulation、news 已经从“目标契约”进入“当前真实契约”

## 8. 监控与部署策略

本地：

- `infra/docker/docker-compose.yml` 提供 Postgres / Redis 基础设施
- `pnpm infra:up` 与 `pnpm migrate:api` 曾验证到 `20260402_0005`
- 如果 Docker daemon 不可用，当前仍可以先用 `pnpm migrate:api:sql` 做离线迁移验证，但不能把它当作 live migration 替代

部署方向：

- 前端优先考虑 Vercel 预览与部署
- 后端保持 Docker 化部署能力

监控方向：

- 结构化日志
- Sentry
- OpenTelemetry 作为后续增强

## 8.1 当前认证形态说明

- 当前主路径使用最小正式认证基线：email + password hash + persisted session + `HttpOnly` cookie。
- `user_profiles` 继续承载产品画像；`auth_users` 与 `auth_sessions` 负责账号与会话真相。
- `/api/auth/session` 是 canonical current-session 接口；`/api/auth/me` 仅作短期兼容别名。
- 这符合“后端拥有业务真相”的架构边界，同时避免把认证真相塞进前端本地存储或 header hack。

## 9. 代码改造策略

应保留：

- 产品与架构文档
- API 契约与数据模型
- `data/dev/fundgene.db` 作为参考对照
- `archive/` 中有价值的课程、情境和分析维度

应重写：

- `apps/web`
- `apps/api`
- 任何当前主干缺失的运行层代码

应废弃或仅归档：

- legacy 前端视觉、路由、组件组织
- legacy 后端的 AutoGen 编排、MCP workbench、硬编码绝对路径
- 归档内的嵌套 `.git` 和 `node_modules`
