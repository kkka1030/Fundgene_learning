# FundGene 本地测试指南

更新日期：2026-04-30
状态：onboarding / questionnaire / coach / dashboard / learning / portfolio / simulation / news 主路径已有工程、迁移 SQL、API 测试与 Web E2E/a11y 验证基线。运行前仍应重新执行本文件命令确认当前机器状态。

## 1. 前端验证

代码检查：

```bash
pnpm lint:web
```

生产构建：

```bash
pnpm build:web
```

当前构建通过的静态路由：

- `/`
- `/start`
- `/dashboard`
- `/onboarding`
- `/coach`
- `/learning`
- `/learning/[courseSlug]`
- `/portfolio`
- `/simulation`
- `/news`

E2E 与可访问性测试：

```bash
pnpm --filter @fundgene/web exec playwright install chromium
pnpm test:web:e2e
```

说明：

- `pnpm test:web:e2e` 当前会同时运行 workspace smoke 和 axe accessibility 测试。
- 首次运行需要安装 Playwright Chromium；最近一次完整记录中 `pnpm test:web:e2e` 通过 `6 passed`。

## 2. 后端验证

依赖同步：

```bash
pnpm sync:api
```

测试入口：

```bash
pnpm test:api
```

说明：

- 当前根脚本使用 `python -m pytest -p no:capture`。
- 原因是本机 Python 3.12 环境下，pytest 默认 capture 插件会触发 `139` 崩溃；禁用 capture 后测试通过。

当前已通过的后端测试：

- `apps/api/tests/test_auth.py`
- `apps/api/tests/test_coach_flow.py`
- `apps/api/tests/test_health.py`
- `apps/api/tests/test_learning_flow.py`
- `apps/api/tests/test_news_flow.py`
- `apps/api/tests/test_onboarding_flow.py`
- `apps/api/tests/test_portfolio_flow.py`
- `apps/api/tests/test_product.py`
- `apps/api/tests/test_simulation_flow.py`

## 3. HTTP 冒烟验证

启动后端：

```bash
pnpm dev:api
```

然后验证：

```bash
curl -sS http://127.0.0.1:8000/api/health
curl -sS http://127.0.0.1:8000/api/product
```

最近一次完整记录中已拿到 `200` 响应；当前会话应以重新执行 curl 的结果为准。

第一条 onboarding + coach 闭环验证：

```bash
COOKIE_JAR="$(mktemp)"

curl -sS -X POST http://127.0.0.1:8000/api/auth/register \
  -H 'Content-Type: application/json' \
  -c "$COOKIE_JAR" \
  -d '{
    "email": "demo@example.com",
    "password": "supersecure123"
  }'

curl -sS http://127.0.0.1:8000/api/auth/session \
  -b "$COOKIE_JAR"

curl -sS -X POST http://127.0.0.1:8000/api/onboarding/profile \
  -H 'Content-Type: application/json' \
  -b "$COOKIE_JAR" \
  -d '{
    "display_name": "Demo Ava",
    "investing_experience": "beginner",
    "monthly_contribution_band": "under_3000",
    "primary_goal": "先建立长期投资纪律。"
  }'

curl -sS -X POST http://127.0.0.1:8000/api/behavior/questionnaires \
  -H 'Content-Type: application/json' \
  -b "$COOKIE_JAR" \
  -d '{
    "questionnaire_version": "v1",
    "answers": {
      "volatility_comfort": 4,
      "drawdown_reaction": 4,
      "investment_horizon": 5,
      "panic_sell_impulse": 4,
      "chase_hot_funds": 5,
      "diversification_habit": 2
    }
  }'

curl -sS -X POST http://127.0.0.1:8000/api/assistant/messages \
  -H 'Content-Type: application/json' \
  -b "$COOKIE_JAR" \
  -d '{
    "message": "我刚开始买基金，怎么理解风险等级和回撤？"
  }'

curl -sS http://127.0.0.1:8000/api/assistant/session \
  -b "$COOKIE_JAR"

curl -sS http://127.0.0.1:8000/api/dashboard \
  -b "$COOKIE_JAR"

curl -sS -X POST http://127.0.0.1:8000/api/auth/logout \
  -b "$COOKIE_JAR"
```

最近一次完整记录中已实际验证：

- `POST /api/auth/register` 与 `GET /api/auth/session` 可建立并读取真实会话
- `GET /api/users/me` 在未 onboarding 前返回 `404`
- `POST /api/onboarding/profile` 可创建用户基础画像
- `POST /api/behavior/questionnaires` 可写入问卷与画像
- `POST /api/assistant/messages` 可为当前登录用户创建或续写 coach 会话，并返回结构化回答
- `GET /api/assistant/session` 可回读最近一次真实 coach 会话与消息历史
- `GET /api/dashboard` 可反映问卷后的风险等级、偏差标签和最近一次 coach 留痕
- `GET /api/news` 与 `POST /api/news/analyze` 可生成并回读新闻/政策结构化解读
- `POST /api/auth/logout` 后，`GET /api/auth/session` 返回 `401`

## 4. 迁移验证

离线验证：

```bash
pnpm migrate:api:sql
```

真实数据库迁移：

```bash
pnpm infra:up
pnpm migrate:api
```

当前已知限制：

- `pnpm migrate:api:sql` 已通过到 revision `20260429_0009`。
- `pnpm infra:up` 与 `pnpm migrate:api` 曾在此前对本地 PostgreSQL 实际跑通，并把 `alembic_version` 推进到 `20260402_0005`。
- 最近一次检查中 `pnpm infra:up` 失败，原因是 Docker daemon 不可用；`20260426_0006`、`20260426_0007`、`20260426_0008`、`20260429_0009` 仍需要在 Docker daemon 可用时做 live local PostgreSQL upgrade。
- Docker 不可用时只能把 `pnpm migrate:api:sql` 视为离线替代验证，不可冒充 live migration。

## 5. Agent Runtime v2 QA 要求

Agent Runtime v2 第一条 deterministic advisor spine 已落地。当前代码已有 `20260429_0009` trace migration、trace models、后端 trace read route、运行时 steps/tool calls/evidence/state proposals 写入、policy guard 状态持久化、前端 trace API client、`TraceAuditPanel` 挂载和对应测试。后续扩展 eval、RAG、跨领域 worker 或 observability 时，必须继续按真实代码验证，不能只改文档。

当前首个 agent eval harness 已落地：

```bash
pnpm test:agent-evals
```

说明：

- eval fixture 位于 `services/evals/agent_eval_cases.json`。
- 当前 20 条用例覆盖安全拒答、工具选择、证据类型、citation presence、新手可读性，以及 learning / portfolio / behavior / simulation / news 意图。
- 该命令使用临时 SQLite 内存库和 deterministic agent mode，不依赖真实模型密钥。

涉及 v2 后端、迁移或 API 契约时，必跑：

```bash
pnpm sync:api
pnpm test:api
pnpm test:agent-evals
pnpm migrate:api:sql
```

DeepSeek V4 接入后的本地测试规则：

- `pnpm test:api` 和 `pnpm test:agent-evals` 默认不依赖真实 DeepSeek key。
- API 测试会覆盖 key 缺失 fallback、mock 模型成功 trace、mock 模型失败 fallback，以及 news/portfolio unsafe model output 回退。
- 如需真实 DeepSeek 冒烟，只在当前 shell 临时设置 `FUNDGENE_DEEPSEEK_API_KEY` 或 `DEEPSEEK_API_KEY`，然后运行：

```bash
pnpm smoke:deepseek
```

这个 smoke test 不应写入 `.env`，也不应进入 CI 的默认必跑链路。

涉及 coach trace panel 或其他前端 trace 展示时，追加：

```bash
pnpm lint:web
pnpm build:web
pnpm test:web:e2e
```

CI 期望：

- `web` job 保留 `pnpm lint:web`、`pnpm build:web`、`pnpm test:web:e2e`。
- `api` job 使用 Python 3.12 + uv，跑 `pnpm sync:api`、`pnpm test:api`、`pnpm test:agent-evals`、`pnpm migrate:api:sql`。
- `pnpm migrate:api:sql` 只能证明迁移 SQL 可展开；live PostgreSQL migration 仍需 Docker daemon 可用后用 `pnpm infra:up` + `pnpm migrate:api` 验证。
