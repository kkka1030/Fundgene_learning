# FundGene 本地启动指南

更新日期：2026-04-30
状态：`apps/web` 与 `apps/api` 已恢复，且 onboarding / questionnaire / coach / dashboard / learning / portfolio / simulation / news 的最小真实用户闭环已可在最小正式认证基线上本地跑通。

## 1. 前置条件

- Node.js `v22+`
- `pnpm 10.6.5`
- Python `3.12+`
- `uv`
- 如需真实执行数据库迁移：本机 Docker daemon 可用，或已有可连接的 PostgreSQL

## 2. 安装依赖

```bash
pnpm install
pnpm sync:api
```

说明：

- `pnpm install` 会安装根 workspace 与 `apps/web` 依赖。
- `pnpm sync:api` 会在 `apps/api/.venv` 中同步 Python 依赖与 dev extra。

## 3. 启动前端

```bash
pnpm dev:web
```

默认本地访问地址：

- `http://127.0.0.1:3000`

前端默认请求 `http://127.0.0.1:8000`。本地 demo 时请保持前端和 API 都使用 `127.0.0.1`，不要在同一轮登录流程里混用 `localhost` 和 `127.0.0.1`，否则浏览器 cookie / dev-server origin 可能导致会话检查卡住或认证失败。

当前前端主路由：

- `/`
- `/start`
- `/dashboard`
- `/onboarding`
- `/coach`
- `/learning`
- `/portfolio`
- `/simulation`
- `/news`

## 4. 启动后端

如需自定义配置，先准备环境文件：

```bash
cp apps/api/.env.example apps/api/.env
```

DeepSeek V4 是当前默认 advisor composer 模型配置：

```bash
FUNDGENE_ADVISOR_MODEL=deepseek:deepseek-v4-pro
FUNDGENE_AGENT_MODE=hybrid
FUNDGENE_AGENT_MODEL_TIMEOUT_MS=30000
FUNDGENE_DEEPSEEK_THINKING=disabled
FUNDGENE_DEEPSEEK_REASONING_EFFORT=high
```

真实 key 不要写入仓库。需要本地真实调用时，只在当前 shell 临时注入：

```bash
export FUNDGENE_DEEPSEEK_API_KEY="你的本地测试 key"
```

也兼容读取 `DEEPSEEK_API_KEY`。如果没有 key，`hybrid` 模式会自动走 deterministic fallback，并在 coach trace 里记录 `fallback_reason=model_not_configured`。

然后启动：

```bash
pnpm dev:api
```

当前已落地的后端接口：

- `GET /api/health`
- `GET /api/ready`
- `GET /api/product`
- `POST /api/auth/register`
- `POST /api/auth/login`
- `GET /api/auth/session`
- `GET /api/auth/me`
- `POST /api/auth/logout`
- `GET /api/assistant/session`
- `POST /api/assistant/messages`
- `GET /api/assistant/runs/{run_id}/trace`
- `POST /api/onboarding/profile`
- `GET /api/users/me`
- `POST /api/behavior/questionnaires`
- `GET /api/behavior/profile`
- `GET /api/behavior/training-plan`
- `GET /api/dashboard`
- `GET /api/learning/path`
- `GET /api/learning/courses/{course_slug}`
- `POST /api/learning/progress`
- `POST /api/portfolio/snapshots`
- `GET /api/portfolio/latest`
- `GET /api/portfolio/history`
- `GET /api/simulations/scenarios`
- `POST /api/simulations/sessions`
- `GET /api/simulations/sessions/{session_id}`
- `POST /api/simulations/actions`
- `GET /api/simulations/review/{session_id}`
- `GET /api/news`
- `GET /api/news/{item_id}`
- `POST /api/news/analyze`
- `GET /api/news/analyses/{analysis_id}`

当前 onboarding slice 的本地认证约定：

- 前端从 `/start` 进入注册/登录流程。
- 后端通过 `HttpOnly` cookie 维护当前会话。
- 主路径不再使用 `X-FundGene-User-Id`。
- `GET /api/auth/me` 目前保留为兼容别名，但新的调用应使用 `GET /api/auth/session`。

## 5. 启动本地数据库与执行迁移

如果本机 Docker daemon 正常：

```bash
pnpm infra:up
pnpm migrate:api
```

已验证：

- 先前 `pnpm infra:up` 可拉起本地 Postgres / Redis，并且 `pnpm migrate:api` 已成功将本地 PostgreSQL 升级到 `20260402_0005`
- 当前离线 head 是 `20260429_0009`
- 最近一次检查时本机 Docker daemon 不可用，`20260426_0006`、`20260426_0007`、`20260426_0008`、`20260429_0009` 仍需在 Docker daemon 可用时重跑 live local PostgreSQL migration

如果当前只是验证迁移链路，但 Docker daemon 不可用：

```bash
pnpm migrate:api:sql
```

说明：

- `pnpm migrate:api:sql` 只验证 Alembic revision 链和 SQL 展开，不等于真实数据库已经迁移成功。

## 5.1 Docker 不可用时的临时 demo SQLite 启动

这个路径只用于本机 UI demo 和排查 `Failed to fetch`，不代表真实迁移通过，也不能替代 PostgreSQL。

```bash
rm -f /tmp/fundgene-demo.db

FUNDGENE_DATABASE_URL=sqlite:////tmp/fundgene-demo.db \
  uv run --project apps/api python -c "from app import models; from app.core.database import Base, engine; Base.metadata.create_all(bind=engine)"

FUNDGENE_DATABASE_URL=sqlite:////tmp/fundgene-demo.db \
  uv run --project apps/api uvicorn app.main:app --host 0.0.0.0 --port 8000
```

前端默认请求 `http://127.0.0.1:8000`。如果前端页面出现 `Failed to fetch`，先确认 API 是否正在监听：

```bash
curl -sS http://127.0.0.1:8000/api/health
```

## 6. 验证 onboarding + coach + news 最小闭环

启动前后端后，可以用以下最小链路验证真实持久化：

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
      "volatility_comfort": 3,
      "drawdown_reaction": 4,
      "investment_horizon": 4,
      "panic_sell_impulse": 2,
      "chase_hot_funds": 4,
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

curl -sS -X POST http://127.0.0.1:8000/api/news/analyze \
  -H 'Content-Type: application/json' \
  -b "$COOKIE_JAR" \
  -d '{
    "headline": "长期资金入市政策继续推进",
    "body": "政策强调长期资金和资本市场稳定，但具体节奏、落地方式与基金组合影响仍需要进一步观察。"
  }'

curl -sS http://127.0.0.1:8000/api/dashboard \
  -b "$COOKIE_JAR"

curl -sS -X POST http://127.0.0.1:8000/api/auth/logout \
  -b "$COOKIE_JAR"
```

## 6.1 DeepSeek 真实调用冒烟

该命令只有在当前 shell 已设置 `FUNDGENE_DEEPSEEK_API_KEY` 或 `DEEPSEEK_API_KEY` 时才会发起真实 DeepSeek 请求：

```bash
pnpm smoke:deepseek
```

未设置 key 时命令会跳过真实调用，不会失败，也不会写入任何 key。

## 7. 当前真实边界

- `data/dev/fundgene.db` 不能作为启动依赖。
- `archive/legacy-frontend` 与 `archive/legacy-backend` 不能作为主干启动入口。
- `packages/` 与 `services/` 目前仍主要是未来结构，不是当前本地运行依赖。
- DeepSeek 只增强 coach 最终回答和 news/portfolio 的解释文本；新闻事实、组合权重、集中度、风险桶、citation 和安全边界仍由后端规则与持久化数据决定。
