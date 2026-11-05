# FundGene Web

这是 FundGene 当前唯一的前端主应用根。

当前基线：

- Next.js 16
- React 19
- TypeScript
- App Router
- Tailwind CSS 4

当前已落地路由：

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

从仓库根运行：

```bash
pnpm dev:web
pnpm lint:web
pnpm build:web
pnpm test:web:e2e
pnpm test:web:a11y
```

约束：

- 不要复用 `archive/legacy-frontend` 的视觉、路由和组件层级。
- 优先在 `apps/web` 内聚演进，不要过早抽 `packages/ui`。
- 业务真相和 schema 仍应以后端为准。
- 账号会话走后端 `HttpOnly` cookie；不要回退到 `X-FundGene-User-Id`。
