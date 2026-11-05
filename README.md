# FundGene

> 面向基金投资新手的 AI 投资陪练与安全决策工作台。

FundGene 帮助基金投资新手把分散的持仓、新闻、政策、学习进度和行为线索整理成清晰的每日判断、解释过程和安全下一步。它不是券商软件，不做交易执行，也不承诺收益；它的核心目标是帮助用户先理解、再判断、最后谨慎行动。

## 产品定位

新手投资者真正缺的往往不是更多信息，而是一个稳定的判断框架。新闻标题、短期回撤、政策变化、朋友建议和平台热榜很容易混在一起，最后变成冲动操作。

FundGene 试图提供一个更稳的流程：

1. 先整理用户的真实投资上下文。
2. 每天给出最重要的一条判断。
3. 用新手能理解的语言解释原因。
4. 展示系统参考过的证据与影响路径。
5. 给出一个安全下一步，而不是直接买卖指令。
6. 对会改变用户画像或长期记录的内容，先生成待确认提案。
7. 通过学习与历史情景训练，让用户逐步提升判断力。

## 核心体验

### Today：每日简报

打开产品后，用户首先看到的是当天最值得关注的判断：发生了什么、为什么重要、和自己的组合或学习状态有什么关系、现在不要做什么、可以做的安全下一步是什么。

### Workspace：投资助理工作台

用户可以直接提出实际问题，例如：

- “这周我的组合为什么波动？”
- “这条政策新闻会影响我持有的基金吗？”
- “我是不是太集中在某个行业或主题了？”
- “调整配置前，我应该先补哪块知识？”

回答会围绕结论、解释、证据、风险边界和下一步行动组织，而不是给出刺激性的买卖建议。

### Automations：授权后台检查

FundGene 可以在用户授权后运行固定检查，例如每日简报、每周组合复盘、新闻影响观察和行为线索观察。自动化只负责分析、总结、提醒和生成待确认提案，不会替用户下单，也不会静默修改关键画像。

### Profile：个人投资上下文

Profile 保存风险偏好、持仓快照、行为证据、学习进度、模型设置、授权范围和待确认写回。系统会区分“原始输入”“推断结论”和“用户确认后的长期记录”，避免一次弱证据直接改变用户画像。

### Learning / Portfolio / News / Simulation

这些模块不是孤立功能，而是投资助理可以调用的工具：

- Learning：把知识缺口转成小的学习任务。
- Portfolio：解释资产配置、集中度、风险来源和历史报告。
- News：把新闻与政策变化连接到用户的真实上下文。
- Simulation：用历史情景训练用户的决策过程和行为反思。

## 安全边界

FundGene 的产品边界非常明确：

- 不执行交易。
- 不承诺收益。
- 不把安全下一步包装成买卖指令。
- 不鼓励高风险、短线或情绪化操作。
- 不用一次对话或一次训练结果静默重写用户长期画像。
- 关键建议必须有解释、证据、风险提示和确认流程。

FundGene 希望给出的不是“立刻买什么”，而是“现在应该先看清什么、避免什么、下一步怎样更稳”。

## 当前能力

当前仓库已经包含一个可运行的全栈原型，覆盖了 FundGene 的主要产品闭环：

- 账号入口与新手 onboarding。
- Today 每日简报与个人化安全下一步。
- 投资助理工作台与多轮问答。
- 学习路径、课程详情和完成状态回写。
- 手动持仓快照录入、组合分析和历史报告。
- 历史情景模拟、行为复盘和待确认画像提案。
- 新闻/政策列表、结构化解读和真实 RSS/Atom 源同步。
- 自动化设置、手动运行、调度基础和通知记录。
- Profile 上下文中心、模型设置、授权范围和待确认提案处理。
- 可持久化的运行记录、事件回放、追踪数据和回归测试。

它仍然是研究与产品原型，不是生产级金融服务。当前重点是把解释、约束、数据持久化和用户确认流程做扎实。

## 技术栈

### Frontend

- Next.js 16
- React 19
- TypeScript
- App Router
- Tailwind CSS 4
- TanStack Query
- Zod
- Apache ECharts

### Backend

- Python 3.12+
- FastAPI
- SQLAlchemy 2.x
- Alembic
- PydanticAI
- PostgreSQL

### Tooling

- pnpm workspace
- Docker local infrastructure
- GitHub Actions
- Playwright E2E / accessibility checks
- pytest API tests
- Alembic offline SQL validation

## 仓库结构

```text
FundGene/
├── apps/
│   ├── api/          # FastAPI 后端、领域服务、持久化与运行时
│   └── web/          # Next.js 前端应用
├── docs/             # 产品、架构、API、审计与设计文档
├── infra/            # 本地基础设施与部署脚手架
├── packages/         # 共享包与未来工作区库
├── services/         # 评测与服务侧支持模块
├── data/             # 开发 fixture 与参考数据
└── archive/          # 历史原型与恢复材料
```

## 本地启动

安装依赖：

```bash
pnpm install
```

启动前端：

```bash
pnpm dev:web
```

启动后端：

```bash
pnpm sync:api
pnpm dev:api
```

需要本地数据库等基础设施时：

```bash
pnpm infra:up
pnpm migrate:api
```

## 验证命令

```bash
pnpm lint:web
pnpm build:web
pnpm test:web:e2e
pnpm test:web:a11y
pnpm test:api
pnpm test:agent-evals
pnpm migrate:api:sql
```

需要真实模型凭证的检查不进入默认 CI：

```bash
pnpm smoke:deepseek
```

## 文档入口

- [项目总览](./PROJECT_OVERVIEW.md)
- [本地启动指南](./LOCAL_STARTUP_GUIDE.md)
- [本地测试指南](./LOCAL_TESTING_GUIDE.md)
- [产品蓝图](./docs/product/PRODUCT_BLUEPRINT.md)
- [系统架构](./docs/architecture/SYSTEM_ARCHITECTURE.md)
- [数据模型](./docs/architecture/DATA_MODEL.md)
- [API 契约](./docs/api/API_CONTRACT.md)

## 一句话

FundGene 是一个让基金投资新手慢下来、看清楚、学会判断的 AI 投资陪练。
