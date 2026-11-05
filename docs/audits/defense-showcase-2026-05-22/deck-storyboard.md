# FundGene 答辩展示 Deck Storyboard

Date: 2026-05-22
Status: Draft v1 for PPT / Keynote / product video

## Deck Direction

答辩主线不要讲“我们做了很多页面”，而要讲：

> FundGene 把画像、组合、资讯、学习和训练记录收束成一个可追踪的投资教练 Agent。它先替新手做第一轮判断，再让用户追问、训练和确认安全下一步。

建议最终材料由三层组成：

1. 60-90 秒主视频：开场展示完整产品闭环。
2. 4-5 个循环短片：插在功能页里，展示特色交互。
3. 1-2 页技术架构：解释为什么这是一个工程系统，不是普通聊天界面。

## Slide Plan

### 1. Cover

Title:

```text
FundGene
新手基金投资的 Agent Command Center
```

Subtitle:

```text
每日判断、可解释追问、行为训练和确认式写回
```

Visual:

- Background: use `assets/png/showcase-contact-sheet.png` as a blurred/cropped strip or use the Today poster as hero screenshot.
- Keep the UI visible; avoid generic AI decoration.

Speaker point:

```text
FundGene 不是一个泛化聊天机器人，也不是交易系统。我们做的是一个面向基金投资新手的可追踪教练工作台。
```

### 2. User Problem

Claim:

```text
新手投资者缺的不是更多信息，而是把信息变成安全判断的流程。
```

Proof bullets:

- 资讯太多：新闻、政策、市场情绪很容易被误读成操作信号。
- 组合难懂：持仓集中度、资产类型、现金缓冲很难自己拆解。
- 行为失控：追热点、回撤焦虑、冲动调整往往发生在解释之前。
- 学习割裂：课程、模拟训练和真实组合没有连成日常判断。

Visual:

- Four-lane tension diagram: 资讯 / 组合 / 行为 / 学习 all feeding into "判断压力".

Speaker point:

```text
所以我们的产品目标不是替用户下单，而是先建立一条解释、训练、确认的路径。
```

### 3. Product Thesis

Claim:

```text
FundGene 的核心闭环：先看今日判断，再让 Agent 深挖，最后只保存用户确认过的变化。
```

Flow:

```text
授权上下文 -> 今日简报 -> Agent 追问 -> 安全下一步 -> 待确认写回 -> 下一次简报
```

Visual:

- Use a horizontal flow with six nodes.
- Mark "用户确认" as the only writeback gate.

Asset:

- `assets/mp4/01-hero-command-center.mp4`

Speaker point:

```text
这个闭环决定了整个产品的信息架构：Today 是默认首页，Agent 是任务执行面，自动任务负责后台整理，资料中心负责确认边界。
```

### 4. Feature 1: Today Daily Brief

Claim:

```text
用户打开产品时，Agent 已经完成第一轮整理。
```

Show:

- headline judgment
- evidence
- one safe next action
- safety boundary

Asset:

- `assets/mp4/02-today-daily-brief.mp4`
- GIF fallback: `assets/gif/02-today-daily-brief.gif`

Speaker point:

```text
这个页面把传统 dashboard 变成了判断入口。用户不用先翻模块，而是先看到今天最值得处理的一件事。
```

### 5. Feature 2: Agent Workspace

Claim:

```text
Agent 不是自由发挥回答，而是按上下文和工具链执行任务。
```

Show:

- user asks: "最近新闻对我的资产有什么影响？"
- process panel: read profile, inspect portfolio, review news, prepare safe action
- final answer: explanation, uncertainty, safe next action

Asset:

- `assets/mp4/03-agent-workspace.mp4`
- GIF fallback: `assets/gif/03-agent-workspace.gif`

Speaker point:

```text
我们把内部工具、证据和风控结果收束成用户能读懂的步骤。普通用户不需要看到原始 trace，但系统保留可审计记录。
```

### 6. Feature 3: News Impact Analysis

Claim:

```text
资讯不是列表，而是和用户组合相关的影响路径。
```

Show:

- source list
- selected item
- fact / impact path / uncertainty
- "让 Agent 结合我的组合解释"

Asset:

- `assets/mp4/04-news-impact.mp4`
- GIF fallback: `assets/gif/04-news-impact.gif`

Speaker point:

```text
对新手来说，新闻最大的风险是把标题当成操作理由。FundGene 会先拆事实和不确定性，再回到用户自己的组合暴露。
```

### 7. Feature 4: Simulation Training

Claim:

```text
FundGene 不只回答问题，还训练用户在压力场景下的判断纪律。
```

Show:

- historical scenario
- market replay chart
- decision task
- rationale, worry, impulse-control plan
- behavior evidence candidate

Asset:

- `assets/mp4/05-simulation-training.mp4`
- GIF fallback: `assets/gif/05-simulation-training.gif`

Speaker point:

```text
训练结果不会直接改写用户画像，而是生成候选线索，后续仍然需要用户确认或更多证据支持。
```

### 8. Feature 5: Automations + Profile

Claim:

```text
后台自动化只能读取、整理和提醒；高影响状态必须由用户确认。
```

Show:

- automation cards
- cadence and read scope
- run result
- pending profile confirmation
- accept / reject boundary

Asset:

- `assets/mp4/06-automation-profile.mp4`
- GIF fallback: `assets/gif/06-automation-profile.gif`

Speaker point:

```text
这页体现安全边界：自动任务可以帮用户持续观察，但不能替用户交易，也不能静默改写关键画像。
```

### 9. Technical Contribution

Claim:

```text
FundGene 的工程贡献是一个可追踪、可恢复、可约束的金融学习 Agent Runtime。
```

Proof objects:

- single user-facing agent entry
- typed worker outputs
- persisted agent runs and event replay
- backend-owned safe next actions
- user-confirmed state update proposals
- eval and UI checks

Suggested diagram:

```text
Frontend Command Center
  -> FastAPI session/auth
  -> Agent Runtime v2
  -> Domain tools: portfolio / news / learning / simulation / profile
  -> SQL persistence: runs / traces / proposals / notifications
  -> Safety policy and eval gates
```

Speaker point:

```text
我们没有把业务规则写在前端，也没有让模型直接决定状态变化。关键判断通过 schema、服务层和确认工作流落到后端。
```

### 10. Verification

Claim:

```text
展示素材和核心页面可以重复生成、重复验证。
```

Evidence:

- `pnpm lint:web`
- `showcase-recording.spec.ts`: 6 passed
- `convert-assets.sh`: generated MP4 / GIF / poster / contact sheet
- existing product tests: e2e, accessibility, API, agent evals as needed

Visual:

- Contact sheet: `assets/png/showcase-contact-sheet.png`

Speaker point:

```text
这不是一次手动摆拍。我们把演示数据、浏览器操作和素材转码做成了可重复流水线，后续改界面可以一键重录。
```

### 11. Closing

Claim:

```text
FundGene 的价值是让新手投资者先理解、再判断、最后确认。
```

Closing line:

```text
它不是交易机器人，而是一个有证据、有边界、有训练闭环的投资教练 Agent。
```

Visual:

- Today poster or hero clip end frame.

## Asset Placement

| Slide | Primary asset | Notes |
| --- | --- | --- |
| 1 | `assets/png/01-hero-command-center-poster.png` | Cover background or UI hero |
| 3 | `assets/mp4/01-hero-command-center.mp4` | Main video |
| 4 | `assets/mp4/02-today-daily-brief.mp4` | Loop |
| 5 | `assets/mp4/03-agent-workspace.mp4` | Loop |
| 6 | `assets/mp4/04-news-impact.mp4` | Loop |
| 7 | `assets/mp4/05-simulation-training.mp4` | Loop |
| 8 | `assets/mp4/06-automation-profile.mp4` | Loop |
| 10 | `assets/png/showcase-contact-sheet.png` | Verification/contact sheet |

## Final Polish Checklist

- Keep each feature slide to one claim and one proof object.
- Use MP4 as primary media in PPT; use GIF only when playback compatibility is uncertain.
- Avoid saying "AI recommends buying/selling"; always say "解释、检查、训练、确认".
- Do not show raw trace IDs or database table names to beginner users.
- Show architecture only after product value is clear.
- End with the safety boundary, not with a feature list.
