# FundGene V1 Agent Command Center 产品蓝图

更新日期：2026-05-17
状态：产品总设计基线。与 `AGENTS.md` 和 `PROJECT_OVERVIEW.md` 保持一致；本文件取代旧的“多模块并列”产品蓝图。

## 1. 产品定位

FundGene V1 是面向基金投资新手的 Agent Command Center。

一句话：

> FundGene 不是让用户自己翻学习、组合、资讯和模拟模块，而是让 Agent 先替用户看完资料、形成判断、整理证据、提出安全选项，用户负责确认、选择和补充偏好。

核心转变：

- 从“模块导航型产品”转为“Agent 指挥台型产品”。
- 从“用户主动找功能”转为“Agent 先完成巡检和排序”。
- 从“AI 问答页”转为“Agent 执行任务的工作台”。
- 从“推荐动作”转为“安全选项 + 用户确认”。
- 旧 `/learning`、`/portfolio`、`/simulation`、`/news` 不再是一级页面，而是 Agent 可调用的工具、任务来源和详情承接页。

产品人格应当是：

- 主动
- 克制
- 透明
- 纪律性强
- 解释优先
- 确认后写回

## 2. 目标用户

主用户：

- 初学或早期基金投资者
- 金融词汇和框架薄弱
- 容易被涨跌和热点信息带节奏
- 不知道新闻、组合、风险偏好和自身行为之间有什么关系
- 希望有人先替自己整理重点，再让自己做选择

典型问题：

- “今天有什么和我有关的市场变化？”
- “这些新闻对我的持仓有什么影响？”
- “我的组合现在最需要注意什么？”
- “我这周应该先做哪件事？”
- “我最近是不是又有追涨倾向？”

## 3. 核心问题

FundGene 要解决的不是“信息缺失”本身，而是以下六类真实痛点：

1. 认知缺口：概念和风险框架混乱。
2. 风险错觉：把短期波动误当成能力或灾难。
3. 噪音过载：信息很多，但不知道与自己有什么关系。
4. 组合不可读：看不懂结构、暴露、集中度与缺口。
5. 偏差不可见：追涨杀跌、损失厌恶等重复出现却没有复盘。
6. 执行负担：知道需要分析，但不想每天自己翻模块、读资讯、算影响。

## 4. V1 一级信息架构

一级导航只保留四个：

| 页面 | 定位 | 主要对象 | 从旧模块吸收什么 |
| --- | --- | --- | --- |
| `/today` 今日简报 | 用户每天进入后的主 home | Daily Brief、证据、今日安全行动 | dashboard、news、portfolio、learning、simulation 的综合结果 |
| `/agent` 工作台 | Agent 执行任务、解释、追问、确认的主界面 | Agent Run、任务队列、对话、确认卡片 | coach、trace、recommended action |
| `/automations` 自动任务 | 用户配置 Agent 定期替自己看什么 | Automation Rule、权限、触发器、通知 | news 监控、组合巡检、学习提醒、模拟复盘 |
| `/profile` 我的资料 | Agent 判断所依赖的用户事实和偏好 | 用户画像、风险问卷、行为证据、偏好、授权 | onboarding、behavior profile、账户状态 |

旧页面处理：

- `/dashboard` 重命名或重定向到 `/today`。
- `/coach` 重命名或重定向到 `/agent`。
- `/learning`、`/portfolio`、`/simulation`、`/news` 降级为工具详情页，可从 Agent 任务卡、证据卡、自动任务结果进入。
- `/start` 保留为登录与新用户入口，但不进入主 IA。

当前仓库还没有完全迁移到上述路由时，未来 agent 应把当前 `/dashboard` 视为 `/today` 的前身，把当前 `/coach` 视为 `/agent` 的前身，不要把旧导航当成长期产品方向。

## 5. 核心任务闭环

V1 闭环固定为：

```text
Agent 观察 -> Agent 判断 -> Agent 给证据 -> Agent 给选项 -> 用户确认/选择 -> 写回状态 -> 下次自动纳入判断
```

### 5.1 今日简报闭环

1. Agent 汇总画像、组合、资讯、学习、模拟和行为记录。
2. 生成一个今日主判断、最多三条证据、一个安全下一步和一个 `do_not_do` 边界。
3. 用户选择“去检查组合 / 去学习 / 去问 Agent / 暂不处理”。
4. 系统写回 action completion、dismissal 或 follow-up intent。

### 5.2 组合巡检闭环

1. Agent 定期检查组合集中度、权益占比、现金比例、近期变化。
2. 只输出“需要理解/检查什么”，不输出买卖指令。
3. 用户确认是否查看详情、保存快照、追问原因。
4. 系统写回 portfolio snapshot、review result、用户反馈或下一次巡检偏好。

### 5.3 资讯影响闭环

1. Agent 先读新闻/政策，不让用户自己刷列表。
2. 只挑“与我有关”的影响路径。
3. 用户选择“忽略 / 学习背景 / 问 Agent / 加入自动观察”。
4. 系统写回 news analysis、interest signal 或 automation preference。

### 5.4 行为偏差闭环

1. Agent 从对话、模拟理由、组合操作记录或用户自述中识别偏差信号。
2. 系统生成行为证据候选，而不是立即给用户贴标签。
3. 用户确认、修正或拒绝。
4. 确认后才写入画像或训练计划。

## 6. V1 自动化范围

V1 做 L1 + L2。

### L1：用户主动任务

用户直接让 Agent 做事，例如：

- “分析最近新闻对我持仓的影响。”
- “检查我的组合风险。”
- “生成本周行动计划。”
- “解释我最近是不是在追涨。”

Agent 应展示自然语言执行过程，并返回结构化结论、证据、安全边界和下一步。

### L2：默认每日简报

系统默认生成 Daily Brief，但用户可以在 `/automations` 里关闭或调整。

Daily Brief 必须包含：

- 一句话主判断
- 为什么和我有关
- 最多三条证据
- 一个安全下一步
- 一个今天不要做什么
- 生成时间和覆盖的数据来源

### L3：主动监控

L3 只写入蓝图，不进入 V1 必做范围。未来可做风险变化、行为偏差和新闻冲击提醒，但仍必须要求用户确认关键写回，不得自动交易。

## 7. 自动化权限边界

自动化的产品语言必须是：

> Agent 替你看完，提醒你确认，不替你交易。

V1 允许：

- 定时生成今日简报。
- 组合健康巡检。
- 新闻/政策影响观察。
- 学习进度提醒。
- 模拟训练复盘提醒。
- 生成待确认的行为画像更新建议。
- 生成待确认的下一步任务。

V1 禁止：

- 自动买入、卖出、调仓、清仓、满仓。
- 生成具体金额或仓位指令。
- 对收益做承诺。
- 将一次行为直接写入 canonical profile。
- 在用户未确认前改变关键用户状态。
- 接入券商、支付、真实交易执行。

权限层级：

| 权限 | V1 是否允许 | 说明 |
| --- | --- | --- |
| Read | 允许 | 读取用户画像、组合快照、学习进度、新闻、模拟记录 |
| Analyze | 允许 | 生成判断、证据、风险边界 |
| Notify | 允许 | 通知用户“Agent 已看完，有一项需要确认” |
| Prepare | 允许 | 准备任务、草稿、待确认建议 |
| Writeback | 条件允许 | 仅写学习进度、任务状态、用户确认后的偏好或行为证据 |
| Execute | 禁止 | 不执行任何交易或金融账户动作 |

## 8. Agent Trace 展示策略

不要把 trace 直接暴露成开发日志。V1 分三层：

| 层级 | 面向谁 | 展示什么 |
| --- | --- | --- |
| 用户默认层 | 新手用户 | Agent 看了哪些来源、形成了什么判断、哪些地方不确定 |
| 用户展开层 | 想追溯的用户 | 证据来源、更新时间、支持强度、安全边界 |
| Dev/Audit 层 | 开发与评审 | run id、steps、tool calls、worker output、policy status、latency |

默认用户文案应类似：

- “我已检查：你的画像、最近组合、今日资讯、学习进度。”
- “这个判断主要基于 3 条证据。”
- “这条证据不能推出买卖动作。”
- “需要你确认后，我才会把它记入你的资料。”

避免默认展示：

- run id
- tool name
- worker name
- raw evidence key
- prompt / internal payload
- orchestrator step 名称

## 9. 产品对象

### 9.1 Daily Brief

Daily Brief 是 FundGene 的主 home 对象，不是普通 dashboard 摘要。

MVP 字段：

- `brief_id`
- `as_of`
- `status`
- `priority_level`
- `headline`
- `beginner_explanation`
- `evidence[]`
- `primary_action`
- `secondary_actions[]`
- `do_not_do`
- `source_coverage`
- `trace_id`

### 9.2 Command Action

`CommandAction` 统一 Daily Brief、Agent Workspace、自动任务和旧模块详情页的动作。

MVP 字段：

- `label`
- `reason`
- `target_route`
- `target_params`
- `expected_writeback`
- `requires_confirmation`
- `safety_note`

动作只能指向理解、检查、学习、训练、记录、追问或授权，不得指向交易执行。

### 9.3 Evidence

Evidence 是 Agent 判断可解释性的最小单位。

MVP 字段：

- `id`
- `source_type`
- `source_id`
- `claim`
- `beginner_translation`
- `support_level`
- `freshness_label`
- `risk_boundary`

### 9.4 Automation Rule

Automation Rule 定义 Agent 被授权定期做什么。

MVP 字段：

- `id`
- `name`
- `enabled`
- `frequency`
- `reads`
- `produces`
- `requires_confirmation_for`
- `last_run_at`
- `next_run_at`
- `safety_boundary`

### 9.5 Behavior Evidence

Behavior Evidence 支持行为画像推理，但不允许从一次事件直接定性用户。

MVP 字段：

- `behavior_evidence_id`
- `bias_type`
- `observed_signal`
- `source_event`
- `confidence`
- `pending_state_proposal_id`

## 10. 重构优先级

### P0：先改信息架构，不先重做所有功能

1. 路由与导航重命名：`/dashboard -> /today`，`/coach -> /agent`。
2. 新增 `/automations` 壳页面，先做规则展示和权限边界，不做复杂后台调度。
3. `/profile` 合并 onboarding、风险画像、行为证据、偏好设置。
4. 旧模块从一级导航移除，变成 Agent action 的目标页。

### P1：统一任务对象

把 `daily_brief.primary_action`、`recommended_action_targets`、自动任务结果统一成 `CommandAction`。

### P2：自动化 MVP

- 每日简报自动生成。
- 组合巡检自动生成。
- 新闻影响观察自动生成。
- 用户可以开启/关闭、选择频率、确认写回。

### P3：Trace 产品化

- `/agent` 显示“Agent 已完成的工作”。
- 用户看到可解释摘要。
- Dev/Audit 模式再看完整 trace。

## 11. 第一阶段验收标准

第一阶段不以旧模块数量为验收标准，而以 Agent Command Center 闭环为标准：

1. 用户登录后默认进入 Today/Daily Brief。
2. Daily Brief 显示一个判断、最多三条证据、一个安全下一步和一个 `do_not_do`。
3. 用户能从 Daily Brief 进入 Agent Workspace 继续分析。
4. Agent Workspace 展示自然语言执行过程。
5. 高级模式可查看 trace/tool/evidence/policy 信息。
6. 自动任务页能说明并控制 Daily Brief、组合巡检、资讯观察的授权。
7. 旧模块不再作为主导航，但能作为 Agent action 的详情落点。
8. 所有高影响写回都需要用户确认。

底线判断：

> 这版 V1 不应该做成“四个新页面 + 旧功能搬家”。它应该明确表达：Agent 是主角，模块是工具；用户不是分析员，用户是确认者和选择者。
