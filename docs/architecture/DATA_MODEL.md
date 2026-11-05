# FundGene 数据模型

状态说明：这是当前数据模型基线。auth / onboarding / questionnaire / behavior / coach / learning / portfolio / simulation / news 的最小持久化表现在已经真实存在于主干。下一阶段数据模型方向是 Agent Command Center：Daily Brief、Agent Workspace run、自动任务、授权与待确认写回。

更新日期：2026-05-17

## 1. 数据设计目标

数据模型要服务 Agent Command Center 主线：

- Today / Daily Brief
- Agent Workspace task runs
- L2 自动任务
- Profile authorization and pending writeback
- 学习、行为、组合、情境模拟、资讯作为 agent 工具和详情来源

设计原则：

- 先围绕业务闭环建模
- 区分“原始输入”和“系统结论”
- Agent 输出必须可追溯
- 报告类结果要保留历史版本
- 高影响状态变化必须先进入 pending confirmation，不从模型输出直接写 canonical state

## 2. 核心实体

### 2.1 用户域

当前已实现：

- `auth_users`
- `auth_sessions`
- `user_profiles`
- `risk_questionnaires`
- `behavior_profiles`

未来仍可能扩展：

- `user_preferences`
- `knowledge_profiles`

### 2.2 学习域

当前已实现：

- `learning_paths`
- `courses`
- `course_sections`
- `user_course_progress`

未来仍可能扩展：

- `learning_recommendations`

### 2.3 组合域

当前已实现：

- `portfolio_snapshots`
- `portfolio_holdings`
- `portfolio_analyses`

未来仍可能扩展：

- `trade_records`
- `rebalance_recommendations`

说明：

- `trade_records` 在当前文档中只表示用户报告的历史交易记录或未来可能的模拟动作记录，不表示实时交易执行能力。

### 2.4 模拟域

当前已实现：

- `scenarios`
- `scenario_events`
- `simulation_sessions`
- `simulation_actions`
- `simulation_reviews`

### 2.5 资讯域

当前已实现：

- `news_items`
- `policy_items`
- `news_analyses`
- `agent_citations`

当前接入说明：

- `GET /api/news?refresh=true` 会从配置的 RSS/Atom feeds 抓取真实条目并 upsert 到 `news_items` / `policy_items`。
- `/news` 前端默认触发真实 feed refresh，并提供手动“同步真实资讯”按钮。
- `news_watch` 自动任务运行前会同步 feeds，再生成通知和待确认 Safe Next Action。
- RSS/Atom 全局条目 `news_items.user_id` 为空；用户手动粘贴内容仍是当前用户私有条目。

### 2.6 Agent 运行域

当前已实现：

- `chat_sessions`
- `chat_messages`
- `agent_runs`
- `agent_citations`

Agent Runtime v2 第一条 deterministic advisor spine 已落地，`20260429_0009` 已加入以下 trace persistence baseline：

- `agent_steps`
- `agent_tool_calls`
- `agent_evidence_refs`
- `agent_state_update_proposals`

同时，`agent_runs` 已补充：

- `run_type`
- `intent`
- `orchestrator_version`
- `policy_status`
- `latency_ms`
- `context_snapshot`
- `context_snapshot_version`

注意：

- 这些表和字段已经被当前 assistant message path 使用：运行时会写入 steps/tool calls/evidence/state proposals/policy status/context snapshot，trace API 会按当前登录用户授权读取，前端 coach panel 会展示。
- 当前未发现独立 `agent_policy_decisions` 表；policy guard 的当前持久化落点应先按 `agent_runs.policy_status` 处理，除非后续迁移明确新增专表。

未来仍可能扩展：

- `agent_context_snapshots`
- `recommendation_records`

### 2.7 Agent Command Center 域

目标新增或重命名：

- `daily_briefs`
- `daily_brief_preferences`
- `agent_workspace_runs`
- `agent_workspace_steps`
- `command_actions`
- `automation_settings`
- `automation_runs`
- `automation_notifications`
- `pending_user_confirmations`
- `profile_authorizations`

说明：

- 当前 `GET /api/dashboard.daily_brief` 和 `agent_runs` 可作为过渡实现资产。
- `daily_briefs` 应沉淀 Agent 自动生成的 Today 主判断、证据、行动和安全边界。
- `agent_workspace_runs` 应表达用户布置的任务，而不只是 chat message。
- `agent_workspace_steps` 应保存用户可读步骤，同时可关联底层 `agent_steps`。
- `command_actions` 应统一 Daily Brief 主动作、assistant recommended action、自动任务结果和详情页动作。
- `automation_settings`、`automation_runs` 和 `daily_brief_preferences` 已在 `20260517_0010` 落地，保存用户授权、频率、读取范围、运行结果和安全边界；`20260517_0011` 继续加入 `automation_notifications`、scheduled/manual trigger metadata、失败错误记录、到期扫描索引，以及后台 worker/CLI 调度路径。
- `automation_notifications` 是用户可见的自动任务完成记录和确认入口，不是外部推送系统；当前通知仍在产品内展示。
- `pending_user_confirmations` 的当前过渡实现复用 `agent_state_update_proposals`，并新增 `user_decision_status`、`decision_note`、`decided_at`、`applied_at`；用户归属通过 `agent_runs.user_id` 校验，只有 allow-listed `behavior_profile_note` 会在用户确认后写入行为证据。
- `profile_authorizations` 应记录 Agent 可读取哪些上下文，以及哪些动作需要确认。

禁止：

- 不建模任何真实交易执行、券商连接、自动买卖或调仓落库能力。
- `trade_records` 类概念如果存在，只能表示用户手动报告的历史行为或模拟动作，不得表示 live execution。

## 3. 当前关键表说明

### `user_profiles`

用途：

- 当前 onboarding slice 的用户基础画像主表
- 承载已认证用户的产品资料与 onboarding 结果

关键字段：

- `id`
- `display_name`
- `investing_experience`
- `monthly_contribution_band`
- `primary_goal`
- `onboarding_completed`
- `created_at`
- `updated_at`

### `auth_users`

用途：

- 承载账号真相：email、password hash，以及与产品画像的关联

关键字段：

- `id`
- `email`
- `password_hash`
- `profile_id`
- `created_at`
- `updated_at`
- `last_login_at`

### `auth_sessions`

用途：

- 承载当前最小正式会话

关键字段：

- `id`
- `user_id`
- `session_token_hash`
- `expires_at`
- `revoked_at`
- `created_at`

### `risk_questionnaires`

用途：

- 存放用户提交的风险与行为问卷原始输入及其风险结果

关键字段：

- `id`
- `user_id`
- `answers`
- `risk_level`
- `risk_score`
- `questionnaire_version`
- `submitted_at`

### `behavior_profiles`

用途：

- 存放当前最小行为偏差画像

关键字段：

- `id`
- `user_id`
- `risk_level`
- `bias_tags`
- `evidence`
- `updated_at`

### `learning_paths`

用途：

- 学习主路径定义表
- 当前 MVP 先承载单条 beginner-first 学习路径

关键字段：

- `id`
- `slug`
- `title`
- `description`
- `created_at`

### `courses`

用途：

- 学习路径下的课程元数据

关键字段：

- `id`
- `path_id`
- `slug`
- `title`
- `focus`
- `description`
- `estimated_duration_minutes`
- `position`
- `created_at`
- `updated_at`

### `course_sections`

用途：

- 某门课程下的细粒度 section 定义

关键字段：

- `id`
- `course_id`
- `slug`
- `title`
- `summary`
- `estimated_duration_minutes`
- `position`
- `created_at`

### `user_course_progress`

用途：

- 记录用户对课程 section 的完成状态

关键字段：

- `id`
- `user_id`
- `course_id`
- `section_id`
- `status`
- `completed_at`
- `created_at`

说明：

- 当前只使用 `completed` 作为最小进度状态，课程与路径完成度由后端聚合计算。

### `portfolio_snapshots`

用途：

- 记录某一时刻的组合整体状态

关键字段：

- `id`
- `user_id`
- `snapshot_date`
- `cash_value`
- `total_value`
- `created_at`

### `portfolio_holdings`

用途：

- 存放组合快照下的持仓明细

关键字段：

- `id`
- `snapshot_id`
- `fund_code`
- `fund_name`
- `fund_type`
- `market_value`
- `weight`
- `created_at`

### `portfolio_analyses`

用途：

- 保存组合解释报告

关键字段：

- `id`
- `user_id`
- `snapshot_id`
- `analysis_version`
- `summary`
- `risk_exposure`
- `concentration_flags`
- `allocation_balance`
- `recommended_next_actions`
- `total_value`
- `generated_at`

### `scenarios`

用途：

- 定义可供新手训练的固定历史情境

关键字段：

- `id`
- `slug`
- `title`
- `summary`
- `bias_focus`
- `difficulty`
- `estimated_duration_minutes`
- `created_at`

### `scenario_events`

用途：

- 定义某条情境中的顺序化决策节点与可选动作

关键字段：

- `id`
- `scenario_id`
- `step_index`
- `date_label`
- `title`
- `narrative`
- `prompt`
- `choices`
- `recommended_choice_key`
- `created_at`

### `simulation_sessions`

用途：

- 记录用户进入某条情境后的训练会话真相

关键字段：

- `id`
- `user_id`
- `scenario_id`
- `status`
- `current_step`
- `started_at`
- `completed_at`

### `simulation_actions`

用途：

- 记录用户在每个情境节点上实际提交的动作与反思文本

关键字段：

- `id`
- `session_id`
- `event_id`
- `step_index`
- `choice_key`
- `choice_label`
- `reflection`
- `is_recommended`
- `created_at`

### `simulation_reviews`

用途：

- 保存一次已完成情境训练的结构化复盘结果

关键字段：

- `id`
- `session_id`
- `decision_summary`
- `bias_observations`
- `coach_feedback`
- `recommended_next_actions`
- `generated_at`

### `news_items`

用途：

- 存放全局新闻 feed 条目，以及当前用户手动粘贴的私有新闻条目

关键字段：

- `id`
- `user_id`
- `source_name`
- `source_url`
- `external_id`
- `title`
- `summary`
- `url`
- `published_at`
- `fetched_at`

说明：

- RSS/Atom feed 条目的 `user_id` 为空，表示全局可见。
- 手动粘贴条目的 `user_id` 指向当前 `user_profiles.id`，用于避免把用户粘贴内容泄露给其他用户。

### `policy_items`

用途：

- 存放被识别为政策或监管相关的 feed 条目

关键字段：

- `id`
- `source_name`
- `source_url`
- `external_id`
- `title`
- `summary`
- `url`
- `policy_area`
- `published_at`
- `fetched_at`

### `news_analyses`

用途：

- 保存一次新闻或政策解读的结构化结果

关键字段：

- `id`
- `user_id`
- `news_item_id`
- `policy_item_id`
- `analysis_version`
- `facts`
- `impact_paths`
- `uncertainty_notes`
- `beginner_translation`
- `related_learning_topics`
- `recommended_next_actions`
- `risk_notice`
- `generated_at`

说明：

- 当前约束要求 `news_item_id` 与 `policy_item_id` 二选一。
- 用户手动粘贴内容会先转成当前用户私有 `news_items`，再生成 `news_analyses`。

### `agent_citations`

用途：

- 保存新闻/政策解读使用的来源引用，当前先绑定 `news_analyses`

关键字段：

- `id`
- `analysis_id`
- `source_type`
- `source_item_id`
- `source_name`
- `title`
- `url`
- `created_at`

### `chat_sessions`

用途：

- AI 助手的会话容器

关键字段：

- `id`
- `user_id`
- `topic`
- `context_type`
- `created_at`
- `updated_at`

### `chat_messages`

用途：

- AI 助手会话中的消息真相

关键字段：

- `id`
- `session_id`
- `role`
- `content`
- `message_type`
- `structured_payload`
- `agent_run_id`
- `created_at`

### `agent_runs`

用途：

- 保存一次 advisor 运行的输入、输出和 coarse trace

关键字段：

- `id`
- `session_id`
- `user_id`
- `model_name`
- `schema_version`
- `run_status`
- `input_payload`
- `output_payload`
- `tool_trace`
- `fallback_reason`
- `started_at`
- `completed_at`
- `created_at`

Agent Runtime v2 当前补充：

- `agent_steps`：记录 classify intent、load context、select tools、execute worker、policy guard、compose response 等步骤
- `agent_tool_calls`：记录内部工具名、输入摘要、输出摘要、耗时、错误、timeout 和权限结果
- `agent_evidence_refs`：记录本次回答引用的用户画像、组合报告、模拟复盘、课程片段、新闻/政策来源等证据
- `agent_state_update_proposals`：记录建议写回状态、目标对象、patch、理由与验证状态
- `agent_runs.policy_status`：记录 policy guard 的 coarse 状态，例如 `allow`、`revise`、`block_with_guidance`
- `agent_runs.context_snapshot` 与 `context_snapshot_version`：记录本次回答使用到的上下文版本，避免未来状态变化后无法复盘旧回答

落地规则：

- `agent_runs` 仍是 advisor run 主表。
- `agent_steps`、`agent_tool_calls`、`agent_evidence_refs` 和 `agent_state_update_proposals` 已通过 service 写入、trace API、API 测试和前端挂载验证，构成当前 deterministic v2 trace spine。
- 前端 trace panel 只能读取后端授权后的 trace API，不应直接拼接或推断内部执行轨迹。

## 4. 当前基线说明

- 当前代码基线已经通过测试和离线 Alembic 展开验证到 revision `20260429_0009`。
- 先前 live local PostgreSQL upgrade 已验证到 `20260402_0005`。
- 新增 learning / portfolio / simulation / news / trace 表的 live local PostgreSQL upgrade 仍需在 Docker daemon 可用时重跑验证。
- Agent Runtime v2 trace migration 不能替代上述 live migration 缺口；`20260429_0009` 仍必须在 Docker 可用后通过 live PostgreSQL upgrade。
