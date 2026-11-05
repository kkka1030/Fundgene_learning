# FundGene API 契约

状态说明：

- 记录当前已经落地并验证过的 API baseline
- 记录后续阶段仍计划实现、但当前尚未落地的接口方向

更新日期：2026-05-17

## 1. 设计原则

- REST 为主
- 响应结构化
- 业务真相以后端 schema 与持久化为准
- 重要分析结果必须可落库
- 当前主路径已经完成最小正式认证基线，并打通 coach、learning、portfolio、simulation、news 的真实持久化闭环
- Agent Runtime v2 第一条 deterministic advisor spine 已落地，但仍应保持现有 assistant 对外入口稳定；新增 trace / debug / eval 能力时不得破坏当前 user-facing 契约
- v2 trace API、runtime writes、policy guard 和前端 trace panel 闭环已经按当前代码、迁移和测试落地；后续 eval / RAG / observability 扩展必须继续按真实代码更新本文档
- Agent Command Center 后端第一批契约已经落地：自动任务授权/运行记录、Profile 聚合上下文、以及基于 `agent_state_update_proposals` 的待确认写回 accept/reject/apply 流程。

统一前缀：

- `/api`

## 2. 当前实现中的身份约定

当前主路径已经不再使用 `X-FundGene-User-Id`。

本地开发与当前前端接入约定：

- 认证入口：`/start`
- 后端认证形态：邮箱 + password hash + 持久化 session
- 会话载体：`HttpOnly` cookie
- 当前会话读取主入口：`GET /api/auth/session`
- 兼容别名：`GET /api/auth/me`

说明：

- `GET /api/auth/me` 仍然保留，但新调用应收敛到 `GET /api/auth/session`
- onboarding、behavior、dashboard、learning、portfolio、simulation、assistant 等主路径接口都基于当前会话识别用户，不再要求前端传本地 user id

## 3. 当前已落地并验证的接口

### `GET /api/health`

用途：

- 健康检查

响应重点：

- `status`
- `service`
- `environment`
- `timestamp`

### `GET /api/ready`

用途：

- 运行时 readiness 检查
- 当前会执行最小数据库连通性检查

响应重点：

- `status`
- `service`
- `environment`
- `database`
- `timestamp`

### `GET /api/product`

用途：

- 返回产品元信息和模块列表

### `POST /api/auth/register`

用途：

- 创建最小账号并建立当前会话

请求：

- `email`
- `password`

响应重点：

- `user.id`
- `user.email`
- `user.profile_id`
- `user.onboarding_completed`
- `user.profile`

### `POST /api/auth/login`

用途：

- 使用已存在账号建立当前会话

请求：

- `email`
- `password`

响应重点：

- `user.id`
- `user.email`
- `user.profile_id`
- `user.onboarding_completed`
- `user.profile`

### `GET /api/auth/session`

用途：

- 读取当前真实会话用户

行为约定：

- 未登录或会话失效时返回 `401`

响应重点：

- `session_expires_at`
- `user.id`
- `user.email`
- `user.profile_id`
- `user.onboarding_completed`
- `user.profile`

### `GET /api/auth/me`

用途：

- `GET /api/auth/session` 的兼容别名

### `POST /api/auth/logout`

用途：

- 失效当前会话并清理 cookie

响应重点：

- `success`
- `signed_out`

### `POST /api/onboarding/profile`

用途：

- 创建或更新当前登录用户的基础画像

认证：

- 需要有效 session cookie

请求：

- `display_name`
- `investing_experience`
- `monthly_contribution_band`
- `primary_goal`

### `GET /api/users/me`

用途：

- 获取当前登录用户的基础画像

认证：

- 需要有效 session cookie

行为约定：

- 如果账号已登录但还没有完成基础画像创建，返回 `404`
- 后端不会在读取时偷偷创建用户画像

### `POST /api/behavior/questionnaires`

用途：

- 提交第一版风险与行为问卷

认证：

- 需要有效 session cookie

请求：

- `questionnaire_version`
- `answers`

当前 `answers` 约定：

- value 为 `1` 到 `5` 的整数分值
- 当前前端工作流使用的 key 包括：
  - `volatility_comfort`
  - `drawdown_reaction`
  - `investment_horizon`
  - `panic_sell_impulse`
  - `chase_hot_funds`
  - `diversification_habit`

### `GET /api/behavior/profile`

用途：

- 获取当前登录用户的最小行为画像

认证：

- 需要有效 session cookie

响应重点：

- `user_id`
- `risk_level`
- `bias_tags`
- `evidence`
- `updated_at`

### `GET /api/behavior/training-plan`

用途：

- 获取当前登录用户的行为训练焦点与推荐情境

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

响应重点：

- `user_id`
- `focus_bias_tag`
- `focus_title`
- `guidance`
- `recommended_scenario_slug`
- `recent_review_summary`
- `next_actions`

### `GET /api/dashboard`

用途：

- 获取 dashboard 所需最小聚合状态

认证：

- 需要有效 session cookie

响应重点：

- `user_id`
- `onboarding_completed`
- `risk_level`
- `bias_tags`
- `latest_risk_score`
- `daily_brief`
- `next_actions`
- `summary_cards`
- `learning_status`
- `portfolio_status`
- `simulation_status`
- `news_status`
- `latest_coach_activity`

说明：

- 当前 dashboard 已经同时聚合问卷、coach、learning、portfolio、simulation、news 的真实状态
- `daily_brief` 是 `/dashboard` 的主 home 对象，后端规则生成，前端只渲染，不自行推断个人判断
- `daily_brief.status` 取值：`ready`、`starter`、`missing_profile`、`missing_portfolio`、`fallback`
- `daily_brief.evidence[]` 最多用于首屏展示 3 条证据，每条包含 `source_type`、`claim`、`beginner_translation`、`support_level`、`freshness_label`、`risk_boundary`
- `daily_brief.primary_action` 是 Safe Next Action，包含 `type`、`label`、`reason`、`target_route`、`target_params`、`expected_writeback`、`safety_note`
- `daily_brief.do_not_do` 必须说明安全边界，避免用户把简报理解成账户操作指令
- `daily_brief.trace_id` 只用于审计或开发，不应作为普通新手 UI 的主要展示内容

### `GET /api/automations`

用途：

- 读取当前登录用户的 Agent Command Center 自动任务授权状态
- 首次读取时会为当前用户创建默认自动任务设置和 Daily Brief 偏好

认证：

- 需要有效 session cookie
- 当前要求用户已完成 onboarding / profile 基线

响应重点：

- `active_count`
- `total_count`
- `automations[]`
  - `key`
  - `title`
  - `enabled`
  - `cadence_key`
  - `cadence_label`
  - `read_scope`
  - `output_scope`
  - `confirmation_boundary`
  - `safety_boundary`
  - `last_run`
  - `next_run_at`
  - `can_run_now`
- `next_queue[]`
- `daily_brief_summary`
- `recent_notifications[]`
  - `id`
  - `automation_key`
  - `title`
  - `message`
  - `action_label`
  - `action_route`
  - `created_at`

说明：

- 当前落地的 key 为 `daily_brief`、`weekly_portfolio`、`news_watch`、`behavior_observation`
- 自动任务只读取授权上下文、生成分析或待确认建议，不执行交易或账户动作
- `recent_notifications` 来自后台/手动运行的持久通知，用于把自动任务结果带回 `/automations` 和相应确认入口

### `PATCH /api/automations/{automation_key}`

用途：

- 更新当前用户对单个自动任务的授权和频率

请求：

- `enabled`（可选）
- `cadence_key`（可选）
- `timezone`（可选；当前主要用于 Daily Brief 偏好）

行为约定：

- `automation_key` 和 `cadence_key` 都是后端 allow-list
- 更新 `daily_brief` 时同步维护 `daily_brief_preferences`

### `POST /api/automations/{automation_key}/run`

用途：

- 手动触发一次已启用的自动任务运行记录

响应重点：

- `run_id`
- `automation_key`
- `status`
- `trigger_type`
- `due_at`
- `started_at`
- `completed_at`
- `summary`
- `output_ref`
- `error_message`
- `created_pending_proposal_ids`
- `output_payload`

说明：

- 手动运行和后台定时运行复用同一执行路径，并都会持久化 `automation_runs`
- 禁用状态或未完成 onboarding 时返回冲突错误

### Automation scheduler / background worker

用途：

- 将已启用且到期的自动任务从“可手动运行”推进为真实后台扫描、运行、通知和待确认建议生成

运行方式：

- 核心服务：`run_due_automations(db, now=None, limit=...)`
- CLI worker：`python -m app.scripts.run_automation_worker --once --limit 20`
- 循环 worker：`python -m app.scripts.run_automation_worker --poll-seconds 60`
- FastAPI lifespan worker 也可显式启用，但默认关闭：
  - `FUNDGENE_AUTOMATION_WORKER_ENABLED=false`
  - `FUNDGENE_AUTOMATION_WORKER_POLL_SECONDS=60`
  - `FUNDGENE_AUTOMATION_WORKER_BATCH_SIZE=20`
  - `FUNDGENE_AUTOMATION_WORKER_RETRY_DELAY_MINUTES=30`

行为约定：

- 只扫描 `enabled=true`、`next_run_at <= now`、且用户已完成 onboarding 的任务
- cadence 按 allow-listed key 和 Asia/Shanghai / Daily Brief preference timezone 计算真实下一次运行时间
- 成功运行会写入 `automation_runs`、`automation_notifications`，并按任务类型生成待确认建议
- `daily_brief` 只生成通知和简报引用，不制造每日待确认噪音
- `weekly_portfolio` / `news_watch` 生成内部 Safe Next Action 待确认建议
- `behavior_observation` 生成 `behavior_profile_note` 待确认建议；只有用户在 Profile 接受后才会追加行为证据
- 失败运行会记录 `status=failed`、`error_message`，并按 retry delay 推迟 `next_run_at`，避免 tight-loop 重试

### `GET /api/profile/context`

用途：

- 读取 Profile / Authorization Center 所需的后端聚合上下文

认证：

- 需要有效 session cookie
- 当前要求用户已完成 onboarding / profile 基线

响应重点：

- `context_readiness`
- `risk_profile`
- `behavior_profile`
- `portfolio_context`
- `learning_context`
- `simulation_context`
- `automation_authorizations`
- `authorization_scope`
- `pending_proposal_count`

### `GET /api/profile/pending-proposals`

用途：

- 读取当前用户由 Agent Run 生成、等待用户确认的 profile/writeback proposals

说明：

- 当前复用 `agent_state_update_proposals`，并通过 `agent_runs.user_id` 做用户归属校验
- 默认只返回 `user_decision_status = pending` 的 proposal
- 不向新手 UI 暴露原始 tool/run 细节作为主界面内容

### `POST /api/profile/pending-proposals/{proposal_id}/accept`

用途：

- 用户确认一个待写回 proposal

行为约定：

- 当前只对 allow-listed `behavior_profile_note` 执行目标写回：追加一条确认过的行为证据
- 不会通用执行任意 JSON patch
- 成功后 proposal 状态进入 `applied` 或 `accepted`

### `POST /api/profile/pending-proposals/{proposal_id}/reject`

用途：

- 用户拒绝一个待写回 proposal

请求：

- `reason`（可选）

行为约定：

- 只更新用户决策状态和备注，不删除原 proposal，保留审计痕迹

### `GET /api/assistant/session`

用途：

- 获取当前登录用户最近一次 coach 会话与完整消息

认证：

- 需要有效 session cookie
- 当前要求用户已完成 onboarding / profile 基线

响应重点：

- `session.id`
- `session.topic`
- `session.context_type`
- `session.latest_intent`
- `session.last_question`
- `session.last_answer_preview`
- `session.last_recommended_action`
- `session.message_count`
- `messages[].role`
- `messages[].message_type`
- `messages[].content`
- `messages[].advisor_response`

### `GET /api/assistant/sessions`

用途：

- 获取当前登录用户可继续的 coach 历史会话列表，供前端像 ChatGPT 一样选择旧会话

认证：

- 需要有效 session cookie
- 当前要求用户已完成 onboarding / profile 基线

响应重点：

- `sessions[]` 按 `updated_at` 倒序返回，当前限制最多 50 条
- 每条 session 包含 `id`、`topic`、`latest_intent`、`last_question`、`last_answer_preview`、`last_recommended_action`、`message_count`、`created_at`、`updated_at`
- 列表接口只返回摘要，不返回完整 messages

### `GET /api/assistant/sessions/{session_id}`

用途：

- 读取当前登录用户指定 coach 历史会话的完整消息，用于点开历史会话后重新显示并继续聊天

认证：

- 需要有效 session cookie
- 只能读取当前登录用户自己的 `coach` 会话

响应重点：

- 响应结构与 `GET /api/assistant/session` 相同
- 如果 `session_id` 不属于当前用户或不存在，返回 `404`

### `POST /api/assistant/messages`

用途：

- 向单入口 advisor agent 发送消息，并把本次提问与回答持久化到当前登录用户名下

认证：

- 需要有效 session cookie
- 当前要求用户已完成 onboarding / profile 基线

请求：

- `message`
- `session_id`（可选；为空且 `start_new_session` 不为 true 时会自动落到最近一次 coach 会话，没有则创建新会话）
- `start_new_session`（可选，默认 `false`；为 true 时忽略最近会话并创建新会话）
- `context`（可选；页面深链进入 Coach 时保留上下文）
  - `context.from_route`
  - `context.focus`
  - `context.source_ids`
  - `context.daily_brief_id`

响应重点：

- `session.id`
- `session.topic`
- `messages[].role`
- `messages[].message_type`
- `messages[].advisor_response.answer`
- `messages[].advisor_response.intent`
- `messages[].advisor_response.recommended_actions`
- `messages[].advisor_response.recommended_action_targets`
  - additive field; keeps `recommended_actions` as plain strings
  - each target is a backend allow-listed internal link with `label`, `href`, `intent`, and `kind = internal_link`
  - targets may include SafeNextAction-compatible metadata: `action_id`, `target_params`, `reason`, `expected_writeback`, and `safety_note`
  - query params such as `from=coach` and `focus=...` are allowed when the path remains allow-listed
- `messages[].agent_run_id`（用于 trace API 读取当前登录用户自己的 advisor run）

### Agent Runtime v2 trace API（当前已落地）

目标用途：

- 让 coach 前端 trace panel 能按当前登录用户读取一次 advisor run 的可解释执行轨迹
- 展示 orchestrator steps、内部工具调用、证据引用和 policy guard 结果
- 支持 QA / eval 复盘，不改变 `POST /api/assistant/messages` 的用户-facing 入口

目标接口：

- `GET /api/assistant/runs/{run_id}/trace`

认证：

- 需要有效 session cookie
- 只能读取当前登录用户自己的 `agent_run`

目标响应重点：

- `run.id`
- `run.intent`
- `run.status`
- `run.policy_status`
- `steps[].step_name`
- `steps[].status`
- `steps[].started_at`
- `steps[].completed_at`
- `tool_calls[].tool_name`
- `tool_calls[].status`
- `tool_calls[].latency_ms`
- `evidence_refs[].source_type`
- `evidence_refs[].source_id`
- `evidence_refs[].support_summary`
- `run.policy_status`

说明：

- `20260429_0009` 已提供 trace table migration，当前后端 route、service 查询、权限校验、运行时写入、前端 `getAgentRunTrace` client、trace panel 挂载和测试已经落地。
- 当前 v2 是 deterministic advisor spine，不等于完整 LLM planner、长期记忆、外部 MCP tool runner 或生产 observability 已完成。
- trace 响应应避免泄露原始提示词、敏感用户输入全文、内部密钥或不适合前端展示的工具 payload。

### `GET /api/learning/path`

用途：

- 读取当前登录用户的主学习路径与课程进度

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

响应重点：

- `path_slug`
- `title`
- `description`
- `overall_progress_percentage`
- `completed_courses_count`
- `total_courses`
- `recommended_course_slug`
- `recommended_course_title`
- `courses[]`

### `GET /api/learning/courses/{course_slug}`

用途：

- 读取某门课程的详情与 section 完成状态

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

响应重点：

- `course_slug`
- `title`
- `focus`
- `description`
- `progress_percentage`
- `status`
- `sections[].completed`

### `POST /api/learning/progress`

用途：

- 将某个 section 标记为已完成，并回写课程进度

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

请求：

- `course_slug`
- `section_slug`
- `status`

当前约定：

- 当前只接受 `status = completed`

### `POST /api/portfolio/snapshots`

用途：

- 提交一份手动持仓快照，并生成解释型组合报告

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

请求：

- `snapshot_date`
- `cash_value`
- `holdings[]`

当前 `holdings[]` 重点字段：

- `fund_code`
- `fund_name`
- `fund_type`
- `market_value`

响应重点：

- `snapshot_id`
- `snapshot_date`
- `total_value`
- `cash_value`
- `summary`
- `risk_exposure`
- `concentration_flags`
- `allocation_balance`
- `recommended_next_actions`
- `holdings[]`

### `GET /api/portfolio/latest`

用途：

- 读取当前登录用户最近一份组合报告

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

行为约定：

- 当前没有报告时返回 `{ "report": null }`

### `GET /api/portfolio/history`

用途：

- 读取当前登录用户的组合报告历史

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

响应重点：

- `items[].snapshot_id`
- `items[].snapshot_date`
- `items[].total_value`
- `items[].summary`
- `items[].generated_at`

### `GET /api/simulations/scenarios`

用途：

- 读取当前可用的情境训练目录与推荐情境

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

响应重点：

- `recommended_scenario_slug`
- `recommended_scenario_title`
- `items[].slug`
- `items[].title`
- `items[].summary`
- `items[].bias_focus`
- `items[].difficulty`
- `items[].estimated_duration_minutes`
- `items[].event_count`
- `items[].status`
- `items[].active_session_id`

### `POST /api/simulations/sessions`

用途：

- 为当前登录用户启动或恢复某条情境训练 session

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

请求：

- `scenario_slug`

响应重点：

- `session_id`
- `scenario_slug`
- `scenario_title`
- `status`
- `current_step`
- `total_steps`
- `active_event`
- `actions`
- `review`

### `GET /api/simulations/sessions/{session_id}`

用途：

- 读取当前登录用户某条 simulation session 的实时状态

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

### `POST /api/simulations/actions`

用途：

- 在当前 session 的当前节点提交一个动作、判断理由、担心点和冲动控制计划

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

请求：

- `session_id`
- `event_id`
- `choice_key`
- `rationale`（必填；兼容旧字段 `reflection`）
- `worry`（可选）
- `impulse_control_plan`（可选）

响应重点：

- 返回更新后的 `SimulationSessionResponse`
- 最后一轮提交会把 `status` 变成 `completed`，并附带 `review`

### `GET /api/simulations/review/{session_id}`

用途：

- 读取某条已完成 simulation session 的最终结构化复盘

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

响应重点：

- `session_id`
- `scenario_slug`
- `scenario_title`
- `bias_focus`
- `decision_summary`
- `bias_observations`
- `strengths`
- `improvement_areas`
- `coach_feedback`
- `recommended_next_actions`
- `reflection_questions`
- `behavior_evidence_candidates[]`
  - `behavior_evidence_id`
  - `bias_type`
  - `observed_signal`
  - `source_event`
  - `confidence`
- `pending_state_proposal`
  - 当前为响应级 pending 说明；单次 simulation review 不直接改 canonical behavior profile
- `generated_at`
- `actions[]`

### `GET /api/news`

用途：

- 读取当前可用的新闻/政策条目列表
- 可通过 `refresh=true` 尝试从配置的 RSS/Atom feeds 刷新条目
- `/news` 前端默认使用 `refresh=true&limit=20`，因此进入页面时会尝试同步真实资讯源

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

查询参数：

- `refresh`：可选布尔值，默认 `false`
- `limit`：可选，`1` 到 `100`

响应重点：

- `items[].id`
- `items[].item_type`
- `items[].title`
- `items[].summary`
- `items[].url`
- `items[].source_name`
- `items[].source_url`
- `items[].published_at`
- `items[].fetched_at`
- `items[].latest_analysis_id`

说明：

- RSS/Atom feed 条目是全局条目。
- feed 客户端使用真实 User-Agent，并尊重系统代理环境，适配本地代理网络。
- 当前默认 feeds 来自 `FUNDGENE_NEWS_FEEDS` 或后端默认列表：Federal Reserve press feed、SEC press releases、U.S. Treasury press releases、中国人民银行新闻 RSS、人民网财经、中新网财经。
- `GET /api/news` 只返回当前已同步数据中最近一个自然日的条目；如果需要更多历史资讯，应另行设计历史检索或分页接口，不要把旧新闻混入默认日常阅读面。
- `news_watch` 自动任务运行前会同步一次真实 feeds，再生成站内通知和待确认 Safe Next Action。
- 用户粘贴的手动新闻条目会写入 `news_items.user_id`，只会返回给当前用户。

### `GET /api/news/{item_id}`

用途：

- 读取某条新闻或政策条目的详情

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

响应重点：

- `item`

### `POST /api/news/analyze`

用途：

- 对某条新闻/政策条目生成结构化新手解读
- 或对用户手动粘贴的 headline/body 生成当前用户私有解读

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

请求二选一：

- `item_id`，可选 `item_type`
- `headline` + `body`

响应重点：

- `id`
- `item`
- `facts`
- `impact_paths`
- `uncertainty_notes`
- `beginner_translation`
- `related_learning_topics`
- `recommended_next_actions`
- `risk_notice`
- `citations`
- `generated_at`

说明：

- 解读只用于学习与决策支持，不构成收益承诺、买卖建议或交易指令。
- 生成结果会写入 `news_analyses`，引用会写入 `agent_citations`。

### `GET /api/news/analyses/{analysis_id}`

用途：

- 读取当前用户某条已持久化的新闻/政策解读

认证：

- 需要有效 session cookie
- 当前要求用户已完成基础画像

## 4. 当前持久化基线

当前已与 runtime 对齐的持久化表：

- `auth_users`
- `auth_sessions`
- `user_profiles`
- `risk_questionnaires`
- `behavior_profiles`
- `chat_sessions`
- `chat_messages`
- `agent_runs`
- `learning_paths`
- `courses`
- `course_sections`
- `user_course_progress`
- `portfolio_snapshots`
- `portfolio_holdings`
- `portfolio_analyses`
- `scenarios`
- `scenario_events`
- `simulation_sessions`
- `simulation_actions`
- `simulation_reviews`
- `news_items`
- `policy_items`
- `news_analyses`
- `agent_citations`

Agent Runtime v2 trace spine:

- `agent_steps`
- `agent_tool_calls`
- `agent_evidence_refs`
- `agent_state_update_proposals`
- `agent_runs.policy_status`

说明：

- 上述 v2 persistence baseline 已在 `20260429_0009` 中出现；route、service 写入、权限校验、前端展示和测试已经随当前 deterministic advisor spine 落地。

迁移状态说明：

- 离线 Alembic 展开已验证到 revision `20260429_0009`
- 先前 live local PostgreSQL upgrade 已验证到 `20260402_0005`
- revisions `20260426_0006`、`20260426_0007`、`20260426_0008` 与 `20260429_0009` 的 live 本地升级仍需在 Docker daemon 可用时重跑验证

## 5. 后续仍计划实现的接口

以下接口仍是目标方向，不应被误读为当前已实现：

### AI 助手扩展

- `POST /api/assistant/stream`

## 6. 当前阶段接口优先级

已经落地并应继续稳定的优先级：

- `GET /api/health`
- `GET /api/ready`
- `GET /api/product`
- `POST /api/auth/register`
- `POST /api/auth/login`
- `GET /api/auth/session`
- `POST /api/auth/logout`
- `POST /api/onboarding/profile`
- `GET /api/users/me`
- `POST /api/behavior/questionnaires`
- `GET /api/behavior/profile`
- `GET /api/behavior/training-plan`
- `GET /api/dashboard`
- `GET /api/assistant/session`
- `POST /api/assistant/messages`
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

下一阶段优先级：

- coach workspace 的更深训练能力
- auth hardening such as email verification and richer session management
- news feed reliability, source management, and richer citation/evaluation checks
- selective RAG, citation faithfulness checks, richer worker composition, and optional eval-run persistence
