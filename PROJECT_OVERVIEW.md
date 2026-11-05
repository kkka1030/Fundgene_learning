# FundGene Project Overview

Last updated: 2026-05-17

## 1. One-Sentence Summary

FundGene is a beginner-first Agent Command Center for fund investing. It lets a user give goals to one disciplined investment coach agent; the agent then checks authorized context, prepares a Daily Brief, runs portfolio/news/learning/simulation/behavior tools when needed, shows its process, and proposes safe next actions without becoming a trading or return-promise system.

## 2. Product Boundary

FundGene is:

- an AI-guided learning product,
- a decision-support and reflection tool,
- an agent-run portfolio and news explanation system,
- a behavior training system,
- a traceable agent application.

FundGene is not:

- a brokerage or trading platform,
- an auto-order or execution system,
- a high-risk recommendation engine,
- a product that promises returns,
- a multi-agent demo built mainly for novelty.

The product tone should be a disciplined investment coach for beginners, not a speculative trading assistant.

As of the 2026-05-17 Agent Command Center decision, the target product should feel less like several parallel finance modules and more like: "the agent has already checked what matters today; the user reviews, asks follow-ups, and confirms safe actions."

## 3. Target Users

Primary users are beginner or early-stage retail fund investors who:

- have limited financial vocabulary,
- need structured explanations before acting,
- struggle with news noise and emotional reactions,
- may chase hot funds or panic sell during drawdowns,
- need repeated practice rather than one-off advice.

The default design target is beginner-first, not advanced trader-first.

## 4. Core Product Loop

The target V1 user path is:

1. Register or log in.
2. Complete onboarding and risk questionnaire.
3. Land on Today, the Daily Brief home.
4. Read one agent-generated judgment, the evidence behind it, one safe next action, and one safety boundary.
5. Continue into Agent Workspace when the user wants the agent to do more work.
6. Watch the agent execute a task through natural-language progress steps, with advanced trace/tool details available on demand.
7. Confirm or reject durable updates such as behavior evidence, recurring automation, or training assignments.
8. Let default daily automation prepare the next brief.

Existing learning, portfolio, simulation, and news loops remain valuable, but their primary V1 role is to serve as agent tools, safe next action targets, and detail pages instead of equal-weight top-level navigation.

## 5. Current Implementation State

The repository is now an early runnable monorepo MVP rather than a blank project.

Current frontend:

- `apps/web`
- Next.js 16, React 19, TypeScript, App Router, Tailwind CSS 4
- Routes include `/`, `/start`, `/dashboard`, `/onboarding`, `/coach`, `/learning`, `/learning/[courseSlug]`, `/portfolio`, `/simulation`, and `/news`
- The UI has been tightened toward a compact, professional, workbench-style experience.

Target frontend IA:

- `/today`: default Daily Brief home.
- `/agent`: Agent Workspace for user-directed tasks.
- `/automations`: authorization and scheduling for daily brief, weekly checks, and behavior observation.
- `/profile`: portfolio, risk profile, behavior evidence, learning state, and data authorization.
- Current `/dashboard` and `/coach` can be migrated or aliased into `/today` and `/agent`; existing domain pages can remain as detail surfaces during migration.

Current backend:

- `apps/api`
- FastAPI, SQLAlchemy 2.x, Alembic, PydanticAI direction
- Session-backed auth with email/password hash and `HttpOnly` cookie
- Real APIs exist for auth, onboarding, behavior profile, dashboard, assistant, automations, profile context / pending proposals, learning, portfolio, simulations, and news.

Current real loops:

- `/start` -> auth session
- `/onboarding` -> profile and questionnaire persistence
- `/coach` -> persisted assistant messages and structured answers
- `/dashboard` -> aggregate state
- `/automations` -> persisted automation authorization and manual run records
- `/profile` -> aggregate context plus pending proposal accept/reject/apply flow
- `/learning` -> persisted path and section completion
- `/portfolio` -> manual snapshot and persisted report
- `/simulation` -> historical training session and review
- `/news` -> persisted news/policy analysis

Target Agent Command Center loop:

- `/today` or current `/dashboard` -> Daily Brief
- `/agent` or current `/coach` -> task run with process visibility
- `/automations` -> default daily brief plus opt-in/opt-out controls
- `/profile` -> context and authorization management

Current verification baseline:

- `pnpm lint:web`
- `pnpm build:web`
- `pnpm test:api`
- `pnpm test:agent-evals`
- `pnpm migrate:api:sql`
- local web and API demo can run at `127.0.0.1:3000` and `127.0.0.1:8000`

Operational caveat:

- PostgreSQL is the intended source of truth.
- A temporary SQLite demo database can be used only for local UI demonstration when Docker/Postgres is unavailable.
- The authoritative database baseline should remain Alembic migrations against PostgreSQL.

## 6. Canonical Stack

Frontend:

- Next.js 16
- React 19
- TypeScript
- App Router
- Tailwind CSS 4
- TanStack Query
- Zod
- Apache ECharts

Backend:

- Python 3.12+
- FastAPI
- PydanticAI direction
- SQLAlchemy 2.x
- Alembic
- PostgreSQL
- Redis only when caching, queues, or coordination require it
- pgvector only after retrieval use cases are validated

Infra and quality:

- pnpm workspace
- Docker for local Postgres/Redis
- GitHub Actions
- Playwright and axe baseline
- Sentry later
- OpenTelemetry later

## 7. Existing Agent Baseline

The current architecture has one user-facing advisor runtime:

- one primary `AdvisorAgent`,
- domain toolchains for learning, portfolio, behavior, simulation, and news,
- structured outputs with answer, risk notice, citations, recommended actions, and follow-up questions,
- persistence through chat records, `agent_runs`, and citation records.

This is intentionally not a free-form multi-agent chat system.

Product-facing implication:

- there is one coherent user-facing FundGene agent,
- internal workers/tools/skills can be many, but they should be invisible by default,
- advanced mode can expose trace/tool details for credibility and debugging,
- beginner mode should show natural-language progress such as "checking your latest portfolio" rather than raw tool names.

## 7.1 V1 Agent Command Center Decisions

The latest product decision is:

- V1 automation level: L1 manual task mode plus L2 default Daily Brief automation.
- Homepage model: Today / Daily Brief first, not chat-first.
- Process visibility: beginner mode shows natural-language agent steps; advanced mode may expand tool calls, trace ID, evidence refs, worker outputs, model/fallback metadata, and policy guard result.
- Old module treatment: portfolio, news, learning, simulation, and behavior are agent tools/detail surfaces, not main navigation.
- Primary pages: Today, Agent Workspace, Automations, Profile.
- Safety boundary: automation can analyze, summarize, monitor, draft recommendations, and create pending proposals; it cannot trade, promise returns, or silently mutate high-impact profile state.

## 8. Agent Runtime v2 Blueprint

The current technical direction is FundGene Agent Runtime v2.

The goal is not to add many agents for show. The goal is to build a traceable, evaluable, safety-constrained agent runtime for financial learning and decision support.

Implementation status as of 2026-04-29:

- Agent Runtime v2 has landed as the first traceable advisor spine.
- The current public assistant entrypoint should remain `POST /api/assistant/messages`.
- `20260429_0009` adds the trace persistence baseline: `agent_steps`, `agent_tool_calls`, `agent_evidence_refs`, `agent_state_update_proposals`, and additional trace fields on `agent_runs`.
- `POST /api/assistant/messages` now creates an advisor run, executes the v2 orchestrator, persists steps/tool calls/evidence/state proposals/policy status/context snapshot, and stores the structured assistant message.
- `GET /api/assistant/runs/{run_id}/trace` now returns the authorized current-user run trace for development and audit workflows.
- The coach workspace keeps internal trace details out of the beginner-facing UI; users see explanation, risk boundary, next actions, and follow-up prompts rather than run IDs, tool names, or citation keys.
- Current v2 is a deterministic, schema-first orchestration spine with a first local eval harness. As of 2026-05-16, it also has typed planning, plan validation, richer read-only tool metadata, scored tool discovery signals, runtime capability gates, cross-domain internal worker fan-out/fan-in, worker-output validation, strict final-response validation, trace observability events, and a 12-step planned trace. It is not yet a full LLM planner, long-term memory system, external MCP tool runner, or production observability layer.

Model integration status as of 2026-04-30:

- The advisor composer defaults to `deepseek:deepseek-v4-pro`; `deepseek:deepseek-v4-flash` is the intended lower-cost switch via `FUNDGENE_ADVISOR_MODEL`.
- DeepSeek is configured only through environment variables: `FUNDGENE_DEEPSEEK_API_KEY` or compatible `DEEPSEEK_API_KEY`, plus optional `FUNDGENE_DEEPSEEK_THINKING=enabled|disabled` and `FUNDGENE_DEEPSEEK_REASONING_EFFORT=high|max`. The current Coach default is `FUNDGENE_DEEPSEEK_THINKING=disabled` with a 30s model timeout so structured JSON composition uses the live model instead of falling back from slow reasoner paths.
- Runtime orchestration capabilities are configured only through environment variables: `FUNDGENE_AGENT_RUNTIME_FLAGS`, `FUNDGENE_AGENT_MAX_TOOL_CALLS`, and `FUNDGENE_AGENT_MAX_WORKERS`. The default capability set is conservative and product-bounded: scored read-only tool discovery, internal worker fan-out, strict final validation, and trace observability.
- No key is committed or documented as a real value. Without a key, `hybrid` mode records deterministic fallback metadata and still returns the backend-composed answer.
- The model factory explicitly builds PydanticAI `OpenAIChatModel` with `DeepSeekProvider` for DeepSeek instead of relying on implicit provider inference.
- News and portfolio use DeepSeek only as a text enhancer after backend rule analysis. The backend keeps ownership of facts, weights, concentration flags, risk buckets, citations, and safety fallback.
- Unsafe model text that looks like a return promise, direct buy/sell instruction, clear/full-position instruction, or automated trading capability is reverted to the rule-generated output.

High-level shape:

```text
User
  ↓
Advisor Orchestrator
  ↓
create_run
  → input_guard
  → classify_intent
  → build_agent_plan
  → load_context_snapshot
  → validate_plan
  → select_tools
  → execute_tools
  → execute_worker(s)
  → validate_worker_output
  → policy_guard
  → compose_response
  → validate_final_response
  → close_run
  ↓
Structured answer + citations + next actions + trace
```

The system should be described as:

```text
single user-facing Advisor Orchestrator
+ typed domain workers
+ evidence grounding
+ policy guardrails
+ persisted traces
+ eval harness
```

It should not be described as:

```text
many agents freely talking to each other
```

It also should not be described as a simple chatbot. The stronger product story is:

```text
Daily Brief automation
+ one user-facing agent workspace
+ visible execution process
+ domain tools and typed workers
+ user confirmation for durable changes
```

## 9. Runtime v2 Components

### 9.1 Advisor Orchestrator

The orchestrator owns:

- intent classification,
- context snapshot creation,
- tool selection,
- worker routing,
- fan-out/fan-in for cross-domain questions,
- final response composition,
- trace persistence,
- safety handling.

It is the only user-facing agent layer.

### 9.2 Typed Domain Workers

Workers are controlled executors, not free chat agents.

Initial workers:

- `LearningWorker`: explains fund concepts and references learning materials.
- `PortfolioWorker`: explains portfolio structure using the latest persisted portfolio report.
- `BehaviorWorker`: identifies behavior bias using questionnaire, behavior profile, and simulation review evidence.
- `SimulationWorker`: turns historical scenario sessions and reviews into decision-training guidance.
- `NewsWorker`: interprets news and policy through facts, impact path, uncertainty, and safe next actions.
- `SafetyPolicyWorker`: validates input, tool use, and output against product and financial safety rules.
- `Composer`: produces the final beginner-facing answer.

Worker rules:

- Workers do not talk to each other.
- Workers are selected by `AgentPlanner` and may run in bounded internal fan-out for cross-domain user questions.
- Worker fan-out is capped and aggregated back into one public assistant response.
- `AgentPlanner` also selects code-owned, versioned coaching skills before tool execution. Current skills are `fund_basics_explainer_v1`, `portfolio_concentration_review_v1`, `behavior_bias_reflection_v1`, `simulation_review_coach_v1`, `news_policy_impact_path_v1`, and `cross_domain_synthesis_v1`.
- Runtime tools remain internal, read-only, schema-described, and checked against intent/tool-budget constraints before execution.
- Skill-required tools must already exist in the internal read-only `ToolRegistry`. Invalid required tools are rejected at registry load, and plan-validation failure falls back to safety-boundary tools before execution.
- Tool selection now produces scored `tool_selection_signals`, so traces can explain why a tool was selected without exposing internal tool names in the beginner-facing UI.
- Skill selection is visible in developer/audit trace through `agent_plan_v1.selected_skills`, `tool_trace.selected_skill_versions`, and the `agent.skills` trace event. Beginner-facing coach UI should not expose these internal names.
- Skill `outputGuidance` and `forbiddenLanguage` are carried into worker output so model composition and final validation can see the selected skill contract. Internal plan-repair failures use `runtime_repair`, not a user-safety violation, so trace analytics can distinguish planner faults from user boundary requests.
- Final response validation removes unsupported citations and unsafe recommended actions before the assistant message is persisted.
- Workers do not directly respond to the user.
- Workers do not write canonical user state.
- Workers receive structured input and return structured output.

Worker output schema:

```ts
type WorkerOutput = {
  workerName: string;
  intent: "learning" | "portfolio" | "behavior" | "simulation" | "news";
  skillNames: string[];
  learningOutcome?: string;
  findings: string[];
  evidenceRefs: EvidenceRef[];
  riskFlags: string[];
  recommendedActions: string[];
  stateUpdateProposals: StateUpdateProposal[];
  confidence: number;
  limitations: string[];
  schemaVersion: string;
};
```

### 9.3 Context Snapshot

Each run should create a context snapshot from backend truth:

- auth user,
- user profile,
- risk questionnaire summary,
- behavior profile,
- learning progress,
- latest portfolio report,
- simulation reviews,
- latest news analyses,
- recent coach messages.

The snapshot should be immutable for that run. This makes old answers reproducible even if user state changes later.

### 9.4 Tool Registry

A minimal internal tool registry should be added before broad MCP adoption.

Each tool should define:

```ts
type ToolDefinition = {
  name: string;
  description: string;
  inputSchema: unknown;
  outputSchema: unknown;
  permission: "read_only" | "write_proposal" | "write_confirmed";
  timeoutMs: number;
  audited: boolean;
};
```

Phase 1 tools should be read-only:

- `profile.current`
- `learning.path`
- `portfolio.latest_report`
- `behavior.profile`
- `simulation.latest_review`
- `news.latest_analysis`

Write tools should not execute directly from model output. They should create proposals that backend validators can approve or reject.

### 9.5 Evidence Grounding

Evidence is not the same as RAG.

Phase 1 evidence refs should point to structured database records:

- portfolio analysis records,
- questionnaire records,
- behavior profiles,
- course sections,
- simulation reviews,
- news analyses.

Evidence ref schema:

```ts
type EvidenceRef = {
  sourceType:
    | "course_section"
    | "portfolio_analysis"
    | "behavior_profile"
    | "risk_questionnaire"
    | "simulation_review"
    | "news_analysis"
    | "policy_item";
  sourceId: string;
  sourceVersion?: string;
  createdAt?: string;
  quoteOrSummary: string;
  supportsClaim: string;
};
```

### 9.6 Policy Guard

Safety must exist at three levels:

- input guard: detect direct trade requests, return promises, unsuitable requests, account-linking intent,
- tool guard: restrict tool permissions and enforce read/write policy,
- output guard: block or revise unsafe answers.

Policy result schema:

```ts
type PolicyResult = {
  decision: "allow" | "revise" | "block_with_guidance";
  violations: string[];
  requiredEdits: string[];
  blockedReason?: string;
  safeAlternative?: string;
};
```

Hard prohibitions:

- no guaranteed return language,
- no direct buy/sell instruction,
- no automated trading,
- no broker/account connection flow,
- no pretending uncertainty is certainty,
- no advice unsupported by evidence when evidence is required.

### 9.7 Trace Persistence

Traces should be written throughout the run, not only at the end.

Required tables:

- `agent_runs`
- `agent_steps`
- `agent_tool_calls`
- `agent_evidence_refs`
- `agent_state_update_proposals`

Recommended fields:

`agent_runs`:

- `id`
- `user_id`
- `chat_session_id`
- `run_type`
- `intent`
- `orchestrator_version`
- `policy_status`
- `status`
- `latency_ms`
- `created_at`
- `completed_at`

`agent_steps`:

- `id`
- `run_id`
- `step_name`
- `status`
- `input_payload`
- `output_payload`
- `error`
- `started_at`
- `completed_at`

`agent_tool_calls`:

- `id`
- `run_id`
- `step_id`
- `tool_name`
- `permission_level`
- `input_payload`
- `output_payload`
- `status`
- `latency_ms`

`agent_evidence_refs`:

- `id`
- `run_id`
- `step_id`
- `source_type`
- `source_id`
- `source_version`
- `claim`
- `support_summary`

`agent_state_update_proposals`:

- `id`
- `run_id`
- `target_type`
- `target_id`
- `patch_payload`
- `reason`
- `validator_status`

### 9.8 State Update and Memory

Canonical state remains in the database.

Memory is a derived view, not the source of truth.

Memory types:

- `ProfileMemory`: investment goal, experience, risk baseline.
- `BehaviorMemory`: chasing gains, panic selling, concentration tendencies.
- `EpisodeMemory`: recent coach topics, portfolio reports, course progress, simulation outcomes.

Agents may propose memory updates, but only validators may approve writes.

Flow:

```text
worker proposes memory_patch
  → validator checks evidence and policy
  → approved patch writes derived memory
  → rejected patch remains in proposal table
```

## 10. RAG Strategy

RAG should be introduced selectively.

Use SQL first for:

- user profile,
- risk questionnaire,
- behavior profile,
- latest portfolio report,
- simulation review,
- persisted news analysis.

Use retrieval for:

- course content,
- fund terminology,
- policy explanation material,
- news source material,
- historical scenario background.

Recommended sequence:

1. Keyword or BM25 retrieval over curated local content.
2. Structured citation mapping.
3. Citation faithfulness eval.
4. pgvector hybrid retrieval only after retrieval quality matters.

Do not let RAG generate trade instructions. RAG is evidence support, not decision authority.

## 11. MCP Strategy

Do not start with broad external MCP access.

Start with internal typed tools and audit logs. Later, expose the same internal tools as an MCP server if there is a real integration need.

MCP should be used for:

- standardizing access to FundGene's internal tools,
- exposing read-only product context to approved agent clients,
- future integration with controlled external data sources.

MCP should not be used for:

- unrestricted filesystem access,
- arbitrary SQL,
- broker execution,
- connecting random third-party tools without permission and audit.

## 12. Evaluation Strategy

The project should prove agent behavior, not only show agent flow diagrams.

Minimum eval set:

- direct buy/sell request must be refused or redirected to risk education,
- return guarantee request must be blocked,
- portfolio concentration question must cite latest portfolio report,
- behavior bias question must cite questionnaire or simulation evidence,
- news question must separate fact, interpretation, uncertainty, and safe next action,
- RAG answer must include citations that support the claim,
- memory update must not permanently change user profile from one weak signal,
- final answer must be understandable to a beginner.

Suggested local command:

```bash
pnpm test:agent-evals
```

Eval outputs should include:

- case id,
- expected behavior,
- observed tool calls,
- observed policy decision,
- citation validity,
- pass/fail,
- failure reason.

## 13. Frontend Agent Process Experience

The user-facing Agent Workspace should stay simple, but it should feel like an agent is doing real work.

Default beginner-facing process display should use natural-language steps:

```text
正在理解任务
正在读取你的风险画像
正在检查最新组合
正在筛选相关资讯
正在评估影响路径
正在生成安全下一步
等待你确认是否写回
```

Advanced mode may expand:

```text
trace id
tool calls
evidence refs
worker outputs
model/fallback metadata
policy guard result
latency
```

Avoid showing a theatrical multi-agent chat transcript. The stronger demo is a clear process trail for users and a deeper trace panel for development/audit users.

## 14. Recommended Execution Plan

### Phase A: Agent Runtime v2 Spine

Goal:

- make every assistant answer traceable, evidence-grounded, and policy-checked.

Status as of 2026-04-29:

- implemented for the main coach message path.
- internal `ToolRegistry`, orchestrator steps, Learning/Portfolio/Behavior/News workers, policy guard, evidence refs, state update proposals, and trace read API are present.
- `POST /api/assistant/messages` remains the public user-facing entrypoint.

Done when:

- every coach response has `agent_run`, `agent_steps`, tool calls, evidence refs, and policy result.
- the trace API can read those records for the current logged-in user.
- the user-facing coach UI stays beginner-oriented while trace records remain inspectable through the authorized trace API.
- `pnpm sync:api`, `pnpm test:api`, `pnpm migrate:api:sql`, and relevant web checks pass.

Current status:

- complete for the deterministic v2 spine.
- the first 20-case local eval harness is implemented and exposed through `pnpm test:agent-evals`.
- next work is richer cross-domain worker composition, selective RAG over curated learning/policy materials, deeper citation faithfulness checks, optional eval persistence, and production observability.

### Phase B: Eval Harness

Goal:

- prove the runtime is safer and more reliable than plain chatbot answers.

Tasks:

- JSON eval fixtures are present under `services/evals/agent_eval_cases.json`,
- the local eval command is `pnpm test:agent-evals`,
- current cases test tool-call correctness, citation use, safety refusal, beginner clarity, and the main learning/portfolio/behavior/simulation/news intents,
- optionally persist eval runs.

Done when:

- the first 20 eval cases run locally and fail loudly on unsafe or ungrounded behavior.

Current status:

- complete for the first local harness.
- next work is to add richer faithfulness scoring, more edge cases, and optional persisted eval-run records.

### Phase C: Evidence Service

Goal:

- centralize evidence retrieval without overusing RAG.

Tasks:

- implement structured evidence refs,
- add keyword retrieval over courses and policy/news material,
- add citation faithfulness checks,
- prepare pgvector only after keyword retrieval reaches its limits.

Done when:

- learning and news answers can cite specific source records or snippets.

### Phase D: Memory

Goal:

- make the coach improve over time without corrupting canonical state.

Tasks:

- define profile, behavior, and episode memory summaries,
- implement memory patch proposals,
- validate before writes,
- expose memory evidence in traces.

Done when:

- memory updates are explainable, reversible, and backed by evidence.

### Phase E: MCP Packaging

Goal:

- expose controlled FundGene tools through a standard protocol only after internal tool boundaries are mature.

Tasks:

- wrap read-only internal tools as MCP tools,
- enforce allowlist and audit,
- avoid arbitrary external MCP tool use.

Done when:

- approved clients can call FundGene context tools without bypassing product safety policy.

## 15. Demo Script

Recommended demo path:

1. Log in as demo user.
2. Complete onboarding and risk questionnaire if needed.
3. Open Today.
   - Expected: Daily Brief shows one judgment, up to three evidence items, one safe next action, one `do_not_do`, source coverage, and generation time.
4. Click "let Agent continue analyzing" or an equivalent safe action.
   - Expected: Agent Workspace opens with the Daily Brief context.
5. Ask: "最近新闻对我的资产有什么影响？"
   - Expected: the workspace shows natural-language steps such as reading profile, checking portfolio, reviewing news, evaluating impact path, and preparing a safe next action.
6. Expand advanced trace only for demo/audit.
   - Expected: show tool calls, evidence refs, worker outputs, and policy decision without exposing them as the default beginner view.
7. Show Automations.
   - Expected: Daily Brief automation is visible and controllable; portfolio check and news impact watch explain what they read, produce, and require confirmation for.
8. Show Profile.
   - Expected: portfolio, risk profile, behavior evidence, learning state, and authorization are treated as Agent context.
9. Run eval and UI checks.
   - Expected: safety, grounding, visual, e2e, and accessibility checks pass.

## 16. How to Explain Technical Contribution

Weak framing:

> We used many agents.

Better framing:

> FundGene implements an Agent Command Center for beginner fund investors. The product opens with a Daily Brief the agent prepared from authorized user context, then lets the user assign follow-up tasks in an Agent Workspace. The agent shows readable execution steps, grounds results in evidence, proposes only safe next actions, and requires confirmation before durable writeback.

Short version:

> FundGene is not a free-form chatbot. It is a traceable agent workflow for financial learning: single-entry orchestration, typed workers, SQL-first evidence, selective RAG, guarded tools, state update proposals, and eval-driven safety.

## 17. Near-Term Priority

The next best engineering milestone is not more pages.

The next best engineering milestone is now:

```text
Selective RAG + citation faithfulness checks + richer worker composition + observability
```

The deterministic v2 spine and first 20-case eval harness are already in place; the next gain is improving evidence quality and runtime reliability without turning the product into a free-form multi-agent demo.
