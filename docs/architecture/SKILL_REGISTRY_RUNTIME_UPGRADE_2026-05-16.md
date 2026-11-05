# Skill Registry Runtime Upgrade - 2026-05-16

## Scope

This upgrade implements the first safe slice from `AGENT_UPGRADE_RESEARCH_PROPOSAL_2026-05-16.md`: Skill Registry first.

It intentionally stays inside Agent Runtime v2. It does not introduce external MCP tools, browser automation, broker connections, editable prompt studios, autonomous agent swarms, or model-directed writes to canonical user state.

## What Changed

- Added code-owned `fundgene_skill_v1` definitions in `apps/api/app/runtime/v2/skills/registry.py`.
- Added six built-in coaching skills:
  - `fund_basics_explainer_v1`
  - `portfolio_concentration_review_v1`
  - `behavior_bias_reflection_v1`
  - `simulation_review_coach_v1`
  - `news_policy_impact_path_v1`
  - `cross_domain_synthesis_v1`
- Extended `AgentPlan` with `selected_skills`.
- Extended `WorkerOutput` with `skill_names` and `learning_outcome`.
- Skill-required tools are merged into planning before scored tool discovery.
- Invalid skill required tools are rejected during registry load.
- Plan validation checks that every selected skill required tool is present in `planned_tools`.
- Plan-validation failures are hard-gated to a `runtime_repair` fallback before tool execution, so internal planner faults are not reported as user safety-boundary violations.
- Trace now records skill selection through `agent_plan_v1.selected_skills`, `tool_trace.skills`, `selected_skill_versions`, and `agent.skills`.
- Skill `output_guidance` and `forbidden_language` are carried into `WorkerOutput` for model composition and final validation.
- Added `SimulationWorker` for historical scenario training and review.
- Behavior reflection can create a pending `behavior_profile_note` proposal, but does not mutate canonical behavior state.
- Eval runner now supports `required_skills`, `required_trace_events`, `forbidden_tools`, and `max_unsupported_findings`.

## Product Boundary

Skills are coaching procedures, not independent chat agents. They select explanation structure, required read-only context, forbidden language, and expected learning outcomes.

The public API remains `POST /api/assistant/messages`; the user still sees one assistant answer. Internal workers and skill names remain developer/audit trace details, not beginner-facing UI.

## Built-In Skill Contracts

| Skill | Main intent | Required tool set | Boundary |
|---|---|---|---|
| `fund_basics_explainer_v1` | learning | profile, learning path, concept map, learning evidence | no return promise |
| `portfolio_concentration_review_v1` | portfolio | profile, latest portfolio report, risk lens, behavior profile | no buy/sell or rebalancing command |
| `behavior_bias_reflection_v1` | behavior | profile, behavior profile, behavior training plan | no permanent diagnosis from one chat |
| `simulation_review_coach_v1` | simulation | profile, behavior profile, training plan, latest simulation review | no hindsight prediction or trading implication |
| `news_policy_impact_path_v1` | news | profile, latest news analysis, impact lens, policy evidence | no title-to-trade shortcut |
| `cross_domain_synthesis_v1` | multi-intent | none by itself | no internal agent chatter in public output |

## Verification

Required checks for this slice:

- `uv run --project apps/api ruff check apps/api/app apps/api/tests services/evals`
- `pnpm test:api`
- `pnpm test:agent-evals`
- `pnpm migrate:api:sql`
- `pnpm smoke:deepseek` with a temporary shell-only DeepSeek API key when live model validation is needed

The smoke test must not require writing credentials to `.env` or committing secrets.
