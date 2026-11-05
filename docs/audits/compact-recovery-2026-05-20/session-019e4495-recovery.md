# Codex Compact Recovery: session 019e4495

Last updated: 2026-05-20
Workspace: `/Users/xueyicheng/Documents/SRTP/FundGene`
Branch: `codex/agentic-beginner-coach`
Source session: `019e4495-3f0f-7153-b000-16110444444b`
Source rollout: `/Users/xueyicheng/.codex/sessions/2026/05/20/rollout-2026-05-20T16-51-32-019e4495-3f0f-7153-b000-16110444444b.jsonl`

## Why this recovery exists

The source Codex session hit a late-context compact failure pattern. Local logs show historical failures on the remote compact path:

- `codex_core::compact_remote: remote compaction failed`
- `stream disconnected before completion`
- `https://chatgpt.com/backend-api/codex/responses/compact`
- Cloudflare `502 Bad Gateway` / `403 Forbidden`

For this source session specifically, the old goal remained `active` with about `299023` goal tokens recorded, while the model context window was around `258400` tokens. That means every resumed turn was likely forced through pre-sampling compact before normal work could continue.

Do not keep waiting for that session to compact. Continue from this recovery file in a fresh session.

## Verified local state when recovered

- `codex login status`: logged in using ChatGPT.
- System proxy: HTTP/HTTPS/SOCKS to `127.0.0.1:7897`.
- `HEAD https://chatgpt.com/backend-api/codex/responses/compact`: reachable, returned `405 Allow: POST`.
- `HEAD https://api.openai.com/v1/responses`: reachable, returned expected unauthenticated `401`.
- FundGene git worktree was clean before writing this recovery file.
- Latest commit at recovery time: `e868ffd fix: smooth auth cold starts`.

The reachability checks prove routing exists, but they do not prove remote compact POST health. The failure mode is still consistent with large compact POST requests being interrupted or rejected upstream.

## What the old session already completed

The first goal in the source session completed a real online Chrome audit against `https://fundgene.onrender.com`.

The audit actually exercised:

- account creation and onboarding,
- Today,
- Portfolio,
- News,
- Agent,
- Simulation,
- Learning,
- Automations,
- Profile.

The audit concluded that the main product loops were reachable, and the repo was clean at the end of that audit.

## Main issues found by the audit

1. Simulation defaults to the first radio action in each stage. The first action can be unsafe or impulsive, such as large redemption or chasing a rebound. The page should start with no selected action and require an explicit user choice.
2. Today and Agent still expose internal values, including `balanced` and `暂无显著行为偏差标签`. These must be mapped into beginner-facing Chinese copy.
3. News analysis can partially succeed while still showing `Failed to fetch`, leaving analysis cards and list state inconsistent.
4. Agent prompt handoff from Simulation or Learning fills the input box but does not send automatically. This needs either explicit copy or an intentional send/confirm behavior.
5. Agent answer after a completed Simulation can loop back into "go do a simulation" instead of using the just-finished review.
6. Online cold starts and long waits are still visible, especially first load and slow report/analysis paths.
7. News can briefly show zero items before real sources appear, which reads like an empty product state.

## Where the old repair attempt stopped

The user then asked to fix the main audit problems, verify locally with Chrome, push, and deploy.

The source session created this active goal:

`逐项修复 FundGene 线上 Chrome 审计发现的主要问题，在本地用 Chrome 浏览核验全部通过后推送并触发上线。`

It did not complete this goal. It stopped while reading files and before applying code changes.

Files it had started inspecting:

- `apps/web/components/simulation-workspace.tsx`
- `apps/web/components/news-workspace.tsx`
- `apps/web/lib/display-labels.ts`
- `apps/web/app/(workspace)/today/page.tsx`
- `apps/web/components/coach-workspace.tsx`
- `apps/web/components/dashboard-workspace.tsx`
- `apps/web/components/profile-workspace.tsx`
- `apps/web/lib/api.ts`
- `apps/web/lib/navigation.ts`
- `apps/api/app/runtime/v2/workers/simulation.py`
- `apps/api/tests/test_agent_runtime_v2_trace.py`
- `apps/api/tests/test_simulation_flow.py`

No actual patch from that repair attempt was left in the worktree.

## Recommended continuation plan

1. Fix Simulation first.
   - Ensure no radio action is selected by default.
   - Disable submit until the user explicitly selects an action and provides the required rationale.
   - Add or update Playwright coverage for this no-default-selection behavior.

2. Fix display label mapping.
   - Centralize mappings in `apps/web/lib/display-labels.ts`.
   - Map risk values such as `balanced` to beginner-facing Chinese text.
   - Map raw behavior tags and `no_major_bias_detected` / `暂无显著行为偏差标签` to user-facing language.
   - Apply this in Today, Profile, Dashboard, and Agent visible surfaces.

3. Fix News analysis state consistency.
   - Inspect `analyzeNews`, `getNewsAnalysis`, and related React Query invalidation.
   - If analysis POST succeeds but refetch fails, keep the returned analysis visible and show a softer refresh warning instead of a red full failure.
   - Make list status reflect the newly generated analysis.

4. Fix Agent handoff semantics.
   - Decide whether Simulation/Learning handoff should auto-send or explicitly require confirmation.
   - If confirmation is required, make button/input copy say so clearly.
   - For completed Simulation context, ensure Agent uses the latest review and does not recommend simply repeating the just-finished training.

5. Verification sequence.
   - `pnpm lint:web`
   - `pnpm build:web`
   - relevant web e2e tests, especially Simulation and News
   - API tests touching Agent Runtime v2 / Simulation if backend copy changes
   - local Chrome walkthrough of Simulation, News, Today, Agent, Learning, Profile

6. Only after local verification:
   - commit,
   - push,
   - confirm deployment status,
   - run a short online smoke test.

## Fresh-session prompt

Use this if a new Codex session needs to continue the work:

```text
Continue FundGene from docs/audits/compact-recovery-2026-05-20/session-019e4495-recovery.md.

Do not resume session 019e4495 directly; it is stuck behind remote compact failure.

Implement the unresolved audit fixes in priority order:
1. Simulation no default unsafe radio selection.
2. Beginner-facing label mapping for risk/behavior/internal values.
3. News analysis partial-success/error-state consistency.
4. Agent handoff/Simulation-review answer quality.

Verify with local tests and Chrome before pushing/deploying.
```

