# Frontend Optimization Review

Date: 2026-05-16

Source audit: `docs/audits/frontend-ux-2026-05-16/frontend-backend-ux-audit.md`

## Scope

This pass optimized the current `apps/web` frontend against the frontend/backend UX audit, with emphasis on beginner-facing product language, complete persisted loops, and screenshot verification across every current route and key workflow.

## Remediation Map

- Internal agent/runtime labels: removed or mapped user-facing labels such as advisor orchestrator, agent route, scenario dossier, structured readout, manual snapshot, trace-style evidence labels, raw bias tags, reference keys, and English review headings. Added `apps/web/lib/display-labels.ts` for display-only label mapping.
- Simulation resume: added `getSimulationSession(sessionId)` and exposed `activeSessionId` as “继续上次训练”, allowing an unfinished historical training session to resume before submitting the next action.
- News workspace: changed catalog fetch to avoid forced `refresh=true`, limited the default list to the first 5 items, moved the analysis canvas beside the selected item, and mapped raw category/source/reference labels before display.
- Portfolio workspace: added field-level validation for cash, fund code, fund name, and market value; added “填入示例快照”; kept invalid submit errors next to affected fields.
- Fake search affordance: replaced shell/auth search-looking pills with static path hints so users do not see a non-functional search input.
- Mock/seed markers: mapped display values such as `FundGene dev fixture`, `POLICY`, and `Beginner Core Path` to Chinese product labels before rendering.

## Verification

- `pnpm lint:web`: passed.
- `pnpm build:web`: passed.
- `pnpm test:web:e2e`: 19 passed.
- `node docs/audits/frontend-ux-2026-05-16/post-optimization/live-screenshot-audit.mjs`: passed with 20 route screenshots, 7 interaction screenshots, 0 errors, and 0 internal term failures.

## Screenshot Evidence

- Desktop routes: `desktop-contact-sheet.png`
- Mobile routes: `mobile-contact-sheet.png`
- Interactions: `interaction-contact-sheet.png`
- Raw screenshots: `screenshots/`
- Machine report: `post-optimization-live-audit.json`

Covered routes:

- `/`
- `/start`
- `/dashboard`
- `/onboarding`
- `/coach`
- `/learning`
- `/learning/fund-basics`
- `/portfolio`
- `/simulation`
- `/news`

Covered interactions:

- Coach question submission.
- Learning section completion.
- Portfolio invalid field-level validation.
- Portfolio valid report generation with example snapshot.
- Simulation start/resume.
- Simulation action submission.
- News item analysis.

## Residual Risk

- The live screenshot audit uses seeded local data and deterministic local services; production data may introduce new raw labels that should be handled through the same display-label mapping layer.
- The frontend still relies on API-provided explanatory copy for several long summaries; future backend copy changes should be screenshot-audited before release.
