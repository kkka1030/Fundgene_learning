# FundGene Frontend Optimization Pass

Date: 2026-05-17

## Scope

Implemented the P1 findings from `docs/audits/frontend-screenshot-review-2026-05-17/README.md` with a focus on mobile scanability and task-first workflows.

Changed surfaces:

- `/`
- `/start`
- `/dashboard`
- `/onboarding`
- `/coach`
- `/portfolio`
- `/simulation`

## Main Changes

- `/onboarding`: mobile now expands only the current questionnaire item, keeps answered items compact, and uses a sticky submit bar.
- `/portfolio`: added mobile segmented jump controls, sticky draft actions, compact mobile holding rows, and collapsed mobile history.
- `/simulation`: hid desktop-only process/status density on mobile, added a compact mobile scenario card, constrained scenario-list height, and made decision submission sticky.
- `/coach`: reduced mobile height by hiding the desktop context rail, shortened the chat panel floor, and visually separated follow-up prompt chips from navigation action links.
- `/start`: tightened mobile spacing and reduced the lower hero section height so account entry stays closer to the first viewport.
- `/` and `/dashboard`: reduced mobile explanatory density while preserving desktop workbench detail.

## Mobile Screenshot Height Change

Measured from Playwright full-page screenshots at `390 x 900`.

| Route | Before | After |
| --- | ---: | ---: |
| `/coach` | 1758px | 1173px |
| `/onboarding` | 4484px | 3375px |
| `/` | 3245px | 2482px |
| `/portfolio` | 5345px | 4477px |
| `/simulation` | 4219px | 2622px |
| `/start` | 1388px | 1251px |

`/dashboard` remains `3120px`; it now has less mobile body copy, but the page still has several full workflow sections. It is the next candidate for a deeper mobile-specific layout pass.

## Verification

Passed:

```bash
pnpm lint:web
pnpm build:web
FUNDGENE_WEB_PORT=3012 pnpm --filter @fundgene/web exec playwright test e2e/workspaces.spec.ts e2e/visual-smoke.spec.ts e2e/accessibility.spec.ts --output=../../docs/audits/frontend-optimization-2026-05-17/playwright-output-3012
```

Results:

- `pnpm lint:web`: passed.
- `pnpm build:web`: passed.
- Playwright workspace + visual smoke + accessibility: 21 passed.
- Visual smoke horizontal overflow checks: passed.

## Artifacts

- Desktop screenshots: `playwright-output-3012/**/**-desktop.png`
- Mobile screenshots: `playwright-output-3012/**/**-mobile.png`
- Desktop stacked sheet: `stack-desktop.png`
- Mobile stacked sheet: `stack-mobile.png`

## Remaining Follow-Up

- Do a dedicated `/dashboard` mobile layout pass if the target is a shorter mobile first run.
- Consider adding mobile screenshot-height budgets for the heaviest routes.
- Extend axe coverage to all current routes, not only the existing five-route subset.
