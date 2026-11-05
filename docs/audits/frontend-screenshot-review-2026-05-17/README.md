# FundGene Frontend Screenshot Review

Date: 2026-05-17

## Scope

Reviewed all current `apps/web` page routes with Playwright screenshots at:

- desktop: `1440 x 980`
- mobile: `390 x 900`

Covered routes:

- `/`
- `/start`
- `/dashboard`
- `/onboarding`
- `/coach`
- `/learning`
- `/learning/risk-basics`
- `/portfolio`
- `/simulation`
- `/news`

## Artifacts

- Desktop screenshots: `playwright-output-3010/**/**-desktop.png`
- Mobile screenshots: `playwright-output-3010/**/**-mobile.png`
- Desktop stacked sheet: `stack-desktop.png`
- Mobile stacked sheet: `stack-mobile.png`

## Verification

Passed:

```bash
FUNDGENE_WEB_PORT=3010 pnpm --filter @fundgene/web exec playwright test e2e/visual-smoke.spec.ts --output=../../docs/audits/frontend-screenshot-review-2026-05-17/playwright-output-3010
FUNDGENE_WEB_PORT=3010 pnpm --filter @fundgene/web exec playwright test e2e/accessibility.spec.ts --output=../../docs/audits/frontend-screenshot-review-2026-05-17/a11y-output-3010
```

Results:

- Visual smoke: 10 passed.
- Horizontal overflow checks: passed in the visual smoke suite.
- Axe critical accessibility checks: 5 passed for `/`, `/dashboard`, `/coach`, `/portfolio`, `/news`.

Local caveat:

- The existing dev server on `127.0.0.1:3000` returned 500 for Next static chunks, causing `/start` to stay on `正在加载认证入口...`.
- Re-running on clean port `3010` fixed the issue, so the screenshots below use `3010`.

## Overall Findings

The current Apple-style light workbench direction is coherent and much stronger than the older dark shell. Desktop pages are generally polished, with consistent navigation, material panels, blue primary actions, and restrained financial-product density.

Main optimization focus:

1. Mobile pages are too tall on portfolio, onboarding, simulation, dashboard, and overview.
2. Several pages use similar cards and section rhythm, so important workflow actions are sometimes buried in repeated surfaces.
3. Some pages are visually complete but not yet fast enough for a beginner to know the immediate next action.
4. The visual smoke test is useful, but the current 3000 dev server failure shows the review workflow should prefer a clean port or explicitly restart the dev server before screenshot audits.

## Priority Recommendations

### P0

No blocking visual defects were found on the clean screenshot run. No horizontal overflow or critical axe failures appeared.

### P1

- Shorten mobile first-screen journeys for `/portfolio`, `/onboarding`, `/simulation`, `/dashboard`, and `/`.
- Add stronger sticky or compact action affordances where the page requires task completion.
- Reduce repeated card blocks on desktop where they compete with the primary workflow.
- Keep `/start` as a functional app entry, but reduce the decorative chart emphasis on mobile.

### P2

- Add route-specific visual regression assertions beyond marker text, especially for `/start` client hydration.
- Extend axe coverage to `/start`, `/onboarding`, `/learning`, `/learning/risk-basics`, and `/simulation`.
- Consider adding screenshot-height budgets for mobile routes to catch accidental page growth.

## Page Review

### `/` Overview

Screenshots:

- `playwright-output-3010/visual-smoke-overview-visual-smoke-chromium/overview-desktop.png`
- `playwright-output-3010/visual-smoke-overview-visual-smoke-chromium/overview-mobile.png`

Current state:

- Strong product dashboard landing, not a generic marketing page.
- Desktop hierarchy is clear: product spine, module matrix, recommendation panel.
- Mobile is very long at `390 x 3245`; module cards stack into a slow scan.

Optimize:

- On mobile, collapse the lower module list into a two-column compact grid or a segmented module index.
- Make the first primary action and current recommended path more visually dominant.
- Reduce duplicated "module explanation" density below the first screen.

### `/start`

Screenshots:

- `playwright-output-3010/visual-smoke-start-visual-smoke-chromium/start-desktop.png`
- `playwright-output-3010/visual-smoke-start-visual-smoke-chromium/start-mobile.png`

Current state:

- Clean and polished desktop entry.
- The page preserves product meaning: account entry, safety profile, no trading execution.
- Mobile height is acceptable at `390 x 1388`.

Optimize:

- On mobile, move the form above the decorative bar chart or make the chart shorter.
- Keep "账号入口" closer to the first viewport so returning users reach login faster.
- Add test coverage for client chunk/hydration failure, since a stale 3000 dev server left this route stuck on Suspense fallback.

### `/dashboard`

Screenshots:

- `playwright-output-3010/visual-smoke-dashboard-visual-smoke-chromium/dashboard-desktop.png`
- `playwright-output-3010/visual-smoke-dashboard-visual-smoke-chromium/dashboard-mobile.png`

Current state:

- Desktop has a strong command-center shape.
- "今天先处理一件事" works well as beginner-first framing.
- Mobile is long at `390 x 3120`, and several panels compete for the same priority.

Optimize:

- On mobile, keep "今日任务流" immediately after the hero action and collapse secondary status cards.
- Make the next recommended action sticky or repeat it near the bottom after the user scrolls through status.
- Reduce card visual weight for historical/summary sections so the page feels less like a report dump.

### `/onboarding`

Screenshots:

- `playwright-output-3010/visual-smoke-onboarding-visual-smoke-chromium/onboarding-desktop.png`
- `playwright-output-3010/visual-smoke-onboarding-visual-smoke-chromium/onboarding-mobile.png`

Current state:

- The two-step profile/questionnaire flow is clear.
- Desktop form layout is readable.
- Mobile is the longest non-portfolio flow at `390 x 4484`.

Optimize:

- Split profile and questionnaire into separate mobile steps with a persistent progress header.
- Keep only the current question group visible on mobile; collapse completed groups.
- Add a bottom sticky "继续/保存" action for questionnaire completion.

### `/coach`

Screenshots:

- `playwright-output-3010/visual-smoke-coach-visual-smoke-chromium/coach-desktop.png`
- `playwright-output-3010/visual-smoke-coach-visual-smoke-chromium/coach-mobile.png`

Current state:

- Conversation-first direction is working.
- Internal trace details are not exposed in the beginner UI.
- Mobile height is reasonable at `390 x 1758`.

Optimize:

- Desktop right rail is useful, but its visual weight could be lowered so the conversation remains primary.
- Make follow-up prompt chips and action links visually distinct enough that users understand which asks another question and which navigates.
- Consider keeping the input fixed at the bottom on mobile.

### `/learning`

Screenshots:

- `playwright-output-3010/visual-smoke-learning-visual-smoke-chromium/learning-desktop.png`
- `playwright-output-3010/visual-smoke-learning-visual-smoke-chromium/learning-mobile.png`

Current state:

- Strong task-first learning path.
- Desktop overview is concise.
- Mobile is acceptable at `390 x 1938`.

Optimize:

- Keep the recommended course CTA pinned higher on mobile.
- Reduce the visual similarity between status cards and course cards.
- Add a more explicit "continue current section" affordance near the top.

### `/learning/risk-basics`

Screenshots:

- `playwright-output-3010/visual-smoke-course-detail-visual-smoke-chromium/course-detail-desktop.png`
- `playwright-output-3010/visual-smoke-course-detail-visual-smoke-chromium/course-detail-mobile.png`

Current state:

- Expanded section content is useful and beginner-readable.
- Desktop gives enough structure for learning, self-check, and reflection.
- Mobile is long at `390 x 2825`.

Optimize:

- Use accordion sections on mobile, with current/next section expanded by default.
- Add a compact section progress tracker above the content.
- Keep "标记为已完成" closer to the section body instead of after long supporting blocks.

### `/portfolio`

Screenshots:

- `playwright-output-3010/visual-smoke-portfolio-visual-smoke-chromium/portfolio-desktop.png`
- `playwright-output-3010/visual-smoke-portfolio-visual-smoke-chromium/portfolio-mobile.png`

Current state:

- The two-step snapshot review is a meaningful UX improvement.
- Desktop report and input are rich, but dense.
- Mobile is the tallest page at `390 x 5345`.

Optimize:

- On mobile, separate "录入快照" and "最近报告" into tabs or a segmented control.
- Keep the draft review summary sticky before saving; the current flow can be buried after holdings.
- Compress the report history and holding list with expandable rows.
- On desktop, reduce right-column report card stacking so the chart, legend, and risk flags scan as one report.

### `/simulation`

Screenshots:

- `playwright-output-3010/visual-smoke-simulation-visual-smoke-chromium/simulation-desktop.png`
- `playwright-output-3010/visual-smoke-simulation-visual-smoke-chromium/simulation-mobile.png`

Current state:

- Historical training framing is clear and avoids prediction/trading language.
- Desktop two-column training layout works.
- Mobile is long at `390 x 4219`.

Optimize:

- Put active scenario, current decision, and required rationale in a single sticky decision panel on mobile.
- Collapse scenario catalog and past review once an active session exists.
- Make "开始/提交" action visually dominant at the decision point.

### `/news`

Screenshots:

- `playwright-output-3010/visual-smoke-news-visual-smoke-chromium/news-desktop.png`
- `playwright-output-3010/visual-smoke-news-visual-smoke-chromium/news-mobile.png`

Current state:

- Good beginner translation framing.
- Desktop selected-item plus interpretation layout is clear.
- Mobile is moderately long at `390 x 2756`.

Optimize:

- Keep selected item and generated interpretation adjacent on mobile by collapsing the custom paste form.
- Put "生成解读" and latest interpretation summary near the selected headline.
- Reduce source/tag chip density when only one fixture item is visible.

## Suggested Next Work Order

1. Fix screenshot workflow reliability: restart or bypass stale `3000` dev server, extend `/start` hydration coverage.
2. Mobile compaction pass: `/portfolio`, `/onboarding`, `/simulation`.
3. Dashboard and overview hierarchy pass.
4. Coach and learning micro-polish.
5. Extend accessibility coverage to every current route.
