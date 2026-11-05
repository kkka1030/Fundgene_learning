# FundGene Defense Showcase Plan

Date: 2026-05-22
Status: GIF/MP4 recording pipeline verified; PPT generation intentionally paused, current deliverable is video only

## 1. Showcase Objective

The defense should not present FundGene as a collection of finance pages. The core story should be:

> FundGene is a beginner-first Agent Command Center for fund investors. It prepares a daily judgment from authorized user context, lets the user ask follow-up tasks, shows readable execution progress, grounds answers in portfolio/news/learning/training evidence, proposes only safe next actions, and requires confirmation before durable state changes.

The strongest deliverable is a short cinematic product video plus several loopable GIF/MP4 clips embedded in the presentation. PowerPoint can use MP4 directly, so MP4 should be the primary asset format; GIFs are useful as fallback or for small looping feature cards.

## 2. Feature Priority

### P0: Must show

1. Today / Daily Brief
   - Value: "The agent has already done the first round of thinking."
   - Show: one headline judgment, why it matters, evidence, safe next action, and safety boundary.
   - Asset: 8-12 second loop, desktop first.

2. Agent Workspace
   - Value: FundGene is not a free-form chatbot; it is a traceable workflow.
   - Show: user asks "最近新闻对我的资产有什么影响？"; workspace displays natural progress, then returns a structured beginner-facing answer.
   - Asset: 20-30 second video segment; optional short GIF of the progress-to-answer moment.

3. News Impact Analysis
   - Value: turns live policy/news into user-specific portfolio impact, uncertainty, and safe action.
   - Show: source list -> selected item -> impact path / uncertainty / safe action -> link to Agent.
   - Asset: 10-15 second loop.

4. Simulation Training
   - Value: the system trains judgment and behavior, not just answers questions.
   - Show: historical scenario, user decision with rationale/worry/impulse-control plan, final review, behavior evidence proposal.
   - Asset: 15-20 second video segment.

5. Automations + Profile Confirmation
   - Value: background work is authorized and safe; user controls what can be read and what can be written.
   - Show: automation card settings, recent notification, pending writeback in Profile, accept/reject boundary.
   - Asset: 15-20 second video segment.

### P1: Good support material

6. Portfolio Snapshot / Report
   - Value: portfolio diagnosis is an agent tool and context source.
   - Show: draft review before save, allocation chart, concentration warning, report history.
   - Asset: static screenshot or 8-12 second loop.

7. Onboarding / Context Building
   - Value: FundGene recommendations are based on the user's declared goal, risk tolerance, and learning state.
   - Show: one-question-at-a-time onboarding and transition to Today.
   - Asset: 8 second intro clip only if the deck needs user journey context.

8. Learning Cockpit
   - Value: learning is action-linked, not a detached course library.
   - Show: Today training task and learning-to-action panel.
   - Asset: static screenshot, unless the defense needs education emphasis.

### P2: Technical proof, mostly static

9. Agent Runtime v2 / Safety Architecture
   - Value: backend-owned orchestration, typed worker outputs, persisted traces, event replay, safety policy, eval checks.
   - Show: architecture diagram and eval/test checklist, not a long UI recording.
   - Asset: one slide diagram plus one screenshot of trace/eval evidence if needed.

## 3. Recommended Presentation Structure

1. Problem slide
   - Beginner fund investors do not need more charts first; they need structured judgment, explanation, and guardrails.

2. Product thesis slide
   - "Daily Brief -> Agent task -> safe action -> confirmed writeback -> next brief."

3. Hero product video
   - 60-90 seconds.
   - Flow: Start/Login -> Today -> Agent follow-up -> News impact -> Simulation review -> Automations/Profile confirmation.
   - Use MP4 in PowerPoint; export a compressed fallback GIF only if needed.

4. Feature cards
   - 4-5 slides, each with one large looping MP4/GIF and one sentence of value.
   - Do not overload with implementation detail.

5. Architecture and safety slide
   - Explain why this is an engineering project: backend-owned business truth, schema-first outputs, traceability, confirmed writebacks, eval checks.

6. Closing slide
   - FundGene is a disciplined investment coach agent for beginners, not a trading bot.

## 4. Asset List

### Main narrative asset

| Asset | Format | Length | Purpose |
| --- | --- | --- | --- |
| `assets/final/fundgene-defense-demo.mp4` | MP4 | 45.73s | Continuous polished demo: Today -> Agent -> News -> Simulation -> Automations/Profile -> brand close |
| `assets/final/fundgene-defense-demo-preview.gif` | GIF | 45.73s | Lightweight preview/fallback; use MP4 as the primary review asset |
| `asset-review.html` | HTML | static | Local review page for the main video and feature GIFs |

### Feature-card assets

| Asset | Route(s) | Format | Length | Purpose |
| --- | --- | --- | --- | --- |
| `01-hero-command-center.mp4` | `/today` | MP4 + GIF fallback | ~4s | Clean Daily Brief command-center overview |
| `02-today-daily-brief.mp4` | `/today` | MP4 + GIF fallback | ~3s | Show Daily Brief value |
| `03-agent-workspace.mp4` | `/agent` | MP4 + GIF fallback | ~8s | Show direct new-task entry, realistic typing, task execution, and a relevant answer naming the news item |
| `04-news-impact.mp4` | `/news`, `/agent` | MP4 + GIF fallback | ~12s | Show news-to-portfolio impact, follow-up jump into Agent, Agent submission, and answer dwell |
| `05-simulation-training.mp4` | `/simulation` | MP4 + GIF fallback | ~8s | Show decision, rationale, worry, submission, review, and behavior evidence proposal |
| `06-automation-profile.mp4` | `/automations`, `/profile` | MP4 + GIF fallback | ~7s | Show authorization, immediate run feedback, notification output, and Profile writeback safety |

## 4.1 Current Video Polish

The latest build intentionally makes the asset feel less like raw screen recording and more like a product showcase:

- The synthetic cursor is smaller, lighter, and fades when idle; clicks use a subtler ripple.
- The opening now starts from the product thesis: `FundGene · Agent Command Center`, `先理解，再决策`, and `每天先给基金新手一个可解释、可确认的判断`.
- Chapter cards use a consistent 1.28s hold, blurred upcoming UI as background context, a lighter command-center visual system, and one short action phrase plus one supporting line.
- UI clips now keep the full interface throughout, with no local zoom or focus crop; product motion is carried by the cursor, visible clicks, page transitions, and chapter cards.
- UI clips do not include overlay captions, so the product interface remains unobstructed during real workflow steps.
- The montage closes on a dedicated FundGene brand card: `FundGene`, `Explain · Train · Confirm`, and `让基金新手先理解，再决策。`

## 5. Product Polish Needed Before Recording

### Frontend

1. Add a showcase recording mode.
   - Trigger: `?showcase=1` or localStorage flag.
   - Behavior: stable timestamps, no dev-only labels, no jittery loading placeholders after content is ready, polished demo-safe copy, consistent viewport height.

2. Add deterministic route states for recording.
   - Today should always have a strong Daily Brief judgment.
   - Agent should have a high-signal seeded conversation plus the ability to record a fresh run.
   - News should expose a visually clear selected item and impact path.
   - Simulation should start from a scenario with an obvious decision task and readable review.
   - Automations/Profile should include at least one pending proposal for the confirmation boundary.

3. Add recording-friendly test ids if missing.
   - Needed for Playwright-controlled camera choreography: tabs, safe action links, composer, submit buttons, profile accept/reject controls.

4. Tighten visual rhythm for clips.
   - Prefer single-viewport desktop compositions for `/today`, `/agent`, `/news`, and `/simulation`.
   - Keep mobile screenshots as supporting material, not the main defense GIFs.

### Backend

1. Add or extend a showcase seed script.
   - Proposed command: `pnpm seed:api:showcase`.
   - It should reset/create a demo user, portfolio, learning state, news items, automation settings, notifications, pending proposals, and a few agent sessions.

2. Keep live-model dependency optional for recording.
   - The defense should have one live-model proof if an API key is configured.
   - Main recordings should remain deterministic enough to avoid failed or slow network-dependent captures.

3. Preserve safety boundaries.
   - No trading execution.
   - No return promise language.
   - No hidden canonical behavior profile mutation from weak evidence.

## 6. Recording Workflow

### Preferred route

1. Use Playwright to drive each scenario.
2. Record browser video in WebM.
3. Convert to MP4 and GIF with `ffmpeg`.
4. Review every asset visually.
5. Embed MP4s in PPT; use GIF only when necessary.

### Proposed folder

```text
docs/audits/defense-showcase-2026-05-22/
├── README.md
├── scripts/
│   └── convert-assets.sh
├── raw/
├── assets/
│   ├── mp4/
│   ├── gif/
│   ├── png/
│   └── final/
└── asset-review.html
```

### Recording command

```bash
FUNDGENE_WEB_PORT=3043 pnpm --dir apps/web exec playwright test e2e/showcase-recording.spec.ts --project=chromium --output=/Users/xueyicheng/Documents/SRTP/FundGene/docs/audits/defense-showcase-2026-05-22/raw
```

The recording spec is `apps/web/e2e/showcase-recording.spec.ts`. It uses the existing deterministic FundGene Playwright API fixture, so it can capture clean product footage before the live backend seed path is finalized.

### Batch conversion command

```bash
bash docs/audits/defense-showcase-2026-05-22/scripts/convert-assets.sh
bash docs/audits/defense-showcase-2026-05-22/scripts/build-montage.sh
```

### Conversion commands

MP4:

```bash
ffmpeg -y -ss 0.85 -i input.webm -vf "scale=1440:-2,fps=30" -c:v libx264 -pix_fmt yuv420p -movflags +faststart output.mp4
```

GIF with palette:

```bash
ffmpeg -y -i input.mp4 -vf "fps=20,scale=1120:-1:flags=lanczos,palettegen" palette.png
ffmpeg -y -i input.mp4 -i palette.png -lavfi "fps=20,scale=1120:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=3" output.gif
```

## 7. Tools And Skills

Already sufficient:

- Playwright: scripted browser actions, screenshots, video recording.
- `ffmpeg`: MP4/GIF conversion.
- ImageMagick: contact sheets and image QA.
- Browser/Chrome plugin: manual visual review when cookies or real browser state matter.
- `frontend-design` skill: product UI polish before recording.
- `Presentations` skill: high-polish editable PPTX if we decide to generate the deck here.

Optional:

- `gifski`: can produce smaller/high-quality GIFs, but it is not required because `ffmpeg` palette conversion is already available.
- Figma: useful for a polished architecture/product storyboard, but not required for the first showcase assets.

## 8. Immediate Execution Plan

1. Build a showcase seed/state contract.
2. Add Playwright showcase recording scripts.
3. Run visual smoke once to confirm current UI quality.
4. Make targeted frontend polish for the P0 routes.
5. Record MP4 clips.
6. Convert GIF fallbacks.
7. Create a contact sheet for quick asset review.
8. Build a PPTX or provide an asset folder plus slide-by-slide script.

## 9. Current Caveats

- The repository currently has an existing dirty worktree from ongoing Agent Runtime v2 / queued follow-up work. Showcase changes should not revert those edits.
- Current Playwright visual smoke already captures many static screenshots, but it does not yet produce defense-ready motion assets.
- Main recordings should use deterministic seeded data; live DeepSeek/model calls are better shown once as proof, not used as the only path for all clips.

## 10. Current Pipeline Result

Implemented:

- `apps/web/e2e/showcase-recording.spec.ts`
- `docs/audits/defense-showcase-2026-05-22/scripts/convert-assets.sh`
- `docs/audits/defense-showcase-2026-05-22/scripts/build-montage.sh`
- `docs/audits/defense-showcase-2026-05-22/asset-review.html`

Polished after first review:

- `/agent` user message bubble contrast fixed for the Agent Command Center shell.
- The 01 showcase clip now avoids cross-page navigation and stays on the Today command center.
- The recording script adds a presentation-only arrow cursor overlay with requestAnimationFrame movement and click ripple.
- Conversion trims the first 0.85 seconds of captured video to remove loading-state dead time.
- GIF output is 1120px wide at 20fps for smoother presentation embedding.
- A continuous 45.33-second main demo video is now generated from the feature clips with product-story chapter slates and soft crossfade transitions.
- The first Agent segment now lands directly in a blank new task, types the user question naturally, submits, and dwells on a portfolio-concentration answer.
- The Agent answer and News follow-up prompt now name the selected policy/news item, avoiding vague "this news" wording without context.
- Agent answer summaries now render without the previous three-line ellipsis truncation in the command-center message card.
- The News-to-Agent segment now uses a separate news-impact answer, so the two Agent visits no longer show the same response.
- The News segment now follows the "let Agent explain with my portfolio" action into the Agent workspace, submits the carried prompt, and dwells on the answer.
- The Simulation segment now completes a decision submission and shows the resulting review/proposal state.
- The Automations/Profile segment now shows stronger immediate-run feedback, then a Profile confirmation page without the previous empty middle area.
- Agent streaming in the showcase fixture now waits about 0.85 seconds before returning, so the pending assistant message animation is visible in the video.
- The Today page now includes an "Agent 已检查" signal board to make the first screen denser and better explain what context the daily judgment used.
- Both Agent showcase replies were expanded into fuller beginner-facing explanations, while keeping the two Agent visits distinct.
- Showcase-mode Agent answer cards now reveal in layers after the waiting state, so the reply feels generated rather than pasted instantly.
- The montage chapter copy now follows one product loop: FundGene -> 用户追问 -> 资讯触发 -> 情境训练 -> 后台跟进 -> 核心价值.

Verified:

```bash
FUNDGENE_WEB_PORT=3043 pnpm --dir apps/web exec playwright test e2e/showcase-recording.spec.ts --project=chromium --output=/Users/xueyicheng/Documents/SRTP/FundGene/docs/audits/defense-showcase-2026-05-22/raw
docs/audits/defense-showcase-2026-05-22/scripts/convert-assets.sh
docs/audits/defense-showcase-2026-05-22/scripts/build-montage.sh
pnpm lint:web
git diff --check -- apps/web/app/globals.css apps/web/components/coach-workspace.tsx apps/web/components/news-workspace.tsx apps/web/components/automations-workspace.tsx apps/web/components/profile-workspace.tsx apps/web/e2e/fixtures.ts apps/web/e2e/showcase-recording.spec.ts docs/audits/defense-showcase-2026-05-22
```

Result:

```text
6 passed
lint:web passed
git diff --check passed
```

Generated assets:

```text
docs/audits/defense-showcase-2026-05-22/assets/mp4/
docs/audits/defense-showcase-2026-05-22/assets/gif/
docs/audits/defense-showcase-2026-05-22/assets/png/showcase-contact-sheet.png
docs/audits/defense-showcase-2026-05-22/assets/final/fundgene-defense-demo.mp4
docs/audits/defense-showcase-2026-05-22/assets/final/fundgene-defense-demo-preview.gif
docs/audits/defense-showcase-2026-05-22/asset-review.html
```

Important note:

- Use `fundgene-defense-demo.mp4` as the opening story video.
- Use the single-feature GIF/MP4 clips as backup material if the defense flow needs shorter feature cuts.
- PPT generation remains out of scope for this iteration; the accepted deliverable is the optimized video asset set.
