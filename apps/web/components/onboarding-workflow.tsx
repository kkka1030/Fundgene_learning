"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { startTransition, useMemo, useState } from "react";

import {
  submitRiskQuestionnaire,
  upsertOnboardingProfile,
  type OnboardingProfileInput,
} from "@/lib/api";

const QUESTION_SET = [
  {
    id: "volatility_comfort",
    title: "面对基金净值波动时，你的舒适度更接近哪一档？",
    choices: [
      { label: "稍有波动就会不安，希望尽快回到稳定状态", score: 1 },
      { label: "能接受小幅波动，但不希望净值起伏太大", score: 2 },
      { label: "中等波动可以接受，只要长期逻辑还在", score: 3 },
      { label: "较大波动也能承受，愿意换取更高长期收益", score: 4 },
      { label: "波动不是问题，更在意长期回报空间", score: 5 },
    ],
  },
  {
    id: "drawdown_reaction",
    title: "如果持仓一个月回撤 12%，你最可能的动作是？",
    choices: [
      { label: "立刻卖出，先止损再说", score: 1 },
      { label: "减仓一部分，先把波动降下来", score: 2 },
      { label: "先停下来复盘，再决定要不要调整", score: 3 },
      { label: "维持计划，不因为单月回撤改策略", score: 4 },
      { label: "如果逻辑没变，甚至会考虑逢低补一点", score: 5 },
    ],
  },
  {
    id: "investment_horizon",
    title: "你打算给这笔基金投资多长的观察周期？",
    choices: [
      { label: "不到一年，主要先试试水", score: 1 },
      { label: "一到两年，看情况再说", score: 2 },
      { label: "两到三年，愿意做阶段性复盘", score: 3 },
      { label: "三到五年，按周期看策略执行", score: 4 },
      { label: "五年以上，用长期视角看复利", score: 5 },
    ],
  },
  {
    id: "panic_sell_impulse",
    title: "市场连续下跌时，你有多容易冒出“先卖掉再说”的念头？",
    choices: [
      { label: "几乎不会，我通常先看计划", score: 1 },
      { label: "偶尔会，但能很快压住", score: 2 },
      { label: "会犹豫，需要再看看", score: 3 },
      { label: "经常会，情绪影响比较明显", score: 4 },
      { label: "非常强烈，通常很难不动手", score: 5 },
    ],
  },
  {
    id: "chase_hot_funds",
    title: "看到朋友讨论热门基金或短期高收益榜单时，你有多想跟进？",
    choices: [
      { label: "几乎不会，先看是否适合自己的计划", score: 1 },
      { label: "会关注，但不急着上车", score: 2 },
      { label: "有时会心动，想再多比较一下", score: 3 },
      { label: "经常会想跟一笔，怕错过机会", score: 4 },
      { label: "很难忍住，通常会优先考虑追热点", score: 5 },
    ],
  },
  {
    id: "diversification_habit",
    title: "在选择基金时，你平时有多重视分散配置？",
    choices: [
      { label: "基本不考虑，哪只看起来强就买哪只", score: 1 },
      { label: "会听过分散，但执行不太稳定", score: 2 },
      { label: "知道需要分散，但还没形成固定方法", score: 3 },
      { label: "通常会控制单一主题或风格集中度", score: 4 },
      { label: "会主动从资产和风格两个层面做分散", score: 5 },
    ],
  },
];

const INITIAL_PROFILE: OnboardingProfileInput = {
  displayName: "",
  investingExperience: "beginner",
  monthlyContributionBand: "under_3000",
  primaryGoal: "先建立一套能长期坚持的基金投资习惯",
};

type QuestionnaireState = Record<string, number>;

const PROFILE_STEPS = [
  {
    title: "怎么称呼你？",
    hint: "用于工作台和 Agent 回答里的称呼。",
  },
  {
    title: "你现在的基金经验更接近哪一档？",
    hint: "这会影响解释的细度，不影响任何账户操作。",
  },
  {
    title: "你每月大概能投入多少？",
    hint: "只用于理解节奏和风险承受，不会生成买卖指令。",
  },
  {
    title: "你现在最想解决的目标是什么？",
    hint: "写一句真实目标就够了，之后可以在资料页改。",
  },
] as const;

const TOTAL_STEPS = PROFILE_STEPS.length + QUESTION_SET.length;

function clampProgress(value: number): number {
  return Math.max(0, Math.min(100, Math.round(value)));
}

function normalizeProfile(profile: OnboardingProfileInput): OnboardingProfileInput {
  return {
    ...profile,
    displayName: profile.displayName.trim(),
    primaryGoal: profile.primaryGoal.trim(),
  };
}

function validateProfileStep(profile: OnboardingProfileInput, stepIndex: number): string | null {
  if (stepIndex === 0 && profile.displayName.trim().length < 2) {
    return "请填写至少 2 个字符的称呼。";
  }

  if (stepIndex === 3 && profile.primaryGoal.trim().length < 8) {
    return "请把目标写得再具体一点，至少 8 个字符。";
  }

  return null;
}

export function OnboardingWorkflow() {
  const router = useRouter();
  const queryClient = useQueryClient();
  const [profile, setProfile] = useState(INITIAL_PROFILE);
  const [questionnaire, setQuestionnaire] = useState<QuestionnaireState>({});
  const [activeStep, setActiveStep] = useState(0);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [isCompleted, setIsCompleted] = useState(false);

  const questionnaireMutation = useMutation({
    mutationFn: async ({
      answers,
      profile: profileInput,
    }: {
      answers: QuestionnaireState;
      profile: OnboardingProfileInput;
    }) => {
      await upsertOnboardingProfile(profileInput);
      return submitRiskQuestionnaire({ answers });
    },
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["dashboard"] }),
        queryClient.invalidateQueries({ queryKey: ["session-user"] }),
        queryClient.invalidateQueries({ queryKey: ["behavior-profile"] }),
        queryClient.invalidateQueries({ queryKey: ["coach-session"] }),
      ]);
      setIsCompleted(true);
      startTransition(() => {
        router.push("/today");
      });
    },
    onError: (error) => {
      setSubmitError(error instanceof Error ? error.message : "提交失败，请稍后重试。");
    },
  });

  const profileStep = activeStep < PROFILE_STEPS.length ? PROFILE_STEPS[activeStep] : null;
  const questionIndex = activeStep - PROFILE_STEPS.length;
  const currentQuestion = questionIndex >= 0 ? QUESTION_SET[questionIndex] : null;
  const answeredCount = Object.keys(questionnaire).length;
  const progress = clampProgress(((activeStep + 1) / TOTAL_STEPS) * 100);
  const stepLabel = `第 ${activeStep + 1} / ${TOTAL_STEPS} 步`;
  const canGoBack = activeStep > 0 && !questionnaireMutation.isPending;

  const completedMarkers = useMemo(
    () =>
      Array.from({ length: TOTAL_STEPS }, (_item, index) => {
        if (index < PROFILE_STEPS.length) {
          return index < activeStep;
        }
        const question = QUESTION_SET[index - PROFILE_STEPS.length];
        return questionnaire[question.id] !== undefined;
      }),
    [activeStep, questionnaire],
  );

  function goNext() {
    setSubmitError(null);

    if (profileStep) {
      const error = validateProfileStep(profile, activeStep);
      if (error) {
        setSubmitError(error);
        return;
      }
      setActiveStep((step) => Math.min(step + 1, TOTAL_STEPS - 1));
      return;
    }

    if (!currentQuestion) {
      return;
    }

    if (questionnaire[currentQuestion.id] === undefined) {
      setSubmitError("先选择一个最接近你的答案，再继续。");
      return;
    }

    if (activeStep < TOTAL_STEPS - 1) {
      setActiveStep((step) => step + 1);
      return;
    }

    const missingQuestion = QUESTION_SET.find(
      (question) => questionnaire[question.id] === undefined,
    );
    if (missingQuestion) {
      setSubmitError(`还差一题：${missingQuestion.title}`);
      setActiveStep(PROFILE_STEPS.length + QUESTION_SET.indexOf(missingQuestion));
      return;
    }

    const normalizedProfile = normalizeProfile(profile);
    const nameError = validateProfileStep(normalizedProfile, 0);
    const goalError = validateProfileStep(normalizedProfile, 3);
    if (nameError || goalError) {
      setSubmitError(nameError ?? goalError);
      setActiveStep(nameError ? 0 : 3);
      return;
    }

    questionnaireMutation.mutate({
      answers: questionnaire,
      profile: normalizedProfile,
    });
  }

  function renderProfileControl() {
    if (activeStep === 0) {
      return (
        <label className="block space-y-3">
          <span className="text-sm font-semibold text-[color:var(--ink-strong)]">称呼</span>
          <input
            className="field-input text-lg"
            value={profile.displayName}
            onChange={(event) =>
              setProfile((current) => ({ ...current, displayName: event.target.value }))
            }
            placeholder="例如：小陈"
            autoFocus
          />
        </label>
      );
    }

    if (activeStep === 1) {
      return (
        <div className="grid gap-2">
          {[
            ["beginner", "刚开始了解基金"],
            ["starter", "已经定投，但还没形成稳定策略"],
            ["intermediate", "有一些配置经验，想把方法做稳"],
          ].map(([value, label]) => (
            <label
              key={value}
              className={`choice-item ${profile.investingExperience === value ? "choice-item-active" : ""}`}
            >
              <input
                type="radio"
                name="investingExperience"
                value={value}
                checked={profile.investingExperience === value}
                onChange={() =>
                  setProfile((current) => ({ ...current, investingExperience: value }))
                }
              />
              <span>{label}</span>
            </label>
          ))}
        </div>
      );
    }

    if (activeStep === 2) {
      return (
        <div className="grid gap-2">
          {[
            ["under_3000", "3000 元以内"],
            ["3000_10000", "3000 - 10000 元"],
            ["above_10000", "10000 元以上"],
          ].map(([value, label]) => (
            <label
              key={value}
              className={`choice-item ${profile.monthlyContributionBand === value ? "choice-item-active" : ""}`}
            >
              <input
                type="radio"
                name="monthlyContributionBand"
                value={value}
                checked={profile.monthlyContributionBand === value}
                onChange={() =>
                  setProfile((current) => ({ ...current, monthlyContributionBand: value }))
                }
              />
              <span>{label}</span>
            </label>
          ))}
        </div>
      );
    }

    return (
      <label className="block space-y-3">
        <span className="text-sm font-semibold text-[color:var(--ink-strong)]">当前目标</span>
        <textarea
          className="field-input min-h-[8.5rem] text-base leading-7"
          value={profile.primaryGoal}
          onChange={(event) =>
            setProfile((current) => ({ ...current, primaryGoal: event.target.value }))
          }
          placeholder="例如：建立长期定投习惯，先把风险控制和基金配置逻辑学明白。"
        />
      </label>
    );
  }

  return (
    <div className="mx-auto grid max-w-4xl gap-5">
      <section className="agent-hero overflow-hidden p-5 sm:p-6">
        <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="section-kicker">开始建档</p>
            <h2 className="mt-2 text-3xl font-semibold leading-tight text-[color:var(--ink-strong)]">
              一次只回答一个问题
            </h2>
            <p className="mt-3 max-w-2xl text-sm leading-7 text-[color:var(--ink-soft)]">
              先完成基础画像，再回答 6 个风险与行为问题。完成后进入 Today，看到第一条个人化判断。
            </p>
          </div>
          <div className="rounded-2xl border border-[color:var(--line-soft)] bg-white/72 px-4 py-3 text-sm">
            <p className="font-semibold text-[color:var(--ink-strong)]">{stepLabel}</p>
            <p className="mt-1 text-[color:var(--ink-muted)]">
              风险问卷已答 {answeredCount}/{QUESTION_SET.length} 题
            </p>
          </div>
        </div>
        <div className="mt-5 h-2 overflow-hidden rounded-full bg-white/72">
          <div
            className="h-full rounded-full bg-[linear-gradient(90deg,var(--accent-teal),var(--accent-cyan))] transition-all duration-300"
            style={{ width: `${progress}%` }}
          />
        </div>
        <div className="mt-4 flex gap-1.5 overflow-x-auto pb-1" aria-label="建档步骤">
          {completedMarkers.map((completed, index) => (
            <span
              key={index}
              className={
                index === activeStep
                  ? "h-2.5 w-8 shrink-0 rounded-full bg-[color:var(--accent-teal)]"
                  : completed
                    ? "h-2.5 w-4 shrink-0 rounded-full bg-[rgba(0,113,227,0.32)]"
                    : "h-2.5 w-4 shrink-0 rounded-full bg-[rgba(118,118,128,0.18)]"
              }
            />
          ))}
        </div>
      </section>

      <section className="paper-panel-strong p-5 sm:p-7">
        <div className="max-w-2xl">
          <p className="section-kicker">
            {profileStep ? "基础画像" : "风险与行为问卷"}
          </p>
          <h3 className="mt-2 text-2xl font-semibold leading-tight text-[color:var(--ink-strong)]">
            {profileStep?.title ?? currentQuestion?.title}
          </h3>
          <p className="mt-2 text-sm leading-6 text-[color:var(--ink-soft)]">
            {profileStep?.hint ?? "选最接近你的真实反应，不需要选择“正确答案”。"}
          </p>
        </div>

        <div className="mt-7">
          {profileStep ? (
            renderProfileControl()
          ) : currentQuestion ? (
            <fieldset className="grid gap-2">
              <legend className="sr-only">{currentQuestion.title}</legend>
              {currentQuestion.choices.map((choice) => {
                const checked = questionnaire[currentQuestion.id] === choice.score;
                return (
                  <label
                    key={`${currentQuestion.id}-${choice.score}`}
                    className={`choice-item ${checked ? "choice-item-active" : ""}`}
                  >
                    <input
                      type="radio"
                      name={currentQuestion.id}
                      value={choice.score}
                      checked={checked}
                      onChange={() => {
                        setQuestionnaire((current) => ({
                          ...current,
                          [currentQuestion.id]: choice.score,
                        }));
                        setSubmitError(null);
                      }}
                    />
                    <span>{choice.label}</span>
                  </label>
                );
              })}
            </fieldset>
          ) : null}
        </div>

        {submitError ? (
          <div className="mt-5 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
            {submitError}
          </div>
        ) : null}

        {isCompleted ? (
          <div className="mt-5 rounded-xl border border-green-200 bg-green-50 px-4 py-3 text-sm text-green-700">
            已完成建档，正在跳转到 Today...
          </div>
        ) : null}

        <div className="mt-7 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <button
            type="button"
            className="action-button-secondary justify-center sm:justify-start"
            onClick={() => setActiveStep((step) => Math.max(step - 1, 0))}
            disabled={!canGoBack}
          >
            上一步
          </button>
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
            <Link href="/start" className="action-button-secondary justify-center">
              回到入口
            </Link>
            <button
              type="button"
              className="action-button justify-center"
              onClick={goNext}
              disabled={questionnaireMutation.isPending}
            >
              {questionnaireMutation.isPending
                ? "提交中..."
                : activeStep === TOTAL_STEPS - 1
                  ? "完成建档，进入 Today"
                  : "继续"}
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
