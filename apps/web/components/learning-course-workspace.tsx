"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import {
  BookOpenCheck,
  CheckCircle2,
  ChevronDown,
  ClipboardList,
  GraduationCap,
  ListChecks,
  MessageCircleQuestion,
  RotateCcw,
  Target,
} from "lucide-react";

import {
  ApiError,
  getLearningCourse,
  getSessionUser,
  type LearningCourseDetail,
  type LearningSectionDetail,
  updateLearningProgress,
} from "@/lib/api";
import { formatProductCopy } from "@/lib/display-labels";
import { buildAgentPromptHref } from "@/lib/navigation";
import { MetricCard } from "./metric-card";
import { SectionBlock } from "./section-block";
import { cn, ProgressBar, StatusPill } from "./ui/primitives";

function isApiError(error: unknown): error is ApiError {
  return error instanceof ApiError;
}

function formatCourseStatus(status: string): string {
  if (status === "completed") {
    return "已完成";
  }
  if (status === "in_progress") {
    return "进行中";
  }
  return "待开始";
}

function formatSectionPosition(position: number): string {
  return `第 ${String(position).padStart(2, "0")} 节`;
}

function getSectionModeLabel(section: LearningSectionDetail, isNextSection: boolean): string {
  if (section.completed) {
    return "复习";
  }

  if (isNextSection) {
    return "当前推进";
  }

  return "预习";
}

function getSectionModeTone(
  section: LearningSectionDetail,
  isNextSection: boolean,
): "positive" | "accent" | "neutral" {
  if (section.completed) {
    return "positive";
  }

  if (isNextSection) {
    return "accent";
  }

  return "neutral";
}

function buildSectionStudyBlocks(
  course: LearningCourseDetail,
  section: LearningSectionDetail,
) {
  const sectionTitle = formatProductCopy(section.title);
  const sectionSummary = formatProductCopy(section.summary);
  const courseFocus = formatProductCopy(course.focus || course.title);

  return [
    {
      title: "学习块",
      icon: BookOpenCheck,
      items: [
        sectionSummary,
        `把「${sectionTitle}」放回主线：${courseFocus}`,
        "用一句自己的话写下它会怎样改变你看基金信息的顺序。",
      ],
    },
    {
      title: "微自检",
      icon: ListChecks,
      items: [
        `我能不能不用“涨了/跌了”来解释「${sectionTitle}」？`,
        "如果看到一只陌生基金，我会先检查哪一类信息，再看短期收益？",
      ],
    },
    {
      title: "反思提示",
      icon: RotateCcw,
      items: [
        "最近一次做基金判断时，我有没有被标题、热度或短期波动带走？",
        "下一次问教练前，我需要补充哪条个人背景，才能得到更贴近自己的解释？",
      ],
    },
  ];
}

function getActiveSection(course: LearningCourseDetail): LearningSectionDetail | null {
  return (
    course.sections.find((section) => section.slug === course.nextSectionSlug) ??
    course.sections.find((section) => !section.completed) ??
    course.sections[0] ??
    null
  );
}

function getCurrentTrainingGoal(
  course: LearningCourseDetail,
  section: LearningSectionDetail | null,
): string {
  if (!section) {
    return `把「${formatProductCopy(course.title)}」整理成一个可复用的判断动作。`;
  }

  if (section.completed && course.status === "completed") {
    return `复盘「${formatProductCopy(course.title)}」，挑一个问题带回教练区。`;
  }

  return `完成「${formatProductCopy(section.title)}」，把它变成一次真实基金判断前的检查动作。`;
}

function getWhyLearnNow(
  course: LearningCourseDetail,
  section: LearningSectionDetail | null,
): string {
  const focus = formatProductCopy(course.focus || course.description);

  if (!section) {
    return `现在先整理这门课，是为了把 ${focus} 从知识点变成可复用的判断顺序。`;
  }

  return `现在学这一节，是因为它直接服务于 ${focus}：先补判断语言，再去看组合、新闻或模拟场景。`;
}

function getCompletionApplication(section: LearningSectionDetail | null): string {
  if (!section) {
    return "回到工作台，选择一个真实场景，用今天整理出的判断顺序完成一次复盘。";
  }

  return `标记完成后，用一句话回答「${formatProductCopy(section.title)} 会怎样改变我下一次看基金信息的顺序」。`;
}

function SectionLearningCard({
  course,
  section,
  isNextSection,
  isSubmitting,
  disableCompletion,
  onComplete,
}: {
  course: LearningCourseDetail;
  section: LearningSectionDetail;
  isNextSection: boolean;
  isSubmitting: boolean;
  disableCompletion: boolean;
  onComplete: (sectionSlug: string) => void;
}) {
  const studyBlocks = buildSectionStudyBlocks(course, section);
  const modeLabel = getSectionModeLabel(section, isNextSection);
  const modeTone = getSectionModeTone(section, isNextSection);

  return (
    <article
      className={cn(
        "paper-panel-strong overflow-hidden",
        isNextSection && !section.completed ? "ring-1 ring-[rgba(70,208,172,0.38)]" : null,
      )}
    >
      <details open={isNextSection && !section.completed} className="group">
        <summary className="flex cursor-pointer list-none flex-col gap-4 px-4 py-4 lg:flex-row lg:items-start lg:justify-between lg:px-5">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <span className="section-kicker">{formatSectionPosition(section.position)}</span>
              <StatusPill tone={modeTone}>{modeLabel}</StatusPill>
              <span className="rounded-full border border-[color:var(--line-soft)] bg-white/72 px-3 py-1 text-xs font-semibold text-[color:var(--ink-muted)]">
                {section.estimatedDurationMinutes} 分钟
              </span>
            </div>
            <h2 className="mt-3 text-xl font-semibold leading-tight text-[color:var(--ink-strong)]">
              {formatProductCopy(section.title)}
            </h2>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-[color:var(--ink-soft)]">
              {formatProductCopy(section.summary)}
            </p>
          </div>
          <span className="flex items-center gap-3 text-sm font-semibold text-[color:var(--ink-soft)]">
            {section.completed ? "展开复习" : "展开学习"}
            <ChevronDown
              aria-hidden="true"
              className="size-4 transition-transform duration-200 group-open:rotate-180"
            />
          </span>
        </summary>

        <div className="border-t border-[color:var(--line-soft)] px-4 pb-5 pt-4 lg:px-5">
          <div className="grid gap-3 xl:grid-cols-3">
            {studyBlocks.map((block) => {
              const Icon = block.icon;

              return (
                <section
                  key={block.title}
                  className="workbench-panel-flat p-4"
                >
                  <div className="flex items-center gap-2">
                    <span className="grid size-8 place-items-center rounded-full border border-[color:var(--line-soft)] bg-white/80 text-[color:var(--accent-teal)]">
                      <Icon aria-hidden="true" className="size-4" />
                    </span>
                    <h3 className="text-sm font-semibold text-[color:var(--ink-strong)]">{block.title}</h3>
                  </div>
                  <ul className="mt-3 space-y-2 text-sm leading-6 text-[color:var(--ink-soft)]">
                    {block.items.map((item) => (
                      <li key={item} className="flex gap-2">
                        <span
                          aria-hidden="true"
                          className="mt-2 size-1.5 flex-none rounded-full bg-[color:var(--accent-teal)]"
                        />
                        <span>{item}</span>
                      </li>
                    ))}
                  </ul>
                </section>
              );
            })}
          </div>

          <div className="mt-4 flex flex-col gap-3 rounded-lg border border-[color:var(--line-soft)] bg-white/72 px-4 py-4 lg:flex-row lg:items-center lg:justify-between">
            <div className="min-w-0">
              <p className="text-sm font-semibold text-[color:var(--ink-strong)]">
                {section.completed ? "已完成的小节仍可复习" : "读完后先做一次微自检"}
              </p>
              <p className="mt-1 text-sm leading-6 text-[color:var(--ink-soft)]">
                {section.completed
                  ? "回看学习块，挑一个反思问题带去教练区继续追问。"
                  : "确认自己能解释学习重点，再把本节记录为完成。"}
              </p>
            </div>
            <div className="flex flex-wrap gap-3">
              <Link
                href={buildAgentPromptHref({
                  focus: "learning",
                  from: "learning",
                  prompt:
                    "请用一个简单例子帮我理解这节内容，并告诉我怎么用到我的基金决策里。",
                  sourceIds: {
                    course_slug: course.courseSlug,
                    section_slug: section.slug,
                  },
                })}
                className="action-button-secondary"
              >
                <MessageCircleQuestion aria-hidden="true" className="size-4" />
                继续问教练
              </Link>
              <button
                type="button"
                className={section.completed ? "action-button-secondary" : "action-button"}
                onClick={() => onComplete(section.slug)}
                disabled={disableCompletion}
              >
                <CheckCircle2 aria-hidden="true" className="size-4" />
                {section.completed
                  ? "已记录完成"
                  : isSubmitting
                    ? "提交中..."
                    : "标记为已完成"}
              </button>
            </div>
          </div>
        </div>
      </details>
    </article>
  );
}

export function LearningCourseWorkspace() {
  const params = useParams<{ courseSlug: string }>();
  const courseSlug = typeof params.courseSlug === "string" ? params.courseSlug : "";
  const queryClient = useQueryClient();
  const [submitError, setSubmitError] = useState<string | null>(null);

  const sessionQuery = useQuery({
    queryKey: ["session-user"],
    queryFn: getSessionUser,
    retry: false,
  });

  const courseQuery = useQuery({
    queryKey: ["learning-course", courseSlug],
    queryFn: () => getLearningCourse(courseSlug),
    enabled: Boolean(sessionQuery.data?.profileId && courseSlug),
    retry: false,
  });

  const progressMutation = useMutation({
    mutationFn: async (sectionSlug: string) =>
      updateLearningProgress({ courseSlug, sectionSlug }),
    onSuccess: async () => {
      setSubmitError(null);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["learning-course", courseSlug] }),
        queryClient.invalidateQueries({ queryKey: ["learning-path"] }),
        queryClient.invalidateQueries({ queryKey: ["dashboard"] }),
      ]);
    },
    onError: (error) => {
      setSubmitError(error instanceof Error ? error.message : "提交失败，请稍后重试。");
    },
  });

  if (sessionQuery.isLoading) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/60 px-5 py-4 text-sm text-[color:var(--ink-soft)]">
        正在初始化课程上下文...
      </div>
    );
  }

  if (isApiError(sessionQuery.error) && sessionQuery.error.status === 401) {
    return (
      <SectionBlock
        eyebrow="课程详情"
        title="课程详情需要先登录。"
        description="登录后才能同步课程进度和小节完成状态。"
      >
        <div className="paper-panel-strong rounded-lg px-5 py-6">
          <div className="flex flex-wrap gap-3">
            <Link href="/start" className="action-button">
              去登录 / 注册
            </Link>
            <Link href="/learning" className="action-button-secondary">
              返回学习中心
            </Link>
          </div>
        </div>
      </SectionBlock>
    );
  }

  if (sessionQuery.error || !sessionQuery.data || !sessionQuery.data.profileId) {
    return (
      <SectionBlock
        eyebrow="课程详情"
        title="先建立基础画像，再进入课程详情。"
        description="学习课程记录会直接绑定到产品用户，因此至少需要先在建档中保存基础资料。"
      >
        <div className="flex flex-wrap gap-3">
          <Link href="/onboarding" className="action-button">
            去建立基础画像
          </Link>
          <Link href="/learning" className="action-button-secondary">
            返回学习中心
          </Link>
        </div>
      </SectionBlock>
    );
  }

  if (courseQuery.isLoading) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/60 px-5 py-4 text-sm text-[color:var(--ink-soft)]">
        正在同步课程详情与 section 完成状态...
      </div>
    );
  }

  if (courseQuery.error || !courseQuery.data) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
        课程详情加载失败：{courseQuery.error?.message ?? "未知错误"}
      </div>
    );
  }

  const course = courseQuery.data;
  const activeSection = getActiveSection(course);
  const activeSectionSlug = activeSection?.slug ?? null;
  const pendingSectionSlug = progressMutation.variables;

  return (
    <div className="space-y-6">
      <SectionBlock
        eyebrow="当前训练目标"
        title={getCurrentTrainingGoal(course, activeSection)}
        description="课程详情页先服务今天的训练任务；下面的小节是完成任务的材料和记录入口。"
      >
        <div className="paper-panel-strong grid gap-4 px-4 py-4 lg:grid-cols-[minmax(0,1fr)_minmax(17rem,0.4fr)] lg:items-start lg:px-5">
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <span className="section-kicker">{formatProductCopy(course.pathTitle)}</span>
              <StatusPill tone={course.status === "completed" ? "positive" : "accent"}>
                {formatCourseStatus(course.status)}
              </StatusPill>
              <span className="text-sm font-semibold text-[color:var(--ink-soft)]">
                {course.completedSectionCount}/{course.sectionCount} 小节已记录
              </span>
            </div>
            <ProgressBar
              className="mt-4"
              value={course.progressPercentage}
              label="训练材料完成度"
            />
            <div className="mt-5 grid gap-3 xl:grid-cols-3">
              <div className="workbench-panel-flat p-4">
                <div className="flex items-center gap-2">
                  <Target
                    aria-hidden="true"
                    className="size-4 text-[color:var(--accent-teal)]"
                  />
                  <h2 className="text-sm font-semibold text-[color:var(--ink-strong)]">
                    训练目标
                  </h2>
                </div>
                <p className="mt-2 text-sm leading-6 text-[color:var(--ink-soft)]">
                  {getCurrentTrainingGoal(course, activeSection)}
                </p>
              </div>
              <div className="workbench-panel-flat p-4">
                <div className="flex items-center gap-2">
                  <BookOpenCheck
                    aria-hidden="true"
                    className="size-4 text-[color:var(--accent-teal)]"
                  />
                  <h2 className="text-sm font-semibold text-[color:var(--ink-strong)]">
                    为什么现在学
                  </h2>
                </div>
                <p className="mt-2 text-sm leading-6 text-[color:var(--ink-soft)]">
                  {getWhyLearnNow(course, activeSection)}
                </p>
              </div>
              <div className="workbench-panel-flat p-4">
                <div className="flex items-center gap-2">
                  <ClipboardList
                    aria-hidden="true"
                    className="size-4 text-[color:var(--accent-teal)]"
                  />
                  <h2 className="text-sm font-semibold text-[color:var(--ink-strong)]">
                    完成后的应用动作
                  </h2>
                </div>
                <p className="mt-2 text-sm leading-6 text-[color:var(--ink-soft)]">
                  {getCompletionApplication(activeSection)}
                </p>
              </div>
            </div>
          </div>
          <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/74 px-4 py-4 shadow-sm backdrop-blur-2xl">
            <p className="section-kicker">训练材料</p>
            <h2 className="mt-2 text-xl font-semibold leading-tight text-[color:var(--ink-strong)]">
              {formatProductCopy(course.title)}
            </h2>
            <p className="mt-2 text-sm leading-6 text-[color:var(--ink-soft)]">
              {formatProductCopy(course.description)}
            </p>
            <p className="mt-4 text-sm font-semibold text-[color:var(--ink-strong)]">
              {course.nextSectionTitle
                ? `下一步：${formatProductCopy(course.nextSectionTitle)}`
                : "下一步：复盘并提问"}
            </p>
            <p className="mt-1 text-sm leading-6 text-[color:var(--ink-soft)]">
              {course.nextSectionTitle
                ? "先打开下一节，把自检问题跑一遍，再标记完成。"
                : "课程完成后仍可以回看任意小节，或带着反思问题问教练。"}
            </p>
            <div className="mt-4 flex flex-wrap gap-3">
              <a href="#course-section-list" className="action-button">
                <GraduationCap aria-hidden="true" className="size-4" />
                进入当前材料
              </a>
              <Link
                href={buildAgentPromptHref({
                  focus: "learning",
                  from: "learning",
                  prompt:
                    "请用一个简单例子帮我理解这节内容，并告诉我怎么用到我的基金决策里。",
                  sourceIds: {
                    course_slug: course.courseSlug,
                    section_slug: activeSection?.slug,
                  },
                })}
                className="action-button-secondary"
              >
                <MessageCircleQuestion aria-hidden="true" className="size-4" />
                问教练
              </Link>
            </div>
          </div>
        </div>
      </SectionBlock>

      <SectionBlock
        eyebrow="训练材料小节"
        title="每一节都要服务一个可执行判断动作。"
        description="当前该推进的小节会默认展开；完成后仍可以复习学习块、做反思，并继续向教练追问。"
      >
        <div id="course-section-list" className="scroll-mt-6 space-y-4">
          {course.sections.map((section) => (
            <SectionLearningCard
              key={section.slug}
              course={course}
              section={section}
              isNextSection={section.slug === activeSectionSlug}
              isSubmitting={pendingSectionSlug === section.slug && progressMutation.isPending}
              disableCompletion={section.completed || progressMutation.isPending}
              onComplete={(sectionSlug) => progressMutation.mutate(sectionSlug)}
            />
          ))}
        </div>

        {submitError ? (
          <div
            className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
            role="alert"
          >
            {submitError}
          </div>
        ) : null}

        <div className="flex flex-wrap gap-3">
          <Link href="/learning" className="action-button-secondary">
            返回学习中心
          </Link>
          <Link href="/dashboard" className="action-button">
            去看工作台更新
          </Link>
        </div>
      </SectionBlock>

      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label="课程状态"
          value={formatCourseStatus(course.status)}
          detail={course.focus}
          accent="moss"
        />
        <MetricCard
          label="课程进度"
          value={`${course.progressPercentage}%`}
          detail={`已完成 ${course.completedSectionCount}/${course.sectionCount} 个小节`}
          accent="gold"
        />
        <MetricCard
          label="预计时长"
          value={`${course.estimatedDurationMinutes} 分钟`}
          detail="先用可完成的小节推进，而不是堆叠大而空的内容。"
          accent="clay"
        />
        <MetricCard
          label="下一节"
          value={
            course.nextSectionTitle ? formatProductCopy(course.nextSectionTitle) : "已全部完成"
          }
          detail="完成当前课后，工作台的学习动作也会一起刷新。"
          accent="moss"
        />
      </div>
    </div>
  );
}
