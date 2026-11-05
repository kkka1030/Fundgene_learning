"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import {
  ArrowRight,
  BookOpenCheck,
  ClipboardList,
  Target,
} from "lucide-react";

import {
  ApiError,
  getLearningPath,
  getSessionUser,
  type LearningCourseSummary,
} from "@/lib/api";
import { formatProductCopy } from "@/lib/display-labels";
import { buildAgentPromptHref } from "@/lib/navigation";
import { MetricCard } from "./metric-card";
import { SectionBlock } from "./section-block";
import { ProgressBar, StatusPill } from "./ui/primitives";

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

function getActiveTrainingCourse(
  courses: LearningCourseSummary[],
): LearningCourseSummary | null {
  return courses.find((course) => course.status !== "completed") ?? null;
}

function getTrainingObjective(course: LearningCourseSummary | null): string {
  if (!course) {
    return "把今天的学习沉淀成一次复盘问题。";
  }

  return `用「${formatProductCopy(course.title)}」补齐一个基金判断动作。`;
}

function getTrainingReason(course: LearningCourseSummary | null): string {
  if (!course) {
    return "基础训练已经完成，下一步不是继续堆课程，而是带着真实组合或新闻问题回到教练区校准。";
  }

  return `现在学这部分，是为了先理解 ${formatProductCopy(course.focus)}，再回到组合、模拟或教练对话里做判断。`;
}

function getTrainingApplication(course: LearningCourseSummary | null): string {
  if (!course) {
    return "打开工作台，选择一个真实场景，把今天学到的判断语言用在一次复盘里。";
  }

  if (course.nextSectionTitle) {
    return `完成「${formatProductCopy(course.nextSectionTitle)}」后，用一句话说明它会怎样改变你看基金信息的顺序。`;
  }

  return "完成课程后，回到工作台看新的安全下一步，或带一个反思问题问教练。";
}

export function LearningWorkspace() {
  const sessionQuery = useQuery({
    queryKey: ["session-user"],
    queryFn: getSessionUser,
    retry: false,
  });

  const learningPathQuery = useQuery({
    queryKey: ["learning-path"],
    queryFn: getLearningPath,
    enabled: Boolean(sessionQuery.data?.profileId),
    retry: false,
  });

  if (sessionQuery.isLoading) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/60 px-5 py-4 text-sm text-[color:var(--ink-soft)]">
        正在初始化学习中心上下文...
      </div>
    );
  }

  if (isApiError(sessionQuery.error) && sessionQuery.error.status === 401) {
    return (
      <SectionBlock
        eyebrow="学习中心"
        title="学习中心需要先登录。"
        description="登录后才能同步学习路径、课程进度和后续推荐。"
      >
        <div className="paper-panel-strong rounded-lg px-5 py-6">
          <div className="flex flex-wrap gap-3">
            <Link href="/start" className="action-button">
              去登录 / 注册
            </Link>
            <Link href="/" className="action-button-secondary">
              回到总览
            </Link>
          </div>
        </div>
      </SectionBlock>
    );
  }

  if (sessionQuery.error || !sessionQuery.data) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
        当前学习中心不可用：{sessionQuery.error?.message ?? "未知错误"}
      </div>
    );
  }

  const sessionUser = sessionQuery.data;
  if (!sessionUser.profileId) {
    return (
      <SectionBlock
        eyebrow="学习中心"
        title="先建立基础画像，再进入个人学习路径。"
        description="基础资料会决定学习路径如何排序。"
      >
        <div className="grid gap-4 lg:grid-cols-3">
          <MetricCard
            label="当前用户"
            value={sessionUser.displayName ?? "已登录用户"}
            detail={sessionUser.email ?? sessionUser.id}
            accent="moss"
          />
          <MetricCard
            label="基础画像"
            value="待建立"
            detail="先保存基础资料，再进入学习路径。"
            accent="gold"
          />
          <MetricCard
            label="学习模式"
            value="路径优先"
            detail="先走一条主路径，避免被内容噪音打散。"
            accent="clay"
          />
        </div>
        <div className="flex flex-wrap gap-3">
          <Link href="/onboarding" className="action-button">
            去建立基础画像
          </Link>
          <Link
            href={buildAgentPromptHref({
              focus: "learning",
              from: "learning",
              prompt:
                "请用一个简单例子帮我理解这节内容，并告诉我怎么用到我的基金决策里。",
            })}
            className="action-button-secondary"
          >
            先去教练提问
          </Link>
        </div>
      </SectionBlock>
    );
  }

  if (learningPathQuery.isLoading) {
    return (
      <div className="rounded-lg border border-[color:var(--line-soft)] bg-white/60 px-5 py-4 text-sm text-[color:var(--ink-soft)]">
        正在同步学习路径与课程进度...
      </div>
    );
  }

  if (learningPathQuery.error || !learningPathQuery.data) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
        学习路径加载失败：{learningPathQuery.error?.message ?? "未知错误"}
      </div>
    );
  }

  const learningPath = learningPathQuery.data;
  const activeTrainingCourse = getActiveTrainingCourse(learningPath.courses);
  const recommendedCourse = activeTrainingCourse ?? learningPath.courses[0] ?? null;
  const totalSectionCount = learningPath.courses.reduce(
    (sum, course) => sum + course.sectionCount,
    0,
  );
  const completedSectionCount = learningPath.courses.reduce(
    (sum, course) => sum + course.completedSectionCount,
    0,
  );
  const activeSectionTitle =
    recommendedCourse?.nextSectionTitle ??
    learningPath.recommendedCourseTitle ??
    "复盘今天的真实问题";
  const actionCards = [
    {
      title: "相关组合检查",
      detail: "把今天的知识点放回持仓、风险和现金比例里看。",
      href: "/portfolio",
      label: "查看组合",
    },
    {
      title: "相关资讯影响",
      detail: "看当天新闻有没有触发同一类风险或行为冲动。",
      href: "/news",
      label: "看资讯",
    },
    {
      title: "向教练提问",
      detail: "把不懂的概念转成一个具体投资判断问题。",
      href: "/agent?focus=learning",
      label: "问教练",
    },
  ];

  return (
    <div className="learning-command-page command-single-page">
      <header className="learning-command-topbar command-page-heading">
        <div>
          <p className="section-kicker">今日训练任务</p>
          <h1>{getTrainingObjective(activeTrainingCourse)}</h1>
          <p>{getTrainingReason(activeTrainingCourse)}</p>
        </div>
        <StatusPill tone="accent">
          {completedSectionCount}/{totalSectionCount || completedSectionCount} 小节
        </StatusPill>
      </header>

      <section className="learning-command-grid">
        <aside className="learning-path-panel paper-panel-strong">
          <div className="learning-panel-heading">
            <div>
              <p className="section-kicker">我的学习路径</p>
              <h2>{formatProductCopy(learningPath.title)}</h2>
            </div>
            <span>{learningPath.overallProgressPercentage}%</span>
          </div>
          <ProgressBar
            className="mt-4"
            value={learningPath.overallProgressPercentage}
            label="学习路径总进度"
          />
          <div className="learning-path-counts">
            <div>
              <span>已完成</span>
              <strong>{learningPath.completedCoursesCount}</strong>
            </div>
            <div>
              <span>总课程</span>
              <strong>{learningPath.totalCourses}</strong>
            </div>
            <div>
              <span>已记录小节</span>
              <strong>{completedSectionCount}</strong>
            </div>
          </div>
          <div className="learning-course-list" id="learning-course-list">
            {learningPath.courses.map((course, index) => (
              <Link
                key={course.slug}
                href={`/learning/${course.slug}`}
                className={
                  course.slug === recommendedCourse?.slug
                    ? "learning-course-row learning-course-row-active"
                    : "learning-course-row"
                }
              >
                <span className="learning-course-index">{index + 1}</span>
                <span className="min-w-0">
                  <strong>{formatProductCopy(course.title)}</strong>
                  <em>
                    {course.nextSectionTitle
                      ? formatProductCopy(course.nextSectionTitle)
                      : formatCourseStatus(course.status)}
                  </em>
                </span>
                <small>
                  {course.completedSectionCount}/{course.sectionCount}
                </small>
              </Link>
            ))}
          </div>
        </aside>

        <main className="learning-task-panel paper-panel-strong">
          <div className="learning-task-head">
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <StatusPill tone="accent">
                  {recommendedCourse
                    ? formatCourseStatus(recommendedCourse.status)
                    : "复盘"}
                </StatusPill>
                <span>{recommendedCourse?.estimatedDurationMinutes ?? 10} 分钟</span>
              </div>
              <h2>
                {recommendedCourse
                  ? formatProductCopy(recommendedCourse.title)
                  : "今天不再堆新课，先做一次复盘"}
              </h2>
              <p>
                {recommendedCourse
                  ? formatProductCopy(recommendedCourse.focus)
                  : "基础训练完成后，把知识转回真实组合、新闻或一次教练追问。"}
              </p>
            </div>
            {recommendedCourse ? (
              <Link
                href={`/learning/${recommendedCourse.slug}`}
                className="action-button"
              >
                <BookOpenCheck aria-hidden="true" className="size-4" />
                开始训练
              </Link>
            ) : (
                <Link
                  href={buildAgentPromptHref({
                    focus: "learning",
                    from: "learning",
                    prompt:
                      "请用一个简单例子帮我理解这节内容，并告诉我怎么用到我的基金决策里。",
                  })}
                  className="action-button"
                >
                  带问题复盘
                </Link>
            )}
          </div>

          <div className="learning-step-track" aria-label="今日学习步骤">
            {["核心概念", "案例理解", "自我检查", "应用思考"].map((step, index) => (
              <div
                key={step}
                className={index < 2 ? "learning-step learning-step-done" : "learning-step"}
              >
                <span>{index + 1}</span>
                <strong>{step}</strong>
                <em>{index < 2 ? "已准备" : index === 2 ? "进行中" : "待完成"}</em>
              </div>
            ))}
          </div>

          <div className="learning-focus-grid">
            <section>
              <div className="learning-mini-heading">
                <Target aria-hidden="true" className="size-4" />
                <h3>关键概念笔记</h3>
              </div>
              <ul>
                <li>{getTrainingApplication(activeTrainingCourse)}</li>
                <li>先分清“波动”“回撤”和“风险承受”，再判断是否需要行动。</li>
                <li>学习结果要回到一次具体问题，而不是停在术语记忆。</li>
              </ul>
            </section>
            <section>
              <div className="learning-mini-heading">
                <ClipboardList aria-hidden="true" className="size-4" />
                <h3>微型自检</h3>
              </div>
              <div className="learning-check-list">
                <label>
                  <input type="radio" name="learning-check" readOnly />
                  我能解释今天的概念
                </label>
                <label>
                  <input type="radio" name="learning-check" readOnly checked />
                  我能把它用于一次基金判断
                </label>
                <label>
                  <input type="radio" name="learning-check" readOnly />
                  我还需要问教练
                </label>
              </div>
            </section>
          </div>

          <div className="learning-next-strip">
            <div>
              <p className="section-kicker">当前材料</p>
              <strong>{formatProductCopy(activeSectionTitle)}</strong>
            </div>
            <a href="#learning-course-list">
              查看全部路径
              <ArrowRight aria-hidden="true" className="size-4" />
            </a>
          </div>
          <div className="learning-task-footer">
            <section>
              <p className="section-kicker">应用顺序</p>
              <strong>先解释概念，再回到组合，最后问教练。</strong>
              <span>学习页只保留今天要推进的一步，避免被课程列表拖走。</span>
            </section>
            <section>
              <p className="section-kicker">完成标准</p>
              <strong>能说清“我下一次会先看什么”。</strong>
              <span>不是背完术语，而是形成一个可复用的判断顺序。</span>
            </section>
          </div>
        </main>

        <aside className="learning-action-panel paper-panel-strong">
          <div className="learning-panel-heading">
            <div>
              <p className="section-kicker">从学习到行动</p>
              <h2>学完立刻连回真实判断</h2>
            </div>
          </div>
          <div className="learning-action-list">
            {actionCards.map((card) => (
              <Link key={card.title} href={card.href} className="learning-action-row">
                <span>
                  {card.title === "相关组合检查" ? (
                    <Target aria-hidden="true" className="size-4" />
                  ) : card.title === "相关资讯影响" ? (
                    <ClipboardList aria-hidden="true" className="size-4" />
                  ) : (
                    <BookOpenCheck aria-hidden="true" className="size-4" />
                  )}
                </span>
                <span>
                  <strong>{card.title}</strong>
                  <em>{card.detail}</em>
                  <small>{card.label}</small>
                </span>
              </Link>
            ))}
          </div>
          <div className="learning-summary-panel">
            <p className="section-kicker">学习数据概览</p>
            <div>
              <span>路径进度</span>
              <strong>{learningPath.overallProgressPercentage}%</strong>
            </div>
            <div>
              <span>本次推荐</span>
              <strong>
                {learningPath.recommendedCourseTitle
                  ? formatProductCopy(learningPath.recommendedCourseTitle)
                  : "已完成"}
              </strong>
            </div>
            <div>
              <span>下一步</span>
              <strong>{formatProductCopy(activeSectionTitle)}</strong>
            </div>
          </div>
        </aside>
      </section>
    </div>
  );
}
