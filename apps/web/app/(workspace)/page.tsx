import Link from "next/link";
import {
  ArrowRight,
  Bot,
  CalendarCheck,
  CheckCircle2,
  FileText,
  GraduationCap,
  Newspaper,
  PlayCircle,
  ShieldCheck,
  SlidersHorizontal,
} from "lucide-react";

import { cn } from "@/components/ui/primitives";

const primaryTasks = [
  {
    title: "看今日判断",
    eyebrow: "Today",
    description: "先知道今天最该检查什么，以及哪些信息不能当成买卖信号。",
    href: "/today",
    accent: "blue",
    icon: CalendarCheck,
  },
  {
    title: "交给 Agent",
    eyebrow: "Ask",
    description: "把问题交给教练整理，得到解释、边界和可执行下一步。",
    href: "/agent",
    accent: "teal",
    icon: Bot,
  },
  {
    title: "确认资料",
    eyebrow: "Context",
    description: "管理风险画像、组合状态和需要你确认的长期记录。",
    href: "/profile",
    accent: "ink",
    icon: FileText,
  },
];

const toolEntries = [
  {
    title: "学习训练",
    description: "把概念学习压缩成今天可完成的一步。",
    href: "/learning",
    icon: GraduationCap,
  },
  {
    title: "资讯解读",
    description: "把新闻拆成事实、影响路径和不确定性。",
    href: "/news",
    icon: Newspaper,
  },
  {
    title: "模拟训练",
    description: "用历史情境练一次不冲动的判断。",
    href: "/simulation",
    icon: PlayCircle,
  },
  {
    title: "自动任务",
    description: "配置每日观察、组合巡检和行为观察。",
    href: "/automations",
    icon: SlidersHorizontal,
  },
];

const timeline = [
  ["授权资料", "只读取你允许进入判断的画像、组合和学习记录。"],
  ["生成简报", "把分散信息整理成一个今日判断和一个安全下一步。"],
  ["进入任务", "需要追问时跳到 Agent，保留来源和上下文。"],
  ["确认写回", "涉及长期画像和行为结论时，先进入待确认状态。"],
];

const guardrails = [
  "不连接券商",
  "不承诺收益",
  "写回先确认",
];

export default function OverviewPage() {
  return (
    <main className="workspace-home-page">
      <div className="workspace-home-board">
        <section className="workspace-home-hero">
          <div className="workspace-home-copy">
            <p className="section-kicker">FundGene Workspace</p>
            <h1>工作区</h1>
            <p>
              一个入口完成今日观察、Agent 分析、资料确认和训练推进。
            </p>
            <div className="workspace-home-actions">
              <Link href="/today" className="action-button">
                查看今日简报
                <ArrowRight aria-hidden="true" className="size-4" />
              </Link>
              <Link href="/agent" className="action-button-secondary">
                交给 Agent
              </Link>
            </div>
          </div>

          <aside className="workspace-home-brief" aria-label="今日工作区摘要">
            <div className="workspace-home-brief-header">
              <div>
                <p className="section-kicker">Today Brief</p>
                <h2>先看一个判断</h2>
              </div>
              <span>75%</span>
            </div>
            <div className="workspace-home-brief-main">
              <p>今天先检查组合集中度，不急着响应单条新闻。</p>
              <small>组合、资讯、学习记录进入同一条任务链。</small>
            </div>
            <div className="workspace-home-signal-grid">
              <span>组合已检查</span>
              <span>画像已建立</span>
              <span>行为待观察</span>
            </div>
          </aside>
        </section>

        <section className="workspace-home-primary" aria-label="核心任务入口">
          {primaryTasks.map((task) => {
            const Icon = task.icon;
            return (
              <Link
                key={task.title}
                href={task.href}
                className={cn(
                  "workspace-home-task",
                  task.accent === "blue" && "workspace-home-task-primary",
                )}
              >
                <span className="workspace-home-task-icon">
                  <Icon aria-hidden="true" className="size-5" />
                </span>
                <span className="section-kicker">{task.eyebrow}</span>
                <strong>{task.title}</strong>
                <span>{task.description}</span>
                <ArrowRight aria-hidden="true" className="workspace-home-arrow size-4" />
              </Link>
            );
          })}
        </section>

        <section className="workspace-home-lower" aria-label="工作区辅助信息">
          <div className="workspace-home-flow">
            <div className="workspace-home-section-copy">
              <p className="section-kicker">任务链条</p>
              <h2>观察到确认</h2>
            </div>
            <div className="workspace-home-timeline">
              {timeline.map(([title, description], index) => (
                <div key={title} className="workspace-home-step">
                  <span>{String(index + 1).padStart(2, "0")}</span>
                  <div>
                    <strong>{title}</strong>
                    <p>{description}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="workspace-home-tools" aria-label="工具入口">
            <div className="workspace-home-section-copy">
              <p className="section-kicker">工具入口</p>
              <h2>模块为任务服务</h2>
            </div>
            <div className="workspace-home-tool-grid">
              {toolEntries.map((tool) => {
                const Icon = tool.icon;
                return (
                  <Link key={tool.title} href={tool.href} className="workspace-home-tool">
                    <Icon aria-hidden="true" className="size-4 text-[color:var(--accent-teal)]" />
                    <strong>{tool.title}</strong>
                  </Link>
                );
              })}
            </div>
          </div>

          <div className="workspace-home-guardrails">
            <div>
              <p className="section-kicker">安全边界</p>
              <h2>自动整理，不自动决策</h2>
            </div>
            <div className="workspace-home-guardrail-list">
              {guardrails.map((item) => (
                <div key={item}>
                  <CheckCircle2 aria-hidden="true" className="size-4" />
                  <span>{item}</span>
                </div>
              ))}
            </div>
            <ShieldCheck aria-hidden="true" className="workspace-home-shield size-10" />
          </div>
        </section>
      </div>
    </main>
  );
}
