"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { MotionConfig, motion } from "motion/react";
import {
  Bot,
  BriefcaseBusiness,
  ClipboardCheck,
  Compass,
  GraduationCap,
  Home,
  LayoutDashboard,
  LogOut,
  Newspaper,
  PanelLeftClose,
  PanelLeftOpen,
  PlayCircle,
  Settings2,
  UserRoundCog,
  type LucideIcon,
} from "lucide-react";

import { ApiError, getSessionUser, logoutAuthSession } from "@/lib/api";
import {
  navigationItems,
  toolNavigationItems,
  workspaceNavigationItems,
} from "@/lib/navigation";
import { cn, InlineNotice, Panel, ProgressBar, StatusPill } from "./ui/primitives";

const navIcons: Record<string, LucideIcon> = {
  "/": Home,
  "/today": LayoutDashboard,
  "/agent": Bot,
  "/automations": Settings2,
  "/profile": UserRoundCog,
  "/dashboard": LayoutDashboard,
  "/onboarding": ClipboardCheck,
  "/coach": Bot,
  "/learning": GraduationCap,
  "/portfolio": BriefcaseBusiness,
  "/simulation": PlayCircle,
  "/news": Newspaper,
};
const SESSION_CHECK_SLOW_MS = 6_000;
const SESSION_CHECK_RETRY_LIMIT = 3;

function shouldRetrySessionCheck(failureCount: number, error: Error): boolean {
  if (error instanceof ApiError && error.status === 401) {
    return false;
  }

  return failureCount < SESSION_CHECK_RETRY_LIMIT;
}

function getWorkspaceItem(pathname: string) {
  return workspaceNavigationItems.find((item) => {
    return item.href === "/"
      ? pathname === "/"
      : pathname === item.href || pathname.startsWith(`${item.href}/`);
  });
}

function getWorkspaceTitle(pathname: string) {
  if (pathname === "/onboarding") {
    return "建档";
  }
  if (pathname === "/start") {
    return "开始";
  }
  if (pathname === "/news") {
    return "资讯";
  }
  if (pathname === "/simulation") {
    return "模拟训练";
  }
  if (pathname.startsWith("/learning")) {
    return "学习";
  }
  return getWorkspaceItem(pathname)?.title ?? "工作区";
}

function getWorkspaceDescription(pathname: string) {
  if (pathname === "/onboarding") {
    return "一次只回答一个问题，完成后进入今日简报。";
  }
  if (pathname === "/start") {
    return "创建账号并开始建档。";
  }
  if (pathname === "/news") {
    return "聚焦重要政策与市场动态，连接您的组合。";
  }
  if (pathname === "/simulation") {
    return "用历史情境练一次不冲动的判断。";
  }
  if (pathname.startsWith("/learning")) {
    return "把基金知识转成今天能执行的判断动作。";
  }
  return getWorkspaceItem(pathname)?.description ?? "继续完成本次投资判断训练。";
}

export function AppShell({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const queryClient = useQueryClient();
  const pathname = usePathname();
  const isPublicOverview = pathname === "/";
  const [sidebarCollapsed, setSidebarCollapsed] = useState(() => {
    if (typeof window === "undefined") {
      return false;
    }
    return window.localStorage.getItem("fundgene-sidebar-collapsed") === "true";
  });
  const [sessionCheckSlow, setSessionCheckSlow] = useState(false);
  const sessionQuery = useQuery({
    queryKey: ["session-user"],
    queryFn: getSessionUser,
    networkMode: "always",
    retry: shouldRetrySessionCheck,
    retryDelay: (attemptIndex) => Math.min(4_000 * (attemptIndex + 1), 12_000),
  });
  const logoutMutation = useMutation({
    mutationFn: logoutAuthSession,
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["session-user"] }),
        queryClient.invalidateQueries({ queryKey: ["dashboard"] }),
        queryClient.invalidateQueries({ queryKey: ["behavior-profile"] }),
        queryClient.invalidateQueries({ queryKey: ["coach-session"] }),
        queryClient.invalidateQueries({ queryKey: ["simulation-scenarios"] }),
        queryClient.invalidateQueries({ queryKey: ["simulation-review"] }),
      ]);
      router.push("/start");
    },
  });

  useEffect(() => {
    window.localStorage.setItem(
      "fundgene-sidebar-collapsed",
      sidebarCollapsed ? "true" : "false",
    );
  }, [sidebarCollapsed]);

  useEffect(() => {
    if (!sessionQuery.isLoading) {
      return;
    }

    const timer = window.setTimeout(() => setSessionCheckSlow(true), SESSION_CHECK_SLOW_MS);
    return () => window.clearTimeout(timer);
  }, [sessionQuery.isLoading]);

  const loginHref = `/start?next=${encodeURIComponent(pathname)}`;

  if (!isPublicOverview && sessionQuery.isLoading) {
    return (
      <div className="figma-app-shell grid min-h-screen place-items-center px-4">
        <Panel className="w-full max-w-[520px] rounded-[30px] px-8 py-8">
          <StatusPill tone="accent">会话</StatusPill>
          <h1 className="mt-4 text-3xl font-semibold">正在验证登录会话</h1>
          <p className="mt-4 text-sm leading-7 text-[color:var(--ink-soft)]">
            {sessionCheckSlow
              ? "服务正在启动，通常需要 20-60 秒。准备好后会自动继续。"
              : "正在读取当前账号状态。"}
          </p>
        </Panel>
      </div>
    );
  }

  if (
    !isPublicOverview &&
    sessionQuery.error instanceof ApiError &&
    sessionQuery.error.status === 401
  ) {
    return (
      <div className="figma-app-shell grid min-h-screen place-items-center px-4">
        <Panel className="w-full max-w-[560px] rounded-[30px] px-8 py-8">
          <StatusPill tone="warning">需要登录</StatusPill>
          <h1 className="mt-4 text-3xl font-semibold">工作区需要先登录</h1>
          <p className="mt-4 text-sm leading-7 text-[color:var(--ink-soft)]">
            登录后才能读取画像、问卷、学习进度和训练记录。
          </p>
          <div className="mt-6 flex flex-wrap gap-3">
            <Link
              href={loginHref}
              className="action-button"
              onClick={(event) => {
                event.preventDefault();
                window.location.assign(loginHref);
              }}
            >
              去登录 / 注册
            </Link>
            <Link
              href="/"
              className="action-button-secondary"
              onClick={(event) => {
                event.preventDefault();
                window.location.assign("/");
              }}
            >
              回到总览
            </Link>
          </div>
        </Panel>
      </div>
    );
  }

  if (!isPublicOverview && (sessionQuery.error || !sessionQuery.data)) {
    return (
      <div className="figma-app-shell grid min-h-screen place-items-center px-4">
        <InlineNotice className="w-full max-w-[640px]">
          服务还在启动中，请稍等后刷新；若已经看到登录页，也可以重新登录继续。
        </InlineNotice>
      </div>
    );
  }

  const sessionUser = sessionQuery.data ?? null;
  const workspaceTitle = getWorkspaceTitle(pathname);
  const workspaceItem = getWorkspaceItem(pathname);
  const workspaceDescription = getWorkspaceDescription(pathname);
  const compactPageChrome =
    pathname === "/agent" ||
    pathname === "/today" ||
    pathname === "/automations" ||
    pathname === "/profile" ||
    pathname === "/news" ||
    pathname === "/portfolio" ||
    pathname === "/simulation" ||
    pathname.startsWith("/learning");
  const hideMobileSidebar = pathname === "/onboarding";

  return (
    <MotionConfig reducedMotion="user">
      <div
        className={cn(
          "figma-app-shell app-shell-frame",
          sidebarCollapsed && "app-shell-frame-collapsed",
        )}
      >
          <motion.aside
            data-testid="workspace-sidebar"
            className={cn(
              "figma-module-rail app-shell-sidebar",
              sidebarCollapsed && "figma-module-rail-collapsed",
              hideMobileSidebar && "app-shell-sidebar-hidden-mobile",
            )}
            initial={false}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.32, delay: 0.05, ease: [0.22, 1, 0.36, 1] }}
          >
            <div className="app-sidebar-brand">
              <Link
                href="/"
                className="app-sidebar-home flex min-w-0 items-center gap-3 text-[24px] font-semibold leading-none text-[color:var(--ink-strong)]"
                aria-label="回到 FundGene 总览"
              >
                <span className="app-sidebar-logo" aria-hidden="true">
                  F
                </span>
                <span className={cn("grid min-w-0 gap-1", sidebarCollapsed && "lg:sr-only")}>
                  <span className="truncate">FundGene</span>
                  <span className="app-sidebar-subtitle">Fund Investing Coach</span>
                </span>
              </Link>
              <button
                type="button"
                className="icon-button"
                aria-label={sidebarCollapsed ? "展开侧边栏" : "收起侧边栏"}
                aria-pressed={sidebarCollapsed}
                title={sidebarCollapsed ? "展开侧边栏" : "收起侧边栏"}
                onClick={() => setSidebarCollapsed((value) => !value)}
              >
                {sidebarCollapsed ? (
                  <PanelLeftOpen aria-hidden="true" className="size-4" />
                ) : (
                  <PanelLeftClose aria-hidden="true" className="size-4" />
                )}
              </button>
            </div>

            {sessionUser && !sidebarCollapsed ? (
              <section
                className="app-sidebar-profile-card"
                aria-label="当前用户画像摘要"
              >
                <div className="flex items-start gap-3">
                  <span className="app-sidebar-medal" aria-hidden="true" />
                  <div className="min-w-0">
                    <p className="text-sm font-black text-[color:var(--ink-strong)]">
                      学徒基因诊断
                    </p>
                    <p className="mt-1 truncate text-xs font-semibold text-[color:var(--ink-muted)]">
                      {sessionUser.primaryGoal ?? "新手成长型资产基因"}
                    </p>
                  </div>
                </div>
                <ProgressBar value={75} label="认知免疫度" className="mt-4" />
              </section>
            ) : null}

            <nav
              aria-label="FundGene 工作区导航"
              className="flex flex-row gap-3 overflow-x-auto lg:flex-col lg:gap-1.5 lg:overflow-visible"
            >
              {navigationItems.map((item) => {
                const active =
                  pathname === item.href ||
                  (item.href !== "/" && pathname.startsWith(item.href));
                const Icon = navIcons[item.href] ?? Compass;

                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    title={item.title}
                    aria-current={active ? "page" : undefined}
                    className={cn(
                      "figma-nav-item",
                      active && "figma-nav-item-active",
                    )}
                  >
                    <span className="figma-nav-icon" aria-hidden="true">
                      <Icon className="size-4" strokeWidth={2.25} />
                    </span>
                    <span
                      className={cn(
                        "figma-nav-label",
                        sidebarCollapsed && "lg:sr-only",
                      )}
                    >
                      {item.title}
                    </span>
                  </Link>
                );
              })}
              <div
                className={cn(
                  "app-sidebar-section-label",
                  sidebarCollapsed && "lg:sr-only",
                )}
                aria-hidden="true"
              >
                工具入口
              </div>
              {toolNavigationItems.map((item) => {
                const active =
                  pathname === item.href ||
                  (item.href !== "/" && pathname.startsWith(item.href));
                const Icon = navIcons[item.href] ?? Compass;

                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    title={item.title}
                    aria-current={active ? "page" : undefined}
                    className={cn(
                      "figma-nav-item",
                      "app-tool-nav-item",
                      active && "figma-nav-item-active",
                    )}
                  >
                    <span className="figma-nav-icon" aria-hidden="true">
                      <Icon className="size-4" strokeWidth={2.25} />
                    </span>
                    <span
                      className={cn(
                        "figma-nav-label",
                        sidebarCollapsed && "lg:sr-only",
                      )}
                    >
                      {item.title}
                    </span>
                  </Link>
                );
              })}
              {sessionUser ? (
                <button
                  type="button"
                  className="figma-nav-item app-logout-item lg:mt-3"
                  onClick={() => logoutMutation.mutate()}
                  disabled={logoutMutation.isPending}
                  title="退出登录"
                >
                  <span className="figma-nav-icon" aria-hidden="true">
                    <LogOut className="size-4" />
                  </span>
                  <span
                    className={cn(
                      "figma-nav-label",
                      sidebarCollapsed && "lg:sr-only",
                    )}
                  >
                    {logoutMutation.isPending ? "退出中" : "退出"}
                  </span>
                </button>
              ) : null}
            </nav>
          </motion.aside>

          <div className="app-main-column">
            <motion.header
              className="app-titlebar"
              initial={false}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.35, ease: [0.22, 1, 0.36, 1] }}
            >
              <div className="flex min-w-0 items-center gap-3">
                <div className="min-w-0">
                  <p className="truncate text-sm font-semibold text-[color:var(--ink-strong)]">
                    {workspaceTitle}
                  </p>
                  <p className="hidden truncate text-xs font-medium text-[color:var(--ink-muted)] md:block">
                    {workspaceDescription}
                  </p>
                </div>
              </div>

              <div className="flex min-w-0 items-center justify-end gap-3">
                {!sessionUser ? (
                  <Link href="/start" className="action-button compact-action-button">
                    登录 / 注册
                  </Link>
                ) : null}
              </div>
            </motion.header>

          <motion.main
            data-testid="workspace-main"
            className="min-w-0"
            initial={false}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.34, delay: 0.08, ease: [0.22, 1, 0.36, 1] }}
          >
            {!compactPageChrome ? (
              <div className="mb-5">
                <p className="section-kicker">{workspaceItem?.eyebrow ?? "工作区"}</p>
                <h1 className="mt-1.5 text-[clamp(1.9rem,2.7vw,2.8rem)] font-semibold leading-tight text-[color:var(--ink-strong)]">
                  {workspaceTitle}
                </h1>
                <p className="mt-2 max-w-[700px] text-sm font-medium leading-6 text-[color:var(--ink-soft)] sm:text-[15px]">
                  {workspaceDescription}
                </p>
              </div>
            ) : null}
            <div className="figma-content-stage">{children}</div>
          </motion.main>
        </div>
        </div>
    </MotionConfig>
  );
}
