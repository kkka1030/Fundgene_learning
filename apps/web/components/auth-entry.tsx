"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { startTransition, useEffect, useState } from "react";
import { ArrowRight, BarChart3, LockKeyhole, ShieldCheck, UserPlus } from "lucide-react";

import {
  ApiError,
  getSessionUser,
  loginAuthAccount,
  registerAuthAccount,
  type SessionUser,
} from "@/lib/api";
import { InlineNotice, StatusPill } from "./ui/primitives";

type AuthMode = "sign-in" | "sign-up";
const REMEMBERED_EMAIL_KEY = "fundgene:last-auth-email";
const SESSION_CHECK_SLOW_MS = 6_000;
const SESSION_CHECK_RETRY_LIMIT = 3;

function getRememberedEmail(): string {
  if (typeof window === "undefined") {
    return "";
  }
  return window.localStorage.getItem(REMEMBERED_EMAIL_KEY) ?? "";
}

function rememberEmail(value: string) {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.setItem(REMEMBERED_EMAIL_KEY, value);
}

function resolveNextPath(nextPath: string | null, session: SessionUser): string {
  if (!session.onboardingCompleted) {
    return "/onboarding";
  }

  if (!nextPath || !nextPath.startsWith("/") || nextPath.startsWith("//")) {
    return "/today";
  }

  return nextPath;
}

function shouldRetrySessionCheck(failureCount: number, error: Error): boolean {
  if (error instanceof ApiError && error.status === 401) {
    return false;
  }

  return failureCount < SESSION_CHECK_RETRY_LIMIT;
}

function getAuthErrorMessage(error: unknown, mode: AuthMode): string {
  if (mode === "sign-in" && error instanceof ApiError && [401, 404].includes(error.status)) {
    return "账号不存在或密码不正确。还没有账号的话，请先创建账号并开始建档。";
  }

  if (error instanceof ApiError && error.status === 408) {
    return "服务正在启动，通常需要 20-60 秒。请稍等一下再试一次。";
  }

  if (error instanceof ApiError && error.status >= 500) {
    return "认证服务还在恢复中，请稍等一下再试。";
  }

  return error instanceof Error ? error.message : "请求失败，请稍后重试。";
}

export function AuthEntry() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const queryClient = useQueryClient();
  const rememberedEmail = getRememberedEmail();
  const [mode, setMode] = useState<AuthMode>(rememberedEmail ? "sign-in" : "sign-up");
  const [email, setEmail] = useState(rememberedEmail);
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [sessionCheckSlow, setSessionCheckSlow] = useState(false);

  const sessionQuery = useQuery({
    queryKey: ["session-user"],
    queryFn: getSessionUser,
    retry: shouldRetrySessionCheck,
    retryDelay: (attemptIndex) => Math.min(4_000 * (attemptIndex + 1), 12_000),
  });

  useEffect(() => {
    if (!sessionQuery.isLoading) {
      return;
    }

    const timer = window.setTimeout(() => setSessionCheckSlow(true), SESSION_CHECK_SLOW_MS);
    return () => window.clearTimeout(timer);
  }, [sessionQuery.isLoading]);

  useEffect(() => {
    if (!sessionQuery.data) {
      return;
    }
    const nextPath = searchParams.get("next");
    startTransition(() => {
      router.replace(resolveNextPath(nextPath, sessionQuery.data));
    });
  }, [router, searchParams, sessionQuery.data]);

  const authMutation = useMutation({
    mutationFn: async () => {
      const trimmedEmail = email.trim();
      if (!trimmedEmail) {
        throw new Error("请输入邮箱地址。");
      }
      if (password.length < 8) {
        throw new Error("密码至少需要 8 位。");
      }

      if (mode === "sign-up") {
        if (password !== confirmPassword) {
          throw new Error("两次输入的密码不一致。");
        }
        await registerAuthAccount({ email: trimmedEmail, password });
      } else {
        await loginAuthAccount({ email: trimmedEmail, password });
      }
      rememberEmail(trimmedEmail);
      const session = await getSessionUser();
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["session-user"] }),
        queryClient.invalidateQueries({ queryKey: ["dashboard"] }),
        queryClient.invalidateQueries({ queryKey: ["behavior-profile"] }),
        queryClient.invalidateQueries({ queryKey: ["coach-session"] }),
      ]);
      return session;
    },
    onSuccess: (session) => {
      const nextPath = searchParams.get("next");
      startTransition(() => {
        router.push(resolveNextPath(nextPath, session));
      });
    },
    onError: (error) => {
      if (mode === "sign-up" && error instanceof ApiError && error.status === 409) {
        rememberEmail(email.trim());
        setMode("sign-in");
        setSubmitError("这个邮箱已经注册过了，已为你切到登录。输入原密码即可继续。");
        return;
      }
      setSubmitError(getAuthErrorMessage(error, mode));
    },
  });

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitError(null);
    authMutation.mutate();
  }

  return (
    <div className="min-h-[calc(100vh-3rem)]">
      <header className="grid gap-5 lg:grid-cols-[190px_minmax(260px,620px)_minmax(180px,1fr)] lg:items-center">
        <Link
          href="/"
          className="text-[34px] font-semibold leading-none text-[color:var(--ink-strong)]"
          aria-label="回到 FundGene 总览"
        >
          <span className="text-[color:var(--accent-teal)]">F</span>undGene
        </Link>
        <div className="hidden h-[58px] items-center gap-4 rounded-full border border-[color:var(--line-soft)] bg-white/72 px-6 text-sm font-medium text-[color:var(--ink-soft)] shadow-sm backdrop-blur-2xl md:flex">
          <ShieldCheck aria-hidden="true" className="size-5" />
          <span>登录后进入建档、工作台和训练闭环</span>
        </div>
        <div className="flex justify-start lg:justify-end">
          <div className="flex h-[50px] min-w-[204px] items-center justify-between gap-4 rounded-full border border-[color:var(--line-soft)] bg-white/76 px-4 text-[color:var(--ink-strong)] shadow-sm backdrop-blur-2xl">
            <span className="truncate text-sm font-bold">你好，学习者</span>
            <span className="grid size-[42px] flex-none place-items-center rounded-full bg-[color:var(--accent-teal)] text-xs font-semibold text-white shadow-[0_10px_20px_rgba(0,113,227,0.22)]">
              FG
            </span>
          </div>
        </div>
      </header>

      <main className="mt-8 grid gap-7 lg:mt-16 lg:grid-cols-[56px_minmax(0,1fr)] lg:gap-10">
        <aside className="figma-module-rail hidden lg:block">
          <nav aria-label="入口页模块预览" className="flex flex-col gap-3">
            {[BarChart3, LockKeyhole, ShieldCheck, UserPlus].map((Icon, index) => (
              <span
                key={index}
                className={index === 0 ? "figma-nav-item figma-nav-item-active" : "figma-nav-item"}
              >
                <Icon aria-hidden="true" className="size-4" />
              </span>
            ))}
          </nav>
        </aside>

        <div className="min-w-0">
          <div className="mb-5 lg:mb-7">
            <h1 className="text-[2.35rem] font-semibold leading-none text-[color:var(--ink-strong)] sm:text-[clamp(2.4rem,4vw,3.8rem)]">
              创建账号，开始建档
            </h1>
            <p className="mt-3 max-w-[700px] text-[15px] font-medium leading-7 text-[color:var(--ink-soft)] sm:mt-4 sm:text-[17px]">
              新用户先完成一轮基础画像，FundGene 才会给出个人化的今日判断。
            </p>
          </div>

          <div className="grid gap-8 xl:grid-cols-[minmax(0,760px)_minmax(310px,420px)] xl:items-stretch">
            <section className="agent-hero order-2 min-h-[0] overflow-hidden px-6 py-7 sm:min-h-[420px] sm:px-11 sm:py-11 xl:order-1 xl:min-h-[560px]">
              <p className="section-kicker">新手优先的基金教练</p>
              <h2 className="mt-4 max-w-[680px] text-[2.1rem] font-semibold leading-[1.08] text-[color:var(--ink-strong)] sm:mt-6 sm:text-[clamp(2.35rem,3.7vw,3.55rem)]">
                把基金投资
                <br />
                变成可训练的判断能力
              </h2>
              <p className="mt-4 max-w-[560px] text-base font-medium leading-7 text-[color:var(--ink-soft)] sm:mt-6 sm:text-lg sm:leading-8">
                FundGene 解释风险、识别行为偏差、复盘历史情境，但不做交易执行和收益承诺。
              </p>
              <div className="mt-6 flex flex-wrap gap-3 sm:mt-12 sm:gap-4">
                <button
                  type="button"
                  className="action-button min-w-[132px] sm:min-w-[150px]"
                  onClick={() => setMode("sign-up")}
                  disabled={authMutation.isPending}
                >
                  创建账号
                </button>
                <button
                  type="button"
                  className="action-button-secondary min-w-[150px] sm:min-w-[172px]"
                  onClick={() => setMode("sign-in")}
                  disabled={authMutation.isPending}
                >
                  已有账号登录
                </button>
              </div>
              <div className="mt-12 hidden h-[180px] items-end gap-6 pl-[38%] md:flex">
                {[
                  ["学习", 90, true],
                  ["提问", 170, false],
                  ["画像", 120, true],
                  ["情境", 208, true],
                  ["资讯", 146, true],
                  ["复盘", 232, true],
                ].map(([label, height, muted]) => (
                  <div key={String(label)} className="grid justify-items-center gap-4">
                    <span
                      className={
                        muted
                          ? "w-[52px] rounded-[18px] bg-[rgba(118,118,128,0.18)]"
                          : "w-[52px] rounded-[18px] bg-[linear-gradient(180deg,var(--accent-cyan),var(--accent-teal))] shadow-[0_16px_32px_rgba(0,113,227,0.2)]"
                      }
                      style={{ height: Number(height) }}
                    />
                    <span className="text-xs font-bold text-[color:var(--ink-soft)]">{label}</span>
                  </div>
                ))}
              </div>
            </section>

            <section className="surface-panel-strong order-1 min-h-[auto] px-6 py-8 sm:px-9 sm:py-10 xl:order-2 xl:min-h-[560px]">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <h2 className="text-3xl font-semibold tracking-normal text-[color:var(--ink-strong)]">
                  {mode === "sign-up" ? "开始建档" : "登录已有账号"}
                </h2>
                <StatusPill tone="accent">
                  {mode === "sign-up" ? "新用户" : "已有账号"}
                </StatusPill>
              </div>

              <div className="mt-7 flex rounded-full border border-[color:var(--line-soft)] bg-[rgba(118,118,128,0.12)] p-1">
                <button
                  type="button"
                  className={mode === "sign-up" ? "action-button flex-1 px-4" : "action-button-secondary flex-1 px-4"}
                  onClick={() => setMode("sign-up")}
                  disabled={authMutation.isPending}
                >
                  创建账号
                </button>
                <button
                  type="button"
                  className={mode === "sign-in" ? "action-button flex-1 px-4" : "action-button-secondary flex-1 px-4"}
                  onClick={() => setMode("sign-in")}
                  disabled={authMutation.isPending}
                >
                  已有账号
                </button>
              </div>

              <form className="mt-7 grid gap-5" onSubmit={handleSubmit}>
                <label className="space-y-2 text-sm">
                  <span className="font-semibold">邮箱</span>
                  <input
                    type="email"
                    name="email"
                    className="field-input"
                    value={email}
                    onChange={(event) => {
                      setEmail(event.target.value);
                      if (event.target.value.trim()) {
                        rememberEmail(event.target.value.trim());
                      }
                    }}
                    placeholder="you@example.com"
                    autoComplete="email"
                  />
                </label>

                <label className="space-y-2 text-sm">
                  <span className="font-semibold">密码</span>
                  <input
                    type="password"
                    name="password"
                    className="field-input"
                    value={password}
                    onChange={(event) => setPassword(event.target.value)}
                    placeholder="至少 8 位"
                    autoComplete={mode === "sign-in" ? "current-password" : "new-password"}
                  />
                </label>

                {mode === "sign-up" ? (
                  <label className="space-y-2 text-sm">
                    <span className="font-semibold">确认密码</span>
                    <input
                      type="password"
                      name="confirmPassword"
                      className="field-input"
                      value={confirmPassword}
                      onChange={(event) => setConfirmPassword(event.target.value)}
                      placeholder="再次输入密码"
                      autoComplete="new-password"
                    />
                  </label>
                ) : null}

                <div className="mt-2 flex flex-wrap gap-3">
                  <button
                    type="submit"
                    className="action-button"
                    disabled={authMutation.isPending}
                  >
                    {authMutation.isPending
                      ? mode === "sign-in"
                        ? "正在登录..."
                        : "正在创建账号..."
                      : mode === "sign-in"
                        ? "登录并继续"
                        : "注册并开始建档"}
                    <ArrowRight aria-hidden="true" className="ml-2 size-4" />
                  </button>
                  <Link href="/" className="action-button-secondary">
                    回到总览
                  </Link>
                </div>

                {submitError ? <InlineNotice tone="danger">{submitError}</InlineNotice> : null}

                {sessionQuery.isLoading ? (
                  <InlineNotice>
                    {sessionCheckSlow
                      ? "服务正在启动，通常需要 20-60 秒。准备好后会自动继续。"
                      : "正在检查登录状态..."}
                  </InlineNotice>
                ) : null}

                {authMutation.isPending ? (
                  <InlineNotice>
                    正在连接认证服务；如果这是第一次打开，可能需要 20-60 秒。
                  </InlineNotice>
                ) : null}

                {sessionQuery.error instanceof ApiError &&
                sessionQuery.error.status !== 401 ? (
                  <InlineNotice>
                    认证服务还在启动中，请稍等或重新点击登录/创建账号。
                  </InlineNotice>
                ) : null}
              </form>

              <div className="mt-7 grid gap-3 border-t border-[color:var(--line-soft)] pt-5 text-sm leading-6 text-[color:var(--ink-soft)]">
                <div className="flex items-start gap-3">
                  <UserPlus
                    aria-hidden="true"
                    className="mt-1 size-4 flex-none text-[color:var(--accent-teal)]"
                  />
                  <p>新账号会先进入建档流程，完成画像后进入工作台。</p>
                </div>
              </div>
            </section>
          </div>
        </div>
      </main>
    </div>
  );
}
