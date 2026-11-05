import { Suspense } from "react";

import { AuthEntry } from "@/components/auth-entry";

export default function StartPage() {
  return (
    <div className="figma-app-shell relative min-h-screen overflow-hidden px-4 py-6 sm:px-8 lg:px-[74px]">
      <div aria-hidden="true" className="figma-glow figma-glow-top" />
      <div aria-hidden="true" className="figma-glow figma-glow-low" />
      <div className="relative z-10 mx-auto w-full max-w-[1728px]">
        <Suspense
          fallback={
            <div className="paper-panel-strong px-5 py-4 text-sm text-[color:var(--ink-soft)]">
              正在加载认证入口...
            </div>
          }
        >
          <AuthEntry />
        </Suspense>
      </div>
    </div>
  );
}
