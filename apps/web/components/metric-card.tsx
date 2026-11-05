import { cn, StatusPill } from "./ui/primitives";

const toneMap = {
  moss: "border-l-[color:var(--accent-teal)]",
  gold: "border-l-[color:var(--accent-gold)]",
  clay: "border-l-[color:var(--accent-clay)]",
} as const;

type Accent = keyof typeof toneMap;

export function MetricCard({
  label,
  value,
  detail,
  accent,
}: {
  label: string;
  value: string;
  detail: string;
  accent: Accent;
}) {
  return (
    <div
      className={cn(
        "metric-card group relative overflow-hidden rounded-[22px] border-l-2 border-y border-r border-[color:var(--line-soft)] bg-white/80 p-3.5 shadow-sm backdrop-blur-2xl transition duration-200 hover:-translate-y-0.5 hover:border-[color:var(--line-strong)] hover:shadow-[0_18px_34px_rgba(0,0,0,0.1)]",
        toneMap[accent],
      )}
    >
      <span className="absolute inset-x-0 top-0 h-px bg-[linear-gradient(90deg,transparent,var(--accent-teal),transparent)] opacity-45" />
      <div className="relative flex h-full min-h-[5.4rem] flex-col justify-between gap-2.5">
        <div className="flex items-start justify-between gap-3">
          <StatusPill tone={accent === "gold" ? "warning" : "positive"}>
            {label}
          </StatusPill>
          <span className="mt-1 size-2 rounded-full bg-[color:var(--accent-teal)] shadow-[0_0_18px_rgba(0,113,227,0.32)]" />
        </div>
        <div>
          <p className="text-xl font-semibold leading-tight tracking-normal text-[color:var(--ink-strong)] sm:text-2xl">
            {value}
          </p>
          <p className="text-clamp-2 mt-1.5 max-w-[22rem] text-xs leading-5 text-[color:var(--ink-soft)]">
            {detail}
          </p>
        </div>
      </div>
    </div>
  );
}
