import type {
  ButtonHTMLAttributes,
  InputHTMLAttributes,
  ReactNode,
  SelectHTMLAttributes,
  TextareaHTMLAttributes,
} from "react";

export function cn(...parts: Array<string | false | null | undefined>) {
  return parts.filter(Boolean).join(" ");
}

const panelVariant = {
  default: "surface-panel",
  strong: "surface-panel-strong",
  muted: "surface-panel-muted",
  inverse: "surface-panel-inverse",
} as const;

type PanelVariant = keyof typeof panelVariant;

export function Panel({
  variant = "default",
  className,
  children,
}: {
  variant?: PanelVariant;
  className?: string;
  children: ReactNode;
}) {
  return <div className={cn(panelVariant[variant], className)}>{children}</div>;
}

export function DataPanel({
  variant = "default",
  className,
  children,
}: {
  variant?: PanelVariant;
  className?: string;
  children: ReactNode;
}) {
  return <div className={cn(panelVariant[variant], className)}>{children}</div>;
}

const buttonVariant = {
  primary: "action-button",
  secondary: "action-button-secondary",
} as const;

type ButtonVariant = keyof typeof buttonVariant;

export function Button({
  variant = "primary",
  className,
  children,
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: ButtonVariant;
}) {
  return (
    <button className={cn(buttonVariant[variant], className)} {...props}>
      {children}
    </button>
  );
}

export function IconButton({
  label,
  className,
  children,
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & {
  label: string;
  children: ReactNode;
}) {
  return (
    <button
      aria-label={label}
      title={label}
      className={cn("icon-button", className)}
      {...props}
    >
      {children}
    </button>
  );
}

export function Field({
  label,
  hint,
  className,
  ...props
}: InputHTMLAttributes<HTMLInputElement> & {
  label: string;
  hint?: string;
}) {
  return (
    <label className={cn("grid gap-2 text-sm font-semibold", className)}>
      <span>{label}</span>
      <input className="field-input" {...props} />
      {hint ? (
        <span className="text-xs font-normal leading-5 text-[color:var(--ink-muted)]">
          {hint}
        </span>
      ) : null}
    </label>
  );
}

export function Textarea({
  label,
  hint,
  className,
  ...props
}: TextareaHTMLAttributes<HTMLTextAreaElement> & {
  label: string;
  hint?: string;
}) {
  return (
    <label className={cn("grid gap-2 text-sm font-semibold", className)}>
      <span>{label}</span>
      <textarea className="field-input" {...props} />
      {hint ? (
        <span className="text-xs font-normal leading-5 text-[color:var(--ink-muted)]">
          {hint}
        </span>
      ) : null}
    </label>
  );
}

export function Select({
  label,
  hint,
  className,
  children,
  ...props
}: SelectHTMLAttributes<HTMLSelectElement> & {
  label: string;
  hint?: string;
  children: ReactNode;
}) {
  return (
    <label className={cn("grid gap-2 text-sm font-semibold", className)}>
      <span>{label}</span>
      <select className="field-input" {...props}>
        {children}
      </select>
      {hint ? (
        <span className="text-xs font-normal leading-5 text-[color:var(--ink-muted)]">
          {hint}
        </span>
      ) : null}
    </label>
  );
}

const badgeTone = {
  neutral: "status-pill-neutral",
  positive: "status-pill-positive",
  warning: "status-pill-warning",
  accent: "status-pill-accent",
  danger: "status-pill-danger",
} as const;

type BadgeTone = keyof typeof badgeTone;

export function StatusBadge({
  tone = "neutral",
  children,
  className,
}: {
  tone?: BadgeTone;
  children: ReactNode;
  className?: string;
}) {
  return (
    <span className={cn("status-pill", badgeTone[tone], className)}>
      {children}
    </span>
  );
}

export const StatusPill = StatusBadge;

export function ProgressBar({
  value,
  label,
  className,
}: {
  value: number;
  label?: string;
  className?: string;
}) {
  const normalized = Math.max(0, Math.min(100, Number.isFinite(value) ? value : 0));

  return (
    <div className={cn("space-y-2", className)}>
      {label ? (
        <div className="flex items-center justify-between gap-3 text-xs font-semibold text-[color:var(--ink-muted)]">
          <span>{label}</span>
          <span>{normalized}%</span>
        </div>
      ) : null}
      <div
        className="progress-track"
        role="progressbar"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={normalized}
      >
        <span className="progress-fill" style={{ width: `${normalized}%` }} />
      </div>
    </div>
  );
}

export function InlineNotice({
  tone = "neutral",
  children,
  className,
}: {
  tone?: "neutral" | "danger";
  children: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        tone === "danger" ? "notice-danger" : "notice-neutral",
        className,
      )}
    >
      {children}
    </div>
  );
}

export function LoadingState({
  children = "正在加载...",
  className,
}: {
  children?: ReactNode;
  className?: string;
}) {
  return <div className={cn("loading-state", className)}>{children}</div>;
}

export function EmptyState({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return <div className={cn("empty-state", className)}>{children}</div>;
}

export function ErrorState({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return <div className={cn("error-state", className)}>{children}</div>;
}
