import type { CSSProperties } from "react";

import { cn } from "./ui/primitives";

const DNA_SEGMENTS = 9;

export function BrandMark({ className }: { className?: string }) {
  return (
    <span
      className={cn(
        "gene-mark relative inline-grid place-items-center overflow-hidden rounded-lg",
        className,
      )}
      aria-hidden="true"
    >
      <span className="gene-glow" />
      <span className="gene-scene">
        {Array.from({ length: DNA_SEGMENTS }).map((_, index) => (
          <span
            key={index}
            className="gene-rung-3d"
            style={{ "--i": index } as CSSProperties}
          >
            <span className="gene-rung-line" />
            <span className="gene-node gene-node-a" />
            <span className="gene-node gene-node-b" />
          </span>
        ))}
      </span>
    </span>
  );
}
