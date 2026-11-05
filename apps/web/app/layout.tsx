import type { Metadata, Viewport } from "next";

import "@fontsource-variable/noto-sans-sc";
import "@fontsource-variable/noto-serif-sc";
import "./globals.css";
import { Providers } from "./providers";

export const metadata: Metadata = {
  title: {
    default: "FundGene | 新手基金投资陪练",
    template: "%s | FundGene",
  },
  description:
    "FundGene 帮助基金投资新手建立可解释、可追踪、可复盘的学习与决策训练流程。",
};

export const viewport: Viewport = {
  colorScheme: "light",
  themeColor: "#f5f5f7",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="zh-CN" data-scroll-behavior="smooth">
      <body className="min-h-dvh font-sans text-[color:var(--ink-strong)] antialiased">
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
