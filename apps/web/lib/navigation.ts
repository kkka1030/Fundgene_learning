export const navigationItems = [
  {
    href: "/today",
    eyebrow: "今日",
    title: "今日",
    description: "先看今日判断和一个下一步",
  },
  {
    href: "/agent",
    eyebrow: "任务",
    title: "Agent",
    description: "描述问题，查看整理依据和下一步",
  },
  {
    href: "/automations",
    eyebrow: "授权",
    title: "自动任务",
    description: "控制定期检查什么",
  },
  {
    href: "/profile",
    eyebrow: "资料",
    title: "资料",
    description: "管理判断所依赖的个人上下文",
  },
];

export const toolNavigationItems = [
  {
    href: "/portfolio",
    eyebrow: "组合",
    title: "持仓分析",
    description: "查看持仓分布和集中度",
  },
  {
    href: "/learning",
    eyebrow: "学习",
    title: "学习训练",
    description: "按今日任务推进基金基础判断能力",
  },
  {
    href: "/news",
    eyebrow: "资讯",
    title: "资讯解读",
    description: "查看政策与市场动态如何影响组合",
  },
  {
    href: "/simulation",
    eyebrow: "训练",
    title: "模拟训练",
    description: "进入历史情境，练习不冲动决策",
  },
];

export const workspaceNavigationItems = [
  ...navigationItems,
  ...toolNavigationItems,
];

export function buildAgentPromptHref({
  focus,
  prompt,
  displayMessage,
  from,
  sourceIds = {},
}: {
  focus: string;
  prompt: string;
  displayMessage?: string | null;
  from?: string | null;
  sourceIds?: Record<string, string | null | undefined>;
}): string {
  const params = new URLSearchParams();
  params.set("focus", focus);
  params.set("prompt", prompt);
  if (displayMessage) {
    params.set("display_message", displayMessage);
  }

  if (from) {
    params.set("from", from);
  }

  Object.entries(sourceIds).forEach(([key, value]) => {
    if (value) {
      params.set(key, value);
    }
  });

  return `/agent?${params.toString()}`;
}
