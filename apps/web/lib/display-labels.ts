const biasTagLabels: Record<string, string> = {
  performance_chasing_risk: "追涨倾向",
  panic_selling_risk: "回撤恐慌倾向",
  concentration_risk: "持仓集中倾向",
  no_major_bias_detected: "暂未发现明显行为偏差",
  "暂无显著行为偏差标签": "暂未发现明显行为偏差",
  overconfidence_risk: "过度自信倾向",
  loss_aversion_risk: "亏损回避倾向",
  diversification_gap: "分散配置不足",
};

const riskLevelLabels: Record<string, string> = {
  conservative: "稳健型",
  balanced: "均衡型",
  growth: "成长型",
};

const tokenLabels: Record<string, string> = {
  performance: "收益表现",
  chasing: "追热点",
  panic: "恐慌",
  selling: "卖出",
  concentration: "集中度",
  overconfidence: "过度自信",
  loss: "亏损",
  aversion: "回避",
  diversification: "分散配置",
  gap: "不足",
  risk: "风险",
  bias: "偏差",
  focus: "重点",
  detected: "观察",
  major: "明显",
  no: "暂无",
};

const referenceLabels: Record<string, string> = {
  "fund-basics": "基金基础",
  "risk-and-drawdown": "风险和回撤",
  "allocation-principles": "配置原则",
  learning_risk_basics: "风险基础",
};

const sourceLabels: Record<string, string> = {
  "FundGene dev fixture": "FundGene 资讯样本",
};

const categoryLabels: Record<string, string> = {
  ASSIGNMENT: "训练任务",
  POLICY: "政策信息",
  NEWS: "市场资讯",
  policy: "政策信息",
  news: "市场资讯",
  capital_market_policy: "资本市场政策",
};

const userVisibleRiskNotice =
  "这次回答用于帮你理解风险和整理下一步，不代表收益承诺，也不会替你买卖或下单。";

function fallbackCodeLabel(value: string): string {
  if (!value.includes("_")) {
    return value;
  }

  const translated = value
    .split("_")
    .map((part) => tokenLabels[part] ?? part)
    .join("");

  return translated === value.replaceAll("_", "") ? value.replaceAll("_", " ") : translated;
}

function stripInternalPromptCopy(value: string): string {
  return value
    .replaceAll(
      "FundGene 提供的是学习与决策支持，不是收益承诺、交易执行指令或自动下单系统。",
      userVisibleRiskNotice,
    )
    .replaceAll(
      "FundGene 提供的是学习与决策支持，不是收益承诺或交易执行指令。",
      userVisibleRiskNotice,
    )
    .replace(/\s*运行时风险约束：[^。]*(?:。|$)/g, " ")
    .replace(/\s*影响路径约束：[^。]*(?:。|$)/g, " ")
    .replace(/\s*检索证据提示：[^。]*(?:。|$)/g, " ")
    .replace(/\s*如果用户问“[^”]+”，先给出这些具体标题，再提醒它们需要逐条解读。?/g, " ")
    .replaceAll("直接回答：", "")
    .replace(/\s+/g, " ")
    .trim();
}

export function formatBiasTag(value: string | null | undefined): string {
  if (!value) {
    return "待继续观察";
  }

  return biasTagLabels[value] ?? fallbackCodeLabel(value);
}

export function formatBiasTags(values: string[]): string[] {
  return values.map((value) => formatBiasTag(value));
}

export function formatRiskLevel(value: string | null | undefined): string {
  if (!value) {
    return "待评估";
  }

  return riskLevelLabels[value] ?? fallbackCodeLabel(value);
}

export function formatProductCopy(value: string): string {
  return stripInternalPromptCopy(value)
    .replaceAll("暂无显著行为偏差标签", "暂未发现明显行为偏差")
    .replace(/\bbalanced\b/g, "均衡型")
    .replace(/\bconservative\b/g, "稳健型")
    .replace(/\bgrowth\b/g, "成长型")
    .replaceAll("no_major_bias_detected", "暂未发现明显行为偏差")
    .replaceAll("Daily Brief", "今日简报")
    .replaceAll("DeepSeek", "模型")
    .replaceAll("Assignment", "训练任务")
    .replaceAll("ASSIGNMENT", "训练任务")
    .replaceAll("L2 自动化", "自动任务")
    .replaceAll("L2", "自动")
    .replaceAll("Simulation", "情境训练")
    .replaceAll("Learning", "学习中心")
    .replaceAll("Portfolio", "组合体检")
    .replaceAll("Coach", "教练")
    .replaceAll("Dashboard", "工作台")
    .replaceAll("Beginner Core Path", "新手核心路径");
}

export function formatUserVisibleCopy(value: string): string {
  return formatProductCopy(value);
}

export function formatReferenceLabel(value: string): string {
  return referenceLabels[value] ?? fallbackCodeLabel(value);
}

export function formatSourceLabel(value: string | null | undefined): string {
  if (!value) {
    return "未标注来源";
  }

  return sourceLabels[value] ?? formatProductCopy(value);
}

export function formatNewsCategory(value: string | null | undefined): string {
  if (!value) {
    return "市场信息";
  }

  return categoryLabels[value] ?? formatProductCopy(value);
}
