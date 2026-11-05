import type { Page, Route } from "@playwright/test";

const sessionUser = {
  id: "user_e2e",
  email: "qa@fundgene.test",
  profile_id: "profile_e2e",
  onboarding_completed: true,
  display_name: "QA 用户",
  investing_experience: "beginner",
  monthly_contribution_band: "under_3000",
  primary_goal: "建立长期基金投资纪律",
};

const behaviorProfile = {
  user_id: "user_e2e",
  risk_level: "balanced",
  bias_tags: ["追热点倾向", "回撤焦虑"],
  evidence: ["问卷显示在热门基金讨论中容易冲动跟进。"],
  updated_at: "2026-04-26T08:00:00Z",
};

const dashboardState = {
  user_id: "user_e2e",
  onboarding_completed: true,
  risk_level: "balanced",
  bias_tags: ["追热点倾向"],
  latest_risk_score: 18,
  daily_brief: {
    brief_id: "daily-brief:e2e:ready",
    as_of: "2026-04-26T09:30:00Z",
    status: "ready",
    priority_level: "attention",
    headline: "今天先检查组合集中度，不急着响应单条新闻。",
    beginner_explanation:
      "你的画像、组合报告和最近新闻已经能形成一个共同判断：先看风险来源，再决定要学习或追问什么。",
    evidence: [
      {
        id: "evidence:portfolio:e2e",
        source_type: "portfolio",
        source_id: "snapshot_e2e",
        claim: "第一大持仓约 34%，需要继续观察集中度。",
        beginner_translation: "单一主题波动更容易影响你的账户感受和判断节奏。",
        support_level: "strong",
        freshness_label: "2026-04-26 快照",
        risk_boundary: "不能推出任何账户操作。",
      },
      {
        id: "evidence:news:e2e",
        source_type: "news_policy",
        source_id: "analysis_e2e",
        claim: "长期资金入市政策更像长期市场结构信号。",
        beginner_translation: "这类新闻适合先理解影响路径，不适合直接当作行动理由。",
        support_level: "medium",
        freshness_label: "最近新闻分析",
        risk_boundary: "新闻相关性不是交易信号。",
      },
      {
        id: "evidence:behavior:e2e",
        source_type: "behavior",
        source_id: "user_e2e",
        claim: "当前行为焦点包含追热点倾向。",
        beginner_translation: "看到热门信息时，教练会提醒你先检查证据和边界。",
        support_level: "medium",
        freshness_label: "最新行为画像",
        risk_boundary: "单个标签不是固定定性。",
      },
    ],
    primary_action: {
      id: "inspect-portfolio-concentration",
      type: "inspect_portfolio",
      label: "检查组合集中度",
      reason: "先看第一大持仓和主题重复，避免被单条新闻带走。",
      target_route: "/portfolio",
      target_params: { from: "dashboard", focus: "concentration" },
      expected_writeback: "portfolio_snapshots/portfolio_analyses",
      safety_note: "组合体检用于解释风险来源，不输出账户操作指令。",
    },
    secondary_actions: [
      {
        id: "ask-coach-brief",
        type: "ask_coach",
        label: "让教练解释这条判断",
        reason: "如果证据还不清楚，先追问为什么和你有关。",
        target_route: "/coach",
        target_params: {
          from: "dashboard",
          focus: "daily-brief",
          daily_brief_id: "daily-brief:e2e:ready",
        },
        expected_writeback: "chat_messages/agent_runs",
        safety_note: "追问用于理解证据和边界，不会触发账户操作。",
      },
    ],
    do_not_do: "先把今天的判断用于理解和检查，需要保存的变化会再请你确认。",
    source_coverage: {
      profile: true,
      portfolio: true,
      news_policy: true,
      learning: true,
      simulation: true,
      behavior: true,
      coach_history: true,
    },
    trace_id: "run_e2e",
  },
  next_actions: [
    "完成一节风险与回撤基础课。",
    "录入一份组合快照并查看集中度。",
  ],
  summary_cards: [
    {
      label: "风险等级",
      value: "balanced",
      detail: "来自最新问卷结果。",
    },
    {
      label: "行为焦点",
      value: "追热点倾向",
      detail: "教练会优先解释冲动跟进风险。",
    },
    {
      label: "组合体检",
      value: "已生成",
      detail: "最近组合报告已回流工作台。",
    },
  ],
  learning_status: {
    overall_progress_percentage: 34,
    completed_courses_count: 1,
    total_courses: 3,
    recommended_course_slug: "risk-basics",
    recommended_course_title: "风险和回撤基础",
  },
  portfolio_status: {
    has_report: true,
    latest_snapshot_id: "snapshot_e2e",
    latest_snapshot_date: "2026-04-26",
    total_value: 52000,
    summary: "组合覆盖权益、债券和现金，但第一大持仓需要继续观察。",
  },
  simulation_status: {
    completed_sessions_count: 1,
    recommended_scenario_slug: "drawdown-discipline",
    recommended_scenario_title: "回撤纪律训练",
    latest_session_id: "sim_session_e2e",
    latest_scenario_slug: "drawdown-discipline",
    latest_scenario_title: "回撤纪律训练",
    latest_review_summary: "最近复盘显示你能先检查计划，再决定是否调整。",
    latest_completed_at: "2026-04-26T09:20:00Z",
  },
  news_status: {
    has_analysis: true,
    latest_analysis_id: "analysis_e2e",
    latest_item_id: "news_e2e",
    latest_item_type: "news",
    latest_title: "长期资金入市政策继续推进",
    source_name: "FundGene fixture",
    latest_summary: "政策强调长期资金和资本市场稳定，但具体节奏仍需观察。",
    beginner_translation: "这条政策更像长期市场结构信号，不是立即买入指令。",
    recommended_action: "先回看自己的权益基金比例。",
    generated_at: "2026-04-26T08:20:00Z",
  },
  latest_coach_activity: {
    session_id: "session_e2e",
    topic: "风险和回撤",
    intent: "learning",
    question: "基金回撤是什么意思？",
    answer_focus: "先解释回撤，再连接风险承受能力。",
    recommended_action: "继续学习风险基础课程。",
    updated_at: "2026-04-26T08:30:00Z",
  },
};

const assistantSession = {
  session: {
    id: "session_e2e",
    topic: "风险和回撤",
    context_type: "advisor",
    latest_intent: "learning",
    last_question: "基金回撤是什么意思？",
    last_answer_preview: "回撤是从阶段高点到低点的下跌幅度。",
    last_recommended_action: "继续学习风险基础课程。",
    message_count: 2,
    created_at: "2026-04-26T08:00:00Z",
    updated_at: "2026-04-26T08:30:00Z",
  },
  messages: [
    {
      id: "msg_user",
      role: "user",
      content: "基金回撤是什么意思？",
      message_type: "text",
      created_at: "2026-04-26T08:29:00Z",
      agent_run_id: null,
      advisor_response: null,
    },
    {
      id: "msg_assistant",
      role: "assistant",
      content: "回撤是从阶段高点到低点的下跌幅度。",
      message_type: "advisor_response",
      created_at: "2026-04-26T08:30:00Z",
      agent_run_id: "run_e2e",
      advisor_response: {
        answer:
          "回撤可以理解为基金净值从阶段高点跌到低点的幅度。它不是亏损承诺，而是衡量波动压力的一种方式。",
        intent: "learning",
        citations: ["learning_risk_basics"],
        risk_notice: "基金投资存在波动风险，解释回撤不等于承诺收益或规避亏损。",
        recommended_actions: ["先完成风险基础课程。", "把组合最大持仓拿来检查波动来源。"],
        recommended_action_targets: [
          {
            label: "进入学习中心",
            href: "/learning",
            intent: "learning",
            kind: "internal_link",
          },
          {
            label: "查看组合体检",
            href: "/portfolio",
            intent: "portfolio",
            kind: "internal_link",
          },
        ],
        follow_up_questions: ["我该怎样判断自己能承受多大回撤？"],
      },
    },
  ],
};

const agentShowcaseQuestion = "今天先检查组合集中度，我具体应该先看哪几项？";
const agentPortfolioJumpQuestion = "帮我检查这份组合里最需要关注的风险来源。";
const agentShowcaseFollowUpQuestion =
  "那我下一步应该学什么，能帮我把检查顺序写成清单吗？";

const agentShowcaseSession = {
  session: {
    id: "session_agent_showcase_e2e",
    topic: "组合集中度检查",
    context_type: "advisor",
    latest_intent: "portfolio",
    last_question: agentShowcaseQuestion,
    last_answer_preview:
      "先看第一大持仓、权益基金比例和主题重复，再决定要不要继续追问。",
    last_recommended_action: "查看组合体检里的第一大持仓和权益占比。",
    message_count: 2,
    created_at: "2026-04-26T09:32:00Z",
    updated_at: "2026-04-26T09:32:03Z",
  },
  messages: [
    {
      id: "msg_agent_showcase_user",
      role: "user",
      content: agentShowcaseQuestion,
      message_type: "text",
      created_at: "2026-04-26T09:32:00Z",
      agent_run_id: null,
      advisor_response: null,
    },
    {
      id: "msg_agent_showcase_assistant",
      role: "assistant",
      content:
        "先看第一大持仓、权益基金比例和主题重复，确认风险来源后再决定要不要追问新闻影响。",
      message_type: "advisor_response",
      created_at: "2026-04-26T09:32:03Z",
      agent_run_id: "run_e2e",
      advisor_response: {
        answer:
          "可以先按三项检查：第一，看第一大持仓是不是已经接近三成以上，它决定了单一主题波动对账户的影响有多大；第二，看权益基金比例是否会让你在下跌时容易失守；第三，把不同基金的行业和主题放在一起看，确认它们是不是其实押在同一方向。结合你这份 2026 年 4 月 26 日快照，第一大持仓约 34%，所以 FundGene 会把“组合集中度检查”放在今天的第一步。这样做的目的不是马上调仓，而是先把风险来源看清楚，再决定新闻、课程或训练哪个更值得继续追问。",
        intent: "portfolio",
        citations: ["portfolio_snapshot_e2e", "behavior_profile"],
        risk_notice:
          "这是风险来源检查，不是买卖建议。任何需要长期记录的变化仍要你确认。",
        recommended_actions: [
          "先查看组合体检中的第一大持仓占比。",
          "把相似主题基金放在一起看，避免重复暴露。",
          "记录这次检查结论，后续再决定是否需要做模拟训练。",
        ],
        recommended_action_targets: [
          {
            label: "查看组合体检",
            href: "/portfolio?from=agent&focus=concentration",
            intent: "portfolio",
            kind: "internal_link",
          },
        ],
        follow_up_questions: [
          "帮我判断第一大持仓是不是过高。",
          "我该怎样识别基金之间的主题重复？",
        ],
      },
    },
  ],
};

const agentShowcaseFollowUpSession = {
  session: {
    ...agentShowcaseSession.session,
    last_question: agentShowcaseFollowUpQuestion,
    last_answer_preview:
      "下一步先学习回撤和集中度的关系，再按三步清单检查第一大持仓、主题重复和情绪触发点。",
    last_recommended_action: "完成风险和回撤基础，再回到组合体检确认集中度。",
    message_count: 4,
    updated_at: "2026-04-26T09:33:40Z",
  },
  messages: [
    ...agentShowcaseSession.messages,
    {
      id: "msg_agent_showcase_followup_user",
      role: "user",
      content: agentShowcaseFollowUpQuestion,
      message_type: "text",
      created_at: "2026-04-26T09:33:36Z",
      agent_run_id: null,
      advisor_response: null,
    },
    {
      id: "msg_agent_showcase_followup_assistant",
      role: "assistant",
      content:
        "可以，把下一步拆成学习、检查、确认三件事，避免直接把集中度问题变成操作冲动。",
      message_type: "advisor_response",
      created_at: "2026-04-26T09:33:40Z",
      agent_run_id: "run_agent_followup_e2e",
      advisor_response: {
        answer:
          "可以。下一步不要急着判断要不要调整账户，先把检查顺序固定下来：第一，学一节“风险和回撤基础”，理解为什么第一大持仓过高会放大账户波动感；第二，回到组合体检，核对第一大持仓约 34% 是否来自单一主题，顺便看权益基金、债券基金和现金的比例；第三，写下一句确认语：如果只是因为新闻或短期涨跌想行动，先停下来补充证据。这样你学完以后再回来问 Agent，系统就能结合学习记录、组合快照和最近资讯，判断下一步更适合做训练、补充资料，还是只保留观察。",
        intent: "learning",
        citations: ["learning_risk_basics", "portfolio_snapshot_e2e", "behavior_profile"],
        risk_notice:
          "这是一份学习和检查清单，不是买卖建议。任何长期画像或计划变化都需要你确认后才会保存。",
        recommended_actions: [
          "先完成风险和回撤基础。",
          "回到组合体检检查第一大持仓和主题重复。",
          "把这次想追问的触发点写下来，作为下一次训练材料。",
        ],
        recommended_action_targets: [
          {
            label: "进入学习中心",
            href: "/learning?from=agent&focus=risk-basics",
            intent: "learning",
            kind: "internal_link",
          },
          {
            label: "查看组合体检",
            href: "/portfolio?from=agent&focus=concentration",
            intent: "portfolio",
            kind: "internal_link",
          },
        ],
        follow_up_questions: [
          "学完以后帮我判断第一大持仓是否过高。",
          "把这套检查顺序变成一次模拟训练。",
        ],
      },
    },
  ],
};

const agentPortfolioJumpSession = {
  session: {
    ...agentShowcaseSession.session,
    id: "session_agent_portfolio_jump_e2e",
    last_question: agentPortfolioJumpQuestion,
    last_answer_preview:
      "这是组合检查任务。我会先进入持仓分析，看第一大持仓、资产分布和集中度。",
    updated_at: "2026-04-26T09:34:03Z",
  },
  messages: [
    {
      ...agentShowcaseSession.messages[0],
      id: "msg_agent_portfolio_jump_user",
      content: agentPortfolioJumpQuestion,
      created_at: "2026-04-26T09:34:00Z",
    },
    {
      ...agentShowcaseSession.messages[1],
      id: "msg_agent_portfolio_jump_assistant",
      content:
        "这是组合检查任务。我会先进入持仓分析，看第一大持仓、资产分布和集中度。",
      created_at: "2026-04-26T09:34:03Z",
      advisor_response: {
        ...agentShowcaseSession.messages[1].advisor_response,
        answer:
          "这是组合检查任务。先不用从页面里自己翻入口，我会把问题转成持仓分析：第一，看第一大持仓是否已经放大单一主题波动；第二，看权益、债券、海外和现金的分布；第三，看不同基金之间有没有重复暴露。结合最近快照，第一大持仓约 34%，所以这次最该先进入组合体检看集中度。",
        recommended_actions: [
          "查看持仓分析里的第一大持仓和资产分布。",
          "把集中度风险作为下一句追问材料。",
        ],
        recommended_action_targets: [
          {
            label: "查看持仓分析",
            href: "/portfolio?from=agent&focus=concentration",
            intent: "portfolio",
            kind: "internal_link",
          },
        ],
      },
    },
  ],
};

const newsQuestion =
  "请结合我的组合，解释《央行：下一阶段将坚持支持性的货币政策立场》可能影响什么、哪些地方不能当成买卖信号？";

const newsAssistantSession = {
  session: {
    id: "session_news_showcase_e2e",
    topic: "资讯影响与组合风险",
    context_type: "advisor",
    latest_intent: "news",
    last_question: newsQuestion,
    last_answer_preview:
      "《央行：下一阶段将坚持支持性的货币政策立场》更像长期资金和市场结构信号，先看它如何影响你的权益暴露和集中度。",
    last_recommended_action: "先检查第一大持仓和权益基金比例。",
    message_count: 2,
    created_at: "2026-04-26T09:35:00Z",
    updated_at: "2026-04-26T09:35:03Z",
  },
  messages: [
    {
      id: "msg_news_user",
      role: "user",
      content: newsQuestion,
      message_type: "text",
      created_at: "2026-04-26T09:35:00Z",
      agent_run_id: null,
      advisor_response: null,
    },
    {
      id: "msg_news_assistant",
      role: "assistant",
      content:
        "这条央行货币政策新闻对你更重要的不是立刻买卖，而是检查权益暴露、主题集中度和情绪反应。",
      message_type: "advisor_response",
      created_at: "2026-04-26T09:35:03Z",
      agent_run_id: "run_e2e",
      advisor_response: {
        answer:
          "结合你的画像、组合报告和最近同步的《央行：下一阶段将坚持支持性的货币政策立场》，这条信息更像长期资金和市场结构信号，而不是一条可以直接操作账户的指令。它可能先影响权益基金的市场情绪和估值预期，也可能让债券基金在组合里的缓冲作用变得更值得观察。对你这类还在建立投资纪律的新手来说，更重要的是把新闻拆成三层：事实是什么，影响路径可能经过哪些资产，和你的组合是否已经在同一主题上暴露过多。安全的第一步仍然是检查第一大持仓、权益基金比例和主题重复，而不是因为“支持性政策”四个字立刻买入或卖出。",
        intent: "news",
        citations: ["news_analysis_e2e", "portfolio_snapshot_e2e", "behavior_profile"],
        risk_notice:
          "新闻相关性不是交易指令。FundGene 只帮你解释影响路径和不确定性，不替你做账户操作。",
        recommended_actions: [
          "先检查组合里权益基金和第一大持仓占比。",
          "把这条央行政策信息当作追问材料，而不是立即行动理由。",
        ],
        recommended_action_targets: [
          {
            label: "查看组合体检",
            href: "/portfolio?from=agent&focus=concentration",
            intent: "portfolio",
            kind: "internal_link",
          },
          {
            label: "打开资讯解读",
            href: "/news?from=agent",
            intent: "news",
            kind: "internal_link",
          },
        ],
        follow_up_questions: [
          "帮我判断我的第一大持仓是不是太集中。",
          "这条央行政策信息有哪些地方不能当作买卖依据？",
        ],
      },
    },
  ],
};

const agentFirstQuestion =
  "今天我应该先处理什么？如果有新闻影响，也帮我结合组合解释。";
const agentFirstWhyQuestion = "为什么这和我的组合有关？";
const agentFirstNextQuestion = "那我下一步应该做训练还是改组合？";

const agentFirstPlanSession = {
  session: {
    id: "session_agent_first_e2e",
    topic: "今日 Agent 任务规划",
    context_type: "advisor",
    latest_intent: "daily_brief",
    last_question: agentFirstQuestion,
    last_answer_preview:
      "今天先检查组合集中度，再把央行政策新闻当成解释材料，不把它当成买卖信号。",
    last_recommended_action: "先核对第一大持仓和权益暴露。",
    message_count: 2,
    created_at: "2026-04-26T10:10:00Z",
    updated_at: "2026-04-26T10:10:04Z",
  },
  messages: [
    {
      id: "msg_agent_first_user_1",
      role: "user",
      content: agentFirstQuestion,
      message_type: "text",
      created_at: "2026-04-26T10:10:00Z",
      agent_run_id: null,
      advisor_response: null,
    },
    {
      id: "msg_agent_first_assistant_1",
      role: "assistant",
      content:
        "今天先处理组合集中度，再把最新政策新闻放进你的组合里解释。",
      message_type: "advisor_response",
      created_at: "2026-04-26T10:10:04Z",
      agent_run_id: "run_agent_first_plan_e2e",
      advisor_response: {
        answer:
          "我建议今天先处理一件事：检查组合集中度，不急着响应单条新闻。我会按这个顺序帮你整理：先读取 Today 的今日判断，再看组合里第一大持仓和权益暴露，然后把《央行：下一阶段将坚持支持性的货币政策立场》这类新闻作为影响路径来解释。这里的边界很明确：新闻只能帮助你理解市场情绪和资产影响，不能直接变成买入、卖出或调仓指令。今天的安全下一步，是先确认你的第一大持仓、主题重复和现金缓冲，而不是立刻做账户操作。",
        intent: "daily_brief",
        citations: ["daily_brief_e2e", "portfolio_snapshot_e2e", "news_analysis_e2e"],
        risk_notice:
          "FundGene 只做解释、训练和待确认建议，不替你执行账户操作。",
        recommended_actions: [
          "先检查第一大持仓和权益暴露。",
          "把央行政策新闻作为解释材料，不作为交易理由。",
          "如果仍然不确定，再做一次回撤纪律训练。",
        ],
        recommended_action_targets: [
          {
            label: "查看今日判断",
            href: "/today?from=agent",
            intent: "daily_brief",
            kind: "internal_link",
          },
          {
            label: "查看组合体检",
            href: "/portfolio?from=agent&focus=concentration",
            intent: "portfolio",
            kind: "internal_link",
          },
        ],
        follow_up_questions: [
          "为什么这和我的组合有关？",
          "那我下一步应该做训练还是改组合？",
        ],
      },
    },
  ],
};

const agentFirstWhySession = {
  session: {
    ...agentFirstPlanSession.session,
    latest_intent: "portfolio",
    last_question: agentFirstWhyQuestion,
    last_answer_preview:
      "因为你的第一大持仓约 34%，新闻带来的情绪波动会先放大集中度感受。",
    last_recommended_action: "先看第一大持仓、主题重复和债券基金缓冲。",
    message_count: 4,
    updated_at: "2026-04-26T10:11:04Z",
  },
  messages: [
    ...agentFirstPlanSession.messages,
    {
      id: "msg_agent_first_user_2",
      role: "user",
      content: agentFirstWhyQuestion,
      message_type: "text",
      created_at: "2026-04-26T10:11:00Z",
      agent_run_id: null,
      advisor_response: null,
    },
    {
      id: "msg_agent_first_assistant_2",
      role: "assistant",
      content:
        "因为你的组合里第一大持仓约 34%，新闻情绪会先放大集中度和波动感。",
      message_type: "advisor_response",
      created_at: "2026-04-26T10:11:04Z",
      agent_run_id: "run_agent_first_why_e2e",
      advisor_response: {
        answer:
          "这和你的组合有关，主要有三层原因。第一，你这份 2026 年 4 月 26 日组合快照里，第一大持仓约 34%，如果它集中在单一主题，新闻带来的市场情绪会更容易放大账户波动感。第二，你的组合同时有权益基金、债券基金和现金，央行支持性货币政策可能影响权益估值预期，也可能改变债券基金作为缓冲的观察价值。第三，你的行为画像里有追热点倾向，所以看到政策或资金流向新闻时，FundGene 会先提醒你拆事实、影响路径和不确定性，而不是把标题当成操作理由。",
        intent: "portfolio",
        citations: ["portfolio_snapshot_e2e", "news_analysis_e2e", "behavior_profile"],
        risk_notice:
          "这是个人化解释，不是账户操作建议。是否保存长期观察，需要你确认。",
        recommended_actions: [
          "先看第一大持仓是否过高。",
          "检查不同基金是否暴露在同一主题。",
          "把新闻影响写成观察线索，不直接改组合。",
        ],
        recommended_action_targets: [
          {
            label: "查看资讯解读",
            href: "/news?from=agent",
            intent: "news",
            kind: "internal_link",
          },
        ],
        follow_up_questions: [
          "那我下一步应该做训练还是改组合？",
          "这条新闻哪些地方不能当成买卖依据？",
        ],
      },
    },
  ],
};

const agentFirstNextSession = {
  session: {
    ...agentFirstPlanSession.session,
    latest_intent: "simulation",
    last_question: agentFirstNextQuestion,
    last_answer_preview:
      "先做训练，不直接改组合；训练结果只生成待确认线索。",
    last_recommended_action: "做一次回撤纪律训练。",
    message_count: 6,
    updated_at: "2026-04-26T10:12:04Z",
  },
  messages: [
    ...agentFirstWhySession.messages,
    {
      id: "msg_agent_first_user_3",
      role: "user",
      content: agentFirstNextQuestion,
      message_type: "text",
      created_at: "2026-04-26T10:12:00Z",
      agent_run_id: null,
      advisor_response: null,
    },
    {
      id: "msg_agent_first_assistant_3",
      role: "assistant",
      content:
        "下一步先做回撤纪律训练，不直接改组合；训练结果只进入待确认。",
      message_type: "advisor_response",
      created_at: "2026-04-26T10:12:04Z",
      agent_run_id: "run_agent_first_next_e2e",
      advisor_response: {
        answer:
          "下一步更适合先做训练，而不是马上改组合。原因是：现在我们已经知道需要检查集中度，也知道政策新闻不能直接当成买卖信号，但还缺一个判断纪律验证。你可以先做一次“市场急跌”的回撤训练，写下自己为什么选择持有、减仓或继续观察。训练完成后，FundGene 只会生成待确认的行为线索，例如“下跌时是否先核对计划”，不会静默改写你的长期画像，更不会执行账户动作。这样闭环就完整了：Agent 先解释，训练帮助你验证判断，资料中心只保存你确认过的变化。",
        intent: "simulation",
        citations: ["simulation_drawdown", "behavior_profile", "profile_confirmation"],
        risk_notice:
          "训练建议用于提升判断纪律，不代表应该买入、卖出或调整仓位。",
        recommended_actions: [
          "先做一次市场急跌情境训练。",
          "训练后只确认你认可的行为线索。",
          "下次简报再把确认过的状态纳入判断。",
        ],
        recommended_action_targets: [
          {
            label: "进入模拟训练",
            href: "/simulation?from=agent&scenario=drawdown-discipline",
            intent: "simulation",
            kind: "internal_link",
          },
          {
            label: "查看待确认资料",
            href: "/profile?from=agent",
            intent: "profile",
            kind: "internal_link",
          },
        ],
        follow_up_questions: [
          "帮我开始一次回撤训练。",
          "训练后哪些内容会写入资料？",
        ],
      },
    },
  ],
};

const assistantHistorySession = {
  session: {
    id: "session_history_e2e",
    topic: "热门基金要不要追",
    context_type: "advisor",
    latest_intent: "behavior",
    last_question: "那我看到热门基金连续上涨时应该怎么处理？",
    last_answer_preview: "先把它拆成事实、自己的组合暴露和情绪冲动三部分。",
    last_recommended_action: "写下追热点前的三步检查。",
    message_count: 4,
    created_at: "2026-04-25T08:00:00Z",
    updated_at: "2026-04-25T08:40:00Z",
  },
  messages: [
    {
      id: "history_msg_user_1",
      role: "user",
      content: "我总想追最近涨得多的基金，怎么办？",
      message_type: "text",
      created_at: "2026-04-25T08:29:00Z",
      agent_run_id: null,
      advisor_response: null,
    },
    {
      id: "history_msg_assistant_1",
      role: "assistant",
      content: "先不要把涨幅当成行动理由，要先检查它涨的来源和你已经持有什么。",
      message_type: "advisor_response",
      created_at: "2026-04-25T08:30:00Z",
      agent_run_id: "run_history_e2e",
      advisor_response: {
        answer:
          "先不要把涨幅当成行动理由。你可以先问三件事：它为什么涨、你的组合里是否已经有类似主题、如果回撤 10% 你是否还能按计划持有。",
        intent: "behavior",
        citations: ["behavior_profile"],
        risk_notice: "热门基金讨论不能直接变成买入建议。",
        recommended_actions: ["把追热点前的三步检查写下来。"],
        recommended_action_targets: [
          {
            label: "做一次情境训练",
            href: "/simulation",
            intent: "simulation",
            kind: "internal_link",
          },
        ],
        follow_up_questions: ["怎么判断我是不是已经持有类似主题？"],
      },
    },
    {
      id: "history_msg_user_2",
      role: "user",
      content: "那我看到热门基金连续上涨时应该怎么处理？",
      message_type: "text",
      created_at: "2026-04-25T08:39:00Z",
      agent_run_id: null,
      advisor_response: null,
    },
    {
      id: "history_msg_assistant_2",
      role: "assistant",
      content: "先把它拆成事实、自己的组合暴露和情绪冲动三部分。",
      message_type: "advisor_response",
      created_at: "2026-04-25T08:40:00Z",
      agent_run_id: "run_history_followup_e2e",
      advisor_response: {
        answer:
          "先把它拆成三部分：事实是它为什么上涨；组合暴露是你是否已经有同类基金；情绪冲动是你是不是因为怕错过才想立刻行动。",
        intent: "behavior",
        citations: ["behavior_profile"],
        risk_notice: "这里是行为检查，不是账户操作指令。",
        recommended_actions: ["写下追热点前的三步检查。"],
        recommended_action_targets: [],
        follow_up_questions: ["帮我把三步检查写成一句话。"],
      },
    },
  ],
};

const continuedAssistantHistorySession = {
  ...assistantHistorySession,
  session: {
    ...assistantHistorySession.session,
    message_count: 6,
    updated_at: "2026-04-25T08:50:00Z",
    last_question: "继续这段历史对话。",
  },
  messages: [
    ...assistantHistorySession.messages,
    {
      id: "history_msg_user_3",
      role: "user",
      content: "继续这段历史对话。",
      message_type: "text",
      created_at: "2026-04-25T08:49:00Z",
      agent_run_id: null,
      advisor_response: null,
    },
    {
      id: "history_msg_assistant_3",
      role: "assistant",
      content: "可以，我们接着用同一套三步检查来看这次冲动。",
      message_type: "advisor_response",
      created_at: "2026-04-25T08:50:00Z",
      agent_run_id: "run_history_continue_e2e",
      advisor_response: {
        answer: "可以，我们接着用同一套三步检查来看这次冲动。",
        intent: "behavior",
        citations: ["behavior_profile"],
        risk_notice: "继续对话仍然只做解释和训练，不替你下账户决定。",
        recommended_actions: ["先写下这次想追涨的触发点。"],
        recommended_action_targets: [],
        follow_up_questions: ["帮我检查这次触发点。"],
      },
    },
  ],
};

const agentRunTrace = {
  run: {
    id: "run_e2e",
    intent: "learning",
    runStatus: "completed",
    policyStatus: "passed",
    orchestratorVersion: "advisor-runtime-v2-e2e",
    startedAt: "2026-04-26T08:29:58Z",
    completedAt: "2026-04-26T08:30:00Z",
    latencyMs: 1280,
  },
  contextSnapshot: {
    riskLevel: "balanced",
    biasTags: ["追热点倾向"],
    latestPortfolioSummary: "组合覆盖权益、债券和现金。",
  },
  steps: [
    {
      id: "step_intake",
      stepName: "意图识别",
      sequence: 1,
      status: "completed",
      latencyMs: 120,
      inputPayload: { message: "基金回撤是什么意思？" },
      outputPayload: { intent: "learning" },
      error: null,
    },
    {
      id: "step_answer",
      stepName: "结构化回答",
      sequence: 2,
      status: "completed",
      latencyMs: 970,
      inputPayload: { intent: "learning" },
      outputPayload: { answerShape: "advisor_response" },
      error: null,
    },
  ],
  toolCalls: [
    {
      id: "tool_learning",
      stepId: "step_answer",
      toolName: "learning_knowledge_lookup",
      permissionLevel: "read",
      status: "completed",
      latencyMs: 85,
      inputPayload: { topic: "drawdown" },
      outputPayload: { citations: ["learning_risk_basics"] },
      error: null,
    },
  ],
  evidenceRefs: [
    {
      id: "evidence_learning",
      stepId: "step_answer",
      workerName: "learning",
      sourceType: "course",
      sourceId: "learning_risk_basics",
      sourceVersion: "v1",
      quoteOrSummary: "风险基础课程解释了回撤和波动压力。",
      claim: "回撤用于衡量阶段性波动压力。",
      supportSummary: "回答引用学习资料解释术语，不给出收益承诺。",
    },
  ],
  stateUpdateProposals: [
    {
      target: "learning_path",
      action: "recommend_course",
      courseSlug: "risk-basics",
    },
  ],
};

const agentRunEvents = {
  schema_version: "agent_run_events_v1",
  run_id: "run_e2e",
  events: [
    {
      id: "run_e2e:0001",
      run_id: "run_e2e",
      sequence: 1,
      event_type: "turn_started",
      phase: "turn",
      title: "开始处理任务",
      status: "completed",
      at: "2026-04-26T08:29:58Z",
      duration_ms: null,
      payload: {},
    },
    {
      id: "run_e2e:0002",
      run_id: "run_e2e",
      sequence: 2,
      event_type: "step_completed",
      phase: "context",
      title: "读取授权上下文",
      status: "completed",
      at: "2026-04-26T08:29:59Z",
      duration_ms: 180,
      payload: {},
    },
    {
      id: "run_e2e:0003",
      run_id: "run_e2e",
      sequence: 3,
      event_type: "agent_message",
      phase: "message",
      title: "生成给用户的回答",
      status: "completed",
      at: "2026-04-26T08:30:00Z",
      duration_ms: null,
      payload: {},
    },
    {
      id: "run_e2e:0004",
      run_id: "run_e2e",
      sequence: 4,
      event_type: "turn_complete",
      phase: "turn",
      title: "任务处理完成",
      status: "completed",
      at: "2026-04-26T08:30:00Z",
      duration_ms: 1280,
      payload: {},
    },
  ],
};

const agentRunStatus = {
  schema_version: "agent_run_status_v1",
  run_id: "run_e2e",
  session_id: "session_e2e",
  status: "completed",
  run_status: "completed",
  active: false,
  cancel_requested: false,
  started_at: "2026-04-26T08:29:58Z",
  completed_at: "2026-04-26T08:30:00Z",
  latency_ms: 1280,
  message: "本次整理已完成。",
};

const activeAgentRuns = {
  schema_version: "active_agent_runs_v1",
  runs: [],
};

const portfolioReport = {
  snapshot_id: "snapshot_e2e",
  snapshot_date: "2026-04-26",
  total_value: 52000,
  cash_value: 6000,
  summary:
    "这份快照总资产约为 52000 元，权益暴露适中，第一大持仓约 34%。",
  risk_exposure: ["组合里既有成长暴露，也保留了一定缓冲。"],
  concentration_flags: ["第一大持仓超过 25%，建议检查单一主题风险。"],
  allocation_balance: ["组合覆盖多个类型，但仍要检查主题是否重复。"],
  recommended_next_actions: ["先检查第一大持仓是否过重。"],
  holdings: [
    {
      fund_code: "161725",
      fund_name: "招商中证白酒指数",
      fund_type: "equity",
      market_value: 18000,
      weight: 34.6,
    },
    {
      fund_code: "110027",
      fund_name: "易方达安心债券",
      fund_type: "bond",
      market_value: 16000,
      weight: 30.8,
    },
    {
      fund_code: "000071",
      fund_name: "华夏恒生 ETF 联接",
      fund_type: "international",
      market_value: 12000,
      weight: 23.1,
    },
  ],
  generated_at: "2026-04-26T09:00:00Z",
};

const learningPath = {
  path_slug: "beginner-core-path",
  title: "新手基金主路径",
  description: "先建立风险、基金和配置的基础语言。",
  overall_progress_percentage: 34,
  completed_courses_count: 1,
  total_courses: 3,
  recommended_course_slug: "risk-basics",
  recommended_course_title: "风险和回撤基础",
  courses: [
    {
      slug: "risk-basics",
      title: "风险和回撤基础",
      focus: "理解波动、回撤和风险承受能力。",
      description: "帮助新手把短期下跌和长期计划分开。",
      estimated_duration_minutes: 18,
      section_count: 3,
      completed_section_count: 1,
      progress_percentage: 33,
      status: "in_progress",
      next_section_slug: "drawdown",
      next_section_title: "如何理解回撤",
    },
  ],
};

const simulationCatalog = {
  scenarios: [
    {
      id: "scenario_e2e",
      slug: "drawdown-discipline",
      title: "市场急跌",
      synopsis: "突发利空引发市场快速下跌，考验风险应对与仓位管理。",
      market_phase: "震荡回撤",
      difficulty: "beginner",
      estimated_duration_minutes: 12,
      decision_count: 4,
      starting_year: 2018,
      headline: "当市场连续下跌时，先稳住判断顺序。",
      description: "这个场景训练回撤中的行动纪律。",
      setup: "市场连续调整，热门主题基金明显回撤。",
      objective: "记录你是否能先检查计划而不是马上卖出。",
      bias_focus: ["恐慌卖出"],
      tags: ["回撤", "纪律"],
      timeline_preview: ["事件出现", "市场发酵", "恐慌扩散", "逐步企稳"],
      recommended: true,
      completed: false,
    },
    {
      id: "scenario_theme_e2e",
      slug: "theme-chasing",
      title: "热门主题上涨",
      synopsis: "资金集中涌入热门赛道，考验追涨冲动与节奏把握。",
      market_phase: "主题升温",
      difficulty: "beginner",
      estimated_duration_minutes: 10,
      decision_count: 3,
      starting_year: 2020,
      headline: "当朋友都在晒收益时，先看风险是否被低估。",
      description: "这个场景训练热门主题里的追涨克制。",
      setup: "单一主题基金连续上涨，社交讨论明显升温。",
      objective: "记录你是否会先检查集中度，而不是直接加仓。",
      bias_focus: ["追涨冲动"],
      tags: ["主题", "追涨"],
      timeline_preview: ["主题升温", "资金拥挤", "波动放大"],
      recommended: false,
      completed: false,
    },
    {
      id: "scenario_policy_e2e",
      slug: "policy-shock",
      title: "政策冲击",
      synopsis: "重要政策突然发布，考验信息消化与应对顺序。",
      market_phase: "政策扰动",
      difficulty: "beginner",
      estimated_duration_minutes: 10,
      decision_count: 3,
      starting_year: 2021,
      headline: "当政策新闻密集出现时，先分清事实和情绪。",
      description: "这个场景训练政策冲击下的冷静判断。",
      setup: "监管与产业政策密集变化，相关基金短期波动加大。",
      objective: "记录你是否能把政策事实、持仓暴露和情绪反应拆开。",
      bias_focus: ["标题驱动"],
      tags: ["政策", "波动"],
      timeline_preview: ["政策发布", "市场反应", "预期修正"],
      recommended: false,
      completed: false,
    },
  ],
  recommended_scenario_id: "scenario_e2e",
  active_session_id: "sim_session_e2e",
};

const activeSimulationSession = {
  id: "sim_session_e2e",
  scenario_id: "scenario_e2e",
  scenario_title: "市场急跌",
  status: "in_progress",
  stage_label: "市场发酵",
  current_step: 2,
  total_steps: 4,
  started_at: "2026-04-26T08:35:00Z",
  completed_at: null,
  opening_brief: "海外市场大幅下跌，部分行业基金低于预期，市场情绪快速转弱。",
  reflection_prompt: "请结合当前信息，说明你的判断依据和考虑。",
  active_event: {
    id: "event_e2e_2",
    index: 2,
    title: "市场发酵",
    date_label: "2024-03-13 10:35",
    market_context:
      "海外市场大幅下跌。部分行业基金低于预期。市场销售快速转弱。",
    prompt: "基于当前阶段的信息，做出你的决策并说明理由。",
    decision_focus: ["指数走势", "板块涨跌", "资金流向", "新闻资讯"],
    available_actions: [
      {
        id: "increase",
        label: "加仓",
        description: "看好后市，增加持仓",
        bias_signal: "performance_chasing_risk",
      },
      {
        id: "hold",
        label: "持有",
        description: "保持现有仓位不变",
        bias_signal: null,
      },
      {
        id: "reduce",
        label: "减仓",
        description: "降低仓位，控制风险",
        bias_signal: "panic_selling_risk",
      },
      {
        id: "clear",
        label: "清仓",
        description: "全部卖出，观望为主",
        bias_signal: "panic_selling_risk",
      },
    ],
  },
  actions: [
    {
      event_id: "event_e2e_1",
      step_index: 1,
      choice_key: "hold",
      choice_label: "持有",
      reflection: "基本面没有明显恶化，短期回调可能是情绪释放，打算先观察。",
      is_recommended: true,
      created_at: "2026-04-26T08:42:00Z",
    },
  ],
};

const completedSimulationSession = {
  ...activeSimulationSession,
  status: "completed",
  current_step: 4,
  completed_at: "2026-04-26T09:05:00Z",
  active_event: null,
  actions: [
    ...activeSimulationSession.actions,
    {
      event_id: "event_e2e_2",
      step_index: 2,
      choice_key: "hold",
      choice_label: "持有",
      reflection: "我先保留仓位，等风险信息更清楚再决定。",
      is_recommended: true,
      created_at: "2026-04-26T08:48:00Z",
    },
  ],
};

const simulationReview = {
  session_id: "sim_session_e2e",
  scenario_slug: "scenario_e2e",
  scenario_title: "市场急跌",
  bias_focus: "回撤焦虑",
  decision_summary: "你在波动阶段选择先观察，没有把短期下跌直接等同于必须卖出。",
  bias_observations: ["出现回撤压力时，你开始检查信息是否充分。"],
  strengths: ["先写下理由，再做动作。", "没有把单日波动当成完整结论。"],
  improvement_areas: ["下一次可以更明确写出仓位边界。"],
  coach_feedback: "这次训练更像一次纪律确认，而不是追涨杀跌。",
  recommended_next_actions: ["回到组合页检查第一大持仓比例。"],
  reflection_questions: ["当你看到快速下跌时，真正担心的是什么？"],
  behavior_evidence_candidates: [
    {
      behavior_evidence_id: "behavior_e2e_review",
      bias_type: "回撤焦虑",
      observed_signal: "下跌时主动检查信息充分性。",
      source_event: "event_e2e_2",
      confidence: "medium",
      pending_state_proposal_id: "proposal_e2e_review",
    },
  ],
  pending_state_proposal: {
    id: "proposal_e2e_review",
    description: "建议记录一次回撤压力下的观察行为。",
  },
  generated_at: "2026-04-26T09:06:00Z",
  actions: completedSimulationSession.actions,
};

const newsCatalog = {
  items: [
    {
      id: "news_e2e",
      item_type: "policy",
      title: "央行：下一阶段将坚持支持性的货币政策立场",
      source: "新华社",
      category: "policy",
      summary:
        "中国人民银行召开货币政策委员会例会，会议指出当前外部环境复杂严峻，国内有效需求仍显不足。后续政策将继续保持支持性立场，加大逆周期调节力度。保持流动性合理充裕，推动社会融资成本稳中有降。",
      url: "https://example.com/news",
      published_at: "2026-04-26T07:00:00Z",
      impact_areas: ["宏观政策", "债券基金", "权益基金"],
      tags: ["政策", "货币政策"],
      relevance_score: 92,
    },
    {
      id: "news_market_e2e",
      item_type: "news",
      title: "北向资金净流入超百亿元 科技板块获加仓",
      source: "中国证券报",
      category: "market",
      summary:
        "市场资金风险偏好回升，科技成长方向获得更多关注。但资金流向变化更适合作为情绪观察，不应直接替代组合纪律。",
      url: "https://example.com/market",
      published_at: "2026-04-26T06:00:00Z",
      impact_areas: ["市场情绪", "权益基金", "科技主题"],
      tags: ["市场", "资金流向"],
      relevance_score: 76,
    },
    {
      id: "news_fund_e2e",
      item_type: "news",
      title: "公募基金一季报披露收官 关注结构性机会",
      source: "上海证券报",
      category: "fund",
      summary:
        "一季报显示基金经理对高股息、科技成长和海外配置的分歧仍然存在。投资者需要先看自己已有暴露，再判断是否需要学习配置原则。",
      url: "https://example.com/fund",
      published_at: "2026-04-25T12:15:00Z",
      impact_areas: ["基金配置", "权益基金", "海外基金"],
      tags: ["基金", "配置"],
      relevance_score: 68,
    },
    {
      id: "news_realestate_e2e",
      item_type: "policy",
      title: "多地优化房地产政策 支持合理住房需求",
      source: "第一财经",
      category: "policy",
      summary:
        "房地产政策继续边际调整，可能影响地产链、银行和宽基指数情绪。对基金组合的实际影响取决于持仓行业和指数权重。",
      url: "https://example.com/realestate",
      published_at: "2026-04-25T10:30:00Z",
      impact_areas: ["宏观政策", "地产链", "宽基基金"],
      tags: ["政策", "地产"],
      relevance_score: 48,
    },
    {
      id: "news_macro_e2e",
      item_type: "news",
      title: "PMI连续两月回升 显示经济景气度改善",
      source: "经济日报",
      category: "market",
      summary:
        "制造业景气指标改善，可能提振顺周期资产预期。但单月数据仍需和盈利、估值、政策节奏一起观察。",
      url: "https://example.com/pmi",
      published_at: "2026-04-25T09:45:00Z",
      impact_areas: ["宏观数据", "顺周期基金"],
      tags: ["市场", "宏观"],
      relevance_score: 55,
    },
    {
      id: "news_overseas_e2e",
      item_type: "news",
      title: "海外市场波动加大 QDII基金短期净值承压",
      source: "财新网",
      category: "market",
      summary:
        "海外利率预期反复，部分 QDII 基金短期净值波动扩大。已有海外仓位的用户应优先核对风险预算。",
      url: "https://example.com/qdii",
      published_at: "2026-04-24T13:20:00Z",
      impact_areas: ["海外基金", "QDII", "风险预算"],
      tags: ["市场", "基金"],
      relevance_score: 64,
    },
    {
      id: "news_consumer_e2e",
      item_type: "news",
      title: "消费板块估值修复 机构提示盈利验证仍关键",
      source: "证券时报",
      category: "market",
      summary:
        "消费主题基金关注度回升，但估值修复并不等于趋势确认。需要继续看盈利恢复和组合集中度。",
      url: "https://example.com/consumer",
      published_at: "2026-04-24T08:30:00Z",
      impact_areas: ["消费主题", "权益基金"],
      tags: ["市场", "基金"],
      relevance_score: 58,
    },
  ],
  policy_items: [],
  generated_at: "2026-04-26T08:00:00Z",
};

function buildAutomationState() {
  return {
    user_id: "user_e2e",
    updated_at: "2026-04-26T09:30:00Z",
    active_count: 1,
    total_count: 4,
    automations: [
      {
        key: "daily_brief",
        title: "每日简报",
        summary: "每天自动整理画像、组合、资讯和训练状态，生成 Today 的第一条判断。",
        enabled: true,
        default_enabled: true,
        cadence_key: "daily_0830",
        cadence_label: "每天 08:30",
        cadence_options: [
          { key: "daily_0830", label: "每天 08:30" },
          { key: "daily_1200", label: "每天 12:00" },
          { key: "workday_0830", label: "工作日 08:30" },
        ],
        read_scope: ["风险画像", "最近组合报告", "资讯/政策分析", "学习与训练状态"],
        output_scope: ["今日判断", "最多三条证据", "安全下一步", "确认边界"],
        confirmation_boundary: "画像、计划或长期偏好写回前必须确认。",
        safety_boundary: "不会生成买卖、清仓、满仓或收益确定语言。",
        status: "ready",
        disabled_reason: null,
        last_run: null,
        next_run_at: "2026-04-27T00:30:00Z",
        can_run_now: true,
      },
      {
        key: "weekly_portfolio",
        title: "每周组合巡检",
        summary: "每周检查集中度、现金比例和资产暴露，把问题变成可追问的任务。",
        enabled: false,
        default_enabled: false,
        cadence_key: "weekly_monday_0900",
        cadence_label: "每周一 09:00",
        cadence_options: [
          { key: "weekly_monday_0900", label: "每周一 09:00" },
          { key: "weekly_friday_1800", label: "每周五 18:00" },
        ],
        read_scope: ["最近组合快照", "持仓权重", "现金比例", "风险问卷"],
        output_scope: ["集中度提醒", "组合健康摘要", "建议追问任务"],
        confirmation_boundary: "只准备检查建议，不自动改组合和计划。",
        safety_boundary: "只解释风险来源，不输出账户操作指令。",
        status: "disabled",
        disabled_reason: null,
        last_run: null,
        next_run_at: null,
        can_run_now: false,
      },
      {
        key: "news_watch",
        title: "资讯影响观察",
        summary: "只观察和你的画像、持仓、风险敏感点有关的资讯，不堆新闻列表。",
        enabled: false,
        default_enabled: false,
        cadence_key: "workday_1600",
        cadence_label: "工作日 16:00",
        cadence_options: [
          { key: "workday_1600", label: "工作日 16:00" },
          { key: "daily_1800", label: "每天 18:00" },
        ],
        read_scope: ["新闻/政策项", "持仓类型", "风险敏感点", "最近简报证据"],
        output_scope: ["影响路径", "不确定性", "是否值得追问"],
        confirmation_boundary: "加入长期观察或偏好前必须确认。",
        safety_boundary: "新闻相关性不是交易信号。",
        status: "disabled",
        disabled_reason: null,
        last_run: null,
        next_run_at: null,
        can_run_now: false,
      },
      {
        key: "behavior_observation",
        title: "行为偏差观察",
        summary: "从对话和训练理由里发现追热点、回撤焦虑等信号，但只生成待确认证据。",
        enabled: false,
        default_enabled: false,
        cadence_key: "weekly_friday_1800",
        cadence_label: "每周五 18:00",
        cadence_options: [
          { key: "weekly_friday_1800", label: "每周五 18:00" },
          { key: "weekly_monday_0900", label: "每周一 09:00" },
        ],
        read_scope: ["用户提问", "模拟理由", "已确认行为证据", "训练复盘"],
        output_scope: ["待确认行为证据", "训练建议", "冲动约束提醒"],
        confirmation_boundary: "弱证据只进入待确认，不直接改长期画像。",
        safety_boundary: "一次行为不会被写成永久标签。",
        status: "disabled",
        disabled_reason: null,
        last_run: null,
        next_run_at: null,
        can_run_now: false,
      },
    ],
    next_queue: [
      {
        automation_key: "daily_brief",
        title: "每日简报",
        next_run_at: "2026-04-27T00:30:00Z",
        cadence_label: "每天 08:30",
      },
    ],
    daily_brief_summary: {
      brief_id: "daily-brief:e2e:ready",
      headline: dashboardState.daily_brief.headline,
      beginner_explanation: dashboardState.daily_brief.beginner_explanation,
      as_of: dashboardState.daily_brief.as_of,
      source_coverage: dashboardState.daily_brief.source_coverage,
    },
    recent_notifications: [] as Array<Record<string, unknown>>,
  };
}

function rebuildAutomationQueue(automationState: ReturnType<typeof buildAutomationState>) {
  automationState.active_count = automationState.automations.filter(
    (item) => item.enabled,
  ).length;
  automationState.next_queue = automationState.automations
    .filter((item) => item.enabled)
    .map((item) => ({
      automation_key: item.key,
      title: item.title,
      next_run_at: item.next_run_at ?? "2026-04-27T00:30:00Z",
      cadence_label: item.cadence_label,
    }));
}

function buildProfileContext() {
  return {
    user_id: "user_e2e",
    display_name: "QA 用户",
    context_readiness: {
      ready_count: 7,
      total_count: 8,
      items: [
        { key: "profile", label: "基础画像", ready: true, last_updated_at: "2026-04-26T08:00:00Z", missing_action_route: null },
        { key: "risk", label: "风险问卷", ready: true, last_updated_at: "2026-04-26T08:00:00Z", missing_action_route: null },
        { key: "portfolio", label: "组合报告", ready: true, last_updated_at: "2026-04-26", missing_action_route: null },
        { key: "news", label: "资讯分析", ready: true, last_updated_at: "2026-04-26T08:20:00Z", missing_action_route: null },
        { key: "learning", label: "学习状态", ready: true, last_updated_at: null, missing_action_route: null },
        { key: "simulation", label: "训练复盘", ready: true, last_updated_at: "2026-04-26T09:20:00Z", missing_action_route: null },
        { key: "behavior", label: "行为证据", ready: true, last_updated_at: "2026-04-26T08:00:00Z", missing_action_route: null },
        { key: "automations", label: "自动任务", ready: false, last_updated_at: null, missing_action_route: "/automations" },
      ],
    },
    risk_profile: {
      risk_level: "balanced",
      latest_risk_score: 18,
      updated_at: "2026-04-26T08:00:00Z",
    },
    behavior_profile: {
      bias_tags: ["追热点倾向", "回撤焦虑"],
      evidence: ["问卷显示在热门基金讨论中容易冲动跟进。"],
      updated_at: "2026-04-26T08:00:00Z",
    },
    portfolio_context: {
      has_report: true,
      latest_snapshot_date: "2026-04-26",
      total_value: 52000,
      summary: "组合覆盖权益、债券和现金，但第一大持仓需要继续观察。",
    },
    learning_context: {
      overall_progress_percentage: 34,
      recommended_course_title: "风险和回撤基础",
    },
    simulation_context: {
      latest_review_summary: "最近复盘显示你能先检查计划，再决定是否调整。",
      latest_completed_at: "2026-04-26T09:20:00Z",
    },
    automation_authorizations: [
      { automation_key: "daily_brief", enabled: true, cadence_label: "每天 08:30" },
      { automation_key: "weekly_portfolio", enabled: false, cadence_label: "每周一 09:00" },
    ],
    authorization_scope: [
      { key: "profile", label: "基础画像", readable: true },
      { key: "portfolio", label: "组合报告", readable: true },
      { key: "news", label: "资讯分析", readable: true },
      { key: "behavior", label: "行为证据", readable: true },
    ],
    pending_proposal_count: 1,
  };
}

function buildPendingProposals() {
  return {
    pending_count: 1,
    resolved_count: 0,
    proposals: [
      {
        id: "proposal_behavior_hot_topic",
        title: "把“追热点倾向”加入待观察行为证据",
        source_label: "最近对话 + 情境训练理由",
        evidence_summary: "用户在热门基金讨论中更容易先问收益表现，而不是先问风险来源。",
        writeback_label: "行为证据，不是永久标签",
        target_type: "behavior_profile_note",
        target_id: null,
        patch_preview: { evidence: "追热点倾向待观察" },
        reason: "这是一条需要用户确认的行为证据候选。",
        validator_status: "passed",
        validator_message: null as string | null,
        status: "pending",
        safety_note: "确认后只追加行为证据，不会触发交易或账户动作。",
        created_at: "2026-04-26T09:40:00Z",
        run_id: "run_e2e",
      },
    ],
  };
}

function buildLlmSettings(): {
  provider: "deepseek";
  model_name: string;
  configured: boolean;
  enabled: boolean;
  source: "user" | "workspace" | "none";
  masked_api_key: string | null;
  updated_at: string | null;
  warning: string | null;
} {
  return {
    provider: "deepseek",
    model_name: "deepseek:deepseek-v4-pro",
    configured: false,
    enabled: true,
    source: "none",
    masked_api_key: null,
    updated_at: null,
    warning: "当前没有可用的 LLM API，Agent 会明确提示模型未配置，不会伪装成模型回答。",
  };
}

function json(route: Route, body: unknown, status = 200) {
  const requestOrigin = new URL(route.request().headers().origin ?? route.request().url()).origin;

  return route.fulfill({
    status,
    contentType: "application/json",
    headers: {
      "Access-Control-Allow-Credentials": "true",
      "Access-Control-Allow-Headers": "content-type",
      "Access-Control-Allow-Methods": "GET,POST,PATCH,PUT,OPTIONS",
      "Access-Control-Allow-Origin": requestOrigin,
    },
    body: JSON.stringify(body),
  });
}

function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function sse(
  route: Route,
  blocks: Array<{ event: string; data: unknown }>,
  delayMs = 0,
) {
  const requestOrigin = new URL(route.request().headers().origin ?? route.request().url()).origin;
  const lineBreak = "\r\n";
  const body = blocks
    .map((block) => {
      const source =
        block.data && typeof block.data === "object" && !Array.isArray(block.data)
          ? { ...(block.data as Record<string, unknown>) }
          : block.data;
      const sourceRecord =
        source && typeof source === "object" && !Array.isArray(source)
          ? (source as Record<string, unknown>)
          : null;
      const eventId = sourceRecord?.id ?? null;
      if (
        block.event === "agent_event" &&
        sourceRecord
      ) {
        delete sourceRecord.id;
      }
      const idLine = typeof eventId === "string" ? `id: ${eventId}${lineBreak}` : "";
      return `event: ${block.event}${lineBreak}${idLine}data: ${JSON.stringify(source)}${lineBreak}${lineBreak}`;
    })
    .join("");

  if (delayMs > 0) {
    await wait(delayMs);
  }

  return route.fulfill({
    status: 200,
    contentType: "text/event-stream",
    headers: {
      "Access-Control-Allow-Credentials": "true",
      "Access-Control-Allow-Headers": "content-type",
      "Access-Control-Allow-Methods": "DELETE,GET,POST,PATCH,PUT,OPTIONS",
      "Access-Control-Allow-Origin": requestOrigin,
      "Cache-Control": "no-cache",
    },
    body,
  });
}

export async function mockFundGeneApi(
  page: Page,
  options: { authenticated?: boolean } = {},
) {
  const authenticated = options.authenticated ?? true;
  const automationState = buildAutomationState();
  const profileContext = buildProfileContext();
  const pendingProposals = buildPendingProposals();
  const llmSettings = buildLlmSettings();
  let queuedFollowUp: Record<string, unknown> | null = null;

  await page.context().route("**/*", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;

    if (!path.startsWith("/api/")) {
      return route.continue();
    }

    if (route.request().method() === "OPTIONS") {
      return json(route, {}, 204);
    }

    if (path === "/api/auth/session") {
      if (!authenticated) {
        return json(route, { detail: "Authentication required." }, 401);
      }
      return json(route, sessionUser);
    }
    if (path === "/api/auth/logout") {
      return json(route, {});
    }
    if (path === "/api/behavior/profile") {
      return json(route, behaviorProfile);
    }
    if (path === "/api/dashboard") {
      return json(route, dashboardState);
    }
    if (path === "/api/automations") {
      return json(route, automationState);
    }
    if (path.startsWith("/api/automations/") && path.endsWith("/run")) {
      const automationKey = path.split("/").at(-2);
      const item = automationState.automations.find(
        (automation) => automation.key === automationKey,
      );
      if (!item) {
        return json(route, { detail: "Automation does not exist." }, 404);
      }
      if (!item.enabled) {
        return json(route, { detail: "Automation is disabled." }, 409);
      }
      const createdPendingProposalIds =
        automationKey === "news_watch" ? ["proposal_news_watch_e2e"] : [];
      const runSummary =
        automationKey === "news_watch"
          ? "资讯影响观察已完成：已同步最新政策新闻，并准备 1 条待确认观察。"
          : `${item.title} 已生成同步运行记录。`;
      const run = {
        run_id: `run_${automationKey}_e2e`,
        agent_run_id: `agent_run_${automationKey}_e2e`,
        status: "succeeded",
        started_at: "2026-04-26T10:00:00Z",
        completed_at: "2026-04-26T10:00:00Z",
        summary: runSummary,
        output_ref: `automation:${automationKey}:e2e`,
      };
      (item as unknown as { last_run: typeof run | null }).last_run = run;
      automationState.recent_notifications.unshift({
        id: `notification_${automationKey}_e2e`,
        automation_key: item.key,
        title: item.title,
        message:
          automationKey === "news_watch"
            ? "已同步最新资讯，央行支持性货币政策信息与权益基金情绪、债券基金缓冲相关；保存为长期观察前仍需你确认。"
            : `${item.title} 已完成本轮同步检查。`,
        action_label: automationKey === "news_watch" ? "查看待确认记录" : "查看运行记录",
        action_route: automationKey === "news_watch" ? "/profile?from=automation" : "/automations",
        created_at: "2026-04-26T10:00:00Z",
        read_at: null,
      });
      if (
        automationKey === "news_watch" &&
        !pendingProposals.proposals.some((proposal) => proposal.id === "proposal_news_watch_e2e")
      ) {
        pendingProposals.proposals.unshift({
          id: "proposal_news_watch_e2e",
          title: "确认记录央行政策新闻的组合影响观察",
          source_label: "资讯影响观察自动任务",
          evidence_summary:
            "央行支持性货币政策信息可能影响权益基金情绪和债券基金缓冲作用，建议先作为待观察线索保存。",
          writeback_label: "组合观察线索，不是交易动作",
          target_type: "behavior_profile_note",
          target_id: null,
          patch_preview: { evidence: "央行政策新闻影响路径待观察" },
          reason: "自动任务只准备候选记录，仍需用户确认。",
          validator_status: "passed",
          validator_message: "新闻相关性不被写成交易信号。",
          status: "pending",
          safety_note: "确认后只追加观察线索，不会修改持仓或执行交易。",
          created_at: "2026-04-26T10:00:00Z",
          run_id: run.run_id,
        });
        pendingProposals.pending_count = pendingProposals.proposals.length;
      }
      return json(route, {
        run_id: run.run_id,
        automation_key: item.key,
        status: "succeeded",
        agent_run_id: run.agent_run_id,
        started_at: "2026-04-26T10:00:00Z",
        completed_at: run.completed_at,
        summary: run.summary,
        output_ref: run.output_ref,
        created_pending_proposal_ids: createdPendingProposalIds,
        steps: [
          {
            key: "read_context",
            label: "读取授权上下文",
            status: "completed",
            detail: "已读取画像、组合、新闻、学习和训练状态。",
          },
          {
            key: "refresh_news",
            label: "同步最新资讯",
            status: "completed",
            detail:
              automationKey === "news_watch"
                ? "已读取央行政策新闻和持仓相关信号。"
                : "本任务不需要额外同步资讯。",
          },
          {
            key: "build_output",
            label: "生成可解释结论",
            status: "completed",
            detail:
              automationKey === "news_watch"
                ? "已把新闻影响拆成权益情绪、债券缓冲和不确定性。"
                : "已生成今日判断、安全下一步和边界说明。",
          },
          {
            key: "notify_user",
            label: "写入通知和运行记录",
            status: "completed",
            detail: "已保存运行记录和通知。",
          },
        ],
        output_payload: {
          automation_key: item.key,
          headline:
            automationKey === "news_watch"
              ? "央行支持性货币政策信息值得观察，但先核对组合暴露，不直接行动。"
              : dashboardState.daily_brief.headline,
          source_coverage: dashboardState.daily_brief.source_coverage,
          primary_action:
            automationKey === "news_watch"
              ? {
                  label: "查看待确认观察",
                  href: "/profile?from=automation",
                }
              : dashboardState.daily_brief.primary_action,
          safety_boundary: item.safety_boundary,
        },
      });
    }
    if (path.startsWith("/api/automations/")) {
      const automationKey = path.split("/").at(-1);
      const item = automationState.automations.find(
        (automation) => automation.key === automationKey,
      );
      if (!item) {
        return json(route, { detail: "Automation does not exist." }, 404);
      }
      let payload: Record<string, unknown> = {};
      try {
        payload = route.request().postDataJSON() as Record<string, unknown>;
      } catch {
        payload = {};
      }
      if (typeof payload.enabled === "boolean") {
        item.enabled = payload.enabled;
        item.status = payload.enabled ? "ready" : "disabled";
        item.can_run_now = payload.enabled;
        item.next_run_at = payload.enabled ? "2026-04-27T00:30:00Z" : null;
      }
      if (typeof payload.cadence_key === "string") {
        const option = item.cadence_options.find(
          (entry) => entry.key === payload.cadence_key,
        );
        if (!option) {
          return json(route, { detail: "cadence_key is not valid for this automation." }, 422);
        }
        item.cadence_key = option.key;
        item.cadence_label = option.label;
      }
      rebuildAutomationQueue(automationState);
      return json(route, item);
    }
    if (path === "/api/profile/context") {
      profileContext.pending_proposal_count = pendingProposals.pending_count;
      profileContext.automation_authorizations = automationState.automations.map(
        (item) => ({
          automation_key: item.key,
          enabled: item.enabled,
          cadence_label: item.cadence_label,
        }),
      );
      return json(route, profileContext);
    }
    if (path === "/api/profile/pending-proposals") {
      return json(route, pendingProposals);
    }
    if (path === "/api/profile/llm-settings") {
      if (route.request().method() === "PUT") {
        let payload: Record<string, unknown> = {};
        try {
          payload = route.request().postDataJSON() as Record<string, unknown>;
        } catch {
          payload = {};
        }
        if (typeof payload.model_name === "string" && payload.model_name.trim()) {
          llmSettings.model_name = payload.model_name.trim();
        }
        if (typeof payload.enabled === "boolean") {
          llmSettings.enabled = payload.enabled;
        }
        if (typeof payload.api_key === "string" && payload.api_key.trim()) {
          const key = payload.api_key.trim();
          llmSettings.configured = true;
          llmSettings.source = "user";
          llmSettings.masked_api_key = `${key.slice(0, 6)}...${key.slice(-4)}`;
          llmSettings.warning = null;
          llmSettings.updated_at = "2026-04-26T10:30:00Z";
        }
        return json(route, { settings: llmSettings });
      }
      return json(route, llmSettings);
    }
    if (path.startsWith("/api/profile/pending-proposals/")) {
      const parts = path.split("/");
      const proposalId = parts.at(-2);
      const action = parts.at(-1);
      const proposal = pendingProposals.proposals.find(
        (item) => item.id === proposalId,
      );
      if (!proposal) {
        return json(
          route,
          { detail: "Pending proposal does not exist for the current user." },
          404,
        );
      }
      pendingProposals.proposals = pendingProposals.proposals.filter(
        (item) => item.id !== proposalId,
      );
      pendingProposals.pending_count = pendingProposals.proposals.length;
      pendingProposals.resolved_count += 1;
      proposal.status = action === "accept" ? "applied" : "rejected";
      return json(route, {
        proposal,
        applied_writeback: action === "accept",
      });
    }
    if (path === "/api/assistant/session") {
      return json(route, assistantSession);
    }
    if (path === "/api/assistant/sessions") {
      return json(route, {
        sessions: [assistantSession.session, assistantHistorySession.session],
      });
    }
    if (path === "/api/assistant/sessions/session_e2e") {
      return json(route, assistantSession);
    }
    if (path === "/api/assistant/sessions/session_e2e/queued-follow-up") {
      if (route.request().method() === "DELETE") {
        queuedFollowUp = null;
        return json(route, {
          schema_version: "queued_follow_up_v1",
          queued_follow_up: null,
          message: "已取消排队的下一句。",
        });
      }
      return json(route, {
        schema_version: "queued_follow_up_v1",
        queued_follow_up: queuedFollowUp,
        message: queuedFollowUp
          ? "已找到排队的下一句。"
          : "当前会话没有排队的下一句。",
      });
    }
    if (
      path === "/api/assistant/sessions/session_e2e/queued-follow-up/submitted"
    ) {
      if (queuedFollowUp) {
        queuedFollowUp = {
          ...queuedFollowUp,
          status: "submitted",
          submitted_at: "2026-04-26T09:36:00Z",
        };
      }
      return json(route, {
        schema_version: "queued_follow_up_v1",
        queued_follow_up: queuedFollowUp,
        message: "排队内容已发送。",
      });
    }
    if (path === "/api/assistant/sessions/session_history_e2e") {
      return json(route, assistantHistorySession);
    }
    if (path.startsWith("/api/assistant/runs/") && path.endsWith("/trace")) {
      return json(route, agentRunTrace);
    }
    if (path.startsWith("/api/assistant/runs/") && path.endsWith("/events")) {
      return json(route, agentRunEvents);
    }
    if (
      path === "/api/assistant/runs/run_active_disconnect_e2e/status"
    ) {
      return json(route, {
        schema_version: "agent_run_status_v1",
        run_id: "run_active_disconnect_e2e",
        session_id: "session_e2e",
        status: "running",
        run_status: "running",
        active: true,
        cancel_requested: false,
        started_at: "2026-04-26T09:35:00Z",
        completed_at: null,
        latency_ms: null,
        message: "本次整理正在当前进程中运行。",
      });
    }
    if (path === "/api/assistant/runs/run_stream_error_recover_e2e/status") {
      return json(route, {
        schema_version: "agent_run_status_v1",
        run_id: "run_stream_error_recover_e2e",
        session_id: "session_e2e",
        status: "completed",
        run_status: "completed",
        active: false,
        cancel_requested: false,
        started_at: "2026-04-26T09:36:00Z",
        completed_at: "2026-04-26T09:36:03Z",
        latency_ms: 3000,
        message: "本次整理已完成。",
      });
    }
    if (path.startsWith("/api/assistant/runs/") && path.endsWith("/status")) {
      return json(route, agentRunStatus);
    }
    if (
      path.startsWith("/api/assistant/runs/") &&
      path.endsWith("/queued-follow-up")
    ) {
      let payload: Record<string, unknown> = {};
      try {
        payload = route.request().postDataJSON() as Record<string, unknown>;
      } catch {
        payload = {};
      }
      queuedFollowUp = {
        id: "queued_followup_e2e",
        session_id: "session_e2e",
        queued_after_run_id: "run_e2e",
        message: String(payload.message ?? ""),
        status: "queued",
        created_at: "2026-04-26T09:35:05Z",
        submitted_at: null,
        discarded_at: null,
      };
      return json(route, {
        schema_version: "queued_follow_up_v1",
        queued_follow_up: queuedFollowUp,
        message: "已排队，上一条完成后发送。",
      });
    }
    if (path.startsWith("/api/assistant/runs/") && path.endsWith("/cancel")) {
      queuedFollowUp = null;
      return json(route, {
        run_id: "run_e2e",
        status: "cancelling",
        cancel_requested: true,
        message: "已请求停止本次整理，排队的下一句不会自动发送。",
      });
    }
    if (path === "/api/assistant/runs/active") {
      return json(route, activeAgentRuns);
    }
    if (path === "/api/assistant/messages/stream") {
      let payload: Record<string, unknown> = {};
      try {
        payload = route.request().postDataJSON() as Record<string, unknown>;
      } catch {
        payload = {};
      }
      const message = typeof payload.message === "string" ? payload.message : "";
      const isAgentFirstNextMessage = message.includes(agentFirstNextQuestion);
      const isAgentFirstWhyMessage = message.includes(agentFirstWhyQuestion);
      const isAgentFirstPlanMessage = message.includes(agentFirstQuestion);
      const isAgentShowcaseMessage = message.includes(agentShowcaseQuestion);
      const isAgentPortfolioJumpMessage = message.includes(agentPortfolioJumpQuestion);
      const isAgentShowcaseFollowUpMessage = message.includes(
        agentShowcaseFollowUpQuestion,
      );
      const isNewsShowcaseMessage =
        typeof payload.message === "string" &&
        (payload.message.includes(newsQuestion) ||
          payload.message.includes("央行：下一阶段将坚持支持性的货币政策立场") ||
          payload.message.includes("支持性货币政策"));
      const conversation =
        isNewsShowcaseMessage
          ? newsAssistantSession
          : isAgentFirstNextMessage
          ? agentFirstNextSession
          : isAgentFirstWhyMessage
          ? agentFirstWhySession
          : isAgentFirstPlanMessage
          ? agentFirstPlanSession
          : isAgentShowcaseFollowUpMessage
          ? agentShowcaseFollowUpSession
          : isAgentPortfolioJumpMessage
          ? agentPortfolioJumpSession
          : isAgentShowcaseMessage || payload.start_new_session === true
          ? agentShowcaseSession
          : payload.session_id === "session_history_e2e"
          ? continuedAssistantHistorySession
          : assistantSession;
      const simulateActiveDisconnect =
        typeof payload.message === "string" &&
        payload.message.includes("触发活跃流断开测试");
      const simulateRecoverableStreamError =
        typeof payload.message === "string" &&
        payload.message.includes("触发错误恢复测试");
      const streamRunId = simulateActiveDisconnect
        ? "run_active_disconnect_e2e"
        : simulateRecoverableStreamError
          ? "run_stream_error_recover_e2e"
        : "run_e2e";
      const omitConversation =
        typeof payload.message === "string" &&
        (payload.message.includes("触发流式恢复测试") ||
          payload.message.includes("触发活跃流断开测试"));
      const streamBlocks: Array<{ event: string; data: unknown }> = [
        {
          event: "agent_event",
          data: {
            id: `${streamRunId}:0001`,
            run_id: streamRunId,
            sequence: 1,
            event_type: "turn_started",
            phase: "turn",
            title: "开始处理任务",
            status: "running",
            at: "2026-04-26T09:35:00Z",
            duration_ms: null,
            payload: {},
          },
        },
        {
          event: "agent_event",
          data: {
            id: `${streamRunId}:0002`,
            run_id: streamRunId,
            sequence: 2,
            event_type: "step_started",
            phase: "context",
            title: "读取授权上下文",
            status: "running",
            at: "2026-04-26T09:35:01Z",
            duration_ms: null,
            payload: {},
          },
        },
      ];
      if (!simulateActiveDisconnect) {
        streamBlocks.push({
          event: "agent_event",
          data: {
            id: `${streamRunId}:0003`,
            run_id: streamRunId,
            sequence: 3,
            event_type: "turn_complete",
            phase: "turn",
            title: "任务处理完成",
            status: "completed",
            at: "2026-04-26T09:35:02Z",
            duration_ms: 1200,
            payload: {},
          },
        });
      }
      if (simulateRecoverableStreamError) {
        streamBlocks.push({
          event: "error",
          data: {
            message: "流式连接在最终会话返回前断开。",
            run_id: streamRunId,
            recoverable: true,
          },
        });
        return sse(route, streamBlocks, 850);
      }
      if (!omitConversation) {
        streamBlocks.push({ event: "conversation", data: conversation });
      }
      return sse(route, streamBlocks, 850);
    }
    if (path === "/api/assistant/messages") {
      let payload: Record<string, unknown> = {};
      try {
        payload = route.request().postDataJSON() as Record<string, unknown>;
      } catch {
        payload = {};
      }
      if (
        typeof payload.message === "string" &&
        (payload.message.includes(newsQuestion) ||
          payload.message.includes("央行：下一阶段将坚持支持性的货币政策立场") ||
          payload.message.includes("支持性货币政策"))
      ) {
        return json(route, newsAssistantSession);
      }
      if (
        typeof payload.message === "string" &&
        payload.message.includes(agentFirstNextQuestion)
      ) {
        return json(route, agentFirstNextSession);
      }
      if (
        typeof payload.message === "string" &&
        payload.message.includes(agentFirstWhyQuestion)
      ) {
        return json(route, agentFirstWhySession);
      }
      if (
        typeof payload.message === "string" &&
        payload.message.includes(agentFirstQuestion)
      ) {
        return json(route, agentFirstPlanSession);
      }
      if (
        typeof payload.message === "string" &&
        payload.message.includes(agentShowcaseFollowUpQuestion)
      ) {
        return json(route, agentShowcaseFollowUpSession);
      }
      if (
        typeof payload.message === "string" &&
        payload.message.includes(agentPortfolioJumpQuestion)
      ) {
        return json(route, agentPortfolioJumpSession);
      }
      if (
        (typeof payload.message === "string" &&
          payload.message.includes(agentShowcaseQuestion)) ||
        payload.start_new_session === true
      ) {
        return json(route, agentShowcaseSession);
      }
      if (payload.session_id === "session_history_e2e") {
        return json(route, continuedAssistantHistorySession);
      }
      return json(route, assistantSession);
    }
    if (path === "/api/learning/path") {
      return json(route, learningPath);
    }
    if (path.startsWith("/api/learning/courses/")) {
      return json(route, {
        ...learningPath.courses[0],
        path_slug: learningPath.path_slug,
        path_title: learningPath.title,
        course_slug: learningPath.courses[0].slug,
        sections: [
          {
            slug: "intro",
            title: "先理解风险",
            summary: "风险不是单日涨跌，而是计划可能承受的波动。",
            estimated_duration_minutes: 6,
            position: 1,
            completed: true,
          },
          {
            slug: "drawdown",
            title: "如何理解回撤",
            summary: "回撤帮助你观察阶段性压力。",
            estimated_duration_minutes: 7,
            position: 2,
            completed: false,
          },
        ],
      });
    }
    if (path === "/api/portfolio/latest") {
      return json(route, { report: portfolioReport });
    }
    if (path === "/api/portfolio/history") {
      return json(route, {
        items: [
          {
            snapshot_id: portfolioReport.snapshot_id,
            snapshot_date: portfolioReport.snapshot_date,
            total_value: portfolioReport.total_value,
            summary: portfolioReport.summary,
            generated_at: portfolioReport.generated_at,
          },
        ],
      });
    }
    if (path === "/api/portfolio/snapshots") {
      return json(route, portfolioReport);
    }
    if (path === "/api/simulations/scenarios") {
      return json(route, simulationCatalog);
    }
    if (path === "/api/simulations/sessions/sim_session_e2e") {
      return json(route, activeSimulationSession);
    }
    if (path === "/api/simulations/actions") {
      return json(route, {
        session: completedSimulationSession,
        feedback: {
          summary: "已记录这次动作，复盘已生成。",
          action_label: "持有",
          bias_signal: null,
          impact: "保持观察，避免冲动动作。",
          discipline_signals: ["先写理由", "控制冲动"],
          next_prompt: null,
        },
        review_ready: true,
      });
    }
    if (path === "/api/simulations/review/sim_session_e2e") {
      return json(route, simulationReview);
    }
    if (path === "/api/news") {
      return json(route, newsCatalog);
    }
    if (path === "/api/news/analyze") {
      const analysisPayload = {
        analysis: {
          id: "analysis_e2e",
          item: newsCatalog.items[0],
          fact_summary:
            "央行继续强调支持性的货币政策立场。会议提到保持流动性合理充裕，推动社会融资成本稳中有降。当前外部环境复杂，国内有效需求仍需继续修复。",
          impact_path: [
            "如果利率中枢继续下行，债券基金的长期配置价值可能提升。",
            "流动性宽松通常有利于权益市场估值修复，但成长风格未必持续占优。",
            "对个人组合的影响取决于权益基金、债券基金和现金的实际比例。",
          ],
          uncertainties: [
            "后续政策力度和节奏仍可能受经济修复、通胀和外部利率影响。",
            "市场短期反应可能领先基本面，不能把政策新闻直接等同于买入信号。",
            "如果组合已经集中在单一主题，利好也可能放大波动。",
          ],
          risk_notice: "资讯解读不构成买卖建议。",
          recommended_actions: ["先回看自己的权益基金比例。", "检查债券基金是否承担缓冲作用。"],
          related_learning: ["风险和回撤基础"],
          citations: ["news_e2e"],
          model_status: "enhanced",
          model_provider: "deepseek",
          model_name: "deepseek:deepseek-v4-pro",
          fallback_reason: null,
          agent_process: [
            {
              key: "deepseek_enhancement",
              label: "调用 DeepSeek 生成解读",
              status: "completed",
              detail: "deepseek:deepseek-v4-pro 已完成结构化解读。",
            },
          ],
          generated_at: "2026-04-26T08:20:00Z",
        },
      };
      return json(route, analysisPayload);
    }
    if (path === "/api/news/analyses/analysis_e2e") {
      return json(route, {
        id: "analysis_e2e",
        item: newsCatalog.items[0],
        facts: [
          "央行继续强调支持性的货币政策立场。",
          "会议提到保持流动性合理充裕，推动社会融资成本稳中有降。",
          "当前外部环境复杂，国内有效需求仍需继续修复。",
        ],
        impact_paths: [
          "如果利率中枢继续下行，债券基金的长期配置价值可能提升。",
          "流动性宽松通常有利于权益市场估值修复，但成长风格未必持续占优。",
          "对个人组合的影响取决于权益基金、债券基金和现金的实际比例。",
        ],
        uncertainty_notes: [
          "后续政策力度和节奏仍可能受经济修复、通胀和外部利率影响。",
          "市场短期反应可能领先基本面，不能把政策新闻直接等同于买入信号。",
          "如果组合已经集中在单一主题，利好也可能放大波动。",
        ],
        beginner_translation: "先理解政策影响路径，再回看自己的权益基金比例。",
        related_learning_topics: ["风险和回撤基础"],
        recommended_next_actions: ["先回看自己的权益基金比例。", "检查债券基金是否承担缓冲作用。"],
        risk_notice: "资讯解读不构成买卖建议。",
        citations: [],
        model_status: "enhanced",
        model_provider: "deepseek",
        model_name: "deepseek:deepseek-v4-pro",
        fallback_reason: null,
        agent_process: [
          {
            key: "deepseek_enhancement",
            label: "调用 DeepSeek 生成解读",
            status: "completed",
            detail: "deepseek:deepseek-v4-pro 已完成结构化解读。",
          },
        ],
        generated_at: "2026-04-26T08:20:00Z",
      });
    }

    return json(route, { detail: `Unhandled test route: ${path}` }, 404);
  });
}
