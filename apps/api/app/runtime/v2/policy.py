from app.runtime.v2.schemas import PolicyResult


DIRECT_TRADING_TERMS = (
    "买入",
    "卖出",
    "清仓",
    "满仓",
    "梭哈",
    "下单",
    "赎回全部",
    "all in",
    "buy now",
    "sell now",
)
RETURN_PROMISE_TERMS = (
    "稳赚",
    "保本",
    "保证收益",
    "一定赚钱",
    "无风险收益",
    "guaranteed return",
)
AUTOMATION_TERMS = (
    "自动交易",
    "自动下单",
    "连接券商",
    "连接交易账户",
    "broker account",
)
SAFE_EDUCATIONAL_OUTPUT_PHRASES = (
    "恐慌卖出",
    "买卖建议",
    "买卖结论",
    "买卖指令",
)
NEGATION_PREFIXES = ("不", "不能", "不要", "不得", "不是", "避免", "防止")


class SafetyPolicy:
    def check_input(self, message: str) -> PolicyResult:
        normalized = message.lower()
        blocked_terms = [
            term
            for term in (
                *DIRECT_TRADING_TERMS,
                *RETURN_PROMISE_TERMS,
                *AUTOMATION_TERMS,
            )
            if term.lower() in normalized
        ]
        if blocked_terms:
            return PolicyResult(
                status="block_with_guidance",
                reason="用户请求触及交易执行、收益保证或账户连接边界。",
                blocked_terms=blocked_terms,
            )
        return PolicyResult(status="allow", reason="输入未触发投资安全边界。")

    def check_output(self, answer: str) -> PolicyResult:
        normalized = answer.lower()
        blocked_terms = [
            term
            for term in (
                *DIRECT_TRADING_TERMS,
                *RETURN_PROMISE_TERMS,
                *AUTOMATION_TERMS,
            )
            if _has_unsafe_output_term(normalized, term.lower())
        ]
        if blocked_terms:
            return PolicyResult(
                status="revise",
                reason="输出包含可能被理解为交易指令、收益承诺或执行能力的表述。",
                blocked_terms=blocked_terms,
                revised_answer=(
                    "这个问题只能作为学习和决策支持来拆解，不能被理解为收益承诺、"
                    "交易指令或自动执行能力。建议先回到风险、期限、组合结构和行为纪律四个维度。"
                ),
            )
        return PolicyResult(status="allow", reason="输出符合学习和决策支持边界。")


def _has_unsafe_output_term(normalized_answer: str, term: str) -> bool:
    start = 0
    while True:
        index = normalized_answer.find(term, start)
        if index == -1:
            return False
        if not _is_safe_output_context(normalized_answer, term=term, index=index):
            return True
        start = index + len(term)


def _is_safe_output_context(normalized_answer: str, *, term: str, index: int) -> bool:
    window_start = max(0, index - 8)
    window_end = min(len(normalized_answer), index + len(term) + 8)
    context = normalized_answer[window_start:window_end]
    if any(phrase.lower() in context for phrase in SAFE_EDUCATIONAL_OUTPUT_PHRASES):
        return True
    prefix = normalized_answer[window_start:index]
    return any(prefix.endswith(negation) for negation in NEGATION_PREFIXES)
