result 是 team.run() 方法的返回值，它是一个包含消息历史记录的对象，具有以下特点：

result 包含 messages 属性，是一个消息列表

列表中的每个消息包含以下关键属性：

source: 消息来源，如 "DBAgent"
content: 消息的实际内容，可能是文本或 JSON 格式的数据
type: 消息类型，如 "TextMessage"、"ToolCallRequestEvent" 等
models_usage: 模型使用信息，包含 tokens 消耗等
消息类型至少包括：

TextMessage: 普通文本消息
ToolCallRequestEvent: 工具调用请求
ToolCallExecutionEvent: 工具执行结果
ToolCallSummaryMessage: 工具调用结果摘要