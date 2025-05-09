# SelectorGroupChat 类详解

`SelectorGroupChat` 是一个基于模型选择发言者的群聊团队类，继承自 `BaseGroupChat` 和 `Component`。它允许团队成员轮流发布消息，并使用聊天完成模型来选择每个消息后的下一个发言者。

## 核心参数及功能

### 基本参数

1. **participants (List[ChatAgent])**
   - 参与群聊的智能体列表
   - 必须提供至少2个参与者
   - 所有参与者必须有唯一的名称

2. **model_client (ChatCompletionClient)**
   - 用于选择下一个发言者的聊天完成模型客户端
   - 必须提供的参数

### 流程控制参数

3. **termination_condition (TerminationCondition | None)**
   - 群聊的终止条件
   - 默认值: None (没有终止条件，群聊将无限运行)
   - 示例: `TextMentionTermination("TERMINATE")`

4. **max_turns (int | None)**
   - 群聊的最大轮次
   - 默认值: None (无限制)
   - 设置后将在达到指定轮次后终止

### 发言者选择参数

5. **selector_prompt (str)**
   - 用于选择下一个发言者的提示模板
   - 默认值: 包含角色列表和对话历史的模板
   - 可用占位符:
     - `{roles}`: 可用角色列表
     - `{participants}`: 参与者列表
     - `{history}`: 对话历史

6. **allow_repeated_speaker (bool)**
   - 是否允许连续选择同一个发言者
   - 默认值: False
   - 即使设为True，模型仍可能选择不同发言者

7. **max_selector_attempts (int)**
   - 模型选择发言者的最大尝试次数
   - 默认值: 3
   - 如果模型多次选择无效发言者，将回退到上一个发言者或第一个参与者

### 自定义选择函数

8. **selector_func (SelectorFuncType | None)**
   - 自定义选择函数，覆盖模型选择逻辑
   - 函数签名: `(messages: Sequence[BaseAgentEvent | BaseChatMessage]) -> str | None`
   - 返回None时将回退到模型选择
   - 默认值: None

9. **candidate_func (CandidateFuncType | None)**
   - 自定义候选者过滤函数
   - 函数签名: `(messages: Sequence[BaseAgentEvent | BaseChatMessage]) -> List[str]`
   - 返回空列表或None将引发错误
   - 仅在未设置selector_func时使用
   - 默认值: None

### 其他参数

10. **emit_team_events (bool)**
    - 是否通过`run_stream`方法发出团队事件
    - 默认值: False

11. **model_client_streaming (bool)**
    - 是否为模型客户端启用流式传输
    - 对推理模型(如QwQ)有用
    - 默认值: False

## 核心功能

### 发言者选择机制

1. **模型选择模式**
   - 使用`model_client`和`selector_prompt`选择下一个发言者
   - 支持多次尝试(`max_selector_attempts`)

2. **自定义选择模式**
   - 当提供`selector_func`时，完全自定义选择逻辑
   - 可以基于对话历史实现复杂的选择策略

3. **候选者过滤**
   - 通过`candidate_func`动态过滤可选的发言者
   - 不影响最终选择逻辑

### 流程控制

1. **终止条件**
   - 支持多种终止条件判断
   - 可自定义终止逻辑

2. **轮次限制**
   - 通过`max_turns`防止无限循环
   - 确保对话在合理范围内结束

## 使用示例

### 基本使用

```python
model_client = OpenAIChatCompletionClient(model="gpt-4")
travel_advisor = AssistantAgent("Travel_Advisor", model_client)
hotel_agent = AssistantAgent("Hotel_Agent", model_client)
flight_agent = AssistantAgent("Flight_Agent", model_client)

team = SelectorGroupChat(
    [travel_advisor, hotel_agent, flight_agent],
    model_client=model_client,
    termination_condition=TextMentionTermination("TERMINATE")
)

await team.run_stream(task="Book a 3-day trip to new york.")
```

### 自定义选择函数

```python
def selector_func(messages):
    if len(messages) == 1 or messages[-1].to_text() == "Incorrect!":
        return "Agent1"
    if messages[-1].source == "Agent1":
        return "Agent2"
    return None

team = SelectorGroupChat(
    [agent1, agent2],
    model_client=model_client,
    selector_func=selector_func,
    termination_condition=TextMentionTermination("Correct!")
)
```

## 设计特点

1. **灵活性**
   - 支持模型选择和自定义选择两种模式
   - 可适应各种对话场景

2. **容错性**
   - 内置多次尝试机制
   - 提供回退策略

3. **可扩展性**
   - 支持自定义终止条件
   - 可集成各种模型客户端

4. **事件驱动**
   - 可选的事件发射机制
   - 便于监控和调试

这个类为实现复杂的多智能体协作对话提供了强大的基础框架，特别适合需要动态决定发言顺序的场景。