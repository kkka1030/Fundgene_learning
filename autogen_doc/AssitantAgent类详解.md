# AssistantAgent 类详解

`AssistantAgent` 是一个提供工具使用辅助的智能体类，继承自 `BaseChatAgent` 和 `Component`。下面我将详细解释这个类的各个参数及其功能。

## 主要参数及功能

### 基本参数

1. **name (str)**
   - 智能体的名称，用于标识该智能体

2. **model_client (ChatCompletionClient)**
   - 用于推理的模型客户端，是智能体的核心组件
   - 必须提供的参数，没有默认值

3. **description (str)**
   - 智能体的描述信息
   - 默认值: "An agent that provides assistance with ability to use tools."

### 工具相关参数

4. **tools (List[BaseTool | Callable] | None)**
   - 注册到智能体的工具列表
   - 可以是 `BaseTool` 实例或可调用对象
   - 默认值: None
   - 示例:
     ```python
     tools=[get_current_time]  # 函数工具
     ```

5. **tool_call_summary_format (str)**
   - 工具调用摘要的格式字符串
   - 默认值: "{result}"
   - 可用变量: `{tool_name}`, `{arguments}`, `{result}`
   - 示例:
     ```python
     tool_call_summary_format="{tool_name}: {result}"  # 输出格式: "tool_name: result"
     ```

### 模型上下文参数

6. **model_context (ChatCompletionContext | None)**
   - 用于存储和检索 `LLMMessage` 的模型上下文
   - 可以预加载初始消息
   - 默认值: None (会自动创建 `UnboundedChatCompletionContext`)

7. **system_message (str | None)**
   - 模型的系统消息
   - 如果提供，会在推理时预置到模型上下文的消息前
   - 设置为 None 可禁用
   - 默认值: "You are a helpful AI assistant. Solve tasks using your tools. Reply with TERMINATE when the task has been completed."

### 流式模式参数

8. **model_client_stream (bool)**
   - 是否使用模型客户端的流式模式
   - 默认值: False
   - 当为 True 时，`on_messages_stream` 和 `run_stream` 方法会产生 `ModelClientStreamingChunkEvent` 消息

### 工具调用行为参数

9. **reflect_on_tool_use (bool | None)**
   - 是否在工具调用后进行反思推理
   - 默认行为:
     - 如果设置了 `output_content_type`，默认为 True
     - 否则默认为 False
   - 当为 True 时，智能体会使用工具调用和结果进行另一次模型推理来生成响应
   - 当为 False 时，工具调用结果会直接作为响应返回

### 结构化输出参数

10. **output_content_type (type[BaseModel] | None)**
    - 结构化输出的 Pydantic 模型类型
    - 如果设置，智能体会响应 `StructuredMessage` 而非 `TextMessage`
    - 默认值: None

11. **output_content_type_format (str | None)**
    - (实验性)用于结构化消息内容的格式字符串
    - 默认值: None

### 记忆参数

12. **memory (Sequence[Memory] | None)**
    - 智能体使用的记忆存储
    - 可以预加载初始内容
    - 默认值: None

### 元数据参数

13. **metadata (Dict[str, str] | None)**
    - 可选的元数据，用于跟踪
    - 默认值: None

### 转交(Handoff)参数

14. **handoffs (List[HandoffBase | str] | None)**
    - 智能体的转交配置
    - 允许通过返回 `HandoffMessage` 转交给其他智能体
    - 可以是 `HandoffBase` 实例或目标智能体名称字符串
    - 默认值: None

## 核心方法

1. **on_messages()**
   - 处理传入的消息并返回最终响应
   - 参数:
     - messages: 新消息序列
     - cancellation_token: 取消令牌
   - 返回: `Response` 对象

2. **on_messages_stream()**
   - 异步生成器，实时处理消息并产生事件/响应
   - 参数同 `on_messages()`
   - 产生: `BaseAgentEvent` | `BaseChatMessage` | `Response`

3. **on_reset()**
   - 重置智能体到初始化状态
   - 清除模型上下文

4. **save_state()**
   - 保存智能体当前状态
   - 返回: 状态字典

5. **load_state()**
   - 加载智能体状态
   - 参数: 状态字典

## 使用示例

### 基础智能体

```python
model_client = OpenAIChatCompletionClient(model="gpt-4")
agent = AssistantAgent(name="assistant", model_client=model_client)
response = await agent.on_messages([TextMessage(content="法国的首都是什么？", source="user")], CancellationToken())
```

### 带工具的智能体

```python
async def get_current_time() -> str:
    return "当前时间是12:00 PM。"

agent = AssistantAgent(name="assistant", model_client=model_client, tools=[get_current_time])
```

### 带结构化输出的智能体

```python
class AgentResponse(BaseModel):
    thoughts: str
    response: Literal["happy", "sad", "neutral"]

agent = AssistantAgent(
    name="assistant",
    model_client=model_client,
    output_content_type=AgentResponse
)
```

### 带记忆的智能体

```python
memory = ListMemory()
await memory.add(MemoryContent(content="用户喜欢披萨。", mime_type="text/plain"))
agent = AssistantAgent(name="assistant", model_client=model_client, memory=[memory])
```

这个类提供了强大的对话和工具调用功能，可以通过组合不同的参数来实现各种复杂的智能体行为。