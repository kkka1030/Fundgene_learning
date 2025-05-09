# Console 函数详解

`Console` 是一个异步函数，用于处理和渲染来自智能体或任务运行器的消息流，将消息输出到控制台。

## 核心参数及功能

### 主要参数

1. **stream (AsyncGenerator)**
   - 输入的消息流，可以来自：
     - `TaskRunner.run_stream()`
     - `ChatAgent.on_messages_stream()`
   - 必需参数
   - 可以产生三种类型的消息：
     - `BaseAgentEvent`: 基础代理事件
     - `BaseChatMessage`: 聊天消息
     - `TaskResult`/`Response`: 任务结果或响应

2. **no_inline_images (bool)**
   - 是否禁用内联图片显示
   - 默认值: False
   - 当终端是iTerm2时，默认会内联显示图片，此参数可禁用该行为

3. **output_stats (bool)**
   - (实验性)是否输出统计信息
   - 默认值: False
   - 会显示消息摘要和token使用情况
   - 注意：当前统计可能不准确，未来版本会改进

4. **user_input_manager (UserInputManager)**
   - 用户输入管理器
   - 默认值: None
   - 用于处理用户输入请求事件

## 返回值

- 返回最后处理的`TaskResult`或`Response`对象
- 如果流来自`TaskRunner.run_stream()`，返回`TaskResult`
- 如果流来自`ChatAgent.on_messages_stream()`，返回`Response`

## 功能特点

### 消息渲染处理

1. **TaskResult处理**
   - 输出任务摘要统计(当output_stats=True时)
   - 包括消息数量、完成原因、token使用和持续时间

2. **Response处理**
   - 输出最终响应内容
   - 支持多模态消息的渲染
   - 输出token使用统计(当output_stats=True时)

3. **流式消息处理**
   - 实时显示流式消息内容
   - 自动处理消息分块
   - 支持多模态消息的显示

4. **用户输入处理**
   - 检测`UserInputRequestedEvent`事件
   - 通过`user_input_manager`通知用户输入请求

### 统计功能

1. **Token统计**
   - 累计提示token和完成token数量
   - 显示在消息摘要中

2. **时间统计**
   - 计算处理总时长
   - 显示在消息摘要中

## 使用示例

### 基本使用
```python
# 从智能体获取消息流
stream = agent.on_messages_stream(messages)
result = await Console(stream)

# 从任务运行器获取消息流
stream = task_runner.run_stream(task)
result = await Console(stream)
```

### 带统计信息
```python
await Console(stream, output_stats=True)
```

### 禁用内联图片
```python
await Console(stream, no_inline_images=True)
```

## 设计特点

1. **异步处理**
   - 完全基于异步IO
   - 支持实时流式消息处理

2. **终端适配**
   - 自动检测iTerm2终端
   - 支持图片内联显示

3. **灵活输入**
   - 支持多种消息源
   - 统一处理不同消息类型

4. **可扩展性**
   - 通过user_input_manager支持自定义用户输入处理
   - 便于集成到不同应用场景

这个函数是AutoGen框架中用于控制台交互的核心组件，为开发者提供了方便的消息渲染和统计功能。