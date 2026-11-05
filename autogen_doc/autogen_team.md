# AutoGen Teams 使用手册

## 概述

AutoGen Teams 模块提供了多种预定义的多智能体团队实现，每个团队都继承自 `BaseGroupChat` 类，用于协调多个智能体之间的协作对话。

## 基础团队类

### `BaseGroupChat`

所有团队类的基类，提供团队管理的基础功能。

#### 核心功能
- **状态管理**：支持保存和加载团队状态
- **运行控制**：提供运行、暂停、恢复和重置团队的方法
- **消息流处理**：支持流式处理和批量处理消息

#### 主要方法
- `run()`: 运行团队并返回结果
- `run_stream()`: 以流式方式运行团队
- `pause()`: 暂停团队运行
- `resume()`: 恢复暂停的团队
- `reset()`: 重置团队状态
- `save_state()`: 保存团队当前状态
- `load_state()`: 加载团队状态

## 预定义团队类型

### 1. RoundRobinGroupChat

轮询式团队，参与者按顺序轮流发言。

#### 特点
- 简单轮询机制
- 支持终止条件和最大轮次限制
- 适用于需要严格轮流发言的场景

#### 示例代码
```python
import asyncio
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_ext.models.openai import OpenAIChatCompletionClient

async def main():
    model_client = OpenAIChatCompletionClient(model="gpt-4o")
    
    agent1 = AssistantAgent("Assistant1", model_client=model_client)
    agent2 = AssistantAgent("Assistant2", model_client=model_client)
    
    team = RoundRobinGroupChat([agent1, agent2])
    await team.run(task="Count from 1 to 10, respond one at a time.")

asyncio.run(main())
```

### 2. SelectorGroupChat

基于模型选择的团队，使用ChatCompletion模型选择下一个发言者。

#### 特点
- 智能选择下一个发言者
- 可自定义选择逻辑
- 支持候选者过滤
- 适用于需要动态路由的场景

#### 示例代码
```python
import asyncio
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.teams import SelectorGroupChat
from autogen_ext.models.openai import OpenAIChatCompletionClient

async def main():
    model_client = OpenAIChatCompletionClient(model="gpt-4o")
    
    travel_agent = AssistantAgent("Travel_Agent", model_client)
    hotel_agent = AssistantAgent("Hotel_Agent", model_client)
    flight_agent = AssistantAgent("Flight_Agent", model_client)
    
    team = SelectorGroupChat(
        [travel_agent, hotel_agent, flight_agent],
        model_client=model_client
    )
    await team.run(task="Plan a 3-day trip to New York")

asyncio.run(main())
```

### 3. Swarm

基于交接消息的团队，通过HandoffMessage决定下一个发言者。

#### 特点
- 显式交接控制
- 支持人工介入
- 适用于需要精确控制对话流的场景

#### 示例代码
```python
import asyncio
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.teams import Swarm
from autogen_ext.models.openai import OpenAIChatCompletionClient

async def main():
    model_client = OpenAIChatCompletionClient(model="gpt-4o")
    
    agent1 = AssistantAgent("Alice", model_client=model_client, handoffs=["Bob"])
    agent2 = AssistantAgent("Bob", model_client=model_client)
    
    team = Swarm([agent1, agent2])
    await team.run(task="What is Bob's birthday?")

asyncio.run(main())
```

### 4. MagenticOneGroupChat

基于Magentic-One架构的团队，使用专门的协调器管理对话流程。

#### 特点
- 复杂任务处理能力
- 自动规划和重新规划
- 支持最终答案生成
- 适用于复杂问题解决场景

#### 示例代码
```python
import asyncio
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.teams import MagenticOneGroupChat
from autogen_ext.models.openai import OpenAIChatCompletionClient

async def main():
    model_client = OpenAIChatCompletionClient(model="gpt-4o")
    
    assistant = AssistantAgent("Assistant", model_client=model_client)
    
    team = MagenticOneGroupChat([assistant], model_client=model_client)
    await team.run(task="Provide a proof for Fermat's last theorem")

asyncio.run(main())
```

## 高级功能

### 终止条件
所有团队类型都支持终止条件配置：
- `MaxMessageTermination`: 最大消息数终止
- `TextMentionTermination`: 特定文本提及终止
- `HandoffTermination`: 交接特定目标终止
- 可组合使用多个终止条件

### 状态持久化
支持保存和恢复团队状态：
```python
# 保存状态
state = await team.save_state()

# 加载状态
await team.load_state(state)
```

### 流式处理
支持实时处理消息流：
```python
stream = team.run_stream(task="...")
async for message in stream:
    print(message)
```

## 最佳实践

1. **团队规模**：根据任务复杂度选择适当数量的参与者
2. **终止条件**：始终设置合理的终止条件防止无限循环
3. **状态管理**：定期保存状态以便恢复重要会话
4. **工具集成**：为参与者配置适当的工具增强能力
5. **监控**：使用流式处理实时监控团队交互

## 参考引用

如使用MagenticOneGroupChat，请引用：
```
@article{fourney2024magentic,
    title={Magentic-one: A generalist multi-agent system for solving complex tasks},
    author={Fourney, Adam and others},
    journal={arXiv preprint arXiv:2411.04468},
    year={2024}
}
```