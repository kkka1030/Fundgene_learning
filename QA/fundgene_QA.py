import asyncio
import os
from autogen_agentchat.agents import AssistantAgent, UserProxyAgent
from autogen_agentchat.teams import Swarm, RoundRobinGroupChat, SelectorGroupChat
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_agentchat.messages import HandoffMessage
from autogen_agentchat.conditions import HandoffTermination, MaxMessageTermination, TextMentionTermination
from autogen_agentchat.ui import Console
from autogen_ext.tools.mcp import StdioServerParams, mcp_server_tools
from autogen_ext.agents.web_surfer import MultimodalWebSurfer
from utils.Console_with_history import *

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY") 

model_client = OpenAIChatCompletionClient(
    model="deepseek-chat", 
    base_url="https://api.deepseek.com",
    api_key=DEEPSEEK_API_KEY, 
    model_info={
        "vision": False,
        "function_calling": True,
        "json_output": True,
        "family": "unknown",
    }
)

# QWEN_API_KEY = os.environ.get("QWEN_API_KEY")

# model_client = OpenAIChatCompletionClient(
#     model="qwen/qwen3-235b-a22b:free", 
#     base_url="https://openrouter.ai/api/v1",
#     api_key=QWEN_API_KEY,
#     model_info={
#         "vision": True,
#         "function_calling": True,
#         "json_output": True,
#         "family": "unknown",
#     },
# )


async def main():

    # 创建老师代理
    teacher = AssistantAgent(
        name='Fund_Teacher',
        model_client=model_client,
        system_message='''
        你是一个基金投资专家，精通一切基金投资相关的知识。
        你会根据用户请求给出基金投资的建议和指导，帮助用户更好地进行基金投资。
        你会将用户知识以通俗直白地方式表达出来，帮助入门级投资者理解。
        ''',
    )

    # 将用户输入创建为学生代理
    student = UserProxyAgent(
        name='Student',
    )

    # 设置终止条件
    termination = MaxMessageTermination(4)

    # 创建轮询团队
    team = RoundRobinGroupChat(
        participants=[teacher, student],
        termination_condition=termination,
    )
    
    _, result = await Console_with_history(
        stream=team.run_stream(task="人机对话启动"),
        output_stats=True,
    )

    chat_history = [item for item in result if item.type == 'TextMessage']

    return chat_history

if __name__ == "__main__":
    chat_history = asyncio.run(main())
    for message in chat_history:
        print(f"{message.source}: {message.content}")



