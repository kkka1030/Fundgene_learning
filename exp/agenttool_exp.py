import asyncio
import os
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.tools import AgentTool
from autogen_agentchat.ui import Console
from autogen_ext.models.openai import OpenAIChatCompletionClient

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

    },
)


async def main() -> None:
    writer = AssistantAgent(
        name="writer",
        description="A English writer agent for generating text.",
        model_client=model_client,
        system_message="Write well.",
    )
    
    translater = AssistantAgent(
        name="translater",
        description="A translater agent for translating text.",
        model_client=model_client,
        system_message="Translate the text to given language.",
    )

    writer_tool = AgentTool(agent=writer)
    translater_tool = AgentTool(agent=translater)
    assistant = AssistantAgent(
        name="assistant",
        model_client=model_client,
        tools=[writer_tool, translater_tool],
        system_message="You are a helpful assistant.",
    )
    await Console(assistant.run_stream(task="Write a poem about the sea. And then translate it to Chinese."))


asyncio.run(main())