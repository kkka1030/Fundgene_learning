import asyncio
import os
from autogen_agentchat.ui import Console
from autogen_agentchat.agents import AssistantAgent, SocietyOfMindAgent
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_agentchat.conditions import TextMentionTermination

# DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY")

# model_client = OpenAIChatCompletionClient(
#     model="deepseek-chat", 
#     base_url="https://api.deepseek.com",
#     api_key=DEEPSEEK_API_KEY, 
#     model_info={
#         "vision": False,
#         "function_calling": True,
#         "json_output": True,
#         "family": "unknown",

#     },
# )

QWEN_API_KEY = os.environ.get("QWEN_API_KEY")

model_client = OpenAIChatCompletionClient(
    model="qwen/qwen3-235b-a22b:free", 
    base_url="https://openrouter.ai/api/v1",
    api_key=QWEN_API_KEY,
    model_info={
        "vision": True,
        "function_calling": True,
        "json_output": True,
        "family": "unknown",
    },
)

async def main() -> None:
    agent1 = AssistantAgent("assistant1", model_client=model_client, system_message="You are a writer, write well.")
    agent2 = AssistantAgent(
        "assistant2",
        model_client=model_client,
        system_message="You are an editor, provide critical feedback. Respond with 'APPROVE' if the text addresses all feedbacks.",
    )
    inner_termination = TextMentionTermination("APPROVE")
    inner_team = RoundRobinGroupChat([agent1, agent2], termination_condition=inner_termination)

    society_of_mind_agent = SocietyOfMindAgent("society_of_mind", team=inner_team, model_client=model_client)

    agent3 = AssistantAgent(
        "assistant3", model_client=model_client, system_message="Translate the text to Spanish."
    )
    team = RoundRobinGroupChat([society_of_mind_agent, agent3], max_turns=2)

    stream = team.run_stream(task="Write a short story with a surprising ending.")
    await Console(stream)

if __name__ == "__main__":
    asyncio.run(main())
