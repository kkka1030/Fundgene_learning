import asyncio
import os

from autogen_ext.agents.web_surfer import MultimodalWebSurfer
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_agentchat.ui import Console

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

    web_surfer_agent = MultimodalWebSurfer(
        name="web_surfer_agent",
        description="A web surfer agent that can browse the web and extract information.",
        model_client=model_client,
        debug_dir="./debug",
        to_save_screenshots=True,
        headless=False,
        start_page="https://www.google.com",
        browser_channel="chrome",
        animate_actions=True,

    )

    team = RoundRobinGroupChat([web_surfer_agent], max_turns=3)
    await Console(team.run_stream(task="Find the most popular programming car in 2023.And make comparisons with other cars."))

if __name__ == "__main__":
    asyncio.run(main())