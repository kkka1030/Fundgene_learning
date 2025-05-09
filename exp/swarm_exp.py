import asyncio
import os
from autogen_agentchat.agents import AssistantAgent, UserProxyAgent
from autogen_agentchat.teams import Swarm
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_agentchat.messages import HandoffMessage
from autogen_agentchat.conditions import HandoffTermination, MaxMessageTermination, TextMentionTermination
from autogen_agentchat.ui import Console


DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY")

async def main():
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
    director = AssistantAgent(
        name="Director_Interviewer",
        model_client=model_client,
        handoffs=["Technical_Interviewer", "Management_Interviewer", "Candidate"],  # 定义允许交接的目标
        system_message=
        "你是一个google公司的主持面试官，负责对Candidate进行提问，遇到专业的技术问题或者管理问题，你可以随时交接给技术部门的面试官Technical_Interviewer"\
        "或管理部门的面试官Management_Interviewer进行面试。" \
        "每次提问后等待Candidate回答后再作下一步提问或者交接"\
        "当面试结束时，你可以总结面试结果，给出是否通过的建议。然后说'谢谢各位，面试结束'。"\
        "每次输出不超过30字。"
    )
    
    # 配置支持交接的智能体
    agent1 = AssistantAgent(
        name="Technical_Interviewer",
        model_client=model_client,
        handoffs=["Management_Interviewer", "Candidate", "Director_Interviewer"],  # 定义允许交接的目标
        system_message=
        "你是一个google公司的技术部门面试官，可以对Candidate进行提问，也可以随时交接给Management_Interviewer进行面试。" \
        "每次提问后等待Candidate回答后再作下一步提问"\
        "当你觉得面试通过时可以交接给主面试官Director_Interviewer,让他作总结。" \
        "每次输出不超过30字。"
    )
    
    agent2 = AssistantAgent(
        name="Management_Interviewer",
        model_client=model_client,
        handoffs=["Technical_Interviewer", "Candidate", "Director_Interviewer"],  # 定义允许交接的目标
        system_message=
        "你是一个google公司的管理部门面试官，可以对Candidate进行提问，也可以随时交接给Technical_Interviewer进行面试。" \
        "每次提问后等待Candidate回答后再作下一步提问"\
        "当你觉得面试通过时可以交接给主面试官Director_Interviewer,让他作总结。" \
        "每次输出不超过30字。"

    )

    candidate = UserProxyAgent(
        name="Candidate",
    )
    
    termination = TextMentionTermination(
        text="谢谢各位，面试结束",  # 终止条件
        sources=["Director_Interviewer"],  # 终止条件来源
    )

    team = Swarm([agent1, agent2, candidate, director], termination_condition=termination)
    
    # 启动对话
    await Console(team.run_stream(task="面试开始,下面请Director进行主持"))

asyncio.run(main())