import asyncio
import os
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.conditions import TextMentionTermination
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_agentchat.ui import Console
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_agentchat.tools import AgentTool
from autogen_core import CancellationToken

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

async def main() -> None:
    try:

        # 1. 旅行规划专家
        planner_agent = AssistantAgent(
            name="planner_agent",
            model_client=model_client,
            description="旅行规划专家，可以根据用户需求制定旅行计划。",
            system_message="""你是一位专业的旅行规划专家，能够根据用户的需求提供详细的旅行计划建议。
            你的职责是：
            1. 分析用户的旅行目的地和时间安排
            2. 提供合理的行程安排，包括景点、住宿和交通方案
            3. 考虑季节性因素和当地特色
            4. 提供预算建议

            请提供专业、详细且实用的旅行计划建议。"""
        )
        
        # 2. 当地活动专家
        local_agent = AssistantAgent(
            name="local_agent",
            model_client=model_client,
            description="当地活动专家，可以推荐特色活动和地点。",
            system_message="""你是一位当地活动和景点专家，精通世界各地的特色体验和隐藏景点。
            你的职责是：
            1. 推荐目的地的特色活动和体验
            2. 提供非旅游路线上的隐藏景点
            3. 推荐当地美食和文化体验
            4. 提供与当地人互动的机会

            请提供真实、有趣且独特的当地体验建议，帮助旅行者深入了解目的地文化。"""
        )
        
        # 3. 语言沟通专家
        language_agent = AssistantAgent(
            name="language_agent",
            model_client=model_client,
            description="语言沟通专家，提供目的地语言和沟通技巧。",
            system_message="""你是一位语言和沟通专家，精通世界各地的语言和文化差异。
            你的职责是：
            1. 提供目的地的语言基本信息
            2. 教授实用的当地语言短语和表达
            3. 解释文化差异和沟通禁忌
            4. 提供语言障碍的解决方案

            请提供实用、准确的语言和沟通建议，帮助旅行者顺利与当地人交流。"""
        )
        
        # 将专家代理转换为工具
        planner_tool = AgentTool(agent=planner_agent)
        local_tool = AgentTool(agent=local_agent)
        language_tool = AgentTool(agent=language_agent)
        
        # 创建旅行总结代理，使用上述三个专家工具
        travel_summary_agent = AssistantAgent(
            name="travel_summary_agent",
            model_client=model_client,
            description="旅行总结专家，整合各方面建议生成完整旅行计划。",
            system_message="""你是一位旅行总结专家，负责整合各专家的建议，创建全面的旅行计划。
            你可以使用以下工具获取专业建议：
            1. planner_tool - 获取详细的旅行计划建议
            2. local_tool - 获取当地特色活动和景点建议
            3. language_tool - 获取语言和沟通技巧建议

            你的职责是：
            1. 理解用户的旅行需求
            2. 咨询各专家获取专业建议
            3. 整合所有建议，创建一个连贯、全面的旅行计划
            4. 确保计划包含行程安排、特色活动和语言技巧

            你的最终回复必须是一个完整的旅行计划。当计划完成并整合了所有专家建议后，请以"计划已完成"结束。""",

            tools=[planner_tool, local_tool, language_tool],
        )
        
        # 运行旅行规划系统
        print("启动旅行规划系统...\n")
        
        # 用户查询示例
        travel_query = "请为我规划一个去尼泊尔的3天旅行计划，包括必去景点、当地特色活动和语言沟通技巧。"
        print(f"用户查询: {travel_query}\n")

        # 使用旅行总结代理处理查询
        await Console(travel_summary_agent.run_stream(
            task=travel_query, 
            cancellation_token=CancellationToken()
        ))
        
    finally:
        # 关闭模型客户端
        await model_client.close()

if __name__ == "__main__":
    asyncio.run(main())

