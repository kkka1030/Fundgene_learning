import asyncio
import json
import os
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_agentchat.conditions import TextMentionTermination, MaxMessageTermination
from autogen_agentchat.ui import Console
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_ext.tools.mcp import StdioServerParams, mcp_server_tools
from autogen_ext.tools.mcp import McpWorkbench, StdioServerParams
from utils.web_fetch import fetch_text_from_url  
from utils.system_file import file_writer

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

async def news_read(task : str, max_len : int=1000000) -> str:
    news_read_agent = AssistantAgent(
        name="NewsReadAgent",
        model_client=model_client,
        tools=[fetch_text_from_url],
        system_message="""你是一个专业的新闻阅读助手，擅长分析财经新闻对基金投资的影响。
        任务流程：
        1. 使用fetch_text_from_url工具获取输入中提供的每个URL的新闻内容，该工具
        2. 对每篇新闻内容进行概括总结，提取关键信息
        3. 以专业金融分析师的角度分析该新闻对不同类型基金投资的具体影响
        4. 将分析结果按照规定JSON格式返回

        返回格式要求：
        {
            {
                "title": "新闻标题",
                "link": "新闻链接",
                "source": "新闻来源",
                "date": "新闻日期",
                "summary": "新闻内容的简明扼要总结，包含关键事实和数据",
                "impact": "详细分析该新闻对不同类型基金(股票型、债券型、商品型等)投资的潜在影响，包括短期和长期影响"
            },
        }

        注意事项：
        - 确保分析客观专业，基于新闻事实
        - impact部分应包含具体的投资策略建议
        - 当处理完所有新闻后，回复"TERMINATE"以结束对话
        """
    )

    # 创建终止条件
    termination = TextMentionTermination("TERMINATE", sources=["NewsReadAgent"])

    team = RoundRobinGroupChat(
        participants=[news_read_agent],
        termination_condition=termination,   
    )

    # await Console(team.run_stream(task=task))
    news_result = await team.run(task=task)
    news_summary = news_result.messages[-1].content

    return news_summary

if __name__ == "__main__":
    task = """{
        "title": "“对等关税”政策刚满月 美国人从头到脚受打击",
        "link": "https://www.yzwb.net/zncontent/4483456.html",
        "source": "紫牛新闻",
        "date": "05/03/2025, 04:41 AM, +0000 UTC"
    },
    {
        "title": "对华小额包裹免税取消=更高价格+更慢物流，美消费者为关税政策买单",
        "link": "https://www.thepaper.cn/newsDetail_forward_30764073?commTag=true",
        "source": "澎湃新闻",
        "date": "05/03/2025, 03:06 AM, +0000 UTC"
    },"""

    result = asyncio.run(news_read(task))
    print(result)
