import asyncio
import json
import os
import sqlite3
import sys
import re
import math
import operator as op
# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.Console_with_history import Console_with_history
from utils.extract_messages_content import extract_messages_content
from utils.calculator_tool import calculator_tool
from autogen_agentchat.agents import AssistantAgent, UserProxyAgent
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_agentchat.conditions import TextMentionTermination
from autogen_agentchat.ui import Console
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_ext.tools.mcp import McpWorkbench, StdioServerParams
from autogen_agentchat.tools import AgentTool
from portfolio_records import portfolio_records

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

async def portfolio_analyze(userid : str) -> str:
    # records_get_tool = asyncio.run(portfolio_records())

    task = f"获取user_id='{userid}'的投资记录"

    records = await portfolio_records(userid)

    print(f"成功获取投资记录数据\n投资记录数据如下：\n{records}\n")

    print("下面开始分析数据...")

    portfolio_analyze_agent = AssistantAgent(
        name="PortfolioAnalyzeAgent",
        model_client=model_client,
        tools= [calculator_tool],
        system_message="""你是一位金融数据分析专家，负责分析用户的投资记录数据并提供投资建议。
        你的任务是根据输入的投资记录数据，计算并给出用户的资产配置比例，然后给出适当建议，返回严格的json格式。
        输入的json格式如下：
        {
            "user_info": {
            "user_id": "用户ID",
            "username": "用户名",
            "risk_tolerance": "风险承受能力",
            "investment_goal": "投资目标",
            "investment_preference": "投资偏好"
            },
            "investment_records": [
            {
                "behavior_id": "行为ID",
                "fund_info": {
                "fund_id": "基金ID",
                "fund_name": "基金名称",
                "fund_code": "基金代码",
                "fund_type": "基金类型",
                "risk_level": "风险等级",
                "current_nav": "当前净值"
                },
                "transaction_info": {
                "action_type": "操作类型",
                "amount": "交易金额",
                "timestamp": "交易时间",
                "nav_price": "交易时净值",
                "fund_shares": "交易份额",
                "platform": "交易平台",
                "transaction_status": "交易状态"
                }
            },
            ]
        }
        输出的json格式如下：
        {
            "资产配置比例": {
                "按资产类型": {
                    "股票": 50,
                    "债券": 30,
                    "现金": 20,
                    "其他": 0
                },
                "按风险等级": {
                    "低风险": 25,
                    "中风险": 45,
                    "高风险": 30
                },
                "按投资平台": {
                    "平台A": 40,
                    "平台B": 60
                },
                "总资产价值": 100000
            },
            "投资表现": {
                "总收益率": 8.5,
                "年化收益率": 6.2,
                "波动率": 12.3
            },
            "建议": {
                "总体建议": "根据用户的风险承受能力，建议适当调整资产配置。",
                "具体操作": [
                    "增加股票投资比例至60%",
                    "减少现金持有比例至10%",
                    "考虑增加指数型基金的配置"
                ],
                "理由": "用户的风险承受能力较高，当前配置过于保守，无法充分利用市场机会。"
            }
        }
        你可以使用以下工具来获取用户的投资记录数据：
         calculator_tool - 你可以使用这个工具来进行数据处理和计算，例如：
           - 计算资产总值
           - 计算各类资产占比百分比
           - 计算投资收益率
           - 执行任何必要的数学运算


        当任务完成后，回复'TERMINATE'以结束会话。
        """,
    )

    # 创建终止条件
    termination = TextMentionTermination("TERMINATE", sources=["PortfolioAnalyzeAgent"])

    # 创建RoundRobinGroupChat团队
    team = RoundRobinGroupChat(
        participants=[portfolio_analyze_agent],
        termination_condition=termination,
    )

    task = f"根据userid='{userid}'的投资记录数据，计算他的资产配置比例和并给出建议，返回为json格式。投资记录数据如下：\n{records}\n" 

    # await Console(team.run_stream(task=task))
    result = await team.run(task=task)

    print(result)
    
    # 提取消息内容
    analyze_result = extract_messages_content(
        result.messages,
        include_sources=["PortfolioAnalyzeAgent"],
        include_types=["json"],
        join_delimiter="\n"
    )

    return analyze_result

if __name__ == "__main__":
    userid = "8c5373a6-f437-41ee-9830-284399af9893"
    analyze_result = asyncio.run(portfolio_analyze(userid))
    print(analyze_result)