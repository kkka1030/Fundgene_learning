import asyncio
import json
import os
import sqlite3
import sys
# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from autogen_agentchat.agents import AssistantAgent, UserProxyAgent
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_agentchat.conditions import TextMentionTermination
from autogen_agentchat.ui import Console
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_ext.tools.mcp import McpWorkbench, StdioServerParams
from autogen_agentchat.tools import AgentTool
from utils.extract_messages_content import extract_messages_content

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

async def behavior_analyze(userid : str) -> str:
    # 数据库文件路径
    db_path = "/Users/xueyicheng/Documents/SRTP/autogen/autogen_mcp/database/behavior/fund_investment.db"
    
    # 创建MCP服务器参数 - 使用SQLite MCP Server
    # 使用正确的命令来启动SQLite MCP Server
    sqlite_server_params = StdioServerParams(
        command="/Users/xueyicheng/Documents/SRTP/autogen/autogen_mcp/venv/bin/mcp-server-sqlite",
        args=["--db-path", db_path],
        read_timeout_seconds=60,
    )

    try:
        print(f"启动SQLite MCP Server... (数据库路径: {db_path})")
        
        # 使用McpWorkbench来与MCP服务器交互
        async with McpWorkbench(server_params=sqlite_server_params) as workbench:
            # 列出可用的工具
            tools = await workbench.list_tools()
            tool_names = [tool["name"] for tool in tools]
            print(f"已加载SQLite MCP工具: {json.dumps(tool_names, indent=2, ensure_ascii=False)}")
            
            # 创建数据库查询代理，负责数据库操作
            db_agent = AssistantAgent(
                name="DBAgent",
                system_message="""你是一位金融数据分析专家，负责调用工具查询基金投资数据库并分析用户的投资行为。

                数据库包含以下表：
                1. users - 用户信息表
                - user_id: 用户唯一标识符(TEXT PRIMARY KEY)
                - username: 用户名(TEXT NOT NULL)
                - age: 用户年龄(INTEGER)
                - gender: 性别(TEXT，'男'/'女')
                - risk_tolerance: 风险承受能力(TEXT，'低'/'中低'/'中'/'中高'/'高')
                - investment_goal: 投资目标(TEXT，如'退休规划'/'子女教育'/'购房'/'创业'/'旅游'/'财富增长')
                - annual_income: 年收入(REAL)
                - registration_date: 注册日期(TEXT，YYYY-MM-DD格式)
                - phone: 电话号码(TEXT)
                - email: 电子邮件(TEXT)
                - account_balance: 账户余额(REAL，单位:元)
                - investment_preference: 投资偏好(TEXT，如'价值型'/'成长型'/'收入型'/'平衡型'/'激进型'/'保守型')
                - last_login_date: 最后登录日期(TEXT，YYYY-MM-DD格式)

                2. funds - 基金信息表
                - fund_id: 基金唯一标识符(TEXT PRIMARY KEY)
                - fund_name: 基金名称(TEXT NOT NULL)
                - fund_code: 基金代码(TEXT UNIQUE)
                - fund_type: 基金类型(TEXT，如'股票型'/'债券型'/'混合型'/'指数型'/'ETF'/'货币市场型'/'QDII')
                - risk_level: 风险等级(TEXT，'低'/'中低'/'中'/'中高'/'高')
                - management_fee: 管理费率(REAL，百分比)
                - annual_return_rate: 年化收益率(REAL，百分比)
                - inception_date: 成立日期(TEXT，YYYY-MM-DD格式)
                - fund_size: 基金规模(REAL，单位:元)
                - fund_manager: 基金经理(TEXT)
                - current_nav: 当前净值(REAL)
                - accumulative_nav: 累计净值(REAL)
                - benchmark: 业绩比较基准(TEXT)
                - investment_strategy: 投资策略(TEXT，如'价值投资'/'成长投资'/'指数增强'/'量化投资'/'主题投资'等)
                - top_holdings: 主要持仓(TEXT)
                - dividend_history: 分红历史(TEXT)
                - subscription_fee: 申购费率(REAL，百分比)
                - redemption_fee: 赎回费率(REAL，百分比)
                - min_subscription_amount: 最低申购金额(REAL，单位:元)
                - custodian_bank: 托管银行(TEXT)
                - update_date: 数据更新日期(TEXT，YYYY-MM-DD格式)

                3. investment_behaviors - 投资行为记录表
                - behavior_id: 行为唯一标识符(TEXT PRIMARY KEY)
                - user_id: 用户ID(TEXT NOT NULL，外键关联users表)
                - fund_id: 基金ID(TEXT NOT NULL，外键关联funds表)
                - action_type: 操作类型(TEXT NOT NULL，'买入'/'卖出'/'定投'/'分红再投'/'转换')
                - amount: 交易金额(REAL，单位:元)
                - timestamp: 交易时间戳(TEXT，YYYY-MM-DD HH:MM:SS格式)
                - holding_period: 持有期限(INTEGER，单位:天，仅适用于卖出操作)
                - return_rate: 收益率(REAL，百分比，仅适用于卖出操作)
                - platform: 交易平台(TEXT，如'银行APP'/'基金公司官网'/'支付宝'/'微信'/'券商APP'/'第三方理财平台')
                - notes: 备注信息(TEXT)
                - nav_price: 交易时基金净值(REAL)
                - fund_shares: 交易份额(REAL)
                - transaction_status: 交易状态(TEXT，如'已确认'/'处理中'/'已完成'/'已撤销')
                - transaction_fee: 交易费用(REAL，单位:元)

                表之间的关系：
                - investment_behaviors.user_id 关联 users.user_id
                - investment_behaviors.fund_id 关联 funds.fund_id

                你的任务是通过SQL查询分析特定用户的投资行为模式，需要分析的内容包括：
                1. 用户的基本信息和投资偏好
                2. 用户的投资行为统计（买入/卖出/定投次数和金额）
                3. 用户偏好的基金类型和风险级别
                4. 用户的投资收益情况
                5. 用户的投资时间模式（如定投频率、市场时机把握等）
                6. 用户的交易平台偏好
                7. 基于行为的投资风格分析
                8. 用户投资组合的资产配置情况
                9. 用户的市场择时能力评估
                10. 风险调整后的收益表现分析

                请使用精确的SQL查询语言分析数据，并以清晰、专业的方式提供结果，同时给出有深度的分析解读和投资建议。
                使用适当的统计方法处理分析数据，如均值、中位数、标准差等，以提供全面的投资行为分析。
                根据分析结果，对用户的投资行为进行分类，若与原来user表中的investment_preference不同就进行更新, 
                分析完成后，回复'TERMINATE'以结束会话。
                """,
                model_client=model_client,
                workbench=workbench,  
            )
            
            # 创建终止条件
            termination = TextMentionTermination("TERMINATE", sources=["DBAgent"])
            
            # 创建RoundRobinGroupChat团队
            team = RoundRobinGroupChat(
                participants=[db_agent],
                termination_condition=termination,
            )
            
            # 开始对话
            # print("\n开始基金投资行为分析...")
            # await Console(team.run_stream(task=f"分析user_id='{userid}'的投资行为"))

            result = await team.run(task=f"分析user_id='{userid}'的投资行为")

            print(result)

            # 提取分析结果
            analyze_result = extract_messages_content(
                result.messages,
                include_sources=["DBAgent"],
                include_types=["TextMessage"],
                join_delimiter="\n"
            )
            return analyze_result
    
    finally:
        # 关闭模型客户端资源
        await model_client.close()

if __name__ == "__main__":
    userid = "8c5373a6-f437-41ee-9830-284399af9893"
    analyze_result=asyncio.run(behavior_analyze(userid))
    print(analyze_result)

