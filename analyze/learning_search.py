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
    api_key="DEEPSEEK_API_KEY", 
    model_info={
        "vision": False,
        "function_calling": True,
        "json_output": True,
        "family": "unknown",
    }
)





async def behavior_analyze(query: str) -> str:
    # 数据库文件路径
    db_path = "/Users/xueyicheng/Documents/SRTP/autogen/autogen_mcp/database/learning/docs/learning_docs.db"
    #db_path = "C:\\Users\\31019\\Desktop\\fundgene_autogen-main\\database\\learning\\docs\\learning_docs.db"  
    
    # 创建MCP服务器参数 - 使用SQLite MCP Server
    # 使用正确的命令来启动SQLite MCP Server
    
    sqlite_server_params = StdioServerParams(
        command="/Users/xueyicheng/Documents/SRTP/autogen/autogen_mcp/venv/bin/mcp-server-sqlite",
        args=["--db-path", db_path],
        read_timeout_seconds=60,
    )
    
    #sqlite_server_params = StdioServerParams(
    #    command=sys.executable,
    #    args=["--db-path", db_path],
    #    read_timeout_seconds=60,
#)


    try:
        print(f"启动SQLite MCP Server... (数据库路径: {db_path})")
        
        # 使用McpWorkbench来与MCP服务器交互
        async with McpWorkbench(server_params=sqlite_server_params) as workbench:
            # 列出可用的工具
            print("MCP Workbench 启动成功，正在列出工具...")
            try:
                tools = await workbench.list_tools()
                tool_names = [tool["name"] for tool in tools]
                print(f"已加载SQLite MCP工具: {json.dumps(tool_names, indent=2, ensure_ascii=False)}")
            except Exception as e:
                print("MCP 工具加载失败，异常如下：")
                print(e)

            # 创建数据库查询代理，负责数据库操作
            db_agent = AssistantAgent(
                name="LearningSearchAgent",
                system_message="""
                你是一位金融学习助理，专职帮助用户从学习资料数据库中检索他们想要了解的金融知识。

                数据库包含以下表：
                1. documents - 学习文档内容表
                - id: 主键
                - source: 原始文档名称（TEXT）
                - section: 内容所属章节/小节（TEXT）
                - content: 具体内容段落（TEXT）

                用户将输入一个或多个关键词（或一句话），你需要执行如下任务：
                1. 将用户问题转化为关键词或主题，执行模糊匹配或全文搜索（可使用LIKE、MATCH、或FTS）。
                2. 精准筛选出相关的学习段落（content），并展示其所属章节（section）和文档名称（source）。
                3. 汇总并解释返回结果，必要时对相关内容进行整合或总结。
                4. 若匹配结果过多，仅展示最相关的前5条，并提示用户可进一步细化问题。
                5. 不要虚构内容，只能基于数据库中的实际资料回答。
                6. 不参与推理性金融分析、资产配置建议、个性化财务建议，仅限知识解释和学习辅助。
                7. 所有回答语言默认为中文，除非用户另有指定。
                8. 如果无法匹配任何资料，请礼貌提示用户，并建议修改查询关键词。

                你只能查询并解释数据库中已有的内容，禁止发挥、推测或提供数据库中没有的知识。

                终端响应以 "TERMINATE" 结束一次任务。
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
            print("\n开始学习资料匹配...")
            # await Console(team.run_stream(task=f"请根据用户输入的关键词 '{query}'，在数据库中查找所有相关的金融学习资料，并进行解释说明。"))

            result = await team.run(task=f"请根据用户输入的关键词 '{query}'，在数据库中查找所有相关的金融学习资料，并进行解释说明。")

            print(result)

            # 提取分析结果
            analyze_result = extract_messages_content(
                result.messages,
                include_sources=["LearningSearchAgent"],
                include_types=["TextMessage"],
                join_delimiter="\n"
            )
            return analyze_result
    
    finally:
        # 关闭模型客户端资源
        await model_client.close()

if __name__ == "__main__":
    query = input("请输入你要查询的金融知识关键词：")
    analyze_result=asyncio.run(behavior_analyze(query))
    print(analyze_result)

