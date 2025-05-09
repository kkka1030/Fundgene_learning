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

SERP_API_KEY = "b155eb94ad29e4c2cc927910428134add56f644c578cd4eebe86b38649274f7b"  # 这里需要替换为你有效的SERP API密钥

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

async def news_fetch(task : str) -> str:
    
    # 创建MCP服务器参数 - 配置 Google News MCP 服务器
    server_params = StdioServerParams(
        command="npx",
        args=["@chanmeng666/google-news-server"],
        read_timeout_seconds=60,
        env={
                "SERP_API_KEY": SERP_API_KEY,
                "API_KEY": SERP_API_KEY,
                "SERPAPI_KEY": SERP_API_KEY,
        } 
    )   

    # 其余代码保持不变
    try:
        print("启动MCP Workbench与Google News服务器...")
        # 使用异步上下文管理器管理McpWorkbench的生命周期
        async with McpWorkbench(server_params=server_params) as workbench:
            # 检查服务器是否成功连接
            print("MCP Workbench已启动，检查可用工具...")
            tools = await workbench.list_tools()
            print(f"可用工具列表: {tools}")
            
            # 创建新闻获取助手    
            newsfetch_agent = AssistantAgent(
                name="NewsAgent",
                model_client=model_client,
                workbench=workbench,
                system_message="""你是一个专业的新闻助手，负责获取特定新闻信息。

                你可以使用 Google News 工具获取最新新闻。请按照以下步骤操作：

                1. 调用 google_news.search_news 函数来搜索新闻
                2. 参数说明：
                - engine: 必须是 "google_news"（固定值）
                - q: 搜索关键词，例如"关税政策"
                - gl: 国家代码，使用"cn"获取中国相关内容
                - hl: 语言代码，使用"zh-cn"获取中文内容
                - tbm: 搜索类型，使用"nws"获取新闻（固定值）

                示例调用:
                ```
                {
                "engine": "google_news",
                "q": "关税政策",
                "gl": "cn",
                "hl": "zh-cn",
                "tbm": "nws"
                }
                ```

                3. 获取结果后，请分析结果中的"news_results"字段，该字段包含新闻条目列表
                4. 对每条新闻进行整理，提取以下信息：
                - 标题 (title)
                - 链接 (link)
                - 来源 (source)
                - 发布日期 (date)

                5. 将整理好的信息以JSON格式返回：
                {
                    "news": [
                        {
                            "title": "新闻标题1",
                            "link": "新闻链接1",
                            "source": "新闻来源1",
                            "date": "新闻日期1"
                        },
                        {
                            "title": "新闻标题2",
                            "link": "新闻链接2",
                            "source": "新闻来源2",
                            "date": "新闻日期2"
                        }
                    ]
                }

                如果搜索未返回结果，请使用不同的关键词再次尝试。
                成功返回结果后，请回复"TERMINATE"以结束对话。
                """,   
            )

            # 创建终止条件
            termination = TextMentionTermination("TERMINATE", sources=["NewsAgent"]) 
            # 创建RoundRobinGroupChat团队
            team = RoundRobinGroupChat(
                participants=[newsfetch_agent],
                termination_condition=termination,
            )

            print("\n任务开始...")


            news_result = await team.run(task=task)
            news_data = news_result.messages[-1].content
            # print("\n获取的新闻数据:")
            # print(news_data)

            return news_data
            
    finally:
        # 关闭模型客户端资源
        await model_client.close()


if __name__ == "__main__":
    task = f"""查找关于"基金投资"的最新新闻，优先查找中国的相关政策。"""
    result =asyncio.run(news_fetch(task))
    print(result)
