# 代码1
import asyncio
import os
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_agentchat.conditions import TextMentionTermination
from autogen_core import CancellationToken
from autogen_ext.tools.mcp import StdioServerParams, mcp_server_tools
from google.search import search

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY") 

model_client = OpenAIChatCompletionClient(model="deepseek-chat", base_url="https://api.deepseek.com",
                                          api_key=DEEPSEEK_API_KEY, model_info={
        "vision": False,
        "function_calling": True,
        "json_output": True,
        "family": "unknown",
    }, )

# 定义Google搜索工具
def search_tool(query: str) -> str:
    """
    使用Google搜索获取相关信息
    
    参数:
        query: 搜索查询字符串
        
    返回:
        格式化的搜索结果
    """
    API_KEY = "AIzaSyDChdVmFOiziWyQKKJfJ1OlK7ntRVuAuVQ"  # 替换为你的API密钥
    SEARCH_ENGINE_ID = "a1e36a35cf02a428c"  # 替换为你的搜索引擎ID
    
    results = google_search(query, API_KEY, SEARCH_ENGINE_ID)
    
    if not results:
        return "未找到相关搜索结果。"
        
    formatted_results = "搜索结果:\n\n"
    for i, result in enumerate(results, 1):
        formatted_results += f"{i}. {result['title']}\n"
        formatted_results += f"   URL: {result['link']}\n"
        formatted_results += f"   {result['snippet']}\n\n"
        
    return formatted_results

async def main():
    # Setup the MCP fetch server parameters
    fetch_mcp_server = StdioServerParams(command="node", args=["mcp-server-fetch"])

    # Get the fetch tool from the MCP server
    tools = await mcp_server_tools(fetch_mcp_server)

    # Create fetch agent with the MCP fetch tool
    fetch_agent = AssistantAgent(
        name="content_fetcher",
        model_client=model_client,
        tools=tools,  # The MCP fetch tool will be included here
        system_message="你是一个网页内容获取助手。使用fetch工具获取网页内容。"
    )

    # Create rewriter Agent (unchanged)
    rewriter_agent = AssistantAgent(
        name="content_rewriter",
        model_client=model_client,
        system_message="""你是一个内容改写专家。将提供给你的网页内容改写为科技资讯风格的文章。
        科技资讯风格特点：
        1. 标题简洁醒目
        2. 开头直接点明主题
        3. 内容客观准确但生动有趣
        4. 使用专业术语但解释清晰
        5. 段落简短，重点突出

        当你完成改写后，回复TERMINATE。"""
    )

    # Set up termination condition and team (unchanged)
    termination = TextMentionTermination("TERMINATE")
    team = RoundRobinGroupChat([fetch_agent, rewriter_agent], termination_condition=termination)

    # Run the workflow (unchanged)
    result = await team.run(
        task="获取https://www.aivi.fyi/llms/introduce-Claude-3.7-Sonnet的内容，然后将其改写为科技资讯风格的文章",
        cancellation_token=CancellationToken()
    )

    print("\n最终改写结果：\n")
    print(result.messages[-1].content)
    return result


# This is the correct way to run async code in a Python script
if __name__ == "__main__":
    asyncio.run(main())
