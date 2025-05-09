import asyncio
import os
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_agentchat.conditions import TextMentionTermination
from autogen_core import CancellationToken
from autogen_ext.tools.mcp import StdioServerParams, mcp_server_tools
from datetime import datetime
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

async def main():

    # 设置MCP fetch服务器参数
    # 这个服务器用于获取网页内容
    fetch_mcp_server = StdioServerParams(command="uvx", args=["mcp-server-fetch"], read_timeout_seconds=10)

    # 设置MCP文件系统服务器参数
    # 这个服务器用于写入本地文件
    write_mcp_server = StdioServerParams(command="npx", args=["-y", "@modelcontextprotocol/server-filesystem", "/Users/xueyicheng/Documents/SRTP/autogen/autogen_mcp/database"], read_timeout_seconds=10)
    
    # 从MCP服务器获取fetch工具
    tools_fetch = await mcp_server_tools(fetch_mcp_server)

    # 从MCP服务器获取filesystem工具
    tools_write = await mcp_server_tools(write_mcp_server)
    
    # 创建内容获取代理
    # 这个代理负责获取网页内容
    fetch_agent = AssistantAgent(
        name="content_fetcher",
        model_client=model_client,
        tools=tools_fetch,
        system_message="你是一个网页内容获取助手。使用fetch工具获取网页内容，不要遗漏这个页面的内容。获取成功后请传递给rewriter_agent。"
    )
    
    # 创建内容改写代理
    # 注意：不再在完成时添加TERMINATE，而是将内容传递给下一个代理    
    rewriter_agent = AssistantAgent(
        name="content_rewriter",
        model_client=model_client,
        system_message="""你是一个内容改写专家。将提供给你的网页内容改写为用户使用指南。
        当你完成改写后，请将内容传递给writer_agent，让它将你的改写内容写入到文件中。"""
    )
    
    # 获取当前日期并格式化为YYYY-MM-DD
    current_date = datetime.now().strftime('%Y-%m-%d')

    # 创建文件写入代理
    # 这个代理负责将改写后的内容写入本地文件
    # 注意：这个代理会在完成任务后添加TERMINATE来结束对话
    write_agent = AssistantAgent(
        name="content_writer",
        model_client=model_client,
        tools=tools_write,
        system_message="""你是一个文件助手。使用filesystem工具将rewriter_agent提供的内容写入.md文件，文件以日期命名（格式为{current_date}.md）。
        当你成功将文件写入后，回复"TERMINATE"以结束对话。"""
    )
    
    # 设置终止条件和团队
    # 当任何代理回复TERMINATE时，对话将结束
    termination = TextMentionTermination("TERMINATE")
    team = RoundRobinGroupChat([fetch_agent, rewriter_agent, write_agent], termination_condition=termination)
    
    task = '''获取https://microsoft.github.io/autogen/stable//user-guide/agentchat-user-guide/selector-group-chat.html,的内容
            然后将其改写为中文版的.md文件，关键字仍保留为英文，不做删改和内容无中生有，不要遗漏内容，保存文件名为{current_date}.md。'''
    
    # 只执行一次任务，使用run方法
    await team.run(task=task, cancellation_token=CancellationToken())

# 在Python脚本中运行异步代码的正确方式
if __name__ == "__main__":
    asyncio.run(main())