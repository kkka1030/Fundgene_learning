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


async def file_writer(task : str, dir : str) -> None:

    # 设置MCP fetch服务器参数
    server_params = StdioServerParams(
        command="npx", 
        args=["-y", "@modelcontextprotocol/server-filesystem", dir], 
        read_timeout_seconds=10
    )

    # 从MCP服务器获取filesystem工具
    tools_write = await mcp_server_tools(server_params)
    
    try:

        async with McpWorkbench(server_params=server_params) as workbench:
            # 检查服务器是否成功连接
            print("MCP Workbench已启动，检查可用工具...")
            tools = await workbench.list_tools()
            print(f"可用工具列表: {tools}")
            
            # 创建文件写入助手
            file_writer_agent = AssistantAgent(
                name="file_writer",
                model_client=model_client,
                workbench=workbench,
                system_message="""你是一个文件助手，负责将给定内容以json格式写入到本地文件。
                你的任务是使用文件系统工具将内容写入到指定的文件中。文件名根据内容进行概括
                成功写入后，回复"TERMINATE"以结束对话。"""
            )
            
            # 创建终止条件
            termination = TextMentionTermination("TERMINATE", sources=["file_writer"])

            # 创建RoundRobinGroupChat团队
            team = RoundRobinGroupChat(
                participants=[file_writer_agent],
                termination_condition=termination,
            )

            await Console(team.run_stream(task=task))
            print("\n新闻数据已成功写入文件。")
            
    finally:
        # 关闭模型客户端资源
        await model_client.close()

if __name__ == "__main__":
    task =  """
   {
    "news": [
        {
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
        },
        {
            "title": "对华小额包裹免税取消=更高价格+更慢物流 美消费者为关税政策买单",
            "link": "https://www.chinanews.com.cn/gj/2025/05-03/10410003.shtml",
            "source": "chinanews.com.cn",
            "date": "05/03/2025, 02:16 AM, +0000 UTC"
        },
        {
            "title": "关税大棒砸到美发业 美国“头等大事”变“头顶危机”",
            "link": "http://news.cnhubei.com/content/2025-05/03/content_19149890.html",
            "source": "荆楚网",
            "date": "05/03/2025, 01:54 AM, +0000 UTC"
        },
        {
            "title": "特朗普关税满月，美多地爆发抗议！耐克、阿迪达斯等巨头：已构成“生存威胁”，关税将致许多企业不得不倒闭！昨夜，中国资产大爆发",
            "link": "https://news.sina.cn/gj/2025-05-03/detail-inevfwne4083251.d.html",
            "source": "新浪新闻_手机新浪网",
            "date": "05/03/2025, 01:54 AM, +0000 UTC"
        }
    ]
}

TERMINATE
    """
    dir = "/Users/xueyicheng/Documents/SRTP/autogen/autogen_mcp/database/news"
    asyncio.run(file_writer(task, dir))

