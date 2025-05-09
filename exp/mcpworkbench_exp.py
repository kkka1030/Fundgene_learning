import asyncio
import json
import os
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_agentchat.conditions import TextMentionTermination
from autogen_agentchat.ui import Console
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_ext.tools.mcp import McpWorkbench, StdioServerParams

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
    """
    实现两个agent轮询交互的脚本:
    1. 研究员agent使用workbench调用MCP fetch服务器获取URL内容
    2. 作家agent将内容改写为科技资讯风格的博客文章
    """
    # 创建MCP服务器参数
    server_params = StdioServerParams(
        command="python",
        args=["-m", "mcp_server_fetch"],
        read_timeout_seconds=60,
    )   

    try:
        print("启动MCP Workbench与fetch服务器...")
        # 使用异步上下文管理器管理McpWorkbench的生命周期
        async with McpWorkbench(server_params=server_params) as workbench:
            # 定义使用MCP fetch工具获取网页内容的函数
            
            async def fetch_web_content(url: str, max_length: int = 5000) -> str:
                """使用MCP fetch工具获取URL内容"""
                print(f"获取URL内容: {url}")
                result = await workbench.call_tool(
                    "fetch", 
                    {
                        "url": url,
                        "max_length": max_length,
                        "start_index": 0,
                    }
                )
                return str(result.result)

            # 创建研究员agent，负责获取和分析网页内容
            researcher = AssistantAgent(
                name="Researcher",  
                system_message="""你是一位网络研究员，负责获取和分析网页内容。
                你的任务是使用工具获取指定URL的内容，然后对内容进行初步分析和整理。
                提取最重要的信息，并将其组织成结构化的形式，以便作家可以将其改写为博客文章。
                确保包含所有关键信息，如技术细节、特点、优势等。

                当你完成分析后，请将结果提供给科技作家，让他将其改写为科技资讯风格的博客文章。""",
                model_client=model_client,
                # tools=[fetch_web_content],
                workbench=workbench,
            )

            # 创建作家agent，负责将内容改写为科技资讯风格的博客文章
            writer = AssistantAgent(
                name="TechWriter",  
                system_message="""你是一位专业的科技博客作家，擅长将技术内容转化为引人入胜的科技资讯文章。
                你的任务是将研究员提供的内容改写为一篇科技资讯风格的博客文章。
                文章应该具有以下特点：
                1. 引人入胜的标题和开头，吸引读者注意
                2. 清晰的结构和流畅的叙述
                3. 技术内容准确但表达通俗易懂
                4. 突出技术的创新点和实际应用价值
                5. 适当使用小标题、列表等格式增强可读性
                6. 结尾部分提供前景展望或总结观点

                保持专业但不晦涩，确保非技术读者也能理解文章的主要内容。

                当你完成博客文章后，请说"TERMINATE"以结束对话。""",
                model_client=model_client,
            )

            # 创建终止条件
            termination = TextMentionTermination("TERMINATE")

            # 创建RoundRobinGroupChat团队
            team = RoundRobinGroupChat(
                participants=[researcher, writer],
                termination_condition=termination,
            )

            # 启动对话
            url = "https://www.voachinese.com/a/8011518.html"
            initial_task = f"""请Researcher获取并分析这个URL的内容: {url}，然后TechWriter将其改写为一篇科技资讯风格的博客文章。"""
            
            print("\n开始代理之间的对话...")

            # 使用Console UI显示对话过程
            await Console(team.run_stream(task=initial_task))
    
    finally:
        # 关闭模型客户端资源
        await model_client.close()

if __name__ == "__main__":
    asyncio.run(main())

