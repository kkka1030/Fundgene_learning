import asyncio
import os
from autogen_agentchat.agents import AssistantAgent, UserProxyAgent
from autogen_agentchat.teams import Swarm, RoundRobinGroupChat, SelectorGroupChat
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_agentchat.messages import HandoffMessage
from autogen_agentchat.conditions import HandoffTermination, MaxMessageTermination, TextMentionTermination
from autogen_agentchat.ui import Console
from autogen_ext.tools.mcp import StdioServerParams, mcp_server_tools
from autogen_ext.agents.web_surfer import MultimodalWebSurfer
from duckduckgo_search import DDGS  
from datetime import datetime

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

def search_web_tool(query: str) -> str:
    """
    Performs a web search using DuckDuckGo and returns the results as a string.
    Args:
        query: The search query string.
    Returns:
        A string containing the search results, or an empty string if no results are found.
    """
    try:
        with DDGS() as ddgs:
            results = [r['body'] for r in ddgs.text(query, max_results=5)]
            return "\n".join(results) if results else "No results found."
    except Exception as e:
        print(f"Error during web search: {e}")
        return "Error performing search."
    
def get_current_time_tool() -> str:
    """
    Returns the current date and time as a string.
    Returns:
        A string containing the current date and time.
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

async def get_tools():
    tools: dict = {}

    # 这个服务器用于获取网页内容
    fetch_mcp_server = StdioServerParams(
        command="uvx", 
        args=["mcp-server-fetch"], 
        read_timeout_seconds=10
    )

    # 这个服务器用于读写本地文件
    filesystem_mcp_server = StdioServerParams(
        command="npx", 
        args=["-y", "@modelcontextprotocol/server-filesystem", "/Users/xueyicheng/Documents/SRTP/autogen/autogen_mcp"], 
        read_timeout_seconds=10
    )

    # 这个服务器用于浏览器自动化（截图、网络交互等）
    puppeteer_mcp_server = StdioServerParams(
        command="npx", 
        args=["-y", "@modelcontextprotocol/server-puppeteer"],
        read_timeout_seconds=20
    )

    # 从MCP服务器获取fetch工具
    tools_fetch = await mcp_server_tools(fetch_mcp_server)
    tools["fetch"] = tools_fetch

    # 从MCP服务器获取filesystem工具
    tools_filesystem = await mcp_server_tools(filesystem_mcp_server)
    tools["filesystem"] = tools_filesystem

    # 从MCP服务器获取puppeteer工具
    tools_puppeteer = await mcp_server_tools(puppeteer_mcp_server)
    tools["puppeteer"] = tools_puppeteer

    return tools

async def web_search_tool(task: str) -> str:
    # 获取工具
    tools = await get_tools()

    # 创建任务规划代理
    planning_agent = AssistantAgent(
        name='Planning_Agent',
        description='The first agent to engage with the given task. It is responsible for breaking down the task into subtasks and assigning them to other agents.',
        # tools=[get_current_time_tool],
        model_client=model_client,
        # tools=[get_current_time_tool],
        system_message='''
        你是一个严谨的规划专家，负责拆解总任务并协调团队工作。请遵循以下流程：
        1. 根据给定的总任务明确将需求目标拆解为子任务
        2. 你管理着以下agents:
            - Web_Search_Agent: 负责网络搜索
            - File_Writer_Agent: 负责将信息转化成用户需求的格式并写入文件
        3. 严格按照格式委派子任务：

        Task_<task number>:
        <agent> : <subtask>

        示例：
        Task_2:
        Web_Search_Agent : 获取2020-2024年中国基金市场规模数据

        4. 监督子任务进度，当且仅当所有子任务被其他助手完成时，总结所有工作，然后回复“任务完成!”，
        5. 使用中文

        
        '''
        # 6. 你可以调用get_current_time_tool获取当前日期和时间
    )

    # 创建网络搜索代理
    web_search_agent = AssistantAgent(
        name='Web_Search_Agent',
        description='网络搜索助手',
        tools=[search_web_tool],
        model_client=model_client,
        system_message='''
        你是一个专业的信息检索专家，请严格遵循：
        1. 每次搜索前验证查询关键词的准确性
        2. 使用布尔搜索技巧（如 "中国基金市场" AND "规模" site:gov.cn）
        3. 对搜索结果进行可信度评估：
        - 优先政府网站(.gov)、学术机构(.edu)
        - 排除论坛类结果
        4. 输出格式：
        【搜索结果】
        • [来源] 标题
        摘要（前200字）
        相关度评分：★☆☆☆ (1-5星)
        '''
    )

    # 创建文件读写代理
    file_writer_agent = AssistantAgent(
        name='File_Writer_Agent',
        description='文件助手',
        tools=tools["filesystem"],
        model_client=model_client,
        system_message='''
        你是一个文件助手，使用中文，负责将信息转化成用户需求的格式（默认为MarkDown格式）并写入文件。
        你有一个文件系统工具 - filesystem, 用它写入文件，
        你根据要写入的文件内容为文件命名，
        文件名格式为<title>.<file_type>
        '''
    )

    # 创建用户代理
    user= UserProxyAgent(
        name='User',
        description='用户，它必须作为第一个发言者，负责给出需求目标。',
    )

    text_mention_termination = TextMentionTermination("任务完成!")
    max_messages_termination = MaxMessageTermination(max_messages=25)
    termination = text_mention_termination | max_messages_termination

    selector_prompt = """Select an agent to perform task.

    {roles}

    Current conversation context:
    {history}

    Read the above conversation, then select an agent from {participants} to perform the next task.
    Make sure the planner agent has assigned tasks before other agents start working.
    Only select one agent.

    """
    
    team = SelectorGroupChat(
        participants=[planning_agent, web_search_agent, file_writer_agent],
        model_client=model_client,
        termination_condition=termination,
        selector_prompt=selector_prompt,
        allow_repeated_speaker=True,  # Allow an agent to speak multiple turns in a row.
    )
    await Console(team.run_stream(task=task))
    # result = await team.run(task=task)
    result = '1'

    return result

if __name__ == "__main__":
    task = "查询最受欢迎的中国景点，不用输出为文档"
    result = asyncio.run(web_search_tool(task))
    print(result)



