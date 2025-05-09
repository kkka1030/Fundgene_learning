import asyncio
import json
import os
import sqlite3
from autogen_agentchat.agents import AssistantAgent, UserProxyAgent
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_agentchat.conditions import TextMentionTermination
from autogen_agentchat.ui import Console
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_ext.tools.mcp import McpWorkbench, StdioServerParams
from autogen_core import CancellationToken

# 首先，让我们直接检查数据库内容，确保数据已正确生成
def check_database(db_path):
    """检查数据库内容，确保数据已正确生成"""
    if not os.path.exists(db_path):
        return False, "数据库文件不存在"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        required_tables = ['products', 'inventory', 'orders', 'order_items', 'customers', 'shipping']
        
        for table in required_tables:
            if table not in table_names:
                return False, f"缺少表: {table}"
        
        # 检查数据是否存在
        data_counts = {}
        for table in required_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            data_counts[table] = count
            if count == 0:
                return False, f"表 {table} 中没有数据"
        
        conn.close()
        return True, f"数据库检查通过，数据统计: {data_counts}"
    
    except Exception as e:
        return False, f"数据库检查失败: {str(e)}"

async def main() -> None:
    """
     🔥 AI超元域平台原创视频
    智能客服系统，使用AutoGen和SQLite MCP Server查询进销存数据库
    功能：
    1. 查询订单状态、库存、物流信息等
    2. 回答客户问题
    
    注意：此脚本假设数据库已经存在，请先运行generate_inventory_data.py创建数据库
    """
    # 数据库文件路径
    db_path = os.path.join(os.getcwd(), "inventory.db")
    
    # 检查数据库是否存在和内容是否正确
    db_ok, db_message = check_database(db_path)
    if not db_ok:
        print(f"错误：{db_message}")
        print("请先运行 python generate_inventory_data.py 创建数据库和示例数据。")
        return
    else:
        print(f"数据库检查: {db_message}")
    
    # 创建MCP服务器参数 - 使用SQLite MCP Server
    # 使用正确的命令来启动SQLite MCP Server
    sqlite_server_params = StdioServerParams(
        command="/Users/charlesqin/Desktop/test-autogen/.venv/bin/mcp-server-sqlite",
        args=["--db-path", db_path],
        read_timeout_seconds=60,
    )
    # 🔥 AI超元域平台原创视频
    # 创建OpenAI模型客户端
    model_client = OpenAIChatCompletionClient(
        model="gpt-4o-mini",  # 使用功能更强大的模型以获得更好的结果
        # API密钥将从环境变量OPENAI_API_KEY自动加载
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
                system_message="""你是一位数据库查询专家，负责查询进销存系统的SQLite数据库。
你的任务是根据客服代表的请求，查询并提供准确的数据库信息。

数据库包含以下表：
1. products(id, name, description, price, category) - 产品信息
2. inventory(product_id, quantity, warehouse_id, last_updated) - 库存信息
3. orders(id, customer_id, order_date, status, total_amount) - 订单信息
4. order_items(order_id, product_id, quantity, unit_price) - 订单项信息
5. customers(id, name, email, phone, address) - 客户信息
6. shipping(id, order_id, carrier, tracking_number, status, estimated_delivery) - 物流信息

请使用SQL查询语言查询数据，并以清晰、专业的方式提供结果。
重要：不要主动发起对话，只回答客服代表的具体查询请求。
如果客服代表请求查询订单状态，你应该查询orders表和shipping表。
如果客服代表请求查询产品库存，你应该查询products表和inventory表。""",
                model_client=model_client,
                workbench=workbench,  # 使用workbench而不是tools
            )
            
            # 创建客服代理，负责回答客户问题
            customer_service = AssistantAgent(
                name="CustomerService",
                system_message="""你是一位专业的客服代表，负责回答客户关于订单、产品和物流的问题。
你的任务是：
1. 理解客户的问题
2. 向数据库代理请求相关信息（提供明确的SQL查询需求）
3. 以友好、专业的方式回答客户问题
4. 提供有用的建议和解决方案

当系统启动时，你应该简短地介绍自己和系统功能，然后等待客户的问题。
不要与数据库代理进行无意义的对话，只有在需要查询数据时才向其发送请求。

例如，如果客户询问订单状态，你应该请求DBAgent查询相关订单信息，例如：
"请查询订单#3的状态和物流信息，使用SQL: SELECT o.id, o.status, s.carrier, s.tracking_number, s.estimated_delivery FROM orders o JOIN shipping s ON o.id = s.order_id WHERE o.id = 3"

你应该使用清晰、礼貌的语言，并确保提供准确的信息。如果你不知道答案，应该诚实地承认并承诺进一步调查。
当完成对话时，请说"CONVERSATION_COMPLETE"。""",
                model_client=model_client,
            )
            
            # 创建终止条件
            termination = TextMentionTermination("CONVERSATION_COMPLETE")
    
            # 创建RoundRobinGroupChat团队
            team = RoundRobinGroupChat(
                participants=[customer_service, db_agent],
                termination_condition=termination,
            )
            # 🔥 AI超元域平台原创视频
            # 测试查询
            print("\n开始测试智能客服系统...")
            
            # 客户查询示例
            customer_queries = [
                # "我想查询一下订单#3的状态和预计送达时间",
                "我需要知道你们有哪些类别的产品，以及每个类别的库存情况",
                # "我的订单#2显示已发货，但我还没收到，能帮我查一下物流信息吗？",
                # "我想修改我的订单#4，能帮我取消吗？",
                # "你们的库存中哪些产品数量不足5件了？"
            ]
            
            # 只运行一个查询示例进行测试
            query = customer_queries[0]
            print(f"\n--- 测试查询 ---\n{query}")
            
            # 直接运行团队对话，使用客户查询作为初始任务
            await Console(team.run_stream(task=query, cancellation_token=CancellationToken()))
    
    finally:
        # 关闭模型客户端资源
        await model_client.close()

if __name__ == "__main__":
    asyncio.run(main())

