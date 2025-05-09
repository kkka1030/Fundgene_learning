import asyncio
import sys
import os

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.system_file import file_writer
from news_fetch import news_fetch
from news_read import news_read

async def news_cognitive(task: str, dir: str) -> None:
    """
    主函数，执行新闻获取和阅读任务。
    """
    # 执行新闻获取任务
    news_data = await news_fetch(task)
    print("\n获取的新闻数据:")
    print(news_data)

    # max_tokens = 60000 # 设置最大token数，deepseek-chat模型的最大token数为65536
    
    # 执行新闻阅读任务
    news_summary = await news_read(news_data)
    print("\n新闻摘要:")
    print(news_summary)
    
    # 将结果写入文件 - 正确调用异步函数
    await file_writer(news_summary, dir)

if __name__ == "__main__":

    task = """查找关于"基金投资"的最新新闻，优先查找和中国相关的。"""
    dir = "/Users/xueyicheng/Documents/SRTP/autogen/autogen_mcp/database/news"

    asyncio.run(news_cognitive(task, dir))
