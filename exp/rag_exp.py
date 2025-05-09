import re  # 正则表达式库，用于处理文本
from typing import List  # 类型提示库，用于指定列表类型
import asyncio  # 异步 I/O 库
import aiofiles  # 异步文件操作库
import aiohttp  # 异步 HTTP 客户端库
from autogen_core.memory import Memory, MemoryContent, MemoryMimeType  
import os  
from pathlib import Path  # 面向对象的文件系统路径库
from autogen_agentchat.agents import AssistantAgent  
from autogen_agentchat.ui import Console  
from autogen_ext.memory.chromadb import ChromaDBVectorMemory, PersistentChromaDBVectorMemoryConfig  # AutoGen 扩展中的 ChromaDB 向量内存
from autogen_ext.models.openai import OpenAIChatCompletionClient  # AutoGen 扩展中的 OpenAI 聊天模型客户端

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
        'multiple_system_messages': True,  # 模型是否支持多条系统消息
    },
)

# 定义一个简单的文档索引器类
class SimpleDocumentIndexer:
    """用于 AutoGen Memory 的基本文档索引器。
    
    该类负责从指定来源（URL 或本地文件）获取文本内容，
    进行基本的清理（如移除 HTML 标签），将文本分割成块，
    并将这些块添加到 AutoGen 的 Memory 对象中。
    """

    def __init__(self, memory: Memory, chunk_size: int = 1500) -> None:
        """初始化索引器。

        Args:
            memory (Memory): 用于存储文档块的 AutoGen Memory 对象。
            chunk_size (int, optional): 将文本分割成的块的大小（字符数）。默认为 1500。
        """
        self.memory = memory  # 存储传入的 Memory 对象
        self.chunk_size = chunk_size  # 存储块大小

    async def _fetch_content(self, source: str) -> str:
        """异步地从 URL 或本地文件获取文本内容。

        Args:
            source (str): 文档来源，可以是 URL (http/https) 或本地文件路径。

        Returns:
            str: 获取到的文本内容。
        
        Raises:
            aiohttp.ClientError: 如果从 URL 获取内容时发生网络错误。
            IOError: 如果读取本地文件时发生错误。
        """
        if source.startswith(("http://", "https://")):  # 判断来源是否为 URL
            async with aiohttp.ClientSession() as session:  # 创建异步 HTTP 会话
                async with session.get(source) as response:  # 发起 GET 请求
                    response.raise_for_status() # 检查请求是否成功
                    return await response.text()  # 返回响应的文本内容
        else:  # 如果来源不是 URL，则假定为本地文件路径
            async with aiofiles.open(source, "r", encoding="utf-8") as f:  # 异步打开文件
                return await f.read()  # 读取并返回文件内容

    def _strip_html(self, text: str) -> str:
        """移除文本中的 HTML 标签并规范化空白字符。

        Args:
            text (str): 可能包含 HTML 标签的原始文本。

        Returns:
            str: 清理后的纯文本。
        """
        text = re.sub(r"<[^>]*>", " ", text)  # 使用正则表达式移除所有 HTML 标签，替换为空格
        text = re.sub(r"\s+", " ", text)  # 将多个连续的空白字符替换为单个空格
        return text.strip()  # 移除开头和结尾的空白字符

    def _split_text(self, text: str) -> List[str]:
        """将长文本分割成指定大小的文本块列表。

        Args:
            text (str): 需要分割的长文本。

        Returns:
            List[str]: 分割后的文本块列表。
        """
        chunks: list[str] = []  # 初始化一个空列表来存储文本块
        # 按照设定的 chunk_size 步长遍历文本
        for i in range(0, len(text), self.chunk_size):
            chunk = text[i : i + self.chunk_size]  # 切片获取当前块
            chunks.append(chunk.strip())  # 移除块两端的空白并添加到列表中
        return chunks  # 返回包含所有文本块的列表

    async def index_documents(self, sources: List[str]) -> int:
        """将指定来源列表中的文档内容索引到 Memory 中。

        Args:
            sources (List[str]): 包含文档来源（URL 或文件路径）的列表。

        Returns:
            int: 成功索引的文本块总数。
        """
        total_chunks = 0  # 初始化成功索引的块计数器

        for source in sources:  # 遍历每个文档来源
            try:
                content = await self._fetch_content(source)  # 异步获取文档内容

                # 如果内容看起来像 HTML（包含 '<' 和 '>'），则移除 HTML 标签
                if "<" in content and ">" in content:
                    content = self._strip_html(content)

                chunks = self._split_text(content)  # 将获取到的内容分割成块

                # 遍历分割后的每个文本块
                for i, chunk in enumerate(chunks):
                    # 创建 MemoryContent 对象，包含文本块内容、MIME 类型和元数据
                    memory_content = MemoryContent(
                        content=chunk,  # 文本块内容
                        mime_type=MemoryMimeType.TEXT,  # 指定内容类型为纯文本
                        metadata={"source": source, "chunk_index": i}  # 添加元数据，包括来源和块索引
                    )
                    # 将 MemoryContent 对象异步添加到 Memory 中
                    await self.memory.add(memory_content)

                total_chunks += len(chunks)  # 更新成功索引的块总数

            except Exception as e:  # 捕获处理单个文档时可能发生的任何异常
                print(f"索引 {source} 时出错: {str(e)}")  # 打印错误信息

        return total_chunks  # 返回总共索引的块数


# --- 2. 定义主异步函数 ---
async def main():
    """主异步函数，用于设置内存、索引文档和运行 RAG 助手。"""
    # 在 main 函数内部初始化向量内存
    # 使用 ChromaDB 作为向量存储，配置持久化路径和检索参数
    rag_memory = ChromaDBVectorMemory(
        config=PersistentChromaDBVectorMemoryConfig(
            collection_name="autogen_docs",  # 指定 ChromaDB 中的集合名称
            # 设置持久化路径，存储在用户主目录下的 .chromadb_autogen 文件夹
            persistence_path=os.path.join(str(Path.home()), ".chromadb_autogen"),
            k=3,  # 在检索时返回最相似的前 3 个结果
            score_threshold=0.4,  # 设置最低相似度分数阈值，低于此阈值的结果将被忽略
        )
    )

    try: # 使用 try/finally 结构确保无论是否发生异常，内存资源都能被正确关闭
        await rag_memory.clear()  # 清除内存中可能存在的旧数据，确保从干净的状态开始

        # 调用辅助函数索引 AutoGen 文档，将 rag_memory 对象传递给它
        await index_autogen_docs(rag_memory)

        # 创建 RAG (Retrieval-Augmented Generation) 助手代理
        # 该代理将使用配置好的模型客户端和向量内存来回答问题
        rag_assistant = AssistantAgent(
            name="rag_assistant",  # 为代理命名
            model_client=model_client, # 指定用于生成回答的模型客户端 (DeepSeek)
            memory=[rag_memory]  # 将配置好的向量内存传递给代理，用于检索相关信息
        )

        # 向 RAG 助手提问关于 AutoGen 的问题
        # run_stream 方法会启动一个异步任务流来处理问题并生成回答
        stream = rag_assistant.run_stream(
            # task="如果我有一个用户行为数据库，请问我如何通过autogen下的rag和memory技术将这些大量数据喂给llm来分析用户行为？"
            # task="查看所有和autogen相关的文档,将其中和RAG和Memory相关的内容翻译为中文,然后给出详细的使用指南和代码说明和示例。不要无中生有编造内容。"
            task="在autogen中，当我创建的一个team完成给定的任务后，如何获取将这个team的所有成员的对话记录",
        ) # 提出具体问题
        # 使用 Console UI 组件来异步处理并显示代理的输出流
        await Console(stream)

    finally:
        # 无论程序是正常结束还是遇到错误，都确保关闭内存连接
        # 这对于释放资源和确保持久化数据正确写入非常重要
        print("正在关闭内存...")
        await rag_memory.close()
        print("内存已关闭。")


# --- 辅助函数：索引 AutoGen 文档 ---
async def index_autogen_docs(rag_memory: Memory) -> None:
    """异步索引一组指定的 AutoGen 文档 URL 到提供的 Memory 对象中。

    Args:
        rag_memory (Memory): 用于存储索引文档块的 AutoGen Memory 对象。
    """
    # 使用传入的 rag_memory 初始化 SimpleDocumentIndexer
    indexer = SimpleDocumentIndexer(memory=rag_memory)
    # 定义要索引的 AutoGen 文档的 URL 列表
    sources = [
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/migration-guide.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/tutorial/index.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/tutorial/models.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/tutorial/messages.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/tutorial/agents.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/tutorial/teams.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/tutorial/human-in-the-loop.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/tutorial/termination.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/tutorial/state.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/custom-agents.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/selector-group-chat.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/swarm.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/magentic-one.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/memory.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/logging.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/serialize-components.html",
        "https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/tracing.html",
    ]
    # 调用 indexer 的 index_documents 方法开始索引过程
    chunks: int = await indexer.index_documents(sources)
    # 打印索引结果，显示索引的块数和文档数
    print(f"从 {len(sources)} 个 AutoGen 文档中索引了 {chunks} 个块")


# --- 3. 脚本入口点 ---
if __name__ == "__main__":
    # 确保在运行脚本前设置了必要的环境变量
    # 特别是 DEEPSEEK_API_KEY，因为 OpenAIChatCompletionClient 需要它
    print("开始执行 RAG 示例...")
    # 使用 asyncio.run() 来启动并运行主异步函数 main()
    asyncio.run(main())
    print("RAG 示例执行完毕。")
