from typing import AsyncGenerator, List, Optional, Union
from autogen_agentchat.base import  TaskResult, Response
from autogen_agentchat.messages import (
    BaseAgentEvent,
    BaseChatMessage,
)
from autogen_agentchat.ui import UserInputManager
from autogen_agentchat.ui import Console


async def Console_with_history(
    stream: AsyncGenerator[BaseAgentEvent | BaseChatMessage | Union[TaskResult, Response], None],
    *,
    no_inline_images: bool = False,
    output_stats: bool = False,
    user_input_manager: UserInputManager | None = None,
) -> tuple[Union[TaskResult, Response], List[BaseChatMessage]]:
    """
    包装 Console 函数，增加保留和返回完整聊天记录的功能。

    Args:
        stream: 消息流，来自 run_stream 或 on_messages_stream。
        no_inline_images: 是否禁用图像内联显示。
        output_stats: 是否输出统计信息。
        user_input_manager: 用户输入管理器。

    Returns:
        tuple: (原始返回值, 聊天记录列表)。
    """
    # 用于收集所有消息
    chat_history: List[BaseChatMessage] = []

    # 自定义流，拦截消息并保存
    async def intercepted_stream():
        async for message in stream:
            # 保存聊天消息
            if isinstance(message, (TaskResult, Response)):
                if isinstance(message, TaskResult):
                    chat_history.extend(message.messages)
                elif isinstance(message, Response) and message.inner_messages:
                    chat_history.extend(message.inner_messages)
                elif isinstance(message, Response):
                    chat_history.append(message.chat_message)
            elif isinstance(message, BaseChatMessage):
                chat_history.append(message)
            yield message

    # 调用原始 Console 函数
    result = await Console(
        intercepted_stream(),
        no_inline_images=no_inline_images,
        output_stats=output_stats,
        user_input_manager=user_input_manager,
    )

    return result, chat_history