def extract_messages_content(messages, include_sources=None, include_types=None, join_delimiter="\n"):
    """
    从消息列表中提取内容并根据条件筛选和拼接。
    
    参数:
        messages (list): 消息列表，每个消息是包含 source、content、type 等属性的对象
        include_sources (list, optional): 要包含的消息来源列表，如果为None则包含所有来源
                                         常见的来源有: "user", "DBAgent" 等
        include_types (list, optional): 要包含的消息类型列表，如果为None则包含所有类型
                                       常见的类型有: 
                                       - "TextMessage": 普通文本消息
                                       - "ToolCallRequestEvent": 工具调用请求事件
                                       - "ToolCallExecutionEvent": 工具执行结果事件
                                       - "ToolCallSummaryMessage": 工具调用摘要消息
                                       - "json": 特殊类型，会筛选内容中包含JSON格式的消息
        join_delimiter (str, optional): 拼接消息内容的分隔符，默认为换行符
        
    返回:
        str: 拼接后的消息内容
    """
    filtered_contents = []
    
    for msg in messages:
        # 默认包含所有消息
        include_msg = True
        
        # 根据来源筛选
        if include_sources and not hasattr(msg, 'source'):
            include_msg = False
        elif include_sources and msg.source not in include_sources:
            include_msg = False
            
        # 根据类型筛选
        if include_types:
            # 特殊处理json类型，检查内容是否包含JSON
            if 'json' in include_types and hasattr(msg, 'content'):
                content = msg.content
                if isinstance(content, str):
                    if '```json' in content:
                        # 提取完整的```json...```代码块（包含标记）
                        import re
                        json_blocks = re.findall(r'(```json\s*[\s\S]*?```)', content)
                        if json_blocks:
                            filtered_contents.extend(json_blocks)
                            continue
            
            # 常规类型检查
            if not hasattr(msg, 'type') or msg.type not in include_types:
                include_msg = False
        
        # 如果消息符合条件，添加到结果列表
        if include_msg and hasattr(msg, 'content'):
            # 处理不同类型的content
            if isinstance(msg.content, str):
                filtered_contents.append(msg.content)
            elif isinstance(msg.content, list) and msg.content:
                # 处理包含函数调用信息的内容
                for item in msg.content:
                    if hasattr(item, 'content') and item.content:
                        filtered_contents.append(str(item.content))
    
    # 使用指定的分隔符拼接所有消息内容
    return join_delimiter.join(filtered_contents)
