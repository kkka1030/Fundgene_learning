import json

def recommend_from_json(query_str: str):
    try:
        with open("doc_library.json", "r", encoding="utf-8") as f:
            docs = json.load(f)
    except FileNotFoundError:
        return [{"source": "错误", "section": "", "content": "找不到 doc_library.json 文件"}]
    except json.JSONDecodeError:
        return [{"source": "错误", "section": "", "content": "JSON 文件格式错误"}]

    # 支持多个关键词，用顿号、逗号、空格等分隔
    keywords = [kw.strip() for kw in query_str.replace("，", ",").replace("、", ",").split(",") if kw.strip()]
    
    matched = []
    for doc in docs:
        content = doc.get("content", "")
        if any(kw in content for kw in keywords):
            matched.append({
                "source": doc.get("source", "未知来源"),
                "section": doc.get("section", "未知章节"),
                "content": content[:300]
            })

    return matched[:5] if matched else [{"source": "无结果", "section": "", "content": "没有找到相关学习资料"}]
