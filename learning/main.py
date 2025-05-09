from autogen import AssistantAgent, UserProxyAgent, GroupChat, GroupChatManager
from dotenv import load_dotenv
import os
from tools import recommend_from_json

load_dotenv()

# API 配置
api_key = os.getenv("CUSTOM_API_KEY")
base_url = os.getenv("BASE_URL")

# 用户代理
user_proxy = UserProxyAgent(
    name="user",
    human_input_mode="ALWAYS",
    code_execution_config=False,
    system_message="你是一个投资者，正在咨询基金投资建议，并希望获取相关学习资料。"
)

# 投资顾问 Agent
advisor_agent = AssistantAgent(
    name="advisor_agent",
    llm_config={
        "model": "gpt-4",
        "temperature": 0.6,
        "api_key": api_key,
        "base_url": base_url,
        "api_type": "openai"
    },
    system_message="你是一个专业基金投资顾问，擅长基金分析、资产配置与心理引导。"
)

# 推荐关键词提取 Agent
recommender_agent = AssistantAgent(
    name="recommender_agent",
    llm_config={
        "model": "gpt-4",
        "temperature": 0.4,
        "api_key": api_key,
        "base_url": base_url,
        "api_type": "openai"
    },
    code_execution_config={"use_docker": False, "work_dir": "."},
    system_message=(
        "你是一个关键词提取助手。\n"
        "当用户说“生成推荐”时，你需要从对话历史中提取出用户关注和需要学习的一些主题关键词。\n"
        "只需要输出关键词，例如：指数基金、风险控制、资产配置等。不要添加解释说明。"
    )
)

# 推荐内容展示 Agent
presenter_agent = AssistantAgent(
    name="presenter_agent",
    llm_config={
        "model": "gpt-4",
        "temperature": 0.5,
        "api_key": api_key,
        "base_url": base_url,
        "api_type": "openai"
    },
    
    system_message=(
        "你是一个学习资料展示助手。\n"
        "你会收到一个学习资料列表（标题 + 内容）。请从中选择最合适的 3 个进行展示（最好是不同来源的三个），输出格式如下：\n"
        "1. 来源：...\n   简介：...\n   推荐理由：...\n\n"
        "理由应贴合用户可能的兴趣或背景，例如新手友好、投资策略实用、心理建设等。"
    )
)

# 群聊设置（不包含 presenter，展示部分手动调用）
groupchat = GroupChat(
    agents=[user_proxy, advisor_agent, recommender_agent],
    messages=[],
    max_round=20
)

manager = GroupChatManager(
    groupchat=groupchat,
    llm_config={
        "model": "gpt-4",
        "temperature": 0.5,
        "api_key": api_key,
        "base_url": base_url,
        "api_type": "openai"
    }
)

def format_docs_for_agent(docs: list) -> str:
    formatted = []
    for i, doc in enumerate(docs, 1):
        formatted.append(
            f"{i}. 来源：{doc['source']}\n   章节：{doc['section']}\n   内容：{doc['content']}"
        )
    return "\n\n".join(formatted)

if __name__ == "__main__":
    print("系统启动。输入问题或“生成推荐”以获取个性化学习资料推荐。\n")

    messages = []
    for round_idx in range(20):  # 最多 20 轮对话
        user_msg = user_proxy.get_human_input(prompt="你想了解什么内容？\n>>> ")
        messages.append({"role": "user", "content": user_msg})
        if "生成推荐" in user_msg:
            print("\n[系统] 检测到推荐请求，正在提取关键词...\n")

            # 获取关键词（返回的是字符串，不是字典）
            keyword = recommender_agent.generate_reply(messages=messages).strip()
            print(f"[系统] 提取关键词：{keyword}")

            # 数据查找
            print("\n[系统] 查询资料中...\n")
            docs = recommend_from_json(keyword)

            # 推荐展示
            formatted_docs = format_docs_for_agent(docs)

            presenter_prompt = (
                "以下是我从学习资料数据库中为你筛选出的相关条目，请从中挑选出最合适的3个资料进行推荐展示（最好是不同来源的），"
                "每个推荐包含标题（source + section）、简介（content 摘要）、推荐理由。\n\n"
            + formatted_docs
            )

            presenter_input = [{"role": "user", "content": presenter_prompt}]
            presenter_reply = presenter_agent.generate_reply(messages=presenter_input)



            print("\n===== 推荐学习资料如下 =====\n")
            print(presenter_reply)
            break

        

        
        # 正常对话：此处默认由 advisor_agent 回复
        reply_msg = advisor_agent.generate_reply(messages=messages)
        print(f"\n{advisor_agent.name}: {reply_msg}\n")
        messages.append({"role": "assistant", "name": advisor_agent.name, "content": reply_msg})

        if user_msg.strip().lower() in ["退出", "quit", "q"]:
            print("聊天结束。")
            break
