
# 基金学习推荐智能助手（AI Fund Learning Assistant）

一个支持自然语言对话、关键词理解、知识查找与推荐的基金学习辅助系统，适用于投资新手和进阶者，通过本地知识库实现个性化内容推荐。

**注意**：输入“生成推荐”时，Agent 将根据历史对话记录智能推荐学习资料。

---

## 功能特性

### 多智能体协作
- **advisor_agent**  
  面向投资者的对话助手，处理自然语言交互
- **recommender_agent**  
  上下文关键词智能提取引擎
- **presenter_agent**  
  学习资料匹配与格式化输出模块


### 知识库支持
- 基于 JSON 的本地知识库 (`doc_library.json`)
- 支持多关键词模糊匹配
- 数据字段包含：
  ```text
  source    - 资料来源
  section   - 章节信息
  content   - 正文内容
  ```

---

## 项目结构

```text
基金学习/
├── main.py                 # 主程序入口
├── tools.py                # 推荐算法与格式化工具
├── doc_library.json        # 学习资料数据库
├── .env                    # API 密钥配置文件
└── README.md               # 项目文档
```

---

## 快速开始

### 环境准备
1. 安装依赖：
```bash
pip install pyautogen python-dotenv
```

2. 配置 API 密钥：
```bash
echo "CUSTOM_API_KEY=your_api_key_here" > .env
echo "BASE_URL=your_api_endpoint_here" >> .env
```

### 运行系统
```bash
python main.py
```

---

## 使用示例

```text
>>> 什么是组合投资

[advisor_agent] 专业解析：
组合投资是通过资产配置分散风险的策略...
（输出内容包含定义、核心要素、案例说明）

>>> 生成推荐

[系统] 检测到推荐请求...
关键词提取：资产配置、风险控制、长期心态

===== 推荐资料 =====
1. 《投资心理学》第二章
   简介：多元化投资策略解析
   匹配度：★★★★☆
```

---

## 数据规范

学习资料 JSON 结构示例：
```json
[
  {
    "source": "投资基础知识入门.docx",
    "section": "第四章 什么是债券型基金？",
    "content": "债券型基金主要投资于国债、金融债..."
  }
]
```

---

