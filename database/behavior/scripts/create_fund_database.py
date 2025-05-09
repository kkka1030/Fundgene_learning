import sqlite3
import pandas as pd
import os
import json
import random
from datetime import datetime, timedelta
import uuid

# 确保当前工作目录是脚本所在目录
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# 创建SQLite数据库连接
db_path = 'fund_investment.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 创建用户表
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    age INTEGER,
    gender TEXT,
    risk_tolerance TEXT,
    investment_goal TEXT,
    annual_income REAL,
    registration_date TEXT
)
''')

# 创建基金表
cursor.execute('''
CREATE TABLE IF NOT EXISTS funds (
    fund_id TEXT PRIMARY KEY,
    fund_name TEXT NOT NULL,
    fund_type TEXT,
    risk_level TEXT,
    management_fee REAL,
    annual_return_rate REAL,
    inception_date TEXT,
    fund_size REAL,
    fund_manager TEXT
)
''')

# 创建用户投资行为表
cursor.execute('''
CREATE TABLE IF NOT EXISTS investment_behaviors (
    behavior_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    fund_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    amount REAL,
    timestamp TEXT,
    holding_period INTEGER,
    return_rate REAL,
    platform TEXT,
    notes TEXT,
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (fund_id) REFERENCES funds (fund_id)
)
''')

# 生成虚拟用户数据
def generate_users(n=50):
    users = []
    risk_levels = ['低', '中低', '中', '中高', '高']
    goals = ['退休规划', '子女教育', '购房', '创业', '旅游', '财富增长']
    
    for i in range(n):
        user_id = str(uuid.uuid4())
        username = f"用户_{i+1}"
        age = random.randint(18, 65)
        gender = random.choice(['男', '女'])
        risk_tolerance = random.choice(risk_levels)
        investment_goal = random.choice(goals)
        annual_income = round(random.uniform(50000, 500000), 2)
        
        # 注册日期：过去1-5年内的随机日期
        days_ago = random.randint(365, 365*5)
        registration_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        users.append((
            user_id, username, age, gender, risk_tolerance, 
            investment_goal, annual_income, registration_date
        ))
    
    return users

# 生成虚拟基金数据
def generate_funds(n=30):
    funds = []
    fund_types = ['股票型', '债券型', '混合型', '指数型', 'ETF', '货币市场型', 'QDII']
    risk_levels = ['低', '中低', '中', '中高', '高']
    fund_managers = ['王经理', '李经理', '张经理', '赵经理', '周经理', '吴经理']
    
    for i in range(n):
        fund_id = str(uuid.uuid4())
        fund_name = f"基金_{i+1}号"
        fund_type = random.choice(fund_types)
        risk_level = random.choice(risk_levels)
        management_fee = round(random.uniform(0.5, 2.0), 2)
        annual_return_rate = round(random.uniform(-5.0, 25.0), 2)
        
        # 成立日期：过去1-10年内的随机日期
        days_ago = random.randint(365, 365*10)
        inception_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        fund_size = round(random.uniform(1000000, 10000000000), 2)
        fund_manager = random.choice(fund_managers)
        
        funds.append((
            fund_id, fund_name, fund_type, risk_level, management_fee,
            annual_return_rate, inception_date, fund_size, fund_manager
        ))
    
    return funds

# 生成虚拟投资行为数据
def generate_behaviors(users, funds, n=200):
    behaviors = []
    action_types = ['买入', '卖出', '定投', '分红再投', '转换']
    platforms = ['银行APP', '基金公司官网', '支付宝', '微信', '券商APP', '第三方理财平台']
    
    for i in range(n):
        behavior_id = str(uuid.uuid4())
        user_id = random.choice(users)[0]
        fund_id = random.choice(funds)[0]
        action_type = random.choice(action_types)
        amount = round(random.uniform(1000, 100000), 2)
        
        # 交易时间：过去1-3年内的随机时间
        days_ago = random.randint(1, 365*3)
        hours = random.randint(9, 15)
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)
        timestamp = (datetime.now() - timedelta(days=days_ago)).replace(hour=hours, minute=minutes, second=seconds).strftime('%Y-%m-%d %H:%M:%S')
        
        holding_period = random.randint(1, 1000) if action_type == '卖出' else None
        return_rate = round(random.uniform(-10.0, 30.0), 2) if action_type == '卖出' else None
        platform = random.choice(platforms)
        notes = f"用户{action_type}基金" if random.random() > 0.7 else None
        
        behaviors.append((
            behavior_id, user_id, fund_id, action_type, amount,
            timestamp, holding_period, return_rate, platform, notes
        ))
    
    return behaviors

# 插入虚拟数据到表中
users_data = generate_users(50)
funds_data = generate_funds(30)

cursor.executemany(
    'INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
    users_data
)

cursor.executemany(
    'INSERT INTO funds VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
    funds_data
)

behaviors_data = generate_behaviors(users_data, funds_data, 200)
cursor.executemany(
    'INSERT INTO investment_behaviors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    behaviors_data
)

# 提交更改并关闭连接
conn.commit()
print(f"数据库创建成功: {db_path}")
print(f"用户数量: {len(users_data)}")
print(f"基金数量: {len(funds_data)}")
print(f"投资行为记录数量: {len(behaviors_data)}")

# 导出一些示例查询结果
# 1. 查询每个用户的总投资金额
print("\n查询每个用户的总投资金额...")
user_investments = pd.read_sql('''
SELECT u.username, SUM(CASE WHEN ib.action_type = '买入' OR ib.action_type = '定投' OR ib.action_type = '分红再投' 
                             THEN ib.amount ELSE 0 END) as total_investment
FROM users u
LEFT JOIN investment_behaviors ib ON u.user_id = ib.user_id
GROUP BY u.username
ORDER BY total_investment DESC
LIMIT 10
''', conn)
print(user_investments)

# 2. 查询最受欢迎的基金
print("\n查询最受欢迎的基金...")
popular_funds = pd.read_sql('''
SELECT f.fund_name, COUNT(ib.behavior_id) as transaction_count
FROM funds f
JOIN investment_behaviors ib ON f.fund_id = ib.fund_id
WHERE ib.action_type = '买入' OR ib.action_type = '定投'
GROUP BY f.fund_name
ORDER BY transaction_count DESC
LIMIT 5
''', conn)
print(popular_funds)

# 3. 查询用户平均收益率
print("\n查询用户平均收益率...")
user_returns = pd.read_sql('''
SELECT u.username, AVG(ib.return_rate) as avg_return_rate
FROM users u
JOIN investment_behaviors ib ON u.user_id = ib.user_id
WHERE ib.action_type = '卖出' AND ib.return_rate IS NOT NULL
GROUP BY u.username
ORDER BY avg_return_rate DESC
LIMIT 5
''', conn)
print(user_returns)

conn.close()
print("\n数据库连接已关闭。")

# 将查询结果保存为JSON，方便后续MCP分析
results = {
    "user_investments": user_investments.to_dict(orient='records'),
    "popular_funds": popular_funds.to_dict(orient='records'),
    "user_returns": user_returns.to_dict(orient='records')
}

with open('fund_analysis_results.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=4)

print("分析结果已保存到 fund_analysis_results.json")