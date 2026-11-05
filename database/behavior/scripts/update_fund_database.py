import sqlite3
import os
import random
from datetime import datetime, timedelta

# 确保当前工作目录是脚本所在目录
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# 连接到数据库
db_path = 'fund_investment.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("开始更新数据库结构...")

# 备份原表数据
print("备份原表数据...")
cursor.execute("CREATE TABLE IF NOT EXISTS funds_backup AS SELECT * FROM funds")
cursor.execute("CREATE TABLE IF NOT EXISTS users_backup AS SELECT * FROM users")
cursor.execute("CREATE TABLE IF NOT EXISTS investment_behaviors_backup AS SELECT * FROM investment_behaviors")

# 修改基金表 - 添加新字段
print("更新基金表结构...")
cursor.execute("PRAGMA foreign_keys=off")
cursor.execute("BEGIN TRANSACTION")

# 创建新的基金表结构
cursor.execute('''
CREATE TABLE funds_new (
    fund_id TEXT PRIMARY KEY,
    fund_name TEXT NOT NULL,
    fund_code TEXT UNIQUE,
    fund_type TEXT,
    risk_level TEXT,
    management_fee REAL,
    annual_return_rate REAL,
    inception_date TEXT,
    fund_size REAL,
    fund_manager TEXT,
    current_nav REAL,
    accumulative_nav REAL,
    benchmark TEXT,
    investment_strategy TEXT,
    top_holdings TEXT,
    dividend_history TEXT,
    subscription_fee REAL,
    redemption_fee REAL,
    min_subscription_amount REAL,
    custodian_bank TEXT,
    update_date TEXT
)
''')

# 从原表中复制数据到新表，为新增字段生成随机数据
cursor.execute("SELECT * FROM funds")
funds_data = cursor.fetchall()

# 准备更新后的数据
benchmarks = ['沪深300指数', '中证500指数', '上证综指', '创业板指数', '中债总指数', 'MSCI中国指数']
strategies = ['价值投资', '成长投资', '指数增强', '量化投资', '主题投资', '行业轮动', '固定收益']
custodian_banks = ['中国银行', '工商银行', '建设银行', '农业银行', '交通银行', '招商银行']

updated_funds_data = []
for fund in funds_data:
    fund_id, fund_name, fund_type, risk_level, management_fee, annual_return_rate, inception_date, fund_size, fund_manager = fund
    
    # 生成基金代码
    fund_code = f"{random.randint(0, 9)}{random.randint(0, 9)}{random.randint(0, 9)}{random.randint(0, 9)}{random.randint(0, 9)}{random.randint(0, 9)}"
    
    # 生成净值信息
    current_nav = round(random.uniform(0.8, 3.5), 4)
    accumulative_nav = round(current_nav + random.uniform(0, 5), 4)
    
    # 其他信息
    benchmark = random.choice(benchmarks)
    investment_strategy = random.choice(strategies)
    
    # 生成主要持仓
    if fund_type in ['股票型', '混合型', '指数型']:
        holdings = ["贵州茅台", "宁德时代", "招商银行", "腾讯控股", "美的集团"]
        random.shuffle(holdings)
        top_holdings = ', '.join(holdings[:random.randint(3, 5)])
    elif fund_type in ['债券型', '货币市场型']:
        holdings = ["国债", "地方债", "企业债", "金融债", "可转债"]
        random.shuffle(holdings)
        top_holdings = ', '.join(holdings[:random.randint(3, 5)])
    else:
        top_holdings = "暂无数据"
    
    # 生成分红历史
    dividend_dates = []
    for i in range(random.randint(0, 5)):
        days_ago = random.randint(30, 1000)
        dividend_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        dividend_amount = round(random.uniform(0.05, 0.5), 2)
        dividend_dates.append(f"{dividend_date}: {dividend_amount}元/份")
    dividend_history = '; '.join(dividend_dates) if dividend_dates else "暂无分红记录"
    
    # 费率信息
    subscription_fee = round(random.uniform(0, 1.5), 2)
    redemption_fee = round(random.uniform(0, 1.5), 2)
    min_subscription_amount = random.choice([1, 10, 100, 1000]) * 100
    
    # 托管银行
    custodian_bank = random.choice(custodian_banks)
    
    # 更新日期
    update_date = datetime.now().strftime('%Y-%m-%d')
    
    updated_funds_data.append((
        fund_id, fund_name, fund_code, fund_type, risk_level, management_fee, 
        annual_return_rate, inception_date, fund_size, fund_manager, current_nav, 
        accumulative_nav, benchmark, investment_strategy, top_holdings, 
        dividend_history, subscription_fee, redemption_fee, min_subscription_amount,
        custodian_bank, update_date
    ))

# 插入更新后的数据
cursor.executemany('''
INSERT INTO funds_new (
    fund_id, fund_name, fund_code, fund_type, risk_level, management_fee, 
    annual_return_rate, inception_date, fund_size, fund_manager, current_nav, 
    accumulative_nav, benchmark, investment_strategy, top_holdings, 
    dividend_history, subscription_fee, redemption_fee, min_subscription_amount,
    custodian_bank, update_date
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', updated_funds_data)

# 替换旧表
cursor.execute("DROP TABLE funds")
cursor.execute("ALTER TABLE funds_new RENAME TO funds")

# 更新用户表
print("更新用户表结构...")
cursor.execute('''
CREATE TABLE users_new (
    user_id TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    age INTEGER,
    gender TEXT,
    risk_tolerance TEXT,
    investment_goal TEXT,
    annual_income REAL,
    registration_date TEXT,
    phone TEXT,
    email TEXT,
    account_balance REAL,
    investment_preference TEXT,
    last_login_date TEXT
)
''')

# 从原表复制数据并添加新字段
cursor.execute("SELECT * FROM users")
users_data = cursor.fetchall()

updated_users_data = []
preferences = ['价值型', '成长型', '收入型', '平衡型', '激进型', '保守型']

for user in users_data:
    user_id, username, age, gender, risk_tolerance, investment_goal, annual_income, registration_date = user
    
    # 生成联系信息
    phone = f"1{random.choice(['3', '5', '7', '8', '9'])}{random.randint(100000000, 999999999)}"
    email = f"{username.lower()}@{random.choice(['gmail.com', '163.com', 'qq.com', 'outlook.com'])}"
    
    # 账户余额和投资偏好
    account_balance = round(random.uniform(1000, 100000), 2)
    investment_preference = random.choice(preferences)
    
    # 最后登录日期
    days_ago = random.randint(0, 30)
    last_login_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    
    updated_users_data.append((
        user_id, username, age, gender, risk_tolerance, investment_goal, 
        annual_income, registration_date, phone, email, account_balance, 
        investment_preference, last_login_date
    ))

cursor.executemany('''
INSERT INTO users_new (
    user_id, username, age, gender, risk_tolerance, investment_goal, 
    annual_income, registration_date, phone, email, account_balance, 
    investment_preference, last_login_date
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', updated_users_data)

cursor.execute("DROP TABLE users")
cursor.execute("ALTER TABLE users_new RENAME TO users")

# 更新投资行为表
print("更新投资行为表结构...")
cursor.execute('''
CREATE TABLE investment_behaviors_new (
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
    nav_price REAL,
    fund_shares REAL,
    transaction_status TEXT,
    transaction_fee REAL,
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (fund_id) REFERENCES funds (fund_id)
)
''')

# 从原表复制数据并添加新字段
cursor.execute("SELECT * FROM investment_behaviors")
behaviors_data = cursor.fetchall()

updated_behaviors_data = []
statuses = ['已确认', '处理中', '已完成', '已撤销']

for behavior in behaviors_data:
    behavior_id, user_id, fund_id, action_type, amount, timestamp, holding_period, return_rate, platform, notes = behavior
    
    # 交易价格
    nav_price = round(random.uniform(0.8, 3.0), 4)
    
    # 计算份额
    fund_shares = round(amount / nav_price, 2) if nav_price > 0 else 0
    
    # 交易状态和手续费
    transaction_status = random.choice(statuses) if random.random() > 0.2 else '已确认'
    transaction_fee = round(amount * random.uniform(0, 0.015), 2)
    
    updated_behaviors_data.append((
        behavior_id, user_id, fund_id, action_type, amount, timestamp, 
        holding_period, return_rate, platform, notes, nav_price, 
        fund_shares, transaction_status, transaction_fee
    ))

cursor.executemany('''
INSERT INTO investment_behaviors_new (
    behavior_id, user_id, fund_id, action_type, amount, timestamp, 
    holding_period, return_rate, platform, notes, nav_price, 
    fund_shares, transaction_status, transaction_fee
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', updated_behaviors_data)

cursor.execute("DROP TABLE investment_behaviors")
cursor.execute("ALTER TABLE investment_behaviors_new RENAME TO investment_behaviors")

# 提交事务
cursor.execute("COMMIT")
cursor.execute("PRAGMA foreign_keys=on")

print(f"数据库更新完成: {db_path}")
print("新增字段已添加，并生成了示例数据")

# 输出更新后的表结构信息
print("\n更新后的基金表结构:")
cursor.execute("PRAGMA table_info(funds)")
print(cursor.fetchall())

print("\n更新后的用户表结构:")
cursor.execute("PRAGMA table_info(users)")
print(cursor.fetchall())

print("\n更新后的投资行为表结构:")
cursor.execute("PRAGMA table_info(investment_behaviors)")
print(cursor.fetchall())

conn.close()
print("\n数据库连接已关闭。")