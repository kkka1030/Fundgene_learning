import sqlite3
import os
import random
import uuid
from datetime import datetime, timedelta

# 确保当前工作目录是脚本所在目录
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# 连接到数据库
db_path = 'fund_investment.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("开始为每个用户添加虚拟投资行为记录...")

# 获取所有用户ID
cursor.execute("SELECT user_id FROM users")
user_ids = [row[0] for row in cursor.fetchall()]

# 获取所有基金ID
cursor.execute("SELECT fund_id, current_nav FROM funds")
funds_data = cursor.fetchall()

# 为每个用户生成30次投资记录
action_types = ['买入', '卖出', '定投', '分红再投', '转换']
platforms = ['银行APP', '基金公司官网', '支付宝', '微信', '券商APP', '第三方理财平台']
statuses = ['已确认', '处理中', '已完成', '已撤销']

# 记录总添加条数
total_records = 0

# 获取最新的更新时间
cursor.execute("SELECT MAX(timestamp) FROM investment_behaviors")
last_timestamp = cursor.fetchone()[0]
if last_timestamp:
    last_date = datetime.strptime(last_timestamp.split()[0], '%Y-%m-%d')
else:
    last_date = datetime.now() - timedelta(days=365*3)

for user_id in user_ids:
    print(f"为用户 {user_id} 生成投资记录...")
    
    # 为每个用户生成30条记录
    for i in range(30):
        behavior_id = str(uuid.uuid4())
        fund_id, nav_price = random.choice(funds_data)
        action_type = random.choice(action_types)
        
        # 生成金额，根据交易类型调整范围
        if action_type == '定投':
            amount = round(random.uniform(100, 3000), 2)  # 定投通常金额较小
        elif action_type == '分红再投':
            amount = round(random.uniform(10, 1000), 2)  # 分红再投金额更小
        else:
            amount = round(random.uniform(1000, 50000), 2)  # 普通买卖金额较大
        
        # 生成交易时间，确保时间是递增的
        days_ago = random.randint(1, 365)
        hours = random.randint(9, 15)
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)
        timestamp = (last_date - timedelta(days=days_ago, hours=random.randint(0, 23))).replace(
            hour=hours, minute=minutes, second=seconds
        ).strftime('%Y-%m-%d %H:%M:%S')
        
        # 生成持有期和收益率（只有卖出操作才有）
        holding_period = random.randint(30, 500) if action_type == '卖出' else None
        
        # 收益率根据风险偏好生成
        cursor.execute("SELECT risk_tolerance FROM users WHERE user_id = ?", (user_id,))
        risk_tolerance = cursor.fetchone()[0]
        
        if action_type == '卖出':
            if risk_tolerance == '高':
                return_rate = round(random.uniform(-15.0, 40.0), 2)
            elif risk_tolerance == '中高':
                return_rate = round(random.uniform(-10.0, 30.0), 2)
            elif risk_tolerance == '中':
                return_rate = round(random.uniform(-8.0, 20.0), 2)
            elif risk_tolerance == '中低':
                return_rate = round(random.uniform(-5.0, 15.0), 2)
            else:  # 低风险
                return_rate = round(random.uniform(-3.0, 10.0), 2)
        else:
            return_rate = None
        
        platform = random.choice(platforms)
        
        # 生成备注
        if action_type == '买入':
            notes_options = [
                "看好该基金长期表现", 
                "市场调整，适合买入", 
                "增加投资组合多样性",
                "基金经理表现优异",
                None
            ]
        elif action_type == '卖出':
            notes_options = [
                "达到目标收益", 
                "止损操作", 
                "调整资产配置",
                "市场高点卖出",
                None
            ]
        elif action_type == '定投':
            notes_options = [
                "每月定期定投", 
                "工资到账，执行定投计划", 
                "长期投资策略",
                None
            ]
        else:
            notes_options = [None, "例行操作", "优化投资组合"]
        
        notes = random.choice(notes_options)
        
        # 计算份额和手续费
        nav_price = round(random.uniform(0.8, 5.0), 4) if not nav_price else nav_price
        fund_shares = round(amount / nav_price, 2) if nav_price > 0 else 0
        transaction_status = random.choice(statuses) if random.random() > 0.8 else '已确认'
        transaction_fee = round(amount * random.uniform(0, 0.015), 2)
        
        # 插入记录
        cursor.execute('''
        INSERT INTO investment_behaviors (
            behavior_id, user_id, fund_id, action_type, amount, timestamp, 
            holding_period, return_rate, platform, notes, nav_price, 
            fund_shares, transaction_status, transaction_fee
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            behavior_id, user_id, fund_id, action_type, amount, timestamp, 
            holding_period, return_rate, platform, notes, nav_price, 
            fund_shares, transaction_status, transaction_fee
        ))
        
        total_records += 1

# 提交更改
conn.commit()

# 验证添加的记录数
cursor.execute("SELECT COUNT(*) FROM investment_behaviors")
total_count = cursor.fetchone()[0]

print(f"成功添加了 {total_records} 条投资行为记录")
print(f"数据库中现有 {total_count} 条投资行为记录")

# 生成一些用户投资统计数据
print("\n用户投资行为统计:")
cursor.execute('''
SELECT u.username, COUNT(ib.behavior_id) as behavior_count, 
       SUM(CASE WHEN ib.action_type = '买入' OR ib.action_type = '定投' THEN ib.amount ELSE 0 END) as total_buy,
       SUM(CASE WHEN ib.action_type = '卖出' THEN ib.amount ELSE 0 END) as total_sell,
       COUNT(CASE WHEN ib.action_type = '定投' THEN 1 ELSE NULL END) as scheduled_count
FROM users u
JOIN investment_behaviors ib ON u.user_id = ib.user_id
GROUP BY u.username
ORDER BY behavior_count DESC
LIMIT 10
''')
user_stats = cursor.fetchall()

for stat in user_stats:
    username, count, buy, sell, scheduled = stat
    print(f"用户: {username}, 交易次数: {count}, 买入总额: {buy:.2f}, 卖出总额: {sell:.2f}, 定投次数: {scheduled}")

conn.close()
print("\n数据库连接已关闭。")

print("投资行为记录添加完成！")