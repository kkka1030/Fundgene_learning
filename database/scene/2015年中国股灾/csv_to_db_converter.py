
import os
import csv
import sqlite3
import pandas as pd
from pathlib import Path
import json
# 基金代码和名称映射（10支基金）
FUND_MAPPING = {
    "000001": "华夏新经济",
    "000002": "嘉实新机遇",
    "000003": "易方达瑞惠",
    "000004": "南方消费活力",
    "000005": "招商丰庆",
    "000404": "易方达新兴成长混合",
    "100056": "富国低碳环保混合",
    "150153": "创业板B",
    "150174": "TMT中证B",
    "519156": "新华行业灵活配置混合A"
}

# 股票代码与名称（5支）
STOCK_MAPPING = {
    "000001": "中国平安保险股份有限公司",
    "000002": "万科企业股份有限公司",
    "000651": "珠海格力电器股份有限公司",
    "600000": "上海浦东发展银行股份有限公司",
    "600519": "贵州茅台酒股份有限公司"
}

def create_db(base_dir, output_db_path):
    conn = sqlite3.connect(output_db_path)
    cursor = conn.cursor()

    # 基金表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS funds (
        fund_code TEXT,
        fund_name TEXT,
        PRIMARY KEY (fund_code)
    )
    ''')

    # 基金净值表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fund_nav (
        fund_code TEXT,
        date TEXT,
        unit_nav REAL,
        acc_nav REAL,
        daily_growth REAL,
        status_purchase TEXT,
        status_redeem TEXT,
        PRIMARY KEY (fund_code, date),
        FOREIGN KEY (fund_code) REFERENCES funds(fund_code)
    )
    ''')

    # 指数表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS indices (
        index_code TEXT,
        index_name TEXT,
        PRIMARY KEY (index_code)
    )
    ''')

    # 指数历史表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS index_data (
        index_code TEXT,
        date TEXT,
        close REAL,
        open REAL,
        high REAL,
        low REAL,
        volume TEXT,
        change_pct TEXT,
        PRIMARY KEY (index_code, date),
        FOREIGN KEY (index_code) REFERENCES indices(index_code)
    )
    ''')
    # 插入基金信息
    for fund_code, fund_name in FUND_MAPPING.items():
        cursor.execute("INSERT OR IGNORE INTO funds (fund_code, fund_name) VALUES (?, ?)",
                      (fund_code, fund_name))
    
    # 插入指数信息
    cursor.execute("INSERT OR IGNORE INTO indices (index_code, index_name) VALUES (?, ?)",
                  ("SH000001", "上证指数"))
    
    
    for file_name in os.listdir(base_dir):
        if file_name.endswith(".csv") and len(file_name) >= 6 and file_name[:6].isdigit():
            fund_code = file_name[:6]
            
            if fund_code in FUND_MAPPING:
                csv_path = os.path.join(base_dir, file_name)
                
                try:
                    # 使用pandas读取CSV文件
                    df = pd.read_csv(csv_path)
                    
                    # 检查CSV的列是否满足条件
                    if "FSRQ" in df.columns and "DWJZ" in df.columns and "LJJZ" in df.columns:
                        # 遍历行并插入数据
                        for _, row in df.iterrows():
                            date = row.get("FSRQ")
                            unit_nav = float(row.get("DWJZ")) if not pd.isna(row.get("DWJZ")) else None
                            acc_nav = float(row.get("LJJZ")) if not pd.isna(row.get("LJJZ")) else None
                            growth = float(row.get("JZZZL")) if not pd.isna(row.get("JZZZL")) else None
                            purchase_status = row.get("SGZT") if not pd.isna(row.get("SGZT")) else None
                            redeem_status = row.get("SHZT") if not pd.isna(row.get("SHZT")) else None
                            
                            cursor.execute('''
                            INSERT OR REPLACE INTO fund_nav 
                            (fund_code, date, unit_nav, acc_nav, daily_growth, status_purchase, status_redeem)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', (fund_code, date, unit_nav, acc_nav, growth, purchase_status, redeem_status))
                except Exception as e:
                    print(f"处理基金文件 {file_name} 时出错: {e}")

    # 读取上证指数CSV（位于 stock_data_2015 子目录）
    index_file = os.path.join(base_dir, "stock_data_2015", "上证指数历史数据 (1).csv")
    if os.path.exists(index_file):
        try:
            with open(index_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)  # 跳过表头
                
                for row in reader:
                    if len(row) >= 7:
                        date = row[0].strip('"')
                        close = row[1].strip('"').replace(',', '')
                        open_price = row[2].strip('"').replace(',', '')
                        high = row[3].strip('"').replace(',', '')
                        low = row[4].strip('"').replace(',', '')
                        volume = row[5].strip('"')
                        change_pct = row[6].strip('"')
                        
                        try:
                            cursor.execute('''
                            INSERT OR REPLACE INTO index_data 
                            (index_code, date, close, open, high, low, volume, change_pct)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ''', ("SH000001", date, float(close), float(open_price), 
                                 float(high), float(low), volume, change_pct))
                        except (ValueError, TypeError) as ve:
                            print(f"上证指数数据转换错误 - 日期: {date}, 错误: {ve}")
        
        except Exception as e:
            print(f"处理上证指数出错: {e}")

    conn.commit()
    conn.close()
    print(f"数据库创建成功：{output_db_path}")



def export_json_from_db(db_path, output_json_path):
    """从数据库导出JSON格式数据"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    result = {
        "funds": {},
        "indices": {}
    }

    # 导出基金信息
    cursor.execute("SELECT fund_code, fund_name FROM funds")
    for fund_code, fund_name in cursor.fetchall():
        cursor.execute("""
            SELECT date, unit_nav, acc_nav, daily_growth, status_purchase, status_redeem 
            FROM fund_nav WHERE fund_code = ?
            ORDER BY date
        """, (fund_code,))
        records = cursor.fetchall()
        result["funds"][fund_code] = {
            "fund_name": fund_name,
            "records": [
                {
                    "date": r[0],
                    "unit_nav": r[1],
                    "acc_nav": r[2],
                    "daily_growth": r[3],
                    "status_purchase": r[4],
                    "status_redeem": r[5]
                } for r in records
            ]
        }

    # 导出指数信息
    cursor.execute("SELECT index_code, index_name FROM indices")
    for index_code, index_name in cursor.fetchall():
        cursor.execute("""
            SELECT date, close, open, high, low, volume, change_pct 
            FROM index_data WHERE index_code = ?
            ORDER BY date
        """, (index_code,))
        records = cursor.fetchall()
        result["indices"][index_code] = {
            "index_name": index_name,
            "records": [
                {
                    "date": r[0],
                    "close": r[1],
                    "open": r[2],
                    "high": r[3],
                    "low": r[4],
                    "volume": r[5],
                    "change_pct": r[6]
                } for r in records
            ]
        }

    conn.close()

    # 写入JSON文件
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"JSON 文件已生成：{output_json_path}")


def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.join(current_dir, "2015年中国股灾")
    output_dir = os.path.join(base_dir, "converted")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    db_path = os.path.join(output_dir, "fund_stock_2015_crisis.db")
    json_path = os.path.join(output_dir, "fund_stock_2015_crisis.json")

    # 创建数据库
    create_db(base_dir, db_path)

    # 导出JSON
    export_json_from_db(db_path, json_path)

    print("数据转换和导出全部完成！")

if __name__ == "__main__":
    main()
