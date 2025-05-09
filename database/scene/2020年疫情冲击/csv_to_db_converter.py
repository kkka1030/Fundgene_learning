#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sqlite3
import pandas as pd
from pathlib import Path

# 基金映射字典
FUND_MAPPING = {
    "110022": "易方达消费行业",
    "161005": "富国天惠成长",
    "050011": "博时信用债券",
    "510050": "华夏上证50ETF",
    "001180": "广发医疗保健",
    "000083": "汇添富消费行业",
    "008888": "华夏科技创新混合",
    "008763": "易方达科技创新混合",
    "003003": "华夏现金增利",
    "110005": "易方达货币市场基金"
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

    # 净值表
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

    # 虽无指数数据，为保持结构仍创建这两张空表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS indices (
        index_code TEXT,
        index_name TEXT,
        PRIMARY KEY (index_code)
    )
    ''')

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

    # 插入基金元数据
    for code, name in FUND_MAPPING.items():
        cursor.execute("INSERT OR IGNORE INTO funds (fund_code, fund_name) VALUES (?, ?)", (code, name))

    # 遍历文件并插入净值数据
    for file in os.listdir(base_dir):
        if file.endswith("_history.csv"):
            code = file.split("_")[0]
            if code in FUND_MAPPING:
                try:
                    df = pd.read_csv(os.path.join(base_dir, file))
                    for _, row in df.iterrows():
                        cursor.execute('''
                            INSERT OR REPLACE INTO fund_nav 
                            (fund_code, date, unit_nav, acc_nav, daily_growth, status_purchase, status_redeem)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            code,
                            row.get("FSRQ"),
                            float(row.get("DWJZ")) if not pd.isna(row.get("DWJZ")) else None,
                            float(row.get("LJJZ")) if not pd.isna(row.get("LJJZ")) else None,
                            float(row.get("JZZZL")) if not pd.isna(row.get("JZZZL")) else None,
                            row.get("SGZT") if not pd.isna(row.get("SGZT")) else None,
                            row.get("SHZT") if not pd.isna(row.get("SHZT")) else None
                        ))
                except Exception as e:
                    print(f"处理文件 {file} 出错: {e}")

    conn.commit()
    conn.close()
    print(f"数据库创建成功：{output_db_path}")

def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.join(current_dir, "2020年疫情冲击")
    output_dir = os.path.join(base_dir, "converted")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    db_path = os.path.join(output_dir, "fund_2020_covid_crisis.db")
    create_db(base_dir, db_path)

if __name__ == "__main__":
    main()
