#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import csv
import json
import sqlite3
import pandas as pd
from pathlib import Path

# 基金代码和名称的映射关系
FUND_MAPPING = {
    "000001": "华夏新经济",
    
    "000003": "易方达瑞惠",
    "000004": "南方消费活力",
    "000005": "招商丰庆",
    "000404": "易方达新兴成长混合",
    "100056": "富国低碳环保混合",
    "150153": "创业板B",
    "150174": "TMT中证B",
    "519156": "新华行业灵活配置混合A"
}




def create_db(csv_directory, output_db_path):
    """将CSV文件转换为SQLite数据库"""
    # 创建SQLite连接
    conn = sqlite3.connect(output_db_path)
    cursor = conn.cursor()
    
    # 创建基金表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS funds (
        fund_code TEXT,
        fund_name TEXT,
        PRIMARY KEY (fund_code)
    )
    ''')
    
    # 创建净值表
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
    
    
    
    # 插入基金信息
    for fund_code, fund_name in FUND_MAPPING.items():
        cursor.execute("INSERT OR IGNORE INTO funds (fund_code, fund_name) VALUES (?, ?)",
                      (fund_code, fund_name))
    
    
    
    
    # 处理基金CSV文件
    for file_name in os.listdir(csv_directory):
        if file_name.endswith(".csv") and len(file_name) >= 6 and file_name[:6].isdigit():
            fund_code = file_name[:6]
            
            if fund_code in FUND_MAPPING:
                csv_path = os.path.join(csv_directory, file_name)
                
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
    
    
    # 提交更改并关闭连接
    conn.commit()
    conn.close()
    
    print(f"已成功创建数据库: {output_db_path}")

def create_json(csv_directory, output_json_path):
    """将CSV文件转换为JSON文件"""
    all_data = {
        "funds": {},
        
    }
    
    # 处理基金数据
    for file_name in os.listdir(csv_directory):
        if file_name.endswith(".csv") and len(file_name) >= 6 and file_name[:6].isdigit():
            fund_code = file_name[:6]
            
            if fund_code in FUND_MAPPING:
                fund_name = FUND_MAPPING[fund_code]
                csv_path = os.path.join(csv_directory, file_name)
                
                try:
                    # 使用pandas读取CSV
                    df = pd.read_csv(csv_path)
                    
                    # 转换为字典列表
                    fund_data = []
                    for _, row in df.iterrows():
                        record = {
                            "date": row.get("FSRQ"),
                            "unit_nav": float(row.get("DWJZ")) if not pd.isna(row.get("DWJZ")) else None,
                            "acc_nav": float(row.get("LJJZ")) if not pd.isna(row.get("LJJZ")) else None,
                            "daily_growth": float(row.get("JZZZL")) if not pd.isna(row.get("JZZZL")) else None,
                            "status_purchase": row.get("SGZT"),
                            "status_redeem": row.get("SHZT")
                        }
                        # 删除None值
                        fund_data.append({k: v for k, v in record.items() if v is not None})
                    
                    all_data["funds"][fund_code] = {
                        "fund_name": fund_name,
                        "records": fund_data
                    }
                except Exception as e:
                    print(f"处理基金文件 {file_name} 时出错: {e}")
    
    
    '''''''''
    # 处理道琼斯指数数据
    dji_index_file = os.path.join(csv_directory, "道琼斯工业平均指数历史数据.csv")
    if os.path.exists(dji_index_file):
        try:
            with open(dji_index_file, 'r', encoding='utf-8') as f:
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
                        
                        # 对于空值的处理
                        if not close or close == "":
                            continue
                            
                        try:
                            record = {
                                "date": date,
                                "close": float(close),
                                "open": float(open_price),
                                "high": float(high),
                                "low": float(low),
                                "volume": volume,
                                "change_pct": change_pct
                            }
                            all_data["indices"]["DJI"]["records"].append(record)
                        except (ValueError, TypeError) as ve:
                            print(f"道琼斯指数JSON转换错误 - 日期: {date}, 错误: {ve}")
        except Exception as e:
            print(f"处理道琼斯指数文件JSON时出错: {e}")
    '''''''''
    # 写入JSON文件
    with open(output_json_path, 'w', encoding='utf-8') as json_file:
        json.dump(all_data, json_file, ensure_ascii=False, indent=2)
    
    print(f"已成功创建JSON文件: {output_json_path}")

def main():
    # 获取当前目录
    current_directory = os.path.dirname(os.path.abspath(__file__))
    csv_directory = os.path.join(current_directory, "2015年中国股灾")
    
    # 创建输出目录（如果不存在）
    output_directory = os.path.join(current_directory, "2015年中国股灾", "converted")
    Path(output_directory).mkdir(parents=True, exist_ok=True)
    
    # 输出文件路径
    db_output_path = os.path.join(output_directory, "fund_crisis.db")
    json_output_path = os.path.join(output_directory, "fund_crisis.json")
    
    # 转换为数据库
    create_db(csv_directory, db_output_path)
    
    # 转换为JSON
    create_json(csv_directory, json_output_path)
    
    print("转换完成!")

if __name__ == "__main__":
    main()