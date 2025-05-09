#!/usr/bin/env python3
"""
此脚本用于减少fund_investment.db数据库中的投资记录，
保留每个用户最新的8条记录，删除其余记录。
"""

import sqlite3
import os
import sys
from pathlib import Path

def reduce_investment_records(db_path, records_per_user=8):
    """
    减少数据库中每个用户的投资记录数量
    
    参数:
        db_path: 数据库文件路径
        records_per_user: 每个用户保留的记录数量，默认为8
    """
    print(f"正在处理数据库: {db_path}")
    
    # 确保数据库文件存在
    if not os.path.exists(db_path):
        print(f"错误: 数据库文件 {db_path} 不存在")
        return False
    
    # 连接到数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 获取投资记录总数
        cursor.execute("SELECT COUNT(*) FROM investment_behaviors")
        total_records_before = cursor.fetchone()[0]
        print(f"处理前总记录数: {total_records_before}")
        
        # 获取用户数量
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM investment_behaviors")
        user_count = cursor.fetchone()[0]
        print(f"用户总数: {user_count}")
        
        # 获取所有用户ID
        cursor.execute("SELECT DISTINCT user_id FROM investment_behaviors")
        user_ids = [row[0] for row in cursor.fetchall()]
        
        # 开始事务
        conn.execute("BEGIN TRANSACTION")
        
        total_deleted = 0
        for user_id in user_ids:
            # 查询该用户的所有记录，按时间戳降序排列
            cursor.execute(
                "SELECT behavior_id FROM investment_behaviors "
                "WHERE user_id = ? "
                "ORDER BY timestamp DESC",
                (user_id,)
            )
            behaviors = [row[0] for row in cursor.fetchall()]
            
            # 如果记录超过指定数量，删除多余的记录
            if len(behaviors) > records_per_user:
                # 要保留的记录
                keep_behaviors = behaviors[:records_per_user]
                # 要删除的记录
                delete_behaviors = behaviors[records_per_user:]
                
                # 构建占位符
                placeholders = ','.join(['?'] * len(delete_behaviors))
                
                # 删除多余的记录
                cursor.execute(
                    f"DELETE FROM investment_behaviors WHERE behavior_id IN ({placeholders})",
                    delete_behaviors
                )
                
                records_deleted = len(delete_behaviors)
                total_deleted += records_deleted
                print(f"用户 {user_id}: 删除了 {records_deleted} 条记录，保留 {records_per_user} 条")
            else:
                print(f"用户 {user_id}: 记录数 {len(behaviors)} 已经不超过 {records_per_user}，无需删除")
        
        # 提交事务
        conn.commit()
        
        # 获取处理后的投资记录总数
        cursor.execute("SELECT COUNT(*) FROM investment_behaviors")
        total_records_after = cursor.fetchone()[0]
        
        print("\n处理结果汇总:")
        print(f"处理前总记录数: {total_records_before}")
        print(f"删除的记录数: {total_deleted}")
        print(f"处理后总记录数: {total_records_after}")
        print(f"平均每个用户记录数: {total_records_after/user_count:.2f}")
        
        # 验证每个用户的记录数量
        cursor.execute(
            "SELECT user_id, COUNT(*) as record_count "
            "FROM investment_behaviors "
            "GROUP BY user_id"
        )
        user_record_counts = cursor.fetchall()
        
        print("\n每个用户的记录数量:")
        for user_id, count in user_record_counts:
            print(f"用户 {user_id}: {count} 条记录")
        
        return True
    
    except sqlite3.Error as e:
        # 如果发生错误，回滚事务
        conn.rollback()
        print(f"SQLite错误: {e}")
        return False
    
    finally:
        # 关闭数据库连接
        conn.close()

if __name__ == "__main__":
    # 数据库路径
    behavior_db_path = str(Path(__file__).parent / "fund_investment.db")
    debug_db_path = str(Path(__file__).parent.parent.parent / "debug" / "fund_investment.db")
    
    # 处理行为数据库
    print("=" * 50)
    print("处理行为数据库")
    print("=" * 50)
    reduce_investment_records(behavior_db_path)
    
    # 处理调试数据库
    print("\n" + "=" * 50)
    print("处理调试数据库")
    print("=" * 50)
    reduce_investment_records(debug_db_path)
    
    print("\n处理完成!")