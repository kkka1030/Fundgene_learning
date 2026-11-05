import os
import pandas as pd
import datetime
import re
import json
import sqlite3
from pathlib import Path

class DataLoader:
    def __init__(self, scene_path):
        """
        初始化数据加载器
        
        Args:
            scene_path: 场景数据目录路径
        """
        self.scene_path = Path(scene_path)
        self.funds_data = {}
        self.news_data = []
        self.timeline = []
        self.simulation_start_date = None  # 初始化模拟开始日期
        # 使用传入的场景路径
        self.db_path = self.scene_path / "converted" / "fund_crisis.db"
        
    def _convert_percentage(self, value):
        """
        将百分比字符串转换为浮点数
        例如："-2.65%" -> -2.65
        """
        if pd.isna(value) or value is None:
            return 0.0
            
        if isinstance(value, (int, float)):
            return float(value)
            
        if isinstance(value, str):
            # 去除百分号并转换为浮点数
            value = value.replace('%', '')
            try:
                return float(value)
            except ValueError:
                print(f"警告：无法转换百分比值 '{value}'，使用0.0替代")
                return 0.0
                
        return 0.0
    
    def load_fund_data(self):
        """从SQLite数据库加载所有基金历史数据"""
        print("从数据库加载基金数据...")
        
        if not self.db_path.exists():
            print(f"错误：数据库文件 {self.db_path} 不存在")
            return self.funds_data
            
        try:
            # 连接到SQLite数据库
            conn = sqlite3.connect(self.db_path)
            
            # 获取所有基金代码
            funds_query = "SELECT fund_code FROM funds"
            fund_codes = pd.read_sql_query(funds_query, conn)['fund_code'].tolist()
            
            for fund_code in fund_codes:
                # 查询基金净值数据
                query = f"""
                SELECT 
                    fund_code, 
                    date as FSRQ, 
                    unit_nav as DWJZ, 
                    acc_nav as LJJZ, 
                    daily_growth as JZZZL, 
                    status_purchase, 
                    status_redeem 
                FROM fund_nav 
                WHERE fund_code = '{fund_code}'
                ORDER BY date
                """
                
                df = pd.read_sql_query(query, conn)
                
                # 格式化日期
                df['date'] = pd.to_datetime(df['FSRQ'])
                
                # 将涨跌幅转为数值类型 - 处理可能的百分比字符串
                df['JZZZL'] = df['JZZZL'].apply(self._convert_percentage)
                
                # 确保净值列为浮点数
                for col in ['DWJZ', 'LJJZ']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                self.funds_data[fund_code] = df
                print(f"成功加载基金 {fund_code} 数据，包含 {len(df)} 条记录")
                
            conn.close()
                
        except Exception as e:
            print(f"从数据库加载基金数据时出错: {e}")
        
        return self.funds_data
    
    def load_index_data(self):
        """从SQLite数据库加载指数数据"""
        print("从数据库加载指数数据...")
        
        if not self.db_path.exists():
            print(f"错误：数据库文件 {self.db_path} 不存在")
            return self.funds_data
            
        try:
            # 连接到SQLite数据库
            conn = sqlite3.connect(self.db_path)
            
            # 获取所有指数代码
            indices_query = "SELECT index_code, index_name FROM indices"
            indices = pd.read_sql_query(indices_query, conn)
            
            for _, row in indices.iterrows():
                index_code = row['index_code']
                index_name = row['index_name']
                
                # 查询指数数据
                query = f"""
                SELECT 
                    date,
                    close as '收盘',
                    open as '开盘',
                    high as '最高',
                    low as '最低',
                    volume as '成交量',
                    change_pct as '涨跌幅'
                FROM index_data 
                WHERE index_code = '{index_code}'
                ORDER BY date
                """
                
                df = pd.read_sql_query(query, conn)
                
                # 格式化日期
                df['date'] = pd.to_datetime(df['date'])
                
                # 转换涨跌幅百分比字符串为浮点数
                if '涨跌幅' in df.columns:
                    df['涨跌幅'] = df['涨跌幅'].apply(self._convert_percentage)
                
                # 确保数值列为浮点数
                for col in ['收盘', '开盘', '最高', '最低']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 确定存储的键名
                key_name = index_code
                if '上证' in index_name:
                    key_name = 'sh_index'
                elif '道琼斯' in index_name:
                    key_name = 'dj_index'
                
                self.funds_data[key_name] = df
                print(f"成功加载指数 {index_name}({index_code}) 数据，包含 {len(df)} 条记录")
                
            conn.close()
                
        except Exception as e:
            print(f"从数据库加载指数数据时出错: {e}")
        
        return self.funds_data
    
    def load_news_data(self):
        """加载新闻数据"""
        try:
            # 使用相对路径指向原始目录下的新闻文件
            news_path = self.scene_path / "新闻.json"
            if news_path.exists():
                with open(news_path, 'r', encoding='utf-8') as f:
                    news_data = json.load(f)
                
                # 处理JSON格式的新闻数据
                news_items = []
                for item in news_data:
                    if 'date' in item and 'content' in item:
                        # 将日期字符串转换为datetime.date对象
                        date_str = item['date']
                        try:
                            date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                            news_items.append({
                                'date': date_obj,
                                'content': item['content']
                            })
                        except ValueError:
                            print(f"日期格式错误: {date_str}，跳过此条新闻")
                
                # 按日期排序
                news_items.sort(key=lambda x: x['date'])
                self.news_data = news_items
                
                print(f"成功加载新闻数据，包含 {len(news_items)} 条新闻")
            else:
                print("未找到新闻数据文件")
        except Exception as e:
            print(f"加载新闻数据时出错: {e}")
        
        return self.news_data
    
    def load_scene_description(self):
        """加载场景介绍"""
        try:
            # 尝试查找介绍.json或者介绍.docx文件
            intro_files = list(self.scene_path.glob("*介绍.*"))
            if intro_files:
                intro_path = intro_files[0]  # 使用找到的第一个介绍文件
                if intro_path.suffix == '.json':
                    with open(intro_path, 'r', encoding='utf-8') as f:
                        description_data = json.load(f)
                    
                    # 假设JSON文件包含一个描述字段
                    if isinstance(description_data, dict) and 'description' in description_data:
                        description = description_data['description']
                    else:
                        # 如果是简单的字符串数组，连接起来
                        if isinstance(description_data, list):
                            description = "\n".join(description_data)
                        else:
                            description = str(description_data)
                elif intro_path.suffix == '.docx':
                    # 如果是docx文件，简单返回文件名作为描述
                    description = f"请查看 {intro_path.name} 获取详细介绍。"
                else:
                    description = f"场景介绍文件格式不支持: {intro_path.name}"
                
                print("成功加载场景介绍")
                return description
            else:
                print("未找到场景介绍文件")
                return "未能加载场景介绍信息。"
        except Exception as e:
            print(f"加载场景介绍时出错: {e}")
            return "加载场景介绍时发生错误。"
    
    def get_earliest_valid_date(self):
        """获取数据库中最早的有效日期（基金数据的最早日期）"""
        if not self.db_path.exists():
            print(f"错误：数据库文件 {self.db_path} 不存在")
            return None
            
        try:
            # 连接到SQLite数据库
            conn = sqlite3.connect(self.db_path)
            
            # 获取fund_nav表的最早日期
            fund_query = "SELECT MIN(date) as min_date FROM fund_nav"
            fund_min_date = pd.read_sql_query(fund_query, conn)['min_date'].iloc[0]
            
            conn.close()
            
            # 转换日期字符串为datetime对象
            if fund_min_date:
                fund_min_date = datetime.datetime.strptime(fund_min_date, '%Y-%m-%d').date()
                print(f"基金数据最早日期: {fund_min_date}")
                print(f"选择的模拟开始日期: {fund_min_date}")
                return fund_min_date
            else:
                print("警告：未找到任何基金数据")
                return None
            
        except Exception as e:
            print(f"获取最早有效日期时出错: {e}")
            return None
    
    def build_timeline(self):
        """构建结合新闻和基金数据的时间线，确保每个交易日都有基金数据"""
        self.timeline = []
        
        # 获取所有日期
        all_dates = set()
        
        # 添加基金数据日期
        for fund_code, fund_data in self.funds_data.items():
            if 'date' in fund_data.columns:
                all_dates.update(fund_data['date'].dt.date)
        
        # 添加新闻日期
        for news in self.news_data:
            all_dates.add(news['date'])
        
        # 排序日期
        sorted_dates = sorted(all_dates)
        
        # 获取最早有效日期
        earliest_valid_date = self.simulation_start_date or self.get_earliest_valid_date()
        if earliest_valid_date:
            # 过滤掉早于最早有效日期的日期
            sorted_dates = [date for date in sorted_dates if date >= earliest_valid_date]
        
        # 获取实际存在数据的基金代码（不包括指数）
        fund_codes = []
        for code in self.funds_data.keys():
            if code not in ['sh_index', 'dj_index']:
                df = self.funds_data[code]
                if not df.empty:  # 只添加有数据的基金
                    fund_codes.append(code)
        
        print(f"找到 {len(fund_codes)} 支有效基金数据")
        
        # 构建时间线
        valid_trading_days = []
        for date in sorted_dates:
            # 检查当天是否所有基金都有数据
            all_funds_have_data = True
            for fund_code in fund_codes:  # 只检查实际存在数据的基金
                if fund_code in self.funds_data:
                    fund_data = self.funds_data[fund_code]
                    if 'date' in fund_data.columns:
                        if fund_data[fund_data['date'].dt.date == date].empty:
                            all_funds_have_data = False
                            break
            
            # 只要基金数据齐全，就认为是有效交易日
            if all_funds_have_data:
                valid_trading_days.append(date)
        
        print(f"筛选后的有效交易日数量: {len(valid_trading_days)}")
        
        # 对有效交易日构建详细数据
        for date in valid_trading_days:
            date_events = {'date': date, 'news': [], 'funds': {}}
            
            # 添加当天新闻
            for news in self.news_data:
                if news['date'] == date:
                    date_events['news'].append(news['content'])
            
            # 添加当天基金数据
            for fund_code, fund_data in self.funds_data.items():
                if 'date' in fund_data.columns:
                    day_data = fund_data[fund_data['date'].dt.date == date]
                    if not day_data.empty:
                        day_info = {}
                        # 基金数据
                        if 'DWJZ' in day_data.columns:
                            day_info['nav'] = float(day_data['DWJZ'].values[0])
                        if 'JZZZL' in day_data.columns:
                            day_info['change_pct'] = float(day_data['JZZZL'].values[0])
                        
                        date_events['funds'][fund_code] = day_info
            
            self.timeline.append(date_events)
        
        print(f"成功构建时间线，包含 {len(self.timeline)} 个有效交易日")
        if self.timeline:
            print(f"模拟开始日期: {self.timeline[0]['date']}")
            print(f"模拟结束日期: {self.timeline[-1]['date']}")
        else:
            print("警告: 没有找到所有基金都有数据的有效交易日")
        
        return self.timeline
    
    def load_all_data(self):
        """加载所有数据"""
        # 首先获取最早有效日期
        self.simulation_start_date = self.get_earliest_valid_date()
        
        self.load_fund_data()
        self.load_news_data()
        description = self.load_scene_description()
        self.build_timeline()
        
        return {
            'funds_data': self.funds_data,
            'news_data': self.news_data,
            'timeline': self.timeline,
            'description': description,
            'simulation_start_date': self.simulation_start_date
        }