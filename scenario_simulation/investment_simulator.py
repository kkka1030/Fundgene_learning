import os
import json
import pandas as pd
import datetime
from pathlib import Path
from data_loader import DataLoader

class InvestmentSimulator:
    def __init__(self, scene_path, initial_capital=100000):
        """
        初始化投资模拟器
        
        Args:
            scene_path: 场景数据目录路径
            initial_capital: 初始资金，默认10万元
        """
        self.scene_path = Path(scene_path)
        self.data_loader = DataLoader(scene_path)
        self.data = self.data_loader.load_all_data()
        
        # 创建保存目录
        self.save_dir = self.scene_path / "save"
        os.makedirs(self.save_dir, exist_ok=True)
        
        # 初始化用户资产信息
        self.initial_capital = initial_capital
        
        # 获取模拟开始日期索引
        self.start_date_index = 0
        if 'simulation_start_date' in self.data and self.data['simulation_start_date']:
            for i, day in enumerate(self.data['timeline']):
                if day['date'] >= self.data['simulation_start_date']:
                    self.start_date_index = i
                    break
        
        # 重置模拟器状态
        self.reset_simulation()
        
        # 模拟器状态
        self.current_date_index = 0
        self.current_date = None
        self.is_simulation_over = False
        
        # 用户行为记录
        self.user_actions = []
        
        # 获取所有可交易的基金列表
        self.available_funds = list(self.data['funds_data'].keys())
        # 从基金列表中移除指数，因为指数不可直接交易
        if 'sh_index' in self.available_funds:
            self.available_funds.remove('sh_index')
        if 'dj_index' in self.available_funds:
            self.available_funds.remove('dj_index')
    
    def reset_simulation(self):
        """重置模拟器，恢复初始状态"""
        # 用户资产
        self.cash = self.initial_capital  # 现金
        self.holdings = {}  # 持仓 {fund_code: shares}
        self.net_worth_history = []  # 净值历史
        
        # 设置初始日期为获取的最早有效日期
        self.current_date_index = self.start_date_index
        self.is_simulation_over = False
        self.user_actions = []
        
        if self.data['timeline']:
            self.current_date = self.data['timeline'][self.current_date_index]['date']
        else:
            self.current_date = None
            
        # 记录初始资产状态
        self._update_net_worth()
    
    def _update_net_worth(self):
        """更新当前净值"""
        total_holdings_value = 0
        
        # 计算持仓价值
        for fund_code, shares in self.holdings.items():
            # 获取当前日期的基金净值
            fund_info = self._get_fund_info_by_date(fund_code, self.current_date)
            if fund_info and 'nav' in fund_info:
                total_holdings_value += shares * fund_info['nav']
        
        # 计算总资产
        total_assets = self.cash + total_holdings_value
        
        # 记录净值历史
        self.net_worth_history.append({
            'date': self.current_date,
            'cash': self.cash,
            'holdings_value': total_holdings_value,
            'total_assets': total_assets
        })
        
        return total_assets
    
    def _get_fund_info_by_date(self, fund_code, date):
        """获取指定日期的基金信息"""
        if date is None:
            return None
            
        # 在时间线中查找对应日期的基金信息
        for day in self.data['timeline']:
            if day['date'] == date and fund_code in day['funds']:
                return day['funds'][fund_code]
        
        return None
    
    def _record_action(self, action_type, details):
        """记录用户行为"""
        action = {
            'date': self.current_date,
            'action_type': action_type,
            'details': details,
            'timestamp': datetime.datetime.now().isoformat(),
            'cash_after': self.cash,
        }
        self.user_actions.append(action)
    
    def get_current_state(self):
        """获取当前状态信息"""
        if self.is_simulation_over:
            return {
                'status': 'ended',
                'message': '模拟已结束'
            }
            
        if self.current_date_index >= len(self.data['timeline']):
            self.is_simulation_over = True
            return {
                'status': 'ended',
                'message': '模拟已结束'
            }
        
        current_day = self.data['timeline'][self.current_date_index]
        self.current_date = current_day['date']
        
        # 准备市场数据信息
        market_info = {}
        indices_info = {}
        
        # 添加上证指数信息
        if 'sh_index' in current_day['funds']:
            indices_info['上证指数'] = {
                '收盘价': current_day['funds']['sh_index'].get('close', '暂无'),
                '涨跌幅': current_day['funds']['sh_index'].get('change_pct', '暂无')
            }
            
        # 添加道琼斯指数信息
        if 'dj_index' in current_day['funds']:
            indices_info['道琼斯指数'] = {
                '收盘价': current_day['funds']['dj_index'].get('close', '暂无'),
                '涨跌幅': current_day['funds']['dj_index'].get('change_pct', '暂无')
            }
        
        # 可交易基金信息
        funds_info = {}
        for fund_code in self.available_funds:
            if fund_code in current_day['funds']:
                funds_info[fund_code] = {
                    '净值': current_day['funds'][fund_code].get('nav', '暂无'),
                    '涨跌幅': current_day['funds'][fund_code].get('change_pct', '暂无')
                }
        
        # 用户当前持仓状态
        holdings_info = []
        for fund_code, shares in self.holdings.items():
            fund_info = self._get_fund_info_by_date(fund_code, self.current_date)
            if fund_info and 'nav' in fund_info:
                value = shares * fund_info['nav']
                holding = {
                    'fund_code': fund_code,
                    'shares': shares,
                    'nav': fund_info['nav'],
                    'value': value,
                    'change_pct': fund_info.get('change_pct', 0)
                }
                holdings_info.append(holding)
        
        # 计算总资产
        total_assets = self._update_net_worth()
        
        return {
            'status': 'active',
            'date': self.current_date.strftime('%Y-%m-%d'),
            'cash': self.cash,
            'total_assets': total_assets,
            'indices': indices_info,
            'funds': funds_info,
            'holdings': holdings_info,
            'news': current_day['news']
        }
    
    def buy_fund(self, fund_code, amount):
        """
        购买基金
        
        Args:
            fund_code: 基金代码
            amount: 购买金额
        
        Returns:
            操作结果字典
        """
        # 检查基金是否可交易
        if fund_code not in self.available_funds:
            return {
                'success': False,
                'message': f'基金{fund_code}不可交易或不存在'
            }
        
        # 检查资金是否充足
        if amount <= 0:
            return {
                'success': False,
                'message': '购买金额必须大于0'
            }
            
        if amount > self.cash:
            return {
                'success': False,
                'message': '现金不足'
            }
        
        # 获取当前基金净值
        fund_info = self._get_fund_info_by_date(fund_code, self.current_date)
        if not fund_info or 'nav' not in fund_info:
            return {
                'success': False,
                'message': f'当前日期无法获取基金{fund_code}净值信息'
            }
        
        # 计算可购买份额
        nav = fund_info['nav']
        shares = amount / nav
        
        # 更新持仓和现金
        self.holdings[fund_code] = self.holdings.get(fund_code, 0) + shares
        self.cash -= amount
        
        # 记录操作
        self._record_action('buy', {
            'fund_code': fund_code,
            'amount': amount,
            'nav': nav,
            'shares': shares
        })
        
        return {
            'success': True,
            'message': f'成功购买基金{fund_code}，金额{amount}元，份额{shares:.2f}份',
            'shares': shares,
            'amount': amount
        }
    
    def sell_fund(self, fund_code, shares=None, percentage=None):
        """
        卖出基金
        
        Args:
            fund_code: 基金代码
            shares: 卖出份额，与percentage二选一
            percentage: 卖出比例，0-1之间，与shares二选一
        
        Returns:
            操作结果字典
        """
        # 检查基金是否持有
        if fund_code not in self.holdings or self.holdings[fund_code] <= 0:
            return {
                'success': False,
                'message': f'未持有基金{fund_code}'
            }
        
        current_shares = self.holdings[fund_code]
        
        # 计算要卖出的份额
        if shares is not None:
            if shares <= 0 or shares > current_shares:
                return {
                    'success': False,
                    'message': f'卖出份额无效，当前持有{current_shares:.2f}份'
                }
            shares_to_sell = shares
        elif percentage is not None:
            if percentage <= 0 or percentage > 1:
                return {
                    'success': False,
                    'message': '卖出比例必须在0-1之间'
                }
            shares_to_sell = current_shares * percentage
        else:
            return {
                'success': False,
                'message': '必须指定卖出份额或比例'
            }
        
        # 获取当前基金净值
        fund_info = self._get_fund_info_by_date(fund_code, self.current_date)
        if not fund_info or 'nav' not in fund_info:
            return {
                'success': False,
                'message': f'当前日期无法获取基金{fund_code}净值信息'
            }
        
        # 计算卖出金额
        nav = fund_info['nav']
        amount = shares_to_sell * nav
        
        # 更新持仓和现金
        self.holdings[fund_code] -= shares_to_sell
        if self.holdings[fund_code] <= 0:
            del self.holdings[fund_code]  # 如果份额为0，删除该基金持仓记录
        self.cash += amount
        
        # 记录操作
        self._record_action('sell', {
            'fund_code': fund_code,
            'shares': shares_to_sell,
            'nav': nav,
            'amount': amount
        })
        
        return {
            'success': True,
            'message': f'成功卖出基金{fund_code}，份额{shares_to_sell:.2f}份，金额{amount:.2f}元',
            'shares': shares_to_sell,
            'amount': amount
        }
    
    def next_day(self):
        """推进到下一个交易日"""
        if self.is_simulation_over:
            return {
                'success': False,
                'message': '模拟已经结束'
            }
            
        # 记录前进操作
        self._record_action('next_day', {
            'from_date': self.current_date.strftime('%Y-%m-%d')
        })
        
        self.current_date_index += 1
        
        # 检查是否已到时间线末尾
        if self.current_date_index >= len(self.data['timeline']):
            self.is_simulation_over = True
            return {
                'success': True,
                'message': '模拟已经结束，已经到达最后一个交易日',
                'simulation_ended': True
            }
        
        # 更新当前日期
        self.current_date = self.data['timeline'][self.current_date_index]['date']
        
        return {
            'success': True,
            'message': f'进入下一个交易日: {self.current_date.strftime("%Y-%m-%d")}',
            'simulation_ended': False
        }
    
    def export_actions(self, output_file=None):
        """导出用户行为记录"""
        if not output_file:
            # 修改输出路径到save目录
            output_file = self.save_dir / f"user_actions_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # 计算最终收益率
        if self.net_worth_history:
            initial_assets = self.initial_capital
            final_assets = self.net_worth_history[-1]['total_assets']
            total_return = (final_assets - initial_assets) / initial_assets * 100
        else:
            total_return = 0
        
        # 准备输出数据
        output_data = {
            'simulation_info': {
                'start_date': self.data['timeline'][0]['date'].strftime('%Y-%m-%d') if self.data['timeline'] else None,
                'end_date': self.current_date.strftime('%Y-%m-%d') if self.current_date else None,
                'initial_capital': self.initial_capital,
                'final_assets': self.net_worth_history[-1]['total_assets'] if self.net_worth_history else self.initial_capital,
                'return_rate': total_return
            },
            'actions': [],
            'net_worth_history': [
                {
                    'date': item['date'].strftime('%Y-%m-%d'),
                    'cash': item['cash'],
                    'holdings_value': item['holdings_value'],
                    'total_assets': item['total_assets'],
                }
                for item in self.net_worth_history
            ],
        }
        
        # 处理用户行为记录，确保日期对象被序列化为字符串
        for action in self.user_actions:
            serialized_action = {
                'date': action['date'].strftime('%Y-%m-%d') if isinstance(action['date'], (datetime.date, datetime.datetime)) else action['date'],
                'action_type': action['action_type'],
                'timestamp': action['timestamp'],
                'cash_after': action['cash_after']
            }
            
            # 处理详情字段，检查其中是否包含日期对象
            details = action['details'].copy() if isinstance(action['details'], dict) else action['details']
            if isinstance(details, dict):
                for key, value in details.items():
                    if isinstance(value, (datetime.date, datetime.datetime)):
                        details[key] = value.strftime('%Y-%m-%d')
            
            serialized_action['details'] = details
            output_data['actions'].append(serialized_action)
        
        # 写入JSON文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        return {
            'success': True,
            'message': f'用户行为记录已导出到: {output_file}',
            'file_path': str(output_file),
            'stats': {
                'action_count': len(self.user_actions),
                'return_rate': total_return
            }
        }
    
    def get_performance_summary(self):
        """获取投资表现总结"""
        if not self.net_worth_history:
            return {
                'success': False,
                'message': '没有足够的数据生成投资表现总结'
            }
        
        # 计算关键指标
        initial_assets = self.initial_capital
        final_assets = self.net_worth_history[-1]['total_assets']
        total_return = (final_assets - initial_assets) / initial_assets * 100
        
        # 计算最大回撤
        max_drawdown = 0
        peak = 0
        
        for item in self.net_worth_history:
            asset = item['total_assets']
            if asset > peak:
                peak = asset
            drawdown = (peak - asset) / peak * 100 if peak > 0 else 0
            max_drawdown = max(max_drawdown, drawdown)
        
        # 统计交易次数
        buy_count = sum(1 for action in self.user_actions if action['action_type'] == 'buy')
        sell_count = sum(1 for action in self.user_actions if action['action_type'] == 'sell')
        
        # 获取市场基准表现（上证指数）
        market_performance = 0
        first_index = None
        last_index = None
        
        for day in self.data['timeline']:
            if 'sh_index' in day['funds'] and 'close' in day['funds']['sh_index']:
                if first_index is None:
                    first_index = day['funds']['sh_index']['close']
                    
                # 持续更新，直到最后一天
                last_index = day['funds']['sh_index']['close']
        
        if first_index and last_index:
            market_performance = (last_index - first_index) / first_index * 100
        
        return {
            'success': True,
            'summary': {
                'initial_capital': initial_assets,
                'final_assets': final_assets,
                'total_return': total_return,
                'market_return': market_performance,
                'outperformance': total_return - market_performance,
                'max_drawdown': max_drawdown,
                'trade_count': {
                    'buy': buy_count,
                    'sell': sell_count,
                    'total': buy_count + sell_count
                },
                'simulation_days': len(self.net_worth_history),
            }
        }

    def get_data_by_date(self, days_ago=0, target_date=None, fund_code=None):
        """
        获取特定日期的市场数据
        
        Args:
            days_ago: 当前日期前多少天
            target_date: 目标日期字符串 (YYYY-MM-DD)
            fund_code: 基金代码，如果指定则只返回该基金数据
            
        Returns:
            包含数据和状态的字典
        """
        try:
            # 确定目标日期
            if target_date:
                try:
                    target_date = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()
                except ValueError:
                    return {"success": False, "message": f"日期格式错误: {target_date}，请使用YYYY-MM-DD格式"}
                
                # 检查日期是否在模拟范围内
                timeline_start = self.data['timeline'][0]['date']
                timeline_end = self.data['timeline'][-1]['date']
                
                if target_date < timeline_start or target_date > timeline_end:
                    return {"success": False, "message": f"日期 {target_date} 超出模拟范围 ({timeline_start} 至 {timeline_end})"}
                
                # 查找最接近的交易日
                closest_day = None
                closest_diff = datetime.timedelta.max
                
                for day in self.data['timeline']:
                    day_date = day['date']
                    # 如果找到完全匹配的日期
                    if day_date == target_date:
                        closest_day = day
                        break
                    
                    # 如果是过去的日期，保存差距最小的
                    if day_date < target_date:
                        diff = target_date - day_date
                        if diff < closest_diff:
                            closest_diff = diff
                            closest_day = day
                
                if closest_day is None:
                    return {"success": False, "message": f"在 {target_date} 之前没有有效交易日"}
                
                date_data = closest_day
                
                # 如果不是精确匹配，提醒用户
                if date_data['date'] != target_date:
                    print(f"注意: {target_date} 不是有效交易日，显示最接近的交易日 {date_data['date']} 的数据")
                
            else:
                # 基于当前日期和days_ago计算
                if days_ago < 0:
                    return {"success": False, "message": "days_ago参数必须为非负数"}
                    
                if days_ago > self.current_date_index:
                    return {"success": False, "message": f"无法查看 {days_ago} 天前的数据，超出模拟开始日期"}
                
                date_data = self.data['timeline'][self.current_date_index - days_ago]
            
            # 构建结果数据
            result_data = {
                "date": date_data['date'].strftime("%Y-%m-%d"),
                "indices": {},
                "funds": {},
                "news": date_data['news']
            }
            
            # 添加所有数据
            for fund_code_key, fund_info in date_data['funds'].items():
                # 区分指数和基金
                if fund_code_key == 'sh_index':
                    result_data['indices']['上证指数'] = {
                        '收盘价': fund_info.get('close', '暂无'),
                        '涨跌幅': fund_info.get('change_pct', '暂无')
                    }
                elif fund_code_key == 'dj_index':
                    result_data['indices']['道琼斯指数'] = {
                        '收盘价': fund_info.get('close', '暂无'),
                        '涨跌幅': fund_info.get('change_pct', '暂无')
                    }
                # 如果指定了基金代码且不匹配，则跳过
                elif fund_code and fund_code_key != fund_code:
                    continue
                else:
                    result_data['funds'][fund_code_key] = {
                        '净值': fund_info.get('nav', '暂无'),
                        '涨跌幅': fund_info.get('change_pct', '暂无')
                    }
            
            # 如果指定了基金代码但结果中没有该基金数据
            if fund_code and fund_code not in result_data['funds']:
                return {"success": True, "data": result_data, "message": f"指定日期没有基金{fund_code}的数据"}
            
            return {"success": True, "data": result_data}
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"success": False, "message": f"获取数据时出错: {str(e)}"}
            
    def get_fund_history(self, fund_code, days=30):
        """
        获取基金或指数的历史数据
        
        Args:
            fund_code: 基金代码或指数代码
            days: 天数
            
        Returns:
            包含历史数据的字典
        """
        try:
            # 确定是基金还是指数
            is_index = False
            data_source = None
            display_name = fund_code
            
            # 处理基金代码，确保格式一致性
            fund_code = fund_code.strip()
            
            # 检查是否为指数
            if fund_code in ['sh_index', 'SH000001', '上证指数']:
                is_index = True
                fund_code = 'sh_index'
                data_source = self.data['indices']['sh_index']
                display_name = '上证指数'
            elif fund_code in ['dj_index', 'DJI', '道琼斯指数']:
                is_index = True
                fund_code = 'dj_index'
                data_source = self.data['indices']['dj_index']
                display_name = '道琼斯指数'
            # 检查是否为基金
            elif fund_code in self.data['funds']:
                data_source = self.data['funds'][fund_code]
            else:
                return {"success": False, "message": f"找不到基金或指数: {fund_code}"}
            
            # 获取当前日期
            current_date = self.data['timeline'][self.current_day_index]['date']
            
            # 找到当前日期在时间线中的位置
            current_idx = -1
            for i, day in enumerate(self.data['timeline']):
                if day['date'] == current_date:
                    current_idx = i
                    break
            
            # 限制天数不超过已有的数据
            days = min(days, current_idx + 1)
            
            # 收集历史数据
            history_data = []
            start_date = None
            end_date = current_date
            total_return = None
            
            for i in range(current_idx, current_idx - days, -1):
                if i < 0:
                    break
                    
                date = self.data['timeline'][i]['date']
                date_str = date.strftime("%Y-%m-%d")
                
                if not start_date:
                    start_date = date
                
                # 检查该日期是否有数据
                if date_str in data_source:
                    item = {
                        'date': date_str
                    }
                    
                    if is_index:
                        item['close'] = data_source[date_str]['close']
                        if 'change_pct' in data_source[date_str]:
                            item['change_pct'] = data_source[date_str]['change_pct']
                    else:
                        item['nav'] = data_source[date_str]['nav']
                        if 'change_pct' in data_source[date_str]:
                            item['change_pct'] = data_source[date_str]['change_pct']
                    
                    history_data.append(item)
            
            # 计算期间总收益率
            if len(history_data) >= 2:
                start_value = history_data[-1]['close' if is_index else 'nav']
                end_value = history_data[0]['close' if is_index else 'nav']
                
                if start_value > 0:
                    total_return = (end_value - start_value) / start_value * 100
            
            # 结果按日期排序
            history_data.sort(key=lambda x: x['date'], reverse=True)
            
            return {
                "success": True,
                "fund_code": fund_code,
                "display_name": display_name,
                "data": history_data,
                "start_date": start_date.strftime("%Y-%m-%d") if start_date else None,
                "end_date": end_date.strftime("%Y-%m-%d"),
                "total_return": total_return
            }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"success": False, "message": f"获取历史数据时出错: {str(e)}"}

    def import_history(self, history_file):
        """导入历史投资记录并恢复投资状态
        
        Args:
            history_file: 历史记录JSON文件路径
            
        Returns:
            操作结果字典
        """
        try:
            # 检查文件是否存在
            history_path = Path(history_file)
            if not history_path.exists():
                return {
                    'success': False,
                    'message': f'历史记录文件不存在: {history_file}'
                }
                
            # 读取历史记录
            with open(history_path, 'r', encoding='utf-8') as f:
                history_data = json.load(f)
                
            # 验证历史记录格式
            required_fields = ['simulation_info', 'actions', 'net_worth_history']
            for field in required_fields:
                if field not in history_data:
                    return {
                        'success': False,
                        'message': f'历史记录文件格式错误，缺少字段: {field}'
                    }
                    
            # 获取最后日期并找到对应的时间线索引
            last_date_str = history_data['simulation_info']['end_date']
            last_date = datetime.datetime.strptime(last_date_str, '%Y-%m-%d').date()
            
            # 查找日期在时间线上的位置
            found_idx = -1
            for i, day in enumerate(self.data['timeline']):
                if day['date'] == last_date:
                    found_idx = i
                    break
                    
            if found_idx == -1:
                return {
                    'success': False,
                    'message': f'无法在当前时间线中找到历史记录的结束日期: {last_date_str}'
                }
            
            # 恢复资金和持仓
            last_net_worth = history_data['net_worth_history'][-1]
            self.cash = last_net_worth['cash']
            
            # 恢复持仓（需要从操作记录中重建）
            self.holdings = {}
            for action in history_data['actions']:
                if action['action_type'] == 'buy':
                    fund_code = action['details']['fund_code']
                    shares = action['details']['shares']
                    self.holdings[fund_code] = self.holdings.get(fund_code, 0) + shares
                elif action['action_type'] == 'sell':
                    fund_code = action['details']['fund_code']
                    shares = action['details']['shares']
                    if fund_code in self.holdings:
                        self.holdings[fund_code] -= shares
                        if self.holdings[fund_code] <= 0:
                            del self.holdings[fund_code]
            
            # 设置当前日期和索引
            self.current_date_index = found_idx
            self.current_date = self.data['timeline'][found_idx]['date']
            
            # 导入历史操作
            self.user_actions = history_data['actions']
            
            # 重建净值历史
            self.net_worth_history = []
            for record in history_data['net_worth_history']:
                date_obj = datetime.datetime.strptime(record['date'], '%Y-%m-%d').date()
                self.net_worth_history.append({
                    'date': date_obj,
                    'cash': record['cash'],
                    'holdings_value': record['holdings_value'],
                    'total_assets': record['total_assets']
                })
                
            return {
                'success': True,
                'message': f'成功导入历史记录，从 {last_date_str} 继续投资',
                'cash': self.cash,
                'holdings': self.holdings,
                'total_actions': len(self.user_actions)
            }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'message': f'导入历史记录时出错: {str(e)}'
            }
