import os
import argparse
import datetime
from pathlib import Path
from investment_simulator import InvestmentSimulator

class SimulationApp:
    def __init__(self, scene_path, initial_capital=100000):
        """
        初始化模拟器应用
        
        Args:
            scene_path: 场景数据目录路径
            initial_capital: 初始资金
        """
        self.scene_path = Path(scene_path)
        # 创建保存目录
        self.save_dir = self.scene_path / "save"
        os.makedirs(self.save_dir, exist_ok=True)
        
        self.simulator = InvestmentSimulator(scene_path, initial_capital)
        self.running = True
        
    def start(self):
        """启动模拟器应用"""
        self._print_welcome()
        self._print_scene_description()
        
        while self.running:
            state = self.simulator.get_current_state()
            
            if state['status'] == 'ended':
                print("\n模拟已结束，谢谢参与！")
                self._show_summary()
                self.running = False
                break
                
            self._display_state(state)
            self._process_command()
    
    def _print_welcome(self):
        """打印欢迎信息"""
        print("\n" + "="*70)
        print("欢迎使用 2008 金融危机基金投资模拟系统".center(60))
        print("="*70)
        print(f"初始资金: {self.simulator.initial_capital:,.2f} 元")
        print(f"模拟开始日期: {self.simulator.data['timeline'][0]['date'].strftime('%Y年%m月%d日')}")
        print(f"模拟结束日期: {self.simulator.data['timeline'][-1]['date'].strftime('%Y年%m月%d日')}")
        print("="*70)
        print("输入 'help' 查看命令帮助")
        print("="*70 + "\n")
    
    def _print_scene_description(self):
        """打印场景描述"""
        description = self.simulator.data['description']
        print("\n" + "="*70)
        print("场景背景".center(64))
        print("="*70)
        print(description)
        print("="*70 + "\n")
        input("按回车键开始模拟...")
    
    def _display_state(self, state):
        """显示当前状态"""
        print("\n" + "="*70)
        print(f"当前日期: {state['date']}".center(60))
        print("="*70)
        
        # 显示市场指数
        if 'indices' in state and state['indices']:
            print("\n市场指数:")
            print("-"*70)
            for name, data in state['indices'].items():
                change_str = ""
                if '涨跌幅' in data and data['涨跌幅'] != '暂无':
                    change = float(data['涨跌幅'])
                    change_color = '\033[92m' if change > 0 else '\033[91m' if change < 0 else ''
                    change_str = f"{change_color}{change:+.2f}%\033[0m"
                print(f"{name}: {data['收盘价']} {change_str}")
        
        # 显示新闻
        if 'news' in state and state['news']:
            print("\n今日新闻:")
            print("-"*70)
            for news in state['news']:
                print(f"- {news}")
        
        # 显示用户资产
        print("\n我的资产:")
        print("-"*70)
        print(f"现金: {state['cash']:,.2f} 元")
        
        if 'holdings' in state and state['holdings']:
            print("\n持仓基金:")
            print("{:<10} {:<15} {:<15} {:<15} {:<15}".format("基金代码", "持有份额", "单位净值", "持仓价值", "日涨跌幅"))
            print("-"*70)
            for holding in state['holdings']:
                change_color = ''
                if holding.get('change_pct', 0) > 0:
                    change_color = '\033[92m'  # 绿色
                elif holding.get('change_pct', 0) < 0:
                    change_color = '\033[91m'  # 红色
                    
                change_str = f"{change_color}{holding.get('change_pct', 0):+.2f}%\033[0m"
                print("{:<10} {:<15.2f} {:<15.4f} {:<15,.2f} {:<15}".format(
                    holding['fund_code'], 
                    holding['shares'], 
                    holding['nav'], 
                    holding['value'],
                    change_str
                ))
        
        print(f"\n总资产: {state['total_assets']:,.2f} 元")
        print(f"收益率: {(state['total_assets'] - self.simulator.initial_capital) / self.simulator.initial_capital * 100:+.2f}%")
        
        # 显示可交易基金
        if 'funds' in state and state['funds']:
            print("\n可交易基金:")
            print("{:<10} {:<15} {:<15}".format("基金代码", "单位净值", "日涨跌幅"))
            print("-"*70)
            for code, data in state['funds'].items():
                change_str = ""
                if '涨跌幅' in data and data['涨跌幅'] != '暂无':
                    change = float(data['涨跌幅'])
                    change_color = '\033[92m' if change > 0 else '\033[91m' if change < 0 else ''
                    change_str = f"{change_color}{change:+.2f}%\033[0m"
                print("{:<10} {:<15} {:<15}".format(code, data['净值'], change_str))
        
        print("="*70)
    
    def _process_command(self):
        """处理用户命令"""
        command = input("\n请输入命令: ").strip().lower()
        
        if command == 'exit' or command == 'quit':
            self._show_summary()
            self.running = False
            return
        
        if command == 'help':
            self._show_help()
            return
        
        if command == 'next' or command == 'n':
            result = self.simulator.next_day()
            print(result['message'])
            return
        
        if command.startswith('buy '):
            try:
                parts = command.split()
                if len(parts) != 3:
                    print("格式错误。正确格式: buy 基金代码 金额")
                    return
                    
                fund_code = parts[1]
                amount = float(parts[2])
                
                result = self.simulator.buy_fund(fund_code, amount)
                print(result['message'])
            except ValueError:
                print("金额必须是有效的数字")
            return
        
        if command.startswith('sell '):
            try:
                parts = command.split()
                if len(parts) != 3:
                    print("格式错误。正确格式: sell 基金代码 份额/百分比")
                    return
                    
                fund_code = parts[1]
                value = parts[2]
                
                # 判断是份额还是百分比
                if value.endswith('%'):
                    percentage = float(value.rstrip('%')) / 100
                    result = self.simulator.sell_fund(fund_code, percentage=percentage)
                else:
                    shares = float(value)
                    result = self.simulator.sell_fund(fund_code, shares=shares)
                
                print(result['message'])
            except ValueError:
                print("份额/百分比必须是有效的数字")
            return
        
        # 查看指定日期的基金数据
        if command.startswith('check '):
            parts = command.split()
            
            if len(parts) == 2 and parts[1] == 'market':
                # 查看当前市场数据
                result = self.simulator.get_data_by_date()
                if result['success']:
                    self._display_market_data(result['data'])
                else:
                    print(result['message'])
                return
                
            if len(parts) >= 2:
                fund_code = parts[1].strip()
                
                # 检查是否指定日期
                if len(parts) >= 3 and parts[2] != 'history':
                    date_str = parts[2]
                    result = self.simulator.get_data_by_date(target_date=date_str, fund_code=fund_code)
                    if result['success']:
                        self._display_fund_data(result['data'], fund_code)
                    else:
                        print(result['message'])
                    return
                
                # 检查是否查询历史
                if len(parts) >= 3 and parts[2] == 'history':
                    days = 30  # 默认显示30天
                    
                    if len(parts) >= 4:
                        try:
                            days = int(parts[3])
                        except ValueError:
                            print(f"天数格式错误: {parts[3]}，使用默认值30")
                    
                    result = self.simulator.get_fund_history(fund_code, days=days)
                    if result['success']:
                        self._display_fund_history(result)
                    else:
                        print(result['message'])
                    return
                
                # 默认查看当前日期的数据
                result = self.simulator.get_data_by_date(fund_code=fund_code)
                if result['success']:
                    self._display_fund_data(result['data'], fund_code)
                else:
                    print(result['message'])
                return
        
        # 查看前几天的数据
        if command.startswith('history '):
            parts = command.split()
            
            if len(parts) >= 2:
                try:
                    days_ago = int(parts[1])
                    
                    # 如果指定了基金代码
                    fund_code = None
                    if len(parts) >= 3:
                        fund_code = parts[2]
                    
                    result = self.simulator.get_data_by_date(days_ago=days_ago, fund_code=fund_code)
                    if result['success']:
                        self._display_market_data(result['data'])
                    else:
                        print(result['message'])
                except ValueError:
                    print(f"天数格式错误: {parts[1]}")
            return
        
        if command == 'export':
            result = self.simulator.export_actions()
            print(result['message'])
            return
        
        if command == 'summary':
            self._show_summary()
            return
        
        if command == 'reset':
            confirm = input("确定要重置模拟吗? (y/n): ").strip().lower()
            if confirm == 'y':
                self.simulator.reset_simulation()
                print("模拟已重置到初始状态")
            return
        
        # 新增命令：导入历史记录
        if command == 'import' or command.startswith('import '):
            parts = command.split(maxsplit=1)
            if len(parts) == 1:
                # 显示导入对话框
                self._show_import_dialog()
            else:
                # 直接导入指定文件
                file_path = parts[1].strip()
                self._import_history(file_path)
            return
        
        print("无效命令。输入 'help' 查看帮助。")
    
    def _display_market_data(self, data):
        """显示市场数据"""
        print("\n" + "="*70)
        print(f"市场数据 - {data['date']}".center(60))
        print("="*70)
        
        # 显示市场指数
        if 'indices' in data and data['indices']:
            print("\n市场指数:")
            print("-"*70)
            for name, index_data in data['indices'].items():
                change_str = ""
                if '涨跌幅' in index_data and index_data['涨跌幅'] != '暂无':
                    change = float(index_data['涨跌幅'])
                    change_color = '\033[92m' if change > 0 else '\033[91m' if change < 0 else ''
                    change_str = f"{change_color}{change:+.2f}%\033[0m"
                print(f"{name}: {index_data['收盘价']} {change_str}")
        
        # 显示基金数据
        if 'funds' in data and data['funds']:
            print("\n基金数据:")
            print("{:<10} {:<15} {:<15}".format("基金代码", "净值", "日涨跌幅"))
            print("-"*70)
            for code, fund_data in data['funds'].items():
                change_str = ""
                if '涨跌幅' in fund_data and fund_data['涨跌幅'] != '暂无':
                    try:
                        change = float(fund_data['涨跌幅'])
                        change_color = '\033[92m' if change > 0 else '\033[91m' if change < 0 else ''
                        change_str = f"{change_color}{change:+.2f}%\033[0m"
                    except (ValueError, TypeError):
                        change_str = fund_data['涨跌幅']
                print("{:<10} {:<15} {:<15}".format(code, fund_data['净值'], change_str))
        
        # 显示新闻
        if 'news' in data and data['news']:
            print("\n新闻:")
            print("-"*70)
            for news in data['news']:
                print(f"- {news}")
        
        print("="*70)
    
    def _display_fund_data(self, data, fund_code):
        """显示特定基金数据"""
        print("\n" + "="*70)
        print(f"基金 {fund_code} - {data['date']}".center(60))
        print("="*70)
        
        # 检查基金数据是否存在
        if 'funds' in data and fund_code in data['funds']:
            fund_data = data['funds'][fund_code]
            
            print("\n基金信息:")
            print("-"*70)
            
            change_str = ""
            if '涨跌幅' in fund_data and fund_data['涨跌幅'] != '暂无':
                try:
                    change = float(fund_data['涨跌幅'])
                    change_color = '\033[92m' if change > 0 else '\033[91m' if change < 0 else ''
                    change_str = f"{change_color}{change:+.2f}%\033[0m"
                except (ValueError, TypeError):
                    change_str = fund_data['涨跌幅']
                    
            print(f"基金代码: {fund_code}")
            print(f"单位净值: {fund_data['净值']}")
            print(f"日涨跌幅: {change_str}")
        else:
            print(f"指定日期没有基金{fund_code}的数据")
        
        # 显示当天的市场指数作为参考
        if 'indices' in data and data['indices']:
            print("\n当日市场指数:")
            print("-"*70)
            for name, index_data in data['indices'].items():
                change_str = ""
                if '涨跌幅' in index_data and index_data['涨跌幅'] != '暂无':
                    try:
                        change = float(index_data['涨跌幅'])
                        change_color = '\033[92m' if change > 0 else '\033[91m' if change < 0 else ''
                        change_str = f"{change_color}{change:+.2f}%\033[0m"
                    except (ValueError, TypeError):
                        change_str = index_data['涨跌幅']
                print(f"{name}: {index_data['收盘价']} {change_str}")
        
        print("="*70)
    
    def _display_fund_history(self, result):
        """显示基金历史数据"""
        print("\n" + "="*70)
        print(f"基金 {result['fund_code']} 历史数据".center(60))
        print(f"时间范围: {result['start_date']} 至 {result['end_date']}".center(60))
        if result['total_return'] is not None:
            print(f"期间总收益率: {result['total_return']:+.2f}%".center(60))
        print("="*70)
        
        # 创建表格并显示历史数据
        print("{:<12} {:<15} {:<15}".format("日期", "净值/收盘价", "日涨跌幅"))
        print("-"*70)
        
        # 判断是否为指数
        is_index = result['fund_code'] in ['sh_index', 'dj_index']
        
        for item in result['data']:
            date = item['date']
            
            # 根据是否为指数显示不同字段
            if is_index:
                value = item.get('close', '暂无')
            else:
                value = item.get('nav', '暂无')
                
            change_pct = item.get('change_pct', 0)
            
            # 为涨跌幅添加颜色
            if change_pct is not None:
                change_color = '\033[92m' if change_pct > 0 else '\033[91m' if change_pct < 0 else ''
                change_str = f"{change_color}{change_pct:+.2f}%\033[0m"
            else:
                change_str = '暂无'
                
            print("{:<12} {:<15} {:<15}".format(date, value, change_str))
        
        print("="*70)
    
    def _show_help(self):
        """显示帮助信息"""
        print("\n可用命令:")
        print("-"*70)
        print("help       - 显示帮助信息")
        print("next (n)   - 进入下一个交易日")
        print("buy 基金代码 金额  - 购买基金")
        print("sell 基金代码 份额  - 卖出指定份额的基金")
        print("sell 基金代码 百分比% - 卖出指定百分比的基金")
        print("check market  - 查看当前市场状况")
        print("check 基金代码  - 查看指定基金当前数据")
        print("check 基金代码 YYYY-MM-DD  - 查看指定日期的基金数据")
        print("check 基金代码 history [天数]  - 查看基金历史数据（默认30天）")
        print("history 天数 [基金代码]  - 查看N天前的市场或基金数据")
        print("summary    - 显示投资表现总结")
        print("export     - 导出用户行为记录")
        print("reset      - 重置模拟")
        print("import     - 显示并导入历史投资记录")
        print("import 文件路径 - 导入指定的历史记录文件")
        print("exit/quit  - 退出模拟")
        print("-"*70)
    
    def _show_summary(self):
        """显示投资表现总结"""
        result = self.simulator.get_performance_summary()
        
        if not result['success']:
            print(result['message'])
            return
        
        summary = result['summary']
        
        print("\n" + "="*70)
        print("投资表现总结".center(60))
        print("="*70)
        print(f"初始资金: {summary['initial_capital']:,.2f} 元")
        print(f"最终资产: {summary['final_assets']:,.2f} 元")
        print(f"总收益率: {summary['total_return']:+.2f}%")
        print(f"市场收益率(上证指数): {summary['market_return']:+.2f}%")
        print(f"超额收益: {summary['outperformance']:+.2f}%")
        print(f"最大回撤: {summary['max_drawdown']:.2f}%")
        print(f"交易次数: {summary['trade_count']['total']} (买入: {summary['trade_count']['buy']}, 卖出: {summary['trade_count']['sell']})")
        print(f"模拟天数: {summary['simulation_days']} 天")
        print("="*70)
        
        # 导出结果
        export = input("\n是否导出投资记录? (y/n): ").strip().lower()
        if export == 'y':
            # 指定保存到save目录
            output_file = self.save_dir / f"investment_summary_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            result = self.simulator.export_actions(output_file)
            print(result['message'])
    
    def _show_import_dialog(self):
        """显示导入历史记录对话框"""
        # 列出save目录下的所有JSON文件
        json_files = list(self.save_dir.glob('*.json'))
        
        if not json_files:
            print("没有找到可导入的历史记录文件")
            return
            
        print("\n可导入的历史记录文件:")
        print("-"*70)
        for i, file_path in enumerate(json_files, 1):
            # 显示文件修改时间和大小
            mod_time = datetime.datetime.fromtimestamp(file_path.stat().st_mtime)
            size_kb = file_path.stat().st_size / 1024
            print(f"{i}. {file_path.name} ({size_kb:.1f}KB, {mod_time.strftime('%Y-%m-%d %H:%M:%S')})")
        
        print("\n请选择要导入的文件编号，或输入完整路径，或输入q取消")
        choice = input("选择: ").strip()
        
        if choice.lower() == 'q':
            return
            
        # 处理用户输入
        try:
            choice_num = int(choice)
            if 1 <= choice_num <= len(json_files):
                file_path = json_files[choice_num - 1]
                self._import_history(str(file_path))
            else:
                print(f"无效的选项: {choice_num}，有效范围是1-{len(json_files)}")
        except ValueError:
            # 如果不是数字，当作文件路径处理
            self._import_history(choice)

    def _import_history(self, file_path):
        """导入历史记录"""
        # 确认操作
        print(f"\n即将导入历史记录: {file_path}")
        print("注意: 导入后将覆盖当前的投资状态，此操作不可撤销")
        confirm = input("确定要继续吗? (y/n): ").strip().lower()
        
        if confirm != 'y':
            print("已取消导入")
            return
        
        # 执行导入
        result = self.simulator.import_history(file_path)
        
        if result['success']:
            print("\n" + "="*70)
            print("历史记录导入成功".center(60))
            print("="*70)
            print(f"资金: {result['cash']:,.2f} 元")
            
            if result['holdings']:
                print("\n持仓:")
                for fund_code, shares in result['holdings'].items():
                    print(f"  - {fund_code}: {shares:,.2f} 份")
                    
            print(f"\n共导入 {result['total_actions']} 条操作记录")
            print("="*70)
        else:
            print(f"\n导入失败: {result['message']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="2008金融危机基金投资模拟系统")
    parser.add_argument('--capital', type=float, default=100000, help="初始资金，默认100000元")
    args = parser.parse_args()
    
    # 获取当前脚本所在目录
    current_dir = Path(__file__).parent
    
    app = SimulationApp(current_dir, args.capital)
    app.start()
