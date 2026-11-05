async def calculator_tool(expression: str) -> dict:
    """
    执行金融数据分析相关的数学计算，包括基本运算和常用金融公式。
    
    参数:
        expression: 一个字符串表达式，如 "2 + 3 * 4" 或 "5000 * 0.05"。
                    也支持特殊命令格式：
                    - "percentage:100,25" 计算百分比(25/100 = 25%)
                    - "roi:1000,1200" 计算投资回报率((1200-1000)/1000 = 20%)
                    - "annualized:20,2" 计算年化收益率(对20%收益率按2年年化)
    
    返回:
        一个包含计算结果的字典 {"result": value}
    
    示例:
        calculator_tool("2 + 3") -> {"result": 5}
        calculator_tool("10 * 5 / 2") -> {"result": 25}
        calculator_tool("percentage:10000,2500") -> {"result": 25.0} (25%)
        calculator_tool("roi:1000,1200") -> {"result": 20.0} (20%的回报率)
        calculator_tool("annualized:20,2") -> {"result": 9.54} (2年内20%收益的年化率)
    """
    import re
    import math
    
    # 检查是否是特殊命令格式
    if ":" in expression:
        cmd, args = expression.split(":", 1)
        cmd = cmd.strip().lower()
        
        try:
            # 处理各种特殊金融计算命令
            if cmd == "percentage":
                # 计算百分比
                total, part = map(float, args.split(","))
                return {"result": (part / total) * 100 if total != 0 else 0}
                
            elif cmd == "roi":
                # 计算投资回报率 (Return on Investment)
                initial, final = map(float, args.split(","))
                return {"result": ((final - initial) / initial) * 100 if initial != 0 else 0}
                
            elif cmd == "annualized":
                # 计算年化收益率
                total_return, years = map(float, args.split(","))
                return {"result": (math.pow(1 + total_return/100, 1/years) - 1) * 100 if years > 0 else 0}
                
            elif cmd == "compound":
                # 计算复利
                principal, rate, years = map(float, args.split(","))
                rate = rate / 100  # 转换为小数
                return {"result": principal * math.pow(1 + rate, years)}
                
            elif cmd == "weighted_avg":
                # 计算加权平均值
                values_weights = args.split(";")
                total_weight = 0
                weighted_sum = 0
                for vw in values_weights:
                    value, weight = map(float, vw.split(","))
                    weighted_sum += value * weight
                    total_weight += weight
                return {"result": weighted_sum / total_weight if total_weight != 0 else 0}
                
            elif cmd == "asset_allocation":
                # 计算资产配置百分比
                values = list(map(float, args.split(",")))
                total = sum(values)
                percentages = [round((v / total) * 100, 2) if total != 0 else 0 for v in values]
                return {"result": percentages}
                
            else:
                return {"error": f"未知的特殊命令: {cmd}"}
                
        except Exception as e:
            return {"error": f"特殊命令计算错误: {str(e)}"}
    
    # 处理常规数学表达式
    try:
        # 创建安全的数学环境
        safe_globals = {
            "abs": abs, "round": round, "max": max, "min": min,
            "sum": sum, "pow": pow, 
            "sqrt": math.sqrt, "log": math.log, "log10": math.log10,
            "exp": math.exp, "floor": math.floor, "ceil": math.ceil,
            "sin": math.sin, "cos": math.cos, "tan": math.tan,
            "pi": math.pi, "e": math.e
        }
        
        # 验证表达式安全性
        if re.search(r"[^0-9\s\.\+\-\*\/\(\)\,\%\^]", re.sub(r"\b(abs|round|max|min|sum|pow|sqrt|log|log10|exp|floor|ceil|sin|cos|tan|pi|e)\b", "", expression)):
            return {"error": "表达式包含不允许的字符或函数"}
            
        # 预处理: 将^转换为**
        expression = expression.replace("^", "**")
        
        # 使用eval计算, 但提供有限的全局变量
        result = eval(expression, {"__builtins__": {}}, safe_globals)
        
        # 对结果进行格式化处理
        if isinstance(result, (int, float, complex)):
            # 对浮点数进行四舍五入到小数点后两位
            if isinstance(result, float):
                result = round(result, 2)
            return {"result": result}
        elif isinstance(result, (list, tuple)):
            # 如果结果是列表或元组，对每个元素进行格式化
            formatted = [round(x, 2) if isinstance(x, float) else x for x in result]
            return {"result": formatted}
        else:
            return {"error": "计算结果类型不支持"}
            
    except ZeroDivisionError:
        return {"error": "除数不能为零"}
    except ValueError as e:
        return {"error": f"值错误: {str(e)}"}
    except SyntaxError:
        return {"error": "表达式语法错误"}
    except NameError as e:
        return {"error": f"未知函数或变量: {str(e)}"}
    except Exception as e:
        return {"error": f"计算错误: {str(e)}"}
    
import json
from collections import defaultdict
from datetime import datetime

def calculate_portfolio_analysis(investment_data_str):
    """
    分析投资记录数据并返回资产配置比例和投资表现分析结果
    
    Args:
        investment_data_str (str): 包含投资记录的JSON字符串，可能包含三引号(''')和语言标记(如json)
    
    Returns:
        str: 格式化的JSON字符串，包含资产配置比例和投资表现分析
        
    数据格式说明:
        输入数据格式:
        {
            "user_info": {
                "user_id": "用户ID",
                "username": "用户名",
                "risk_tolerance": "风险承受能力", // 低、中、高
                "investment_goal": "投资目标",   // 如: 退休规划、教育金等
                "investment_preference": "投资偏好" // 如: 收入型、成长型等
            },
            "investment_records": [
                {
                    "behavior_id": "行为ID",
                    "fund_info": {
                        "fund_id": "基金ID",
                        "fund_name": "基金名称",
                        "fund_code": "基金代码",
                        "fund_type": "基金类型", // 股票型、债券型、货币市场型等
                        "risk_level": "风险等级", // 低、中低、中、中高、高
                        "current_nav": "当前净值" // 数值类型
                    },
                    "transaction_info": {
                        "action_type": "操作类型", // 申购、赎回、定投等
                        "amount": "交易金额",     // 数值类型
                        "timestamp": "交易时间",  // 格式: "YYYY-MM-DD HH:MM:SS"
                        "nav_price": "交易时净值", // 数值类型
                        "fund_shares": "交易份额", // 数值类型
                        "platform": "交易平台",   // 如: 银行APP、支付宝等
                        "transaction_status": "交易状态" // 如: 已确认、处理中等
                    }
                }
            ]
        }
        
        输出数据格式:
        {
            "资产配置比例": {
                "按资产类型": {
                    "股票": 百分比,
                    "债券": 百分比,
                    "现金": 百分比,
                    "其他": 百分比
                },
                "按风险等级": {
                    "低风险": 百分比,
                    "中风险": 百分比,
                    "高风险": 百分比
                },
                "按投资平台": {
                    "平台名称1": 百分比,
                    "平台名称2": 百分比,
                    ...
                },
                "总资产价值": 数值
            },
            "投资表现": {
                "总收益率": 百分比,
                "年化收益率": 百分比,
                "波动率": 百分比
            }
        }
    """
    # 清理输入字符串中可能的格式标记
    if isinstance(investment_data_str, str):
        # 处理三引号标记
        if investment_data_str.startswith("'''") and investment_data_str.endswith("'''"):
            investment_data_str = investment_data_str[3:-3]
        
        # 处理语言标记（如'json'）
        lines = investment_data_str.split('\n', 1)
        if len(lines) > 0 and (lines[0].strip() == 'json' or lines[0].strip() == 'JSON'):
            investment_data_str = lines[1] if len(lines) > 1 else ""
    
    # 确保字符串是有效的JSON
    try:
        investment_data_str = investment_data_str.strip()
        investment_data = json.loads(investment_data_str)
    except json.JSONDecodeError as e:
        return json.dumps({"error": f"JSON解析错误: {str(e)}"}, ensure_ascii=False, indent=4)
    
    # 获取用户信息和投资记录
    user_info = investment_data.get("user_info", {})
    investment_records = investment_data.get("investment_records", [])
    
    if not investment_records:
        return json.dumps({"error": "未找到投资记录"}, ensure_ascii=False, indent=4)
    
    # 初始化计算变量
    total_asset_value = 0.0                       # 总资产价值
    asset_type_values = defaultdict(float)        # 按资产类型分类的金额
    risk_level_values = defaultdict(float)        # 按风险等级分类的金额
    platform_values = defaultdict(float)          # 按投资平台分类的金额
    earliest_date = datetime.now()                # 最早的交易日期
    total_initial_investment = 0.0                # 总初始投资金额
    
    # 基金类型到资产类型的映射
    fund_type_to_asset = {
        "股票型": "股票",
        "混合型": "股票",
        "指数型": "股票",
        "QDII": "其他",
        "债券型": "债券",
        "货币市场型": "现金",
        "ETF": "股票"
    }
    
    # 风险等级映射
    risk_level_map = {
        "低": "低风险",
        "中低": "低风险",
        "中": "中风险",
        "中高": "中风险",
        "高": "高风险"
    }
    
    # 处理每条投资记录
    for record in investment_records:
        # 提取基金信息和交易信息
        fund_info = record.get("fund_info", {})
        transaction_info = record.get("transaction_info", {})
        
        # 安全地提取需要的数值，提供默认值防止数据异常
        current_nav = float(fund_info.get("current_nav", 0))
        fund_shares = float(transaction_info.get("fund_shares", 0))
        
        # 计算当前持仓价值 (当前净值 * 持有份额)
        current_value = current_nav * fund_shares
        total_asset_value += current_value
        
        # 按资产类型分类
        fund_type = fund_info.get("fund_type", "其他")
        asset_type = fund_type_to_asset.get(fund_type, "其他")
        asset_type_values[asset_type] += current_value
        
        # 按风险等级分类
        risk_level_raw = fund_info.get("risk_level", "中")
        risk_level = risk_level_map.get(risk_level_raw, "中风险")
        risk_level_values[risk_level] += current_value
        
        # 按投资平台分类
        platform = transaction_info.get("platform", "未知平台")
        platform_values[platform] += current_value
        
        # 记录交易日期和初始投资金额
        try:
            timestamp = transaction_info.get("timestamp", "")
            if timestamp:
                transaction_date = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                if transaction_date < earliest_date:
                    earliest_date = transaction_date
        except ValueError:
            # 忽略日期格式错误
            pass
        
        # 累计初始投资金额
        amount = float(transaction_info.get("amount", 0))
        total_initial_investment += amount
    
    # 计算各类资产占比百分比
    asset_type_percentage = {}
    risk_level_percentage = {}
    platform_percentage = {}
    
    if total_asset_value > 0:
        # 按资产类型计算百分比
        asset_type_percentage = {
            asset: round(100 * value / total_asset_value)
            for asset, value in asset_type_values.items()
        }
        
        # 按风险等级计算百分比
        risk_level_percentage = {
            level: round(100 * value / total_asset_value)
            for level, value in risk_level_values.items()
        }
        
        # 按投资平台计算百分比
        platform_percentage = {
            platform: round(100 * value / total_asset_value)
            for platform, value in platform_values.items()
        }
    
    # 确保所有标准资产类型都存在于结果中
    for asset_type in ["股票", "债券", "现金", "其他"]:
        if asset_type not in asset_type_percentage:
            asset_type_percentage[asset_type] = 0
    
    # 确保所有风险等级都存在于结果中
    for risk_level in ["低风险", "中风险", "高风险"]:
        if risk_level not in risk_level_percentage:
            risk_level_percentage[risk_level] = 0
    
    # 计算投资表现指标
    # 1. 投资持有时间(天)
    days_invested = max(1, (datetime.now() - earliest_date).days)
    
    # 2. 总收益率 = (当前总价值 - 总投入) / 总投入 * 100%
    if total_initial_investment > 0:
        total_return_rate = ((total_asset_value / total_initial_investment) - 1) * 100
    else:
        total_return_rate = 0
    
    # 3. 年化收益率 = 总收益率 * (365 / 持有天数)
    annualized_return_rate = total_return_rate * (365 / days_invested)
    
    # 4. 波动率 (这里使用简化的计算方法，实际应基于历史净值数据计算标准差)
    # 由于我们没有足够的历史数据，此处使用预设值或基于风险分布的估算
    if "高风险" in risk_level_percentage and risk_level_percentage["高风险"] > 50:
        volatility = 18.5  # 高风险配置的波动率较高
    elif "低风险" in risk_level_percentage and risk_level_percentage["低风险"] > 50:
        volatility = 5.8   # 低风险配置的波动率较低
    else:
        volatility = 12.3  # 中等风险的默认波动率
    
    # 构建结果字典
    result = {
        "资产配置比例": {
            "按资产类型": asset_type_percentage,
            "按风险等级": risk_level_percentage,
            "按投资平台": platform_percentage,
            "总资产价值": round(total_asset_value, 2)
        },
        "投资表现": {
            "总收益率": round(total_return_rate, 1),
            "年化收益率": round(annualized_return_rate, 1),
            "波动率": round(volatility, 1)
        }
    }
    
    # 转换为JSON字符串并返回
    return json.dumps(result, ensure_ascii=False, indent=4)

# 测试函数
if __name__ == "__main__":
    # 可以将示例数据传入函数进行测试
    with open("example_investment_data.json", "r", encoding="utf-8") as f:
        example_data = f.read()
    
    result = calculate_portfolio_analysis(example_data)
    print(result)