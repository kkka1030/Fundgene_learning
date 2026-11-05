#!/usr/bin/env python3
import os
import sys
import argparse
from pathlib import Path
from simulation_app import SimulationApp

SCENE_CHOICES = {
    "2008": "2008金融危机",
    "2015": "2015年中国股灾",
    "2020": "2020年疫情冲击"
}

def select_scene():
    """交互式选择场景"""
    print("\n" + "="*70)
    print("欢迎使用多场景基金投资模拟系统".center(60))
    print("="*70)
    print("\n请选择要模拟的历史场景：")
    for code, name in SCENE_CHOICES.items():
        print(f"{code}. {name}")
    
    while True:
        choice = input("\n请输入场景编号 (2008/2015/2020): ").strip()
        if choice in SCENE_CHOICES:
            return choice
        print("输入无效，请重新选择")

def main():
    parser = argparse.ArgumentParser(description="多场景基金投资模拟系统")
    parser.add_argument('--capital', type=float, default=100000, help="初始资金，默认100000元")
    parser.add_argument('--debug', action='store_true', help="启用调试模式")
    parser.add_argument('--import-file', type=str, help="导入历史投资记录文件路径")
    args = parser.parse_args()

    # 交互式选择场景
    scene_code = select_scene()

    # 获取场景目录路径
    current_dir = Path(__file__).parent
    database_dir = Path(os.path.dirname(current_dir)) / "database" / "scene"
    scene_dir = database_dir / SCENE_CHOICES[scene_code]

    # 检查场景目录是否存在
    if not scene_dir.exists():
        print(f"错误：场景目录不存在: {scene_dir}")
        print("请确保以下目录存在并包含必要的数据文件：")
        for scene_code, scene_name in SCENE_CHOICES.items():
            expected_path = database_dir / scene_name
            print(f"- {scene_name}: {expected_path}")
        sys.exit(1)

    print(f"\n启动 {SCENE_CHOICES[scene_code]} 场景模拟...")
    print(f"数据目录: {scene_dir}")

    try:
        app = SimulationApp(scene_dir, args.capital)

        if args.import_file:
            print(f"正在导入历史记录: {args.import_file}")
            import_result = app.simulator.import_history(args.import_file)
            if import_result['success']:
                print(f"成功导入历史记录，从 {import_result.get('last_date', '未知日期')} 继续投资")
            else:
                print(f"导入历史记录失败: {import_result['message']}")
                return

        app.start()
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"\n程序遇到错误: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()

    print("\n感谢使用多场景基金投资模拟系统！")

if __name__ == "__main__":
    main()



