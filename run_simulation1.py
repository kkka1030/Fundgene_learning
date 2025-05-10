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

def main():
    parser = argparse.ArgumentParser(description="多场景基金投资模拟系统")
    parser.add_argument('--scene', type=str, choices=SCENE_CHOICES.keys(), required=True,
                        help="选择场景: 2008 | 2015 | 2020")
    parser.add_argument('--capital', type=float, default=100000, help="初始资金，默认100000元")
    parser.add_argument('--debug', action='store_true', help="启用调试模式")
    parser.add_argument('--import-file', type=str, help="导入历史投资记录文件路径")
    args = parser.parse_args()

    # 根目录结构
    current_dir = Path(__file__).parent
    scene_dir = Path(os.path.dirname(current_dir)) / "database" / "scene" / SCENE_CHOICES[args.scene]

    print(f"启动 {SCENE_CHOICES[args.scene]} 场景模拟...")
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



