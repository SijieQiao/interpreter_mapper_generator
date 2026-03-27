#!/usr/bin/env python3
"""
移动AI DataVault文件到新文件夹
"""

import os
import shutil
from pathlib import Path

def move_files():
    """移动文件到新文件夹"""

    project_root = Path(__file__).parent

    # 创建新文件夹
    ai_folder = project_root / 'ai_datavault'
    ai_folder.mkdir(exist_ok=True)

    templates_folder = ai_folder / 'templates'
    templates_folder.mkdir(exist_ok=True)

    print("创建文件夹结构...")
    print(f"AI DataVault文件夹: {ai_folder}")
    print(f"模板文件夹: {templates_folder}")

    # 需要移动的文件
    files_to_move = {
        'ai_datavault_generator.py': ai_folder,
        'run_ai_app.py': ai_folder,
        'start_ai_app.py': ai_folder,
        'LAUNCH.py': ai_folder,
        'diagnose.py': ai_folder,
        'test_app_startup.py': ai_folder,
        'test_new_interface.py': ai_folder,
        'AI_GENERATOR_README.md': ai_folder,
        'debug_svg.html': ai_folder,
        'DIAGNOSIS.md': ai_folder,
        'check_deps.py': ai_folder,
        'install_deps.py': ai_folder,
        'test_api_key.py': ai_folder,
        'API_KEY_SETUP.md': ai_folder,
        'sample_requirements.txt': ai_folder,
    }

    # 移动ai_templates的内容
    old_templates = project_root / 'ai_templates'
    if old_templates.exists():
        print("\n移动模板文件...")
        for file_path in old_templates.glob('*'):
            if file_path.is_file():
                shutil.move(str(file_path), str(templates_folder / file_path.name))
                print(f"✓ 移动: ai_templates/{file_path.name}")

        # 删除空的旧文件夹
        try:
            old_templates.rmdir()
            print("✓ 删除旧模板文件夹")
        except:
            print("⚠️ 无法删除旧模板文件夹")

    # 移动其他文件
    print("\n移动主文件...")
    for filename, target_dir in files_to_move.items():
        src = project_root / filename
        dst = target_dir / filename

        if src.exists():
            shutil.move(str(src), str(dst))
            print(f"✓ 移动: {filename}")
        else:
            print(f"⚠️ 文件不存在: {filename}")

    # 创建__init__.py
    init_file = ai_folder / '__init__.py'
    init_file.write_text('"""AI DataVault 模块"""')
    print("✓ 创建 __init__.py")

    # 创建新的启动脚本
    new_launch = ai_folder / 'launch.py'
    new_launch.write_text('''#!/usr/bin/env python3
"""
AI DataVault 启动脚本
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 导入并运行
from ai_datavault.ai_datavault_generator import main
main()
''')
    print("✓ 创建新的启动脚本")

    print("
🎉 文件移动完成！"    print(f"新文件夹: {ai_folder}")
    print("\n启动方式:")
    print("python ai_datavault/launch.py")

if __name__ == "__main__":
    move_files()