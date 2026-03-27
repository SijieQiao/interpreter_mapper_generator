#!/usr/bin/env python3
"""
创建AI DataVault文件夹并移动相关文件
"""

import os
import shutil
from pathlib import Path

def create_ai_folder():
    """创建AI DataVault文件夹并移动文件"""

    project_root = Path(__file__).parent
    ai_folder = project_root / 'ai_datavault'

    # 创建文件夹
    ai_folder.mkdir(exist_ok=True)
    templates_folder = ai_folder / 'templates'
    templates_folder.mkdir(exist_ok=True)

    print("创建文件夹结构...")
    print(f"AI DataVault文件夹: {ai_folder}")
    print(f"模板文件夹: {templates_folder}")

    # 需要移动的文件列表
    files_to_move = [
        'ai_datavault_generator.py',
        'run_ai_app.py',
        'start_ai_app.py',
        'LAUNCH.py',
        'diagnose.py',
        'test_app_startup.py',
        'test_new_interface.py',
        'AI_GENERATOR_README.md',
        'debug_svg.html',
        'DIAGNOSIS.md',
        'check_deps.py',
        'install_deps.py',
        'test_api_key.py',
        'API_KEY_SETUP.md',
        'sample_requirements.txt'
    ]

    # 移动ai_templates文件夹的内容到新位置
    old_templates = project_root / 'ai_templates'
    if old_templates.exists():
        print("\n移动模板文件...")
        for file_path in old_templates.glob('*'):
            if file_path.is_file():
                shutil.move(str(file_path), str(templates_folder / file_path.name))
                print(f"✓ 移动: {file_path.name}")

        # 删除空的旧文件夹
        try:
            old_templates.rmdir()
            print("✓ 删除旧模板文件夹")
        except:
            print("⚠️ 无法删除旧模板文件夹")

    # 移动其他文件
    print("\n移动主文件...")
    for filename in files_to_move:
        src = project_root / filename
        if src.exists():
            dst = ai_folder / filename
            shutil.move(str(src), str(dst))
            print(f"✓ 移动: {filename}")
        else:
            print(f"⚠️ 文件不存在: {filename}")

    # 创建__init__.py
    init_file = ai_folder / '__init__.py'
    init_file.write_text('"""AI DataVault 模块"""')
    print("✓ 创建 __init__.py")

    print("
🎉 文件移动完成！"    print(f"新文件夹: {ai_folder}")
    print("\n请运行以下命令测试:")
    print("python ai_datavault/LAUNCH.py")

if __name__ == "__main__":
    create_ai_folder()