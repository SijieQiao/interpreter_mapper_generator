#!/usr/bin/env python3
"""
清理旧的AI DataVault文件
删除已移动到ai_datavault/文件夹的文件
"""

import os
from pathlib import Path

def cleanup_old_files():
    """清理旧文件"""

    project_root = Path(__file__).parent

    # 需要删除的文件列表
    files_to_remove = [
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
        'sample_requirements.txt',
        'create_ai_folder.py',
        'move_files.py',
        'QUICK_START.md',
        'TROUBLESHOOTING.md',
        'DEMO.md',
        'USAGE.md',
        'RUNNING_SCRIPTS.md'
    ]

    # 需要删除的文件夹
    dirs_to_remove = [
        'ai_templates'
    ]

    print("🧹 清理旧的AI DataVault文件...")

    # 删除文件
    for filename in files_to_remove:
        file_path = project_root / filename
        if file_path.exists():
            try:
                file_path.unlink()
                print(f"✓ 删除文件: {filename}")
            except Exception as e:
                print(f"⚠️ 无法删除文件 {filename}: {e}")

    # 删除文件夹
    for dirname in dirs_to_remove:
        dir_path = project_root / dirname
        if dir_path.exists() and dir_path.is_dir():
            try:
                import shutil
                shutil.rmtree(dir_path)
                print(f"✓ 删除文件夹: {dirname}/")
            except Exception as e:
                print(f"⚠️ 无法删除文件夹 {dirname}: {e}")

    # 删除批处理文件
    batch_files = [
        'fix_dependencies.bat',
        'test_api_key.bat',
        'start_ai_generator.bat',
        'setup_env.bat',
        'manual_setup.bat',
        'install_deps.bat',
        'install_deps_manual.bat',
        'check_deps.bat'
    ]

    for batch_file in batch_files:
        batch_path = project_root / batch_file
        if batch_path.exists():
            try:
                batch_path.unlink()
                print(f"✓ 删除批处理文件: {batch_file}")
            except Exception as e:
                print(f"⚠️ 无法删除批处理文件 {batch_file}: {e}")

    print("\n✅ 清理完成！")
    print("\n新的项目结构:")
    print("legacy_interpreter_web/")
    print("├── interpreter/          # 解释器模块")
    print("├── mapper/              # 映射器模块")
    print("├── ai_datavault/        # AI DataVault模块 ⭐")
    print("├── data/                # 数据文件")
    print("└── 其他配置文件...")

if __name__ == "__main__":
    cleanup_old_files()