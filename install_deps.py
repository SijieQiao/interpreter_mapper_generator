#!/usr/bin/env python3
"""
手动安装依赖项的Python脚本
如果批处理文件无法工作，请运行此脚本
"""

import subprocess
import sys
import os

def install_package(package_name):
    """安装单个包"""
    try:
        print(f"Installing {package_name}...")
        result = subprocess.run([sys.executable, "-m", "pip", "install", package_name],
                              capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✓ {package_name} installed successfully")
            return True
        else:
            print(f"✗ Failed to install {package_name}")
            print(f"Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"✗ Error installing {package_name}: {e}")
        return False

def main():
    """主安装函数"""
    print("========================================")
    print("Installing AI Data Vault Dependencies")
    print("========================================")
    print()

    # 检查Python版本
    print(f"Python version: {sys.version}")
    print()

    # 要安装的包
    packages = [
        "requests",
        "flask",
        "flask-cors",
        "werkzeug",
        "pandas",
        "openpyxl",
        "pydantic",
        "PyYAML"
    ]

    installed_count = 0
    failed_packages = []

    for package in packages:
        if install_package(package):
            installed_count += 1
        else:
            failed_packages.append(package)

    print()
    print("========================================")
    print(f"Installation Summary: {installed_count}/{len(packages)} packages installed")

    if failed_packages:
        print(f"Failed packages: {', '.join(failed_packages)}")
        print()
        print("Manual installation commands:")
        for package in failed_packages:
            print(f"pip install {package}")
    else:
        print("✓ All packages installed successfully!")
        print()
        print("Next steps:")
        print("1. Set your DeepSeek API key: setup_env.bat")
        print("2. Test the API key: test_api_key.bat")
        print("3. Start the AI generator: start_ai_generator.bat")

    print()
    input("Press Enter to exit...")

if __name__ == "__main__":
    main()