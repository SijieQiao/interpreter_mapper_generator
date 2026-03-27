#!/usr/bin/env python3
"""
检查依赖项安装状态
"""

import sys
import importlib

def check_package(package_name, import_name=None):
    """检查包是否已安装"""
    if import_name is None:
        import_name = package_name

    try:
        importlib.import_module(import_name)
        print(f"✓ {package_name} - INSTALLED")
        return True
    except ImportError:
        print(f"✗ {package_name} - MISSING")
        return False

def main():
    """主检查函数"""
    print("========================================")
    print("Checking AI Data Vault Dependencies")
    print("========================================")
    print()

    # 要检查的包
    packages = [
        ("requests", "requests"),
        ("flask", "flask"),
        ("flask-cors", "flask_cors"),
        ("werkzeug", "werkzeug"),
        ("pandas", "pandas"),
        ("openpyxl", "openpyxl"),
        ("pydantic", "pydantic"),
        ("PyYAML", "yaml")
    ]

    installed_count = 0
    missing_packages = []

    for package_name, import_name in packages:
        if check_package(package_name, import_name):
            installed_count += 1
        else:
            missing_packages.append(package_name)

    print()
    print("========================================")
    print(f"Status: {installed_count}/{len(packages)} packages installed")

    if missing_packages:
        print(f"Missing packages: {', '.join(missing_packages)}")
        print()
        print("To install missing packages:")
        print("pip install " + " ".join(missing_packages))
        print()
        print("Or run:")
        print("install_deps_manual.bat")
    else:
        print("✓ All dependencies are installed!")
        print()
        print("You can now:")
        print("1. Set API key: setup_env.bat")
        print("2. Test API key: test_api_key.bat")
        print("3. Start generator: start_ai_generator.bat")

    print()
    input("Press Enter to exit...")

if __name__ == "__main__":
    main()