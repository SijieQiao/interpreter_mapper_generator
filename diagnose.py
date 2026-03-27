#!/usr/bin/env python3
"""
AI DataVault 诊断脚本
自动诊断常见问题并提供解决方案
"""

import os
import sys
import subprocess

def run_command(cmd, description=""):
    """运行命令并返回结果"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout.strip(), result.stderr.strip()
    except Exception as e:
        return False, "", str(e)

def check_python():
    """检查Python环境"""
    print("🔍 检查Python环境...")
    success, stdout, stderr = run_command("python --version")
    if success:
        print(f"✅ Python版本: {stdout}")
        return True
    else:
        print(f"❌ Python未找到: {stderr}")
        print("解决方案: 安装Python 3.8+ 并添加到PATH")
        return False

def check_dependencies():
    """检查Python依赖"""
    print("\n🔍 检查Python依赖...")
    required_modules = [
        ('flask', 'pip install flask'),
        ('requests', 'pip install requests'),
        ('pandas', 'pip install pandas'),
        ('openpyxl', 'pip install openpyxl'),
        ('pydantic', 'pip install pydantic'),
        ('yaml', 'pip install PyYAML')
    ]

    missing = []
    for module, install_cmd in required_modules:
        try:
            __import__(module)
            print(f"✅ {module}")
        except ImportError:
            print(f"❌ {module} - 缺失")
            missing.append(install_cmd)

    if missing:
        print(f"\n📦 缺少 {len(missing)} 个依赖包")
        print("运行以下命令安装:")
        for cmd in missing:
            print(f"  {cmd}")
        print("\n或运行: python install_deps.py")
        return False

    print("✅ 所有依赖已安装")
    return True

def check_api_key():
    """检查API Key"""
    print("\n🔑 检查DeepSeek API Key...")
    api_key = os.getenv('DEEPSEEK_API_KEY')
    if api_key:
        print(f"✅ API Key已设置 (长度: {len(api_key)} 字符)")
        return True
    else:
        print("❌ 未设置DEEPSEEK_API_KEY环境变量")
        print("解决方案:")
        print("1. 运行: python setup_env.bat")
        print("2. 或手动设置: set DEEPSEEK_API_KEY=your_api_key_here")
        return False

def check_app_import():
    """检查应用模块导入"""
    print("\n🔍 检查应用模块...")
    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from ai_datavault_generator import AI_DataVault_WebApp
        print("✅ AI DataVault模块导入成功")
        return True
    except ImportError as e:
        print(f"❌ 模块导入失败: {e}")
        print("解决方案: 确保所有依赖都已正确安装")
        return False

def check_port():
    """检查端口占用"""
    print("\n🔍 检查端口5003...")
    success, stdout, stderr = run_command("netstat -ano | findstr 5003")
    if stdout.strip():
        print("⚠️ 端口5003可能被占用")
        print("进程信息:")
        print(stdout)
        return False
    else:
        print("✅ 端口5003可用")
        return True

def main():
    """主诊断函数"""
    print("=" * 60)
    print("🔧 AI DataVault 自动诊断")
    print("=" * 60)

    checks = [
        ("Python环境", check_python),
        ("依赖包", check_dependencies),
        ("API Key", check_api_key),
        ("应用模块", check_app_import),
        ("端口可用性", check_port)
    ]

    results = []
    for name, check_func in checks:
        result = check_func()
        results.append((name, result))

    print("\n" + "=" * 60)
    print("📊 诊断结果总结")
    print("=" * 60)

    passed = 0
    failed = []

    for name, result in results:
        status = "✅" if result else "❌"
        print(f"{status} {name}")
        if result:
            passed += 1
        else:
            failed.append(name)

    print(f"\n通过: {passed}/{len(checks)} 项")

    if failed:
        print(f"失败: {', '.join(failed)}")
        print("\n🔧 请按上述建议修复问题，然后重新运行诊断")
        print("python diagnose.py")
    else:
        print("\n🎉 所有检查通过！您可以启动应用了")
        print("运行: python LAUNCH.py")

    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()