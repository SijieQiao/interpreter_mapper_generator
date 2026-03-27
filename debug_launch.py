#!/usr/bin/env python3
"""
调试启动脚本
"""

import os
import sys
from pathlib import Path

# 设置环境变量
os.environ['DEEPSEEK_API_KEY'] = 'test_key'

# 添加项目根目录到路径
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

print("🔍 调试AI DataVault启动...")

# 检查文件路径
current_file = Path(__file__)
script_dir = current_file.parent
ai_dir = script_dir / 'ai_datavault'
template_dir = ai_dir / 'templates'
template_file = template_dir / 'ai_datavault.html'

print(f"当前脚本: {current_file}")
print(f"AI目录: {ai_dir}")
print(f"模板目录: {template_dir}")
print(f"模板文件: {template_file}")
print(f"模板文件存在: {template_file.exists()}")

if template_file.exists():
    with open(template_file, 'r', encoding='utf-8') as f:
        content = f.read()
    print(f"模板文件大小: {len(content)} 字符")

# 尝试导入和启动
try:
    from ai_datavault.ai_datavault_generator import AI_DataVault_WebApp
    print("✅ 模块导入成功")

    app = AI_DataVault_WebApp('test_key')
    print("✅ 应用创建成功")

    # 测试模板加载
    with app.app.test_request_context():
        try:
            from flask import render_template
            result = render_template('ai_datavault.html')
            print("✅ 模板渲染成功")
            print(f"渲染结果长度: {len(result)}")
        except Exception as e:
            print(f"❌ 模板渲染失败: {e}")

except Exception as e:
    print(f"❌ 启动失败: {e}")
    import traceback
    traceback.print_exc()

input("\n按Enter键退出...")