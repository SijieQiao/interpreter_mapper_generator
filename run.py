"""
启动遗留模型解释器 Web 应用

使用方法：
    python run.py

访问地址: http://localhost:5000
"""
from interpreter.web_app import app

if __name__ == '__main__':
    print("=" * 60)
    print("老旧模型解释器 - Web界面")
    print("=" * 60)
    print("\n访问地址: http://localhost:5000")
    print("按 Ctrl+C 停止服务器\n")
    app.run(debug=True, host='0.0.0.0', port=5000)
