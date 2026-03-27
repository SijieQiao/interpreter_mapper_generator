#!/usr/bin/env python
"""
启动 BIAN Party 映射器 Web 应用

访问地址: http://localhost:5001
"""
from mapper.web_app import app

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)