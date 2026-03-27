# 🔑 DeepSeek API Key 设置指南

## 获取 API Key

1. 访问 [DeepSeek 官网](https://platform.deepseek.com/)
2. 注册账号并登录
3. 在控制台创建 API Key
4. 复制生成的 API Key

## 设置方法

### 方法一：使用自动设置脚本（推荐）

```bash
# 双击运行
setup_env.bat
```

按照提示输入您的 API Key，脚本会自动设置为系统环境变量。

### 方法二：命令行临时设置

```cmd
# 打开命令提示符，输入以下命令：
set DEEPSEEK_API_KEY=你的API密钥

# 然后在同一个命令提示符窗口中启动应用：
python ai_datavault_generator.py
```

**注意**：这种方法只在当前命令提示符窗口有效，关闭窗口后需要重新设置。

### 方法三：永久设置环境变量

#### Windows 系统

**命令行方式：**
```cmd
# 以管理员身份运行命令提示符，输入：
setx DEEPSEEK_API_KEY "你的API密钥" /M
```

**图形界面方式：**
1. 右键点击"此电脑"或"我的电脑"
2. 选择"属性"
3. 点击"高级系统设置"
4. 点击"环境变量"按钮
5. 在"系统变量"部分点击"新建"
6. 变量名输入：`DEEPSEEK_API_KEY`
7. 变量值输入：您的 API Key
8. 点击"确定"保存

#### Linux/Mac 系统

```bash
# 临时设置
export DEEPSEEK_API_KEY="你的API密钥"

# 永久设置 (添加到 ~/.bashrc 或 ~/.zshrc)
echo 'export DEEPSEEK_API_KEY="你的API密钥"' >> ~/.bashrc
source ~/.bashrc
```

## 验证设置

### 方法一：检查环境变量

```cmd
# Windows
echo %DEEPSEEK_API_KEY%

# Linux/Mac
echo $DEEPSEEK_API_KEY
```

### 方法二：Python 检查

```python
import os
api_key = os.getenv('DEEPSEEK_API_KEY')
print(f"API Key 设置: {'✓' if api_key else '✗'}")
if api_key:
    print(f"API Key 长度: {len(api_key)} 字符")
```

## 启动应用

设置好 API Key 后，启动 AI Data Vault 生成器：

```bash
python ai_datavault_generator.py
```

访问：http://localhost:5003

## 故障排除

### 错误："请设置DEEPSEEK_API_KEY环境变量"

- 检查环境变量是否正确设置
- 重启命令提示符窗口
- 在 PowerShell 和 CMD 中分别检查

### 错误："API请求失败: 401 Unauthorized"

- 检查 API Key 是否正确
- 确认 DeepSeek 账号是否有余额
- 检查 API Key 是否有正确的权限

### 错误："API请求失败: 429 Too Many Requests"

- 等待一段时间后重试
- 检查您的使用配额

## 安全提醒

- 🔒 **不要将 API Key 硬编码到代码中**
- 🔒 **不要将 API Key 提交到版本控制系统**
- 🔒 **定期轮换 API Key**
- 🔒 **妥善保管 API Key，不要分享给他人**

## 技术支持

如果仍然遇到问题：

1. 检查浏览器开发者工具的控制台错误
2. 查看服务器端的错误日志
3. 确认网络连接正常
4. 联系 DeepSeek 技术支持

---

**最后更新**: 2024年2月