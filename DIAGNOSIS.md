# 🔍 图表显示问题诊断指南

## 问题描述

用户反映Raw Vault和Business Vault的图表没有显示。

## 诊断步骤

### 步骤1: 测试SVG显示功能
1. 打开浏览器访问 http://localhost:5003
2. 点击 **"🧪 测试SVG显示"** 按钮
3. 检查Raw Vault和Business Vault标签页是否显示测试图表

**预期结果**：
- 如果显示测试图表：SVG显示功能正常，问题在数据生成阶段
- 如果不显示测试图表：SVG显示功能异常

### 步骤2: 检查浏览器开发者工具
1. 按 F12 打开开发者工具
2. 切换到 **Console** 标签
3. 执行完整流程（上传文件 → AI分析 → 生成模型）
4. 查看控制台输出信息

**需要检查的信息**：
```
Processing results: {Object}           # 检查返回的数据结构
Raw vault diagram: <svg...             # 检查SVG内容前200字符
Business vault diagram: <svg...        # 检查SVG内容前200字符
Raw vault diagram set successfully     # 确认前端设置成功
Business vault diagram set successfully
```

### 步骤3: 检查服务器端日志
查看命令提示符窗口中的服务器输出，寻找：
```
Raw diagram length: XXX               # SVG长度
Business diagram length: XXX           # SVG长度
Raw vault entities: X                  # 实体数量
Business vault entities: X
Starting Data Vault transformation...  # 转换开始
Data Vault transformation completed    # 转换完成
```

### 步骤4: 常见问题及解决方案

#### 问题1: 控制台显示 "图表生成失败"
**原因**：后端没有返回SVG内容
**检查**：
- 服务器日志中是否有错误信息
- API返回的数据是否包含 `diagrams` 字段

#### 问题2: SVG长度为0
**原因**：图表生成器没有生成内容
**检查**：
- Data Vault实体数量是否为0
- AI分析结果是否正确

#### 问题3: 实体被过滤掉
**原因**：AI分析配置过滤掉了所有实体
**检查**：
- AI分析结果中的 `generate_*` 字段是否为true
- 默认分析结果是否被正确应用

#### 问题4: 前端无法解析数据
**原因**：API响应格式不正确
**检查**：
- 控制台中的 "Processing results" 对象
- 数据结构是否与前端期望匹配

### 手动测试

如果自动测试失败，可以手动验证：

#### 测试1: 直接API调用
```bash
# 上传文件后，复制filepath
curl -X POST http://localhost:5003/api/generate \
  -H "Content-Type: application/json" \
  -d '{"filepath": "your_file_path", "analysis": {"raw_vault": {"generate_hubs": true, "generate_links": true, "generate_satellites": true}, "business_vault": {"generate_pit": true, "generate_bridge": true, "generate_historic": true}}}'
```

#### 测试2: 检查文件是否存在
```bash
dir "d:\桌面\legacy_interpreter_web\data\input\uploads\"
```

### 紧急修复

如果问题无法解决，可以：

1. **重启应用**：
   ```bash
   # 关闭当前服务器 (Ctrl+C)
   start_ai_generator.bat  # 重新启动
   ```

2. **清除浏览器缓存**：
   - Ctrl+F5 强制刷新
   - 或清除浏览器缓存

3. **使用测试数据**：
   - 使用 `source.csv` 作为测试文件
   - 使用简单的需求描述

### 收集诊断信息

如果问题持续，请提供以下信息：

1. **浏览器控制台输出**（F12 → Console）
2. **服务器命令行输出**
3. **使用的文件和需求描述**
4. **浏览器类型和版本**

### 技术支持

基于诊断信息，我们可以快速定位问题：

- **前端问题**：修改HTML/JavaScript
- **后端问题**：修改Python代码
- **数据问题**：检查文件格式和内容
- **AI问题**：检查API配置和分析逻辑

---

**重要**：请先运行测试按钮来确认SVG显示功能是否正常！