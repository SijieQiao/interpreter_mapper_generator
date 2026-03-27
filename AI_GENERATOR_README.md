# 🤖 AI Data Vault 生成器

基于DeepSeek AI的智能Data Vault 2.0模型生成器，专为银行业BIAN标准设计。

## 🚀 核心功能

- **AI需求理解**：通过自然语言理解您的业务需求
- **智能模型生成**：自动生成符合Data Vault 2.0标准的模型
- **定制化图表**：根据需求生成专业的SVG图表
- **合规支持**：内置银行业风险管理和合规功能
- **多格式输出**：Raw Vault、Business Vault、数据字典

## 📋 前置要求

### 1. Python 环境和依赖

**快速修复依赖问题：**
```bash
# 如果遇到 "ModuleNotFoundError" 错误，双击运行
fix_dependencies.bat
```

**自动安装依赖：**
```bash
# 运行Python安装脚本
python install_deps.py
```

**手动安装：**
```bash
pip install -r requirements.txt
```

**必需的包：**
- flask>=2.0.0 (Web框架)
- flask-cors>=3.0.0 (跨域支持)
- requests>=2.25.0 (HTTP客户端 - AI API调用)
- pandas>=1.3.0 (数据处理)
- openpyxl>=3.0.0 (Excel文件支持)
- pydantic>=2.0.0 (数据验证)
- PyYAML>=6.0 (YAML配置)
- werkzeug>=2.0.0 (WSGI工具)

### 2. DeepSeek API Key
```bash
# 设置环境变量
set DEEPSEEK_API_KEY=your_api_key_here

# 或者在代码中直接设置（不推荐）
```

### 2. Python依赖
```bash
pip install flask requests pydantic pandas openpyxl
```

## 🎯 快速开始

### 方法一：使用启动脚本（推荐）
```bash
# 双击运行
start_ai_generator.bat
```

### 方法二：命令行启动
```bash
# 设置API Key
set DEEPSEEK_API_KEY=your_api_key_here

# 启动应用
```bash
# 推荐方式：使用启动脚本
python start_ai_app.py

# 或直接运行
python ai_datavault_generator.py
```

### 访问地址
- **AI生成器**：http://localhost:5003

## 📖 使用指南

### 1. 上传数据表
- 支持 `.csv`、`.xlsx`、`.xls` 格式
- 格式参考 `source.csv` 或 `sample_data_dic2.xlsx`

### 2. 描述需求
在对话框中输入类似这样的需求描述：

```
我需要将这个银行业Party数据模型转换为Data Vault 2.0格式。
重点关注客户风险管理和合规审计功能。
生成包含Party基本信息、角色关系、联系方式的完整模型。
需要支持历史数据追踪和时间点查询。
```

### 3. AI分析
点击"AI分析需求"，系统会：
- 识别您的数据表结构
- 理解您的业务需求
- 确定需要生成的组件类型

### 4. 生成模型
点击"生成完整模型"，AI会根据分析结果生成：
- Raw Data Vault（Hubs、Links、Satellites）
- Business Data Vault（PIT、Bridge、Historic）
- 定制化的SVG图表
- 完整的数据字典

## 🎨 AI分析能力

### 支持的需求类型
- **基础转换**：标准Data Vault 2.0转换
- **合规重点**：风险管理、监管报告、审计追踪
- **特定实体**：Party、Party Role、Contact、Address、Relationship
- **时间维度**：历史追踪、时间点查询
- **业务场景**：客户管理、关系网络分析

### 智能决策
- 根据需求自动选择实体类型
- 根据合规要求过滤组件
- 根据业务重点调整图表样式
- 优化生成的内容结构

## 📊 输出结果

### Raw Data Vault
- **Hubs**：业务主键实体（Party、Party_Role等）
- **Links**：实体间关系
- **Satellites**：描述性属性（按类别分组）

### Business Data Vault
- **PIT Tables**：时间点视图（Party、Contact等）
- **Bridge Tables**：多对多关系处理
- **Historic Tables**：SCD Type 2历史表

### 可视化图表
- Raw Vault专业图表（SVG）
- Business Vault高级图表（SVG）
- AI定制的布局和样式

### 数据字典
- Markdown格式
- 包含所有实体、属性、关系
- Visual Paradigm兼容

## 🔧 技术架构

```
AI Data Vault Generator (Port 5003)
├── ai_datavault_generator.py     # 主应用
├── ai_templates/
│   └── ai_datavault.html        # 前端界面
└── AI Components
    ├── DeepSeekAI               # AI需求分析
    ├── AIDiagramGenerator       # 智能图表生成
    └── Existing Modules         # 使用现有解释器和映射器
        ├── LegacyModelReader
        ├── BIANMappingLayer
        └── DataVaultTransformer
```

## 🎯 示例需求描述

### 基础Party模型
```
将银行业Party数据转换为Data Vault 2.0格式，
包含客户基本信息、联系方式和地址信息。
```

### 合规重点模型
```
生成支持风险管理和合规审计的Party模型，
重点关注客户风险评级、合规检查和审计追踪功能。
```

### 完整关系网络
```
创建完整的客户关系网络模型，
包括Party实体、角色关系、联系历史和时间点查询能力。
```

## 🚨 故障排除

### API Key问题
```
错误: 请设置DEEPSEEK_API_KEY环境变量
解决: set DEEPSEEK_API_KEY=your_actual_api_key
```

### 文件上传失败
```
检查文件格式是否正确
确保文件大小不超过16MB
```

### AI分析失败
```
检查网络连接
确认API Key有效性
查看浏览器控制台错误信息
```

### 生成失败
```
检查输入文件格式
确认需求描述清晰具体
查看详细错误信息
```

## 🔄 与现有系统的关系

- **不修改现有模块**：完全使用现有的解释器和映射器
- **新增AI层**：在上层添加AI理解和定制生成功能
- **向下兼容**：可以与现有的5002端口应用协同工作

## 📈 扩展计划

- 支持更多AI模型（GPT-4、Claude等）
- 添加模型验证和优化建议
- 支持批量处理和模板库
- 集成更多行业标准（BCBS、SOX等）

## 🤝 贡献

欢迎提交Issue和Pull Request来改进AI生成器的功能和用户体验。

---

**技术支持**: 如遇问题，请查看浏览器开发者工具的控制台输出，或查看服务器端的错误日志。