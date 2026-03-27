# AI DataVault 生成器

基于DeepSeek AI的智能Data Vault 2.0模型生成器，专为银行业BIAN标准设计。

## 🚀 快速启动

```bash
# 设置API Key
set DEEPSEEK_API_KEY=your_api_key_here
```
启动
python .\ai_datavault\ai_datavault_generator.py

访问：http://localhost:5003

## 📁 文件结构

```
ai_datavault/
├── __init__.py                 # 模块初始化
├── ai_datavault_generator.py   # 主应用
├── launch.py                   # 启动脚本
├── templates/
│   └── ai_datavault.html      # 前端界面
└── README.md                  # 本文件
```

## 🎯 功能特性

- **AI需求理解**：自然语言需求分析
- **智能模型生成**：自动生成Data Vault 2.0模型
- **定制化图表**：专业SVG图表输出
- **合规支持**：银行业风险管理合规
- **单图表显示**：智能选择最合适的图表展示

## 📖 使用说明

1. **上传数据文件**：支持 `.csv`, `.xlsx`, `.xls` 格式
2. **输入需求描述**：用自然语言描述您的需求
3. **AI分析**：系统自动分析并确定生成策略
4. **生成模型**：自动生成Data Vault模型和图表
5. **下载结果**：下载SVG图表和数据字典

## 🔧 技术实现

- **后端**：Flask + Python
- **AI**：DeepSeek API 集成
- **前端**：纯HTML/CSS/JavaScript
- **图表**：SVG格式专业图表
- **数据处理**：使用现有解释器和映射器模块

## 🤝 与其他模块的关系

- **继承**：使用 `interpreter` 模块进行数据解析
- **集成**：使用 `mapper` 模块进行BIAN映射
- **扩展**：在此基础上添加AI驱动的智能生成功能

---

**注意**：确保已设置 `DEEPSEEK_API_KEY` 环境变量