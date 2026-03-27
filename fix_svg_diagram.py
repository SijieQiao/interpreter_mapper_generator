#!/usr/bin/env python3
"""
修复SVG图表生成器 - 使其完全匹配原始专业样式
"""

import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, r'd:\桌面\legacy_interpreter_web')

def create_professional_svg(entities_data, vault_type="raw"):
    """基于原始SVG样式创建专业图表"""

    # 颜色方案 - 精确匹配原始
    colors = {
        "hub": "#D4EDDA",
        "link": "#FFF3CD",
        "satellite": "#E8DAEF"
    }

    border_colors = {
        "hub": "#28a745",
        "link": "#ffc107",
        "satellite": "#6c757d"
    }

    # 按类型分组实体
    hubs = [e for e in entities_data if e["type"] == "hub"]
    links = [e for e in entities_data if e["type"] == "link"]
    satellites = [e for e in entities_data if e["type"] == "satellite"]

    # 计算尺寸
    width = max(1400, 200 + len(hubs) * 260)
    height = 900

    # 开始构建SVG - 精确匹配原始格式
    svg_parts = []

    # SVG头部
    svg_parts.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%" style="background:#fafafa;">')
    svg_parts.append('<defs><marker id="arrow" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto"><polygon points="0 0, 8 3, 0 6" fill="#666"/></marker></defs>')
    svg_parts.append(f'<text x="20" y="20" font-size="14" fill="#333">{vault_type.title()} Vault 2.0 (Party – BIAN aligned)</text>')

    # 图例
    svg_parts.append('''
  <g transform="translate(20, 30)">
    <rect x="0" y="0" width="12" height="12" rx="2" fill="#D4EDDA" stroke="#28a745" stroke-width="1"/>
    <text x="18" y="10" font-size="10" fill="#333">Hub</text>
    <rect x="60" y="0" width="12" height="12" rx="2" fill="#FFF3CD" stroke="#ffc107" stroke-width="1"/>
    <text x="78" y="10" font-size="10" fill="#333">Link</text>
    <rect x="120" y="0" width="12" height="12" rx="2" fill="#E8DAEF" stroke="#6c757d" stroke-width="1"/>
    <text x="138" y="10" font-size="10" fill="#333">Satellite</text>
  </g>''')

    # 实体位置跟踪
    entity_positions = {}

    # 绘制Hubs - 第一层 (y=30)
    for i, hub in enumerate(hubs):
        x = 40 + i * 260
        y = 30
        entity_positions[hub["name"]] = (x + 90, y)  # 中心点

        # Hub实体框
        svg_parts.append(f'''<g transform="translate({x}, {y})">
    <rect x="0" y="0" width="180" height="96" rx="6" fill="#D4EDDA" stroke="#28a745" stroke-width="2"/>
    <text x="90.0" y="18" text-anchor="middle" font-weight="bold" font-size="12" fill="#1a1a2e">{hub["name"]}</text>
    <text x="90.0" y="32" text-anchor="middle" font-size="10" fill="#333">(HUB)</text>''')

        # Hub字段
        fields = hub.get("columns", [])
        for j, field in enumerate(fields[:4]):
            y_pos = 50 + j * 18
            field_name = field.get("name", "")[:18] + "..." if len(field.get("name", "")) > 18 else field.get("name", "")
            pk_indicator = " (PK)" if "_hk" in field.get("name", "").lower() else ""
            svg_parts.append(f'''    <text x="8" y="{y_pos}" font-size="10" fill="#333">{field_name}{pk_indicator}</text>''')

        svg_parts.append('  </g>')

    # 绘制Links - 第二层 (y=220)
    for i, link in enumerate(links):
        x = 80 + i * 320  # 调整间距
        y = 220
        entity_positions[link["name"]] = (x + 100, y)

        # Link实体框
        svg_parts.append(f'''<g transform="translate({x}, {y})">
    <rect x="0" y="0" width="200" height="114" rx="6" fill="#FFF3CD" stroke="#ffc107" stroke-width="2"/>
    <text x="100.0" y="18" text-anchor="middle" font-weight="bold" font-size="12" fill="#1a1a2e">{link["name"]}</text>
    <text x="100.0" y="32" text-anchor="middle" font-size="10" fill="#333">(LINK)</text>''')

        # Link字段
        fields = link.get("columns", [])
        for j, field in enumerate(fields[:5]):
            y_pos = 50 + j * 18
            field_name = field.get("name", "")[:18] + "..." if len(field.get("name", "")) > 18 else field.get("name", "")
            pk_indicator = " (PK)" if "_hk" in field.get("name", "").lower() else " (FK)" if "_hk" in field.get("name", "").lower() else ""
            svg_parts.append(f'''    <text x="8" y="{y_pos}" font-size="10" fill="#333">{field_name}{pk_indicator}</text>''')

        svg_parts.append('  </g>')

    # 绘制Satellites - 第三层 (y=420)
    for i, sat in enumerate(satellites):
        x = 40 + i * 280
        y = 420
        entity_positions[sat["name"]] = (x + 110, y)

        # Satellite实体框
        svg_parts.append(f'''<g transform="translate({x}, {y})">
    <rect x="0" y="0" width="220" height="600" rx="6" fill="#E8DAEF" stroke="#6c757d" stroke-width="2"/>
    <text x="110.0" y="18" text-anchor="middle" font-weight="bold" font-size="12" fill="#1a1a2e">{sat["name"]}</text>
    <text x="110.0" y="32" text-anchor="middle" font-size="10" fill="#333">(SATELLITE)</text>''')

        # Satellite字段
        fields = sat.get("columns", [])
        for j, field in enumerate(fields[:25]):
            y_pos = 50 + j * 18
            field_name = field.get("name", "")[:18] + "..." if len(field.get("name", "")) > 18 else field.get("name", "")
            pk_indicator = " (PK)" if "_hk" in field.get("name", "").lower() or "load_dts" in field.get("name", "").lower() else ""
            svg_parts.append(f'''    <text x="8" y="{y_pos}" font-size="9" fill="#333">{field_name}{pk_indicator}</text>''')

        if len(fields) > 25:
            svg_parts.append(f'''    <text x="8" y="{50 + 25 * 18}" font-size="9" fill="#666">...{len(fields) - 25} more</text>''')

        svg_parts.append('  </g>')

    # 生成关系箭头 - 基于原始SVG的复杂连接逻辑
    relationships = []

    # Hub到所有Link的连接
    for hub_name, hub_pos in entity_positions.items():
        if "HUB_" in hub_name:
            for link_name, link_pos in entity_positions.items():
                if "LNK_" in link_name:
                    relationships.append(f'<path d="M{hub_pos[0]}.0,{hub_pos[1] + 66} L{link_pos[0]}.0,{link_pos[1] - 10}" fill="none" stroke="#666" stroke-width="1" marker-end="url(#arrow)"/>')

    # Link到所有Satellite的连接
    for link_name, link_pos in entity_positions.items():
        if "LNK_" in link_name:
            for sat_name, sat_pos in entity_positions.items():
                if "SAT_" in sat_name:
                    relationships.append(f'<path d="M{link_pos[0]}.0,{link_pos[1] + 74} L{sat_pos[0]}.0,{sat_pos[1] - 10}" fill="none" stroke="#666" stroke-width="1" stroke-dasharray="4,2" marker-end="url(#arrow)"/>')

    svg_parts.extend(relationships)

    # 结束SVG
    svg_parts.append('</svg>')

    return ''.join(svg_parts)

def main():
    # 测试数据 - 基于原始Party模型
    test_entities = [
        {
            "name": "HUB_Party",
            "type": "hub",
            "columns": [
                {"name": "party_hk", "type": "VARCHAR(32)"},
                {"name": "party_bk", "type": "VARCHAR(50)"},
                {"name": "load_dts", "type": "TIMESTAMP"},
                {"name": "record_source", "type": "VARCHAR(100)"}
            ]
        },
        {
            "name": "HUB_Party_Role",
            "type": "hub",
            "columns": [
                {"name": "party_role_hk", "type": "VARCHAR(32)"},
                {"name": "party_role_bk", "type": "VARCHAR(50)"},
                {"name": "load_dts", "type": "TIMESTAMP"},
                {"name": "record_source", "type": "VARCHAR(100)"}
            ]
        },
        {
            "name": "LNK_Party_Role",
            "type": "link",
            "columns": [
                {"name": "lnk_party_role_hk", "type": "VARCHAR(32)"},
                {"name": "party_hk", "type": "VARCHAR(32)"},
                {"name": "party_role_hk", "type": "VARCHAR(32)"},
                {"name": "load_dts", "type": "TIMESTAMP"},
                {"name": "record_source", "type": "VARCHAR(100)"}
            ]
        },
        {
            "name": "SAT_Party",
            "type": "satellite",
            "columns": [
                {"name": "party_hk", "type": "VARCHAR(32)"},
                {"name": "load_dts", "type": "TIMESTAMP"},
                {"name": "load_end_dts", "type": "TIMESTAMP"},
                {"name": "party_type", "type": "VARCHAR(20)"},
                {"name": "legal_name", "type": "VARCHAR(200)"},
                {"name": "birth_date", "type": "DATE"},
                {"name": "registration_date", "type": "DATE"},
                {"name": "gender", "type": "VARCHAR(10)"},
                {"name": "national_id_type", "type": "VARCHAR(20)"},
                {"name": "national_id_number", "type": "VARCHAR(50)"}
            ]
        },
        {
            "name": "SAT_Party_Role",
            "type": "satellite",
            "columns": [
                {"name": "party_role_hk", "type": "VARCHAR(32)"},
                {"name": "load_dts", "type": "TIMESTAMP"},
                {"name": "load_end_dts", "type": "TIMESTAMP"},
                {"name": "role_type", "type": "VARCHAR(50)"},
                {"name": "valid_from", "type": "DATE"},
                {"name": "valid_to", "type": "DATE"},
                {"name": "status", "type": "VARCHAR(20)"}
            ]
        }
    ]

    # 生成Raw Vault图表
    raw_svg = create_professional_svg(test_entities, "raw")

    # 保存文件
    output_file = r'd:\桌面\legacy_interpreter_web\improved_raw_vault.svg'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(raw_svg)

    print(f"✅ 改进的Raw Vault图表已保存到: {output_file}")
    print("请打开文件查看结果，应该与原始的专业样式完全一致。")

if __name__ == "__main__":
    main()