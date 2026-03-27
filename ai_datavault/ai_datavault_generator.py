#!/usr/bin/env python3
"""
AI驱动的Data Vault生成器

基于DeepSeek AI解读用户需求，自动生成Data Vault 2.0模型和图表
支持银行业BIAN标准的Party领域数据模型转换

运行端口: 5003
"""

import os
import sys
import json
import time
import requests
from html import escape
from pathlib import Path
from typing import Dict, List, Any, Optional
from flask import Flask, render_template, request, jsonify, url_for, redirect
from flask_cors import CORS
from werkzeug.utils import secure_filename

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from interpreter.legacy_model_reader_standalone import LegacyModelReader
from mapper.bian_mapping_layer import BIANMappingLayer
from mapper.data_vault_layer import DataVaultTransformer, DataVaultModel


# ====== UI 序列化辅助函数（给前端表格/折叠面板使用）======
def _serialize_table(table) -> Dict[str, Any]:
    """将解释器的 TableMetadata 转成 JSON 可序列化结构。"""
    def _data_type_to_str(dt: Any) -> str:
        if hasattr(dt, "value"):
            return str(dt.value)
        return str(dt)

    cols = []
    for col in (getattr(table, "columns", None) or []):
        cols.append(
            {
                "name": getattr(col, "name", ""),
                "data_type": _data_type_to_str(getattr(col, "data_type", "")),
                "length": getattr(col, "length", None),
                "precision": getattr(col, "precision", None),
                "scale": getattr(col, "scale", None),
                "nullable": getattr(col, "nullable", None),
                "comment": getattr(col, "comment", ""),
                "is_primary_key": getattr(col, "is_primary_key", False),
                "is_foreign_key": getattr(col, "is_foreign_key", False),
                "referenced_table": getattr(col, "referenced_table", None),
                "referenced_column": getattr(col, "referenced_column", None),
            }
        )

    primary_keys = getattr(table, "primary_keys", None)
    foreign_keys = getattr(table, "foreign_keys", None)

    return {
        "name": getattr(table, "name", ""),
        "comment": getattr(table, "comment", ""),
        "columns": cols,
        "primary_keys": primary_keys if primary_keys is not None else [],
        "foreign_keys": [
            {
                "name": getattr(fk, "name", ""),
                "columns": getattr(fk, "columns", None) or [],
                "referenced_table": getattr(fk, "referenced_table", None),
                "referenced_columns": getattr(fk, "referenced_columns", None) or [],
            }
            for fk in (foreign_keys or [])
        ],
    }


def _serialize_bian_mapping(result) -> Dict[str, Any]:
    """将 BIANMappingResult 转成 JSON 可序列化结构。"""
    def _data_type_to_str(dt: Any) -> str:
        if hasattr(dt, "value"):
            return str(dt.value)
        return str(dt)

    table_mappings = []
    for tm in (getattr(result, "table_mappings", None) or []):
        cols = []
        for c in (getattr(tm, "columns", None) or []):
            cols.append(
                {
                    "legacy_column": getattr(c, "legacy_column", ""),
                    "bian_attribute": getattr(c, "bian_attribute", ""),
                    "bian_entity": getattr(c, "bian_entity", ""),
                    "sub_attribute": getattr(c, "sub_attribute", None),
                    "data_type": _data_type_to_str(getattr(c, "data_type", "")),
                    "value_mapping_ref": getattr(c, "value_mapping_ref", None),
                }
            )

        table_mappings.append(
            {
                "legacy_table": getattr(tm, "legacy_table", ""),
                "bian_entity": getattr(tm, "bian_entity", ""),
                "description": getattr(tm, "description", ""),
                "subtype": getattr(tm, "subtype", None),
                "parent_entity": getattr(tm, "parent_entity", None),
                "columns": cols,
                "unmapped_columns": getattr(tm, "unmapped_columns", None) or [],
            }
        )

    return {
        "source_file": getattr(result, "source_file", None),
        "domain": getattr(result, "domain", None),
        "summary": getattr(result, "summary", None),
        "table_mappings": table_mappings,
        "unmapped_tables": getattr(result, "unmapped_tables", None) or [],
    }


class DeepSeekAI:
    """DeepSeek AI 客户端"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.deepseek.com/v1"
        self.model = "deepseek-chat"

    def analyze_requirement(self, user_input: str, available_tables: List[str], template_id: str = "standard", granularity_override: Optional[str] = None) -> Dict[str, Any]:
        """
        分析用户需求，确定需要的Data Vault组件和图表定制要求

        Args:
            user_input: 用户输入的需求描述
            available_tables: 可用的表列表
            template_id: 用户选择的提示词模板
            granularity_override: 用户显式选择粒度

        Returns:
            分析结果，包含需要的实体类型和转换策略
        """

        template = self._get_prompt_template(template_id)
        granularity_hint = granularity_override or "balanced"

        prompt = f"""
你是一个资深的银行业数据建模架构师，专门负责将遗留系统转换为符合Data Vault 2.0标准和BIAN Party规范的模型。

用户提供的遗留表包括: {', '.join(available_tables)}

【选择的分析模板】{template['name']}
【模板目标】{template['goal']}
【模板约束】{template['constraints']}
【用户粒度选择（最高优先级）】{granularity_hint}

用户需求: {user_input}

**重要指导：基于BIAN Party领域标准，实体的正确分类规则：**

1. **HUB实体**（核心业务标识）:
   - Party（参与者基本信息）
   - Party_Role（参与者角色）
   - Contact（联系方式）
   - Address（地址信息）
   - Relationship（关系网络）

2. **LINK实体**（实体间关系）:
   - Party_Role（参与者与角色的关系）
   - Party_Contact（参与者与联系方式的关系）
   - Party_Address（参与者与地址的关系）
   - Party_Relationship（参与者间的关系）

3. **SATELLITE实体**（描述性属性和历史数据）:
   - 所有实体的详细属性、历史记录、状态信息等

**注意**：Party实体应该是HUB类型，而不是PIT类型。

请深度分析用户需求，确定以下方面：

1. **核心实体识别**:
   - 识别用户最关心的核心业务实体
   - 严格按照上述规则确定这些实体在Data Vault中的正确类型(Hub/Link/Satellite)

2. **关系映射**:
   - 分析实体间的业务关系
   - 确定Link的连接方式
   - 识别Satellite的分类和属性分组

3. **合规与业务重点**:
   - 风险管理要求
   - 合规审计需求
   - 业务规则约束

4. **图表展示偏好**:
   - 是否需要显示详细的关系箭头
   - 是否需要特定实体的展开展示
   - 图表布局偏好(水平/垂直/分组)

5. **输出定制**:
   - 重点展示的字段映射关系
   - 特定的Data Vault结构要求
   - 合规检查的优先级

请以JSON格式返回详细分析结果，格式如下:
{{
    "analysis": "对用户需求的深度分析总结",
    "core_entities": {{
        "hubs": ["最重要的Hub实体列表"],
        "links": ["关键的Link关系列表"],
        "satellites": ["主要的Satellite分组"]
    }},
    "relationships": {{
        "hub_to_link": ["Hub到Link的连接关系"],
        "link_to_satellite": ["Link到Satellite的连接关系"],
        "entity_connections": ["实体间的重要业务关系"]
    }},
    "compliance_requirements": {{
        "risk_management": true/false,
        "audit_trail": true/false,
        "regulatory_compliance": true/false,
        "data_governance": true/false
    }},
    "diagram_customization": {{
        "show_relationship_arrows": true/false,
        "detailed_connections": true/false,
        "entity_expansion": ["需要详细展示的实体"],
        "layout_preference": "hierarchical/networked/grouped",
        "color_scheme": "standard/professional/accentuate_compliance"
    }},
    "field_mapping_focus": {{
        "key_fields": ["业务键字段"],
        "compliance_fields": ["合规相关字段"],
        "relationship_fields": ["关系相关字段"],
        "temporal_fields": ["时间相关字段"]
    }},
    "output_priorities": {{
        "primary_vault_type": "raw/business",
        "emphasis_on_relationships": true/false,
        "include_business_rules": true/false,
        "generate_compliance_checks": true/false
    }},
    "special_instructions": "任何特殊的图表或结构要求"
}}

请确保返回有效的JSON格式，并根据用户需求进行深度分析。
"""

        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": "你是一个专业的银行业Data Vault架构师，请详细分析用户需求并以JSON格式返回结果。"},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.2,  # 降低随机性，提高一致性
                    "max_tokens": 2500
                },
                timeout=45
            )

            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']

                # 尝试提取JSON
                try:
                    # 清理可能的前后缀文本
                    content = content.strip()
                    if content.startswith('```json'):
                        content = content[7:]
                    if content.startswith('```'):
                        content = content[3:]
                    if content.endswith('```'):
                        content = content[:-3]

                    content = content.strip()
                    analysis_result = json.loads(content)

                    # 验证和完善分析结果
                    return self._validate_and_enhance_analysis(analysis_result)

                except json.JSONDecodeError as e:
                    print(f"JSON解析失败: {e}")
                    print(f"原始内容: {content[:500]}...")
                    return self._get_enhanced_default_analysis(user_input)
            else:
                print(f"API请求失败: {response.status_code} - {response.text}")
                return self._get_enhanced_default_analysis(user_input)

        except Exception as e:
            print(f"AI分析失败: {e}")
            return self._get_enhanced_default_analysis(user_input)

    def _validate_and_enhance_analysis(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """验证和增强分析结果"""
        # 确保必需的字段存在
        defaults = self._get_enhanced_default_analysis("")

        # 深度合并分析结果
        enhanced = self._deep_merge(defaults, analysis)

        # 验证核心实体
        if not enhanced.get("core_entities", {}).get("hubs"):
            enhanced["core_entities"]["hubs"] = ["Party", "Party_Role", "Contact", "Address"]

        # 确保图表定制设置合理
        diagram_customization = enhanced.get("diagram_customization", {})
        if not diagram_customization.get("show_relationship_arrows"):
            diagram_customization["show_relationship_arrows"] = True

        enhanced["diagram_customization"] = diagram_customization

        # 标准化粒度设置
        enhanced["granularity"] = self._normalize_granularity(enhanced)

        return enhanced

    def _ensure_granularity(self, analysis: Dict[str, Any], user_requirement: str) -> Dict[str, Any]:
        """确保粒度设置：支持用户强制指定"""
        forced = self._extract_granularity_from_prompt(user_requirement)
        if forced:
            analysis = analysis or {}
            analysis["granularity"] = forced
            return analysis

        if not analysis:
            return {"granularity": "balanced"}

        if not analysis.get("granularity"):
            analysis["granularity"] = "balanced"

        return analysis

    def _extract_granularity_from_prompt(self, prompt: str) -> Optional[str]:
        """从用户输入中解析强制粒度指令"""
        if not prompt:
            return None

        lowered = prompt.lower()
        patterns = [
            "粒度:粗", "粒度：粗", "粒度=粗", "granularity:coarse", "granularity=coarse",
            "粒度:中", "粒度：中", "粒度=中", "granularity:balanced", "granularity=balanced",
            "粒度:细", "粒度：细", "粒度=细", "granularity:fine", "granularity=fine"
        ]

        for item in patterns:
            if item in lowered:
                if "粗" in item or "coarse" in item:
                    return "coarse"
                if "细" in item or "fine" in item:
                    return "fine"
                if "中" in item or "balanced" in item:
                    return "balanced"

        # 允许用户直接写“粗粒度/中粒度/细粒度”等关键词
        if any(token in lowered for token in ["粗粒度", "粗一点", "粗略", "coarse"]):
            return "coarse"
        if any(token in lowered for token in ["细粒度", "细一点", "细化", "fine", "detailed"]):
            return "fine"
        if any(token in lowered for token in ["中粒度", "中等粒度", "balanced", "标准"]):
            return "balanced"

        return None

    def _deep_merge(self, base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
        """深度合并字典"""
        result = base.copy()

        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    def _normalize_granularity(self, analysis: Dict[str, Any]) -> str:
        """规范化粒度配置：默认中粒度，支持强制指定"""
        raw_value = analysis.get("granularity")
        if raw_value:
            return self._normalize_granularity_value(raw_value)

        return "balanced"

    def _normalize_granularity_value(self, value: str) -> str:
        """将各种输入映射为标准粒度值"""
        if not value:
            return "balanced"

        normalized = str(value).strip().lower()
        mapping = {
            "coarse": "coarse",
            "粗": "coarse",
            "粗粒度": "coarse",
            "粗略": "coarse",
            "简": "coarse",
            "简单": "coarse",
            "简单粒度": "coarse",
            "粗糙": "coarse",
            "balanced": "balanced",
            "中": "balanced",
            "中粒度": "balanced",
            "normal": "balanced",
            "标准": "balanced",
            "fine": "fine",
            "细": "fine",
            "细粒度": "fine",
            "细致": "fine",
            "细化": "fine",
            "detail": "fine",
            "detailed": "fine"
        }

        return mapping.get(normalized, "balanced")

    def _get_prompt_template(self, template_id: str) -> Dict[str, str]:
        """获取提示词模板定义"""
        templates = {
            "standard": {
                "name": "Standard Modeling Template",
                "goal": "Generate a balanced Hub/Link/Satellite structure while preserving Data Vault 2.0 conventions",
                "constraints": "Use balanced granularity by default and prioritize structural stability and readability"
            },
            "compliance": {
                "name": "Compliance & Audit Template",
                "goal": "Strengthen audit and compliance field recognition to improve traceability",
                "constraints": "Prioritize risk/audit/status fields; fine granularity is recommended"
            },
            "relationship": {
                "name": "Relationship Analysis Template",
                "goal": "Enhance relationship discovery and fully represent Hub-Link connectivity",
                "constraints": "Prioritize foreign keys and relationship paths while maintaining connectivity"
            },
            "prototype": {
                "name": "Rapid Prototype Template",
                "goal": "Reduce entity count and quickly generate a demo-ready model",
                "constraints": "Prefer simpler satellite splitting; coarse granularity is recommended"
            },
            "governance": {
                "name": "Fine-Grained Governance Template",
                "goal": "Increase semantic precision of fields and improve governance usability",
                "constraints": "Preserve richer attribute grouping; fine granularity is recommended"
            }
        }
        return templates.get(template_id, templates["standard"])

    def _extract_granularity_from_text(self, text: str) -> str:
        """从用户输入中强制提取粒度设置"""
        if not text:
            return "balanced"

        lower_text = text.lower()
        tokens = [
            "粒度:", "粒度：", "granularity:", "granularity：",
            "粗粒度", "细粒度", "中粒度",
            "粗一点", "细一点", "中等粒度",
            "coarse", "fine", "balanced"
        ]

        if any(token in lower_text for token in tokens):
            if any(token in lower_text for token in ["粗粒度", "coarse", "粗一点", "简单粒度", "粗糙"]):
                return "coarse"
            if any(token in lower_text for token in ["细粒度", "fine", "细一点", "细化", "detailed"]):
                return "fine"
            if any(token in lower_text for token in ["中粒度", "balanced", "中等粒度"]):
                return "balanced"

        return "balanced"

    def _get_enhanced_default_analysis(self, user_input: str = "") -> Dict[str, Any]:
        """返回增强的默认分析结果"""
        # 基于用户输入进行智能默认设置
        user_input_lower = user_input.lower()

        # 检测用户是否关心特定方面
        risk_focused = any(keyword in user_input_lower for keyword in ['风险', 'risk', 'compliance', '监管', 'audit'])
        relationship_focused = any(keyword in user_input_lower for keyword in ['关系', 'relationship', '网络', 'network'])
        temporal_focused = any(keyword in user_input_lower for keyword in ['时间', 'temporal', '历史', 'history'])

        granularity = self._extract_granularity_from_text(user_input)

        return {
            "analysis": f"基于用户需求的智能Data Vault转换策略 - 检测到{'合规重点，' if risk_focused else ''}{'关系网络分析，' if relationship_focused else ''}{'时间维度管理' if temporal_focused else '全面数据建模'}",
            "core_entities": {
                "hubs": ["Party", "Party_Role", "Contact", "Address", "Relationship"],
                "links": ["Party_Role", "Party_Contact", "Party_Address", "Party_Relationship"],
                "satellites": ["Party_Basic", "Party_Role_Details", "Contact_Information", "Address_Details", "Relationship_Details"]
            },
            "relationships": {
                "hub_to_link": ["Party→Party_Role", "Party→Party_Contact", "Party→Party_Address", "Party→Party_Relationship"],
                "link_to_satellite": ["Party_Role→Party_Role_Details", "Party_Contact→Contact_Information"],
                "entity_connections": ["Party拥有多个角色", "Party有多个联系方式", "Party有多个地址", "Party间存在关系"]
            },
            "compliance_requirements": {
                "risk_management": risk_focused,
                "audit_trail": risk_focused or temporal_focused,
                "regulatory_compliance": risk_focused,
                "data_governance": True
            },
            "diagram_customization": {
                "show_relationship_arrows": True,
                "detailed_connections": relationship_focused,
                "entity_expansion": ["Party"] if not relationship_focused else ["Party", "Party_Role"],
                "layout_preference": "hierarchical",
                "color_scheme": "professional"
            },
            "granularity": granularity,
            "raw_vault": {
                "generate_hubs": True,
                "generate_links": True,
                "generate_satellites": True
            },
            "business_vault": {
                "generate_pit": True,
                "generate_bridge": True,
                "generate_historic": True
            },
            "field_mapping_focus": {
                "key_fields": ["party_id", "party_role_id", "contact_id", "address_id", "relationship_id"],
                "compliance_fields": ["risk_rating", "compliance_status", "audit_trail"] if risk_focused else [],
                "relationship_fields": ["relationship_type", "relationship_strength"] if relationship_focused else [],
                "temporal_fields": ["valid_from", "valid_to", "created_date", "last_updated_date"] if temporal_focused else []
            },
            "output_priorities": {
                "primary_vault_type": "raw",
                "emphasis_on_relationships": relationship_focused,
                "include_business_rules": risk_focused,
                "generate_compliance_checks": risk_focused
            },
            "special_instructions": f"生成完整的Raw Vault和Business Vault结构。{'特别关注合规和风险管理字段的映射' if risk_focused else ''}{'重点展示实体间的关系连接' if relationship_focused else ''}{'包含完整的时间维度管理' if temporal_focused else ''}"
        }


class AIDiagramGenerator:
    """AI驱动的图表生成器"""

    def __init__(self, deepseek_client: DeepSeekAI):
        self.ai_client = deepseek_client

    def generate_custom_diagram(self, dv_model: DataVaultModel, analysis: Dict[str, Any],
                               vault_type: str = "raw") -> str:
        """
        根据AI分析结果生成定制的图表

        Args:
            dv_model: Data Vault模型
            analysis: AI分析结果
            vault_type: raw 或 business

        Returns:
            SVG格式的图表
        """

        entities = dv_model.raw_vault if vault_type == "raw" else dv_model.business_vault

        # 根据AI分析过滤实体
        filtered_entities = self._filter_entities_by_analysis(entities, analysis, vault_type)

        # 生成SVG图表
        return self._generate_svg_diagram(filtered_entities, analysis, vault_type)

    def _filter_entities_by_analysis(self, entities: List[Any], analysis: Dict[str, Any],
                                   vault_type: str) -> List[Any]:
        """根据AI分析结果过滤实体，并基于原始图表映射关系校正实体类型"""

        priority_entities = analysis.get("priority_entities", [])
        compliance_focus = analysis.get("compliance_focus", {})

        # 基于原始raw_vault_diagram.svg的正确映射关系
        # 严格按照原始SVG的实体分类进行映射
        correct_mappings = {
            # HUB实体 - 核心业务标识（严格按照原始SVG）
            "party": "hub",              # HUB_Party
            "party_role": "hub",         # HUB_Party_Role（原始SVG中是HUB）
            "contact": "hub",            # HUB_Contact
            "address": "hub",            # HUB_Address
            "relationship": "hub",       # HUB_Relationship

            # LINK实体 - 连接关系（严格按照原始SVG）
            "party_role_link": "link",   # LNK_Party_Role
            "party_contact_link": "link", # LNK_Party_Contact
            "party_address_link": "link", # LNK_Party_Address
            "party_relationship_link": "link", # LNK_Party_Relationship

            # SATELLITE mappings - 描述性属性（默认）
        }

        filtered = []
        seen_names = set()  # 按名称去重，避免同一实体显示两次（如 hub_location_involvement）

        for entity in entities:
            entity_name = getattr(entity, 'name', '').strip()
            entity_name_lower = entity_name.lower()

            # 去重：同名实体只保留第一次出现的
            if entity_name in seen_names:
                continue
            seen_names.add(entity_name)

            # 排除应仅作为 Link 的 Hub：已有 link_party_party_relationship 时不再显示 hub_party_party_relationship
            if entity_name_lower == "hub_party_party_relationship":
                continue
            # 排除其他“关系型”重复 Hub（名称形如 hub_*_*_relationship 且对应 link 存在时只保留 link）
            if entity_name_lower.startswith("hub_") and entity_name_lower.endswith("_relationship") and "party_party" in entity_name_lower:
                continue

            # 基于原始图表映射关系校正实体类型
            corrected_entity_type = self._correct_entity_type(entity_name_lower, correct_mappings)

            # 如果需要校正，修改实体的entity_type
            if corrected_entity_type and corrected_entity_type != entity.entity_type.value:
                # 创建一个新的实体对象，修改其类型
                original_entity = entity
                entity = type('CorrectedEntity', (), {})()
                entity.__dict__.update(original_entity.__dict__)
                entity.entity_type = type('EntityType', (), {'value': corrected_entity_type})()

            # 基础过滤
            should_include = True

            if vault_type == "raw":
                raw_config = analysis.get("raw_vault", {})
                if entity.entity_type.value == "hub" and not raw_config.get("generate_hubs", True):
                    should_include = False
                elif entity.entity_type.value == "link" and not raw_config.get("generate_links", True):
                    should_include = False
                elif entity.entity_type.value == "satellite" and not raw_config.get("generate_satellites", True):
                    should_include = False

            elif vault_type == "business":
                business_config = analysis.get("business_vault", {})
                if entity.entity_type.value == "pit" and not business_config.get("generate_pit", True):
                    should_include = False
                elif entity.entity_type.value == "bridge" and not business_config.get("generate_bridge", True):
                    should_include = False
                elif entity.entity_type.value == "historic" and not business_config.get("generate_historic", True):
                    should_include = False

            # 合规重点过滤
            if compliance_focus.get("risk_management") and "risk" in entity_name_lower:
                should_include = True
            if compliance_focus.get("regulatory_reporting") and any(term in entity_name_lower for term in ["compliance", "regulatory", "audit"]):
                should_include = True

            if should_include:
                filtered.append(entity)

        # 按类型排序确保 Hub -> Link -> Satellite 顺序，并提高显示上限以包含所有 Hub
        type_order = {"hub": 0, "link": 1, "satellite": 2, "pit": 3, "bridge": 4, "historic": 5}
        filtered.sort(key=lambda e: (type_order.get(e.entity_type.value, 9), getattr(e, "name", "")))
        return filtered[:50]  # 提高上限，确保 Hub/Link/Satellite 都能显示

    def _correct_entity_type(self, entity_name: str, correct_mappings: Dict[str, str]) -> str:
        """
        基于原始图表映射关系校正实体类型

        Args:
            entity_name: 实体名称
            correct_mappings: 正确的映射关系字典

        Returns:
            校正后的实体类型，如果不需要校正则返回None
        """
        entity_name_lower = entity_name.lower()

        # 映射层命名：hub_*, link_*, sat_*。先按前缀保留类型，避免误校正
        if entity_name_lower.startswith("link_"):
            return "link"
        if entity_name_lower.startswith("hub_"):
            pass  # 继续用 table_corrections 校正（保持 hub）
        elif entity_name_lower.startswith("sat_"):
            return None  # 保持 satellite
        elif entity_name_lower.startswith("pit_") or entity_name_lower.startswith("lnk_"):
            return None  # 保持原类型

        # 移除常见前缀进行匹配
        clean_name = entity_name_lower.replace("hub_", "").replace("lnk_", "").replace("sat_", "").replace("pit_", "").replace("link_", "")

        # 基于原始SVG的精确映射关系校正（仅用于 hub_ 开头的实体）
        table_corrections = {
            # 按照原始SVG的分类进行校正
            "party": "hub",           # HUB_Party
            "party_role": "hub",      # HUB_Party_Role（原始SVG中是HUB）
            "contact": "hub",         # HUB_Contact
            "address": "hub",         # HUB_Address
            "relationship": "hub",    # HUB_Relationship

            # LINK表（当存在时）
            "party_role_link": "link",    # LNK_Party_Role
            "party_contact_link": "link", # LNK_Party_Contact
            "party_address_link": "link", # LNK_Party_Address
        }

        # 精确表名匹配校正
        if clean_name in table_corrections:
            return table_corrections[clean_name]

        # 基于原始SVG的实体名匹配
        for key, correct_type in correct_mappings.items():
            if key == clean_name:
                return correct_type

        # 概念实体识别（用于party_contact, party_address等派生表）
        if "contact" in clean_name and "party" not in clean_name:
            return "hub"  # HUB_Contact
        if "address" in clean_name and "party" not in clean_name:
            return "hub"  # HUB_Address
        if "relationship" in clean_name and "party" not in clean_name:
            return "hub"  # HUB_Relationship

        # 其他实体默认保持satellite类型
        return None

    def _generate_svg_diagram(self, entities: List[Any], analysis: Dict[str, Any], vault_type: str) -> str:
        """生成详细的SVG图表，完全匹配原始专业样式"""

        # 从分析结果中提取图表定制参数
        diagram_customization = analysis.get("diagram_customization", {})
        show_arrows = diagram_customization.get("show_relationship_arrows", True)

        # 按类型分组实体
        hubs = [e for e in entities if e.entity_type.value == "hub"]
        links = [e for e in entities if e.entity_type.value == "link"]
        satellites = [e for e in entities if e.entity_type.value == "satellite"]
        pits = [e for e in entities if e.entity_type.value == "pit"]
        bridges = [e for e in entities if e.entity_type.value == "bridge"]
        historics = [e for e in entities if e.entity_type.value == "historic"]

        # 计算SVG尺寸
        hub_count = len(hubs)
        link_count = len(links)
        sat_count = len(satellites)
        max_entities_per_row = 5

        def _rows(count: int) -> int:
            return max(1, (count + max_entities_per_row - 1) // max_entities_per_row)

        def _row_spacing(t: str) -> int:
            if t == "hub":
                return 140
            if t == "link":
                return 160
            return 420  # satellite/pit/bridge/historic

        base_y = 100
        layer_gap = 80
        # 先根据各层行数计算所需总高度，避免下方 sat 被裁切
        content_height = base_y
        if hubs:
            content_height += _rows(hub_count) * _row_spacing("hub") + layer_gap
        if links:
            content_height += _rows(link_count) * _row_spacing("link") + layer_gap
        if satellites:
            content_height += _rows(sat_count) * _row_spacing("satellite") + layer_gap
        if pits:
            content_height += _rows(len(pits)) * _row_spacing("pit") + layer_gap
        if bridges:
            content_height += _rows(len(bridges)) * _row_spacing("bridge") + layer_gap
        if historics:
            content_height += _rows(len(historics)) * _row_spacing("historic") + layer_gap
        content_height += 120  # 底部留白

        if vault_type.lower() == "raw":
            width = max(1600, 300 + hub_count * 280)
            height = max(1200, content_height)
        else:
            width = max(1800, 300 + max(hub_count, link_count, sat_count) * 280)
            height = max(1400, content_height)

        # 颜色方案 - 完全匹配原始SVG的专业配色
        colors = {
            "hub": "#D4EDDA",        # 绿色 - Hub (与原始完全一致)
            "link": "#FFF3CD",       # 黄色 - Link (与原始完全一致)
            "satellite": "#E8DAEF",  # 紫色 - Satellite (与原始完全一致)
            "pit": "#D1ECF1",        # 青色 - PIT
            "bridge": "#F8D7DA",     # 粉色 - Bridge
            "historic": "#E2E3E5"    # 灰色 - Historic
        }

        # 边框颜色 - 匹配原始SVG
        border_colors = {
            "hub": "#28a745",        # 绿色边框
            "link": "#ffc107",       # 黄色边框
            "satellite": "#6c757d",  # 灰色边框
            "pit": "#17a2b8",        # 青色边框
            "bridge": "#dc3545",     # 红色边框
            "historic": "#6c757d"    # 灰色边框
        }

        svg_content = f'''<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%" style="background:#fafafa;">
  <defs>
    <marker id="arrow" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto" markerUnits="strokeWidth">
      <polygon points="0 0, 8 3, 0 6" fill="#666"/>
    </marker>
    <marker id="arrow-solid" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto" markerUnits="strokeWidth">
      <polygon points="0 0, 8 3, 0 6" fill="#333"/>
    </marker>
  </defs>

<text x="{width//2}" y="25" text-anchor="middle" font-size="16" font-weight="bold" fill="#1a1a2e">{vault_type.title()} Vault 2.0 (Party – BIAN aligned)</text>

  <!-- 图例 - 放在更合适的位置 -->
  <g transform="translate(20, 60)">
    <rect x="0" y="0" width="15" height="15" rx="3" fill="{colors['hub']}" stroke="{border_colors['hub']}" stroke-width="1"/>
    <text x="22" y="12" font-size="12" font-weight="500" fill="#333">Hub</text>
    <rect x="80" y="0" width="15" height="15" rx="3" fill="{colors['link']}" stroke="{border_colors['link']}" stroke-width="1"/>
    <text x="102" y="12" font-size="12" font-weight="500" fill="#333">Link</text>
    <rect x="160" y="0" width="15" height="15" rx="3" fill="{colors['satellite']}" stroke="{border_colors['satellite']}" stroke-width="1"/>
    <text x="182" y="12" font-size="12" font-weight="500" fill="#333">Satellite</text>
  </g>
'''

        # 绘制实体和关系
        # 存储实体位置用于绘制关系线
        entity_positions = {}

        # 多行布局：每层的 y 按上一层占用的行数累加
        current_y = base_y

        if hubs:
            svg_content += self._draw_entity_row(hubs, "hub", current_y, colors, border_colors, entity_positions, max_entities_per_row)
            current_y += _rows(len(hubs)) * _row_spacing("hub") + layer_gap

        if links:
            svg_content += self._draw_entity_row(links, "link", current_y, colors, border_colors, entity_positions, max_entities_per_row)
            current_y += _rows(len(links)) * _row_spacing("link") + layer_gap

        if satellites:
            svg_content += self._draw_entity_row(satellites, "satellite", current_y, colors, border_colors, entity_positions, max_entities_per_row)
            current_y += _rows(len(satellites)) * _row_spacing("satellite") + layer_gap

        if pits:
            svg_content += self._draw_entity_row(pits, "pit", current_y, colors, border_colors, entity_positions, max_entities_per_row)
            current_y += _rows(len(pits)) * _row_spacing("pit") + layer_gap

        if bridges:
            svg_content += self._draw_entity_row(bridges, "bridge", current_y, colors, border_colors, entity_positions, max_entities_per_row)
            current_y += _rows(len(bridges)) * _row_spacing("bridge") + layer_gap

        if historics:
            svg_content += self._draw_entity_row(historics, "historic", current_y, colors, border_colors, entity_positions, max_entities_per_row)
            current_y += _rows(len(historics)) * _row_spacing("historic") + layer_gap

        # 绘制关系箭头 - 仅按真实引用关系绘制（Hub->Link、Link->Satellite）
        if show_arrows:
            svg_content += self._draw_relationships(entity_positions, analysis, entities)

        # 添加说明文字
        special_instructions = analysis.get("special_instructions", "")
        if special_instructions:
            svg_content += f'''
  <text x="30" y="{height - 30}" font-size="12" fill="#666">
    Note: {special_instructions}
  </text>
'''

        svg_content += '''
</svg>'''

        return svg_content

    def _draw_entity_row(self, entities: List[Any], entity_type: str, y_offset: int,
                        colors: Dict[str, str], border_colors: Dict[str, str],
                        entity_positions: Dict[str, tuple], max_per_row: int = 5) -> str:
        """绘制一排实体 - 匹配原始SVG的精确样式"""
        svg_content = ""

        # 匹配原始SVG的尺寸
        if entity_type == "hub":
            entity_width = 180  # Hub使用180宽度
            base_height = 96   # Hub基础高度
        elif entity_type == "link":
            entity_width = 200  # Link使用200宽度
            base_height = 114  # Link基础高度
        else:  # satellite
            entity_width = 220  # Satellite使用220宽度
            base_height = 600  # Satellite可以使用更大高度

        entity_spacing = 260  # 增加间距
        # 每行的垂直间距：避免换行后实体堆叠（尤其是 Satellite）
        if entity_type == "hub":
            row_spacing = 140
        elif entity_type == "link":
            row_spacing = 160
        else:
            row_spacing = 420

        for i, entity in enumerate(entities):
            row = i // max_per_row
            col = i % max_per_row

            x = 40 + col * entity_spacing  # 从40开始，与原始SVG一致
            y = y_offset + row * row_spacing

            # 存储实体位置用于绘制关系线
            entity_positions[entity.name] = (x + entity_width // 2, y)

            # 计算实体高度 - 匹配原始SVG
            if entity_type == "hub":
                entity_height = 96  # Hub固定高度
                max_display_cols = 4  # Hub显示4个字段
            elif entity_type == "link":
                entity_height = 114  # Link固定高度
                max_display_cols = 5  # Link显示5个字段
            else:  # satellite
                max_display_cols = min(len(entity.columns), 25)  # Satellite显示更多字段
                entity_height = 50 + max_display_cols * 12

            # 绘制实体框 - 精确匹配原始样式，并为交互埋点
            safe_entity_name = getattr(entity, "name", "")
            svg_content += f'''  <g transform="translate({x}, {y})" data-dv-entity="{safe_entity_name}">
    <rect x="0" y="0" width="{entity_width}" height="{entity_height}" rx="6" fill="{colors[entity_type]}" stroke="{border_colors[entity_type]}" stroke-width="2"/>
    <text class="dv-entity-name" data-dv-entity="{safe_entity_name}" x="{entity_width//2}.0" y="18" text-anchor="middle" font-weight="bold" font-size="12" fill="#1a1a2e">{safe_entity_name}</text>
    <text x="{entity_width//2}.0" y="32" text-anchor="middle" font-size="10" fill="#333">({entity_type.upper()})</text>
'''

            # 绘制字段 - 匹配原始样式
            start_x = 8 if entity_type != "hub" else 48  # Hub字段从48开始
            field_y_start = 50 if entity_type == "hub" else 45

            for j, col in enumerate(entity.columns[:max_display_cols]):
                y_pos = field_y_start + j * 12
                full_col_name = getattr(col, "name", "")
                display_name = full_col_name[:22] + "..." if len(full_col_name) > 22 else full_col_name

                # 添加PK标识
                lower_col = full_col_name.lower()
                pk_indicator = " (PK)" if any(keyword in lower_col for keyword in ["hk", "key"]) and lower_col.endswith("_hk") else ""

                svg_content += f'''    <text class="dv-column" data-dv-entity="{safe_entity_name}" data-dv-column="{full_col_name}" x="{start_x}" y="{y_pos}" font-size="10" fill="#333">{display_name}{pk_indicator}</text>'''

            # Satellite显示省略号
            if entity_type == "satellite" and len(entity.columns) > max_display_cols:
                remaining = len(entity.columns) - max_display_cols
                svg_content += f'''    <text x="{start_x}" y="{field_y_start + max_display_cols * 12}" font-size="9" fill="#666">...{remaining} more</text>'''

            svg_content += '''  </g>
'''

        return svg_content

    def _draw_relationships(self, entity_positions: Dict[str, tuple], analysis: Dict[str, Any],
                            entities: Optional[List[Any]] = None) -> str:
        """按真实引用关系绘制箭头：Hub->Link 实线，Hub->Satellite 虚线（Sat 属于其父 Hub）"""
        svg_content = ""

        hubs = [n for n in entity_positions if n.lower().startswith("hub_")]
        links = [n for n in entity_positions if n.lower().startswith(("lnk_", "link_"))]
        satellites = [n for n in entity_positions if n.lower().startswith("sat_")]

        HUB_HEIGHT = 96
        LINK_HEIGHT = 114

        # 按实体名推断：Link 名 link_A_B 表示引用 hub_A 与 hub_B；Sat 名 sat_A_xxx 表示属于实体 A
        def hub_key(hub_name: str) -> str:
            return hub_name.lower().replace("hub_", "")

        def link_referenced_hub_keys(link_name: str) -> List[str]:
            """从 link 名推断引用了哪些 hub（按 key 匹配，长 key 优先避免 party 吃掉 party_role）"""
            raw = link_name.lower().replace("link_", "").replace("lnk_", "")
            refs = []
            # 按 hub key 长度从长到短，这样先匹配 party_role 再匹配 party
            for hub in sorted(hubs, key=lambda h: -len(hub_key(h))):
                k = hub_key(hub)
                if k in raw and k not in refs:
                    refs.append(k)
            return refs

        # 与 mapper 的 _group_attributes_by_category 一致，用于解析 sat_{entity}_{group}
        _KNOWN_GROUPS = frozenset(["basic", "contact", "address", "profile", "role", "other", "core", "extended", "all", "compliance", "audit", "details", "info", "information"])

        def sat_parent_name(sat_name: str) -> Optional[str]:
            """获取 Satellite 的父实体：HK 列优先，其次通过名称匹配父 Hub/Link"""
            parent_key = None
            # 1. 优先从 HK 列解析（最可靠来源）
            if entities:
                for e in entities:
                    et = getattr(e.entity_type, 'value', str(e.entity_type))
                    if et != 'satellite':
                        continue
                    if getattr(e, 'name', '').lower() != sat_name.lower():
                        continue
                    for col in getattr(e, 'columns', []) or []:
                        col_name = (getattr(col, 'name', '') or '').lower()
                        if not col_name.endswith('_hk') or col_name == 'hash_diff':
                            continue
                        ct = getattr(getattr(col, 'column_type', None), 'value', None) or str(getattr(col, 'column_type', ''))
                        if 'hash_key' in str(ct).lower():
                            parent_key = col_name[:-3]
                            break
                    if parent_key:
                        break

            # 2. sat_{entity}_{group} 命名解析
            if not parent_key:
                raw = sat_name.lower().replace("sat_", "", 1)
                parts = raw.split("_")
                if len(parts) >= 2 and parts[-1] in _KNOWN_GROUPS:
                    parent_key = "_".join(parts[:-1])

            # 3. 最终回退：用最长 hub_key 前缀匹配 sat 名
            if not parent_key:
                raw = sat_name.lower().replace("sat_", "", 1)
                hub_candidates = sorted([hub_key(h) for h in hubs], key=len, reverse=True)
                for key in hub_candidates:
                    if raw.startswith(key + "_") or raw == key:
                        parent_key = key
                        break

            if not parent_key:
                return None

            # 4. 在 entity_positions 中查找父实体：先 Hub 后 Link
            for k in entity_positions:
                kl = k.lower()
                if kl.startswith("hub_") and hub_key(k) == parent_key:
                    return k
            for k in entity_positions:
                kl = k.lower()
                if (kl.startswith("link_") or kl.startswith("lnk_")):
                    r = kl.replace("link_", "").replace("lnk_", "")
                    if r.startswith(parent_key + "_") or r == parent_key:
                        return k
            return None

        # Hub -> Link：仅当该 Link 引用该 Hub 时连线
        for link in links:
            ref_keys = link_referenced_hub_keys(link)
            link_pos = entity_positions[link]
            lx, ly = link_pos[0], link_pos[1]

            for hub in hubs:
                if hub_key(hub) not in ref_keys:
                    continue
                hub_pos = entity_positions[hub]
                hx, hy = hub_pos[0], hub_pos[1] + HUB_HEIGHT
                svg_content += f'''  <path d="M{hx}.0,{hy} L{lx}.0,{ly}" fill="none" stroke="#666" stroke-width="1" marker-end="url(#arrow)"/>'''

        # Hub/Link -> Satellite：每个 Sat 仅连接其父实体（1:1，用 HK 列确定）
        for sat in satellites:
            parent = sat_parent_name(sat)
            if parent is None or parent not in entity_positions:
                continue
            sat_pos = entity_positions[sat]
            sx, sy = sat_pos[0], sat_pos[1]
            parent_pos = entity_positions[parent]
            # 父为 Hub 用 HUB_HEIGHT，父为 Link 用 LINK_HEIGHT
            ph = HUB_HEIGHT if parent.lower().startswith("hub_") else LINK_HEIGHT
            px, py = parent_pos[0], parent_pos[1] + ph
            svg_content += f'''  <path d="M{px}.0,{py} L{sx}.0,{sy}" fill="none" stroke="#666" stroke-width="1" stroke-dasharray="4,2" marker-end="url(#arrow)"/>'''

        # 若无语义关系则回退到配置的关系
        if not svg_content and entity_positions:
            relationships = analysis.get("relationships", {})
            hub_to_link = relationships.get("hub_to_link", [])
            link_to_satellite = relationships.get("link_to_satellite", [])

            for rel in hub_to_link + link_to_satellite:
                if "→" in rel:
                    from_entity, to_entity = rel.split("→")
                    from_pos = self._find_entity_position(entity_positions, from_entity.strip())
                    to_pos = self._find_entity_position(entity_positions, to_entity.strip())

                    if from_pos and to_pos:
                        from_bottom = from_pos[1] + HUB_HEIGHT if from_entity.lower().startswith("hub") else from_pos[1] + LINK_HEIGHT
                        stroke_style = "stroke-dasharray=\"4,2\" " if "sat" in to_entity.lower() or "satellite" in to_entity.lower() else ""
                        svg_content += f'''  <path d="M{from_pos[0]}.0,{from_bottom} L{to_pos[0]}.0,{to_pos[1]}" fill="none" stroke="#666" stroke-width="1" {stroke_style}marker-end="url(#arrow)"/>'''

        return svg_content

    def _find_entity_position(self, entity_positions: Dict[str, tuple], entity_name: str) -> tuple:
        """查找实体位置，支持模糊匹配"""
        # 精确匹配
        if entity_name in entity_positions:
            return entity_positions[entity_name]

        # 模糊匹配（移除前缀）
        for name, pos in entity_positions.items():
            if entity_name.lower() in name.lower() or name.lower().replace("hub_", "").replace("lnk_", "").replace("sat_", "") == entity_name.lower():
                return pos

        return None


class AI_DataVault_WebApp:
    """AI驱动的Data Vault Web应用"""

    def __init__(self, deepseek_api_key: str):
        self.app = Flask(__name__,
                        template_folder='templates',
                        static_folder='static')
        CORS(self.app)

        # 配置
        self.app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB
        self.app.config['SECRET_KEY'] = 'ai_datavault_secret_key_2024'

        # 创建上传目录
        self.upload_folder = project_root / 'data' / 'input' / 'uploads'
        self.upload_folder.mkdir(parents=True, exist_ok=True)

        # 初始化AI客户端
        self.ai_client = DeepSeekAI(deepseek_api_key)
        self.diagram_generator = AIDiagramGenerator(self.ai_client)

        # 初始化其他组件
        self.reader = LegacyModelReader()
        self.bian_mapper = BIANMappingLayer()
        self.dv_transformer = DataVaultTransformer()

        self.setup_routes()

    def _ensure_granularity(self, analysis: Dict[str, Any], user_requirement: str) -> Dict[str, Any]:
        """确保粒度设置：委托给 AI 客户端处理"""
        return self.ai_client._ensure_granularity(analysis, user_requirement)

    def _build_lineage_payload(self, dv_model: DataVaultModel, granularity: str) -> Dict[str, Any]:
        """构建血缘图与审计数据"""
        entities = list(dv_model.raw_vault) + list(dv_model.business_vault)

        nodes = []
        field_lineage = []
        node_map = {}
        for e in entities:
            name = getattr(e, 'name', '')
            et = getattr(getattr(e, 'entity_type', None), 'value', 'unknown')
            cols = getattr(e, 'columns', []) or []
            source_tables = sorted(list(set(getattr(e, 'source_tables', []) or [])))
            node = {
                'id': name,
                'label': name,
                'type': et,
                'column_count': len(cols),
                'source_tables': source_tables,
                'layer': 'raw' if e in dv_model.raw_vault else 'business'
            }
            nodes.append(node)
            node_map[name] = node

            for c in cols:
                src_t = getattr(c, 'source_table', None)
                src_c = getattr(c, 'source_column', None)
                if src_t or src_c:
                    field_lineage.append({
                        'entity': name,
                        'entity_type': et,
                        'column': getattr(c, 'name', ''),
                        'source_table': src_t or '',
                        'source_column': src_c or '',
                        'status': 'ok' if (src_t and src_c) else 'partial'
                    })

        edges = []
        edge_keys = set()

        for rel in dv_model.relationships:
            f = rel.from_entity
            t = rel.to_entity
            if f in node_map and t in node_map:
                k = (f, t)
                if k not in edge_keys:
                    edge_keys.add(k)
                    edges.append({
                        'source': f,
                        'target': t,
                        'type': getattr(rel, 'relationship_type', 'FK')
                    })

        # 补充 Sat -> Hub/Link 的兜底关系（按 *_hk 列）
        for e in entities:
            et = getattr(getattr(e, 'entity_type', None), 'value', '')
            if et != 'satellite':
                continue
            e_name = getattr(e, 'name', '')
            for c in getattr(e, 'columns', []) or []:
                cn = getattr(c, 'name', '') or ''
                if not cn.endswith('_hk'):
                    continue
                parent_key = cn[:-3]
                candidates = [f'hub_{parent_key}', f'link_{parent_key}', f'lnk_{parent_key}']
                parent = next((x for x in candidates if x in node_map), None)
                if parent:
                    k = (e_name, parent)
                    if k not in edge_keys:
                        edge_keys.add(k)
                        edges.append({'source': e_name, 'target': parent, 'type': 'FK'})
                    break

        incoming = {n['id']: 0 for n in nodes}
        outgoing = {n['id']: 0 for n in nodes}
        for e in edges:
            outgoing[e['source']] = outgoing.get(e['source'], 0) + 1
            incoming[e['target']] = incoming.get(e['target'], 0) + 1

        orphan_satellites = [n['id'] for n in nodes if n['type'] == 'satellite' and outgoing.get(n['id'], 0) == 0]
        hub_without_sat = [n['id'] for n in nodes if n['type'] == 'hub' and incoming.get(n['id'], 0) == 0]
        link_without_hub_refs = [n['id'] for n in nodes if n['type'] == 'link' and outgoing.get(n['id'], 0) == 0]

        ok_fields = sum(1 for f in field_lineage if f['status'] == 'ok')
        completeness = round((ok_fields / len(field_lineage) * 100), 2) if field_lineage else 0

        overview = {
            'node_count': len(nodes),
            'edge_count': len(edges),
            'hub_count': sum(1 for n in nodes if n['type'] == 'hub'),
            'link_count': sum(1 for n in nodes if n['type'] == 'link'),
            'satellite_count': sum(1 for n in nodes if n['type'] == 'satellite'),
            'orphan_count': len(orphan_satellites),
            'lineage_completeness': completeness,
            'granularity': granularity
        }

        checks = {
            'orphan_satellites': orphan_satellites,
            'hub_without_sat': hub_without_sat,
            'link_without_hub_refs': link_without_hub_refs
        }

        return {
            'overview': overview,
            'nodes': nodes,
            'edges': edges,
            'field_lineage': field_lineage,
            'checks': checks
        }

    def _build_static_lineage_html(self, lineage: Dict[str, Any], filepath: str, granularity: str) -> str:
        """生成静态血缘HTML页面内容"""
        payload = {
            'lineage': lineage,
            'filepath': filepath,
            'granularity': granularity
        }
        payload_json = json.dumps(payload, ensure_ascii=False)

        return f"""<!DOCTYPE html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>Lineage Snapshot</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background:#0f172a; color:#e2e8f0; margin:0; padding:16px; }}
    .wrap {{ max-width:1500px; margin:0 auto; }}
    .header {{ background:#111827; border:1px solid #1f2937; border-radius:10px; padding:14px; margin-bottom:12px; }}
    .title {{ font-size:20px; font-weight:700; }}
    .sub {{ color:#94a3b8; margin-top:4px; font-size:13px; }}
    .metrics {{ display:grid; grid-template-columns:repeat(6,minmax(0,1fr)); gap:10px; margin-bottom:12px; }}
    .card {{ background:#111827; border:1px solid #1f2937; border-radius:10px; padding:10px; }}
    .k {{ color:#93c5fd; font-size:12px; }} .v {{ font-size:22px; font-weight:700; margin-top:4px; }}
    .main {{ display:grid; grid-template-columns:2fr 1fr; gap:12px; }}
    .panel {{ background:#111827; border:1px solid #1f2937; border-radius:10px; padding:10px; }}
    .pt {{ color:#bfdbfe; font-weight:600; margin-bottom:8px; }}
    #graph {{ width:100%; min-height:900px; overflow:auto; background:#0b1220; border:1px solid #1f2937; border-radius:8px; }}
    table {{ width:100%; border-collapse:collapse; font-size:12px; }}
    th,td {{ border-bottom:1px solid #1f2937; padding:6px 8px; text-align:left; }}
    th {{ color:#93c5fd; }}
  </style>
</head>
<body>
<div class=\"wrap\">
  <div class=\"header\">
    <div class=\"title\">Data Lineage Snapshot</div>
    <div class=\"sub\">文件：{escape(filepath)} · 粒度：{escape(granularity)} · 本页为分析后静态快照</div>
  </div>
  <div class=\"metrics\" id=\"metrics\"></div>
  <div class=\"main\">
    <div class=\"panel\"><div class=\"pt\">关系图谱</div><div id=\"graph\"></div></div>
    <div class=\"panel\">
      <div class=\"pt\">质量检查</div>
      <div><b>孤立Satellite</b><div id=\"sat\"></div></div>
      <div style=\"margin-top:8px;\"><b>未挂载Satellite的Hub</b><div id=\"hub\"></div></div>
      <div style=\"margin-top:8px;\"><b>无Hub引用的Link</b><div id=\"link\"></div></div>
    </div>
  </div>
  <div class=\"panel\" style=\"margin-top:12px;\">
    <div class=\"pt\">字段级血缘</div>
    <table>
      <thead><tr><th>Entity</th><th>Type</th><th>Column</th><th>Source Table</th><th>Source Column</th><th>Status</th></tr></thead>
      <tbody id=\"tbody\"></tbody>
    </table>
  </div>
</div>
<script>
const payload = {payload_json};
const L = payload.lineage;
function esc(s){{if(s==null)return '';return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\"/g,'&quot;').replace(/'/g,'&#39;')}}
function renderMetrics(){{
 const ov=L.overview||{{}};
 const cards=[['节点总数',ov.node_count],['关系总数',ov.edge_count],['Hub',ov.hub_count],['Link',ov.link_count],['Satellite',ov.satellite_count],['完整率',(ov.lineage_completeness||0)+'%']];
 document.getElementById('metrics').innerHTML=cards.map(c=>`<div class=\"card\"><div class=\"k\">${{c[0]}}</div><div class=\"v\">${{c[1]??0}}</div></div>`).join('');
}}
function fillList(id,arr){{document.getElementById(id).innerHTML=(arr&&arr.length)?arr.map(x=>`<div>${{esc(x)}}</div>`).join(''):'<div style=\"color:#94a3b8\">无</div>';}}
function renderChecks(){{const c=L.checks||{{}};fillList('sat',c.orphan_satellites);fillList('hub',c.hub_without_sat);fillList('link',c.link_without_hub_refs);}}
function colorByType(t){{if(t==='hub')return '#22c55e';if(t==='link')return '#f59e0b';if(t==='satellite')return '#a78bfa';return '#94a3b8';}}
function renderGraph(){{
 const nodes=L.nodes||[]; const edges=L.edges||[];
 const hubs=nodes.filter(n=>n.type==='hub'), links=nodes.filter(n=>n.type==='link'), sats=nodes.filter(n=>n.type==='satellite');
 const pos={{}}; const place=(arr,y)=>arr.forEach((n,i)=>pos[n.id]={{x:40+(i%5)*220,y:y+Math.floor(i/5)*170}});
 place(hubs,40); place(links,260); place(sats,500);
 let svg=`<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 1300 1000\" width=\"100%\" height=\"1000\"><defs><marker id=\"arr\" markerWidth=\"8\" markerHeight=\"6\" refX=\"7\" refY=\"3\" orient=\"auto\"><polygon points=\"0 0,8 3,0 6\" fill=\"#94a3b8\"/></marker></defs>`;
 edges.forEach(e=>{{const s=pos[e.source],t=pos[e.target]; if(!s||!t) return; svg += `<line x1=\"${{s.x+85}}\" y1=\"${{s.y+46}}\" x2=\"${{t.x+85}}\" y2=\"${{t.y}}\" stroke=\"#94a3b8\" stroke-width=\"1.2\" marker-end=\"url(#arr)\"/>`;}});
 nodes.forEach(n=>{{const p=pos[n.id]; if(!p) return; const c=colorByType(n.type); svg += `<g><rect x=\"${{p.x}}\" y=\"${{p.y}}\" width=\"170\" height=\"46\" rx=\"8\" fill=\"#111827\" stroke=\"${{c}}\" stroke-width=\"2\"/><text x=\"${{p.x+8}}\" y=\"${{p.y+20}}\" fill=\"#e5e7eb\" font-size=\"11\">${{esc(n.id)}}</text><text x=\"${{p.x+8}}\" y=\"${{p.y+35}}\" fill=\"#93c5fd\" font-size=\"10\">${{esc(n.type)}} · cols:${{n.column_count}}</text></g>`;}});
 svg += '</svg>'; document.getElementById('graph').innerHTML=svg;
}}
function renderField(){{
 const rows=L.field_lineage||[];
 document.getElementById('tbody').innerHTML=rows.map(r=>`<tr><td>${{esc(r.entity)}}</td><td>${{esc(r.entity_type)}}</td><td>${{esc(r.column)}}</td><td>${{esc(r.source_table)}}</td><td>${{esc(r.source_column)}}</td><td>${{esc(r.status)}}</td></tr>`).join('');
}}
renderMetrics(); renderChecks(); renderGraph(); renderField();
</script>
</body>
</html>"""

    def setup_routes(self):
        """设置路由"""

        @self.app.route('/')
        def index():
            return render_template('ai_datavault.html')

        @self.app.route('/api/upload', methods=['POST'])
        def upload_file():
            """上传文件"""
            try:
                if 'file' not in request.files:
                    return jsonify({'success': False, 'error': 'No file'}), 400

                file = request.files['file']
                if file.filename == '':
                    return jsonify({'success': False, 'error': 'No file selected'}), 400

                if file and self.allowed_file(file.filename):
                    filename = secure_filename(file.filename)
                    filepath = self.upload_folder / filename
                    file.save(str(filepath))

                    return jsonify({
                        'success': True,
                        'filename': filename,
                        'filepath': str(filepath)
                    })
                else:
                    return jsonify({'success': False, 'error': 'Unsupported file type'}), 400

            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/analyze', methods=['POST'])
        def analyze_requirement():
            """AI分析用户需求"""
            try:
                data = request.json or {}
                filepath = data.get('filepath')
                user_requirement = data.get('requirement', '')

                if not filepath:
                    return jsonify({'success': False, 'error': 'Missing file path'}), 400

                # 读取文件获取表列表
                tables = self.reader.read(filepath)
                available_tables = [table.name for table in tables]

                template_id = data.get('template_id', 'standard')
                granularity_override = data.get('granularity')

                # AI分析需求
                analysis = self.ai_client.analyze_requirement(
                    user_requirement,
                    available_tables,
                    template_id=template_id,
                    granularity_override=granularity_override
                )
                analysis = self._ensure_granularity(analysis, user_requirement)

                # 显式粒度选择优先级最高
                if granularity_override:
                    analysis['granularity'] = self.ai_client._normalize_granularity_value(granularity_override)
                analysis['template_id'] = template_id

                # 分析后自动生成最新静态血缘快照
                final_granularity = analysis.get('granularity', 'balanced')
                bian_result = self.bian_mapper.map_tables(tables, source_file=filepath)
                dv_model = self.dv_transformer.transform(bian_result, granularity=final_granularity)
                lineage_payload = self._build_lineage_payload(dv_model, final_granularity)

                static_dir = project_root / 'ai_datavault' / 'static' / 'lineage'
                static_dir.mkdir(parents=True, exist_ok=True)
                static_file = static_dir / 'latest.html'
                html = self._build_static_lineage_html(lineage_payload, filepath, final_granularity)
                static_file.write_text(html, encoding='utf-8')

                lineage_url = url_for('static', filename='lineage/latest.html', _external=False)

                return jsonify({
                    'success': True,
                    'analysis': analysis,
                    'available_tables': available_tables,
                    'lineage_url': lineage_url
                })

            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/generate', methods=['POST'])
        def generate_datavault():
            """生成Data Vault模型和图表"""
            try:
                data = request.json or {}
                filepath = data.get('filepath')
                analysis = data.get('analysis', {})

                if not filepath:
                    return jsonify({'success': False, 'error': 'Missing file path'}), 400

                # 1. 读取和映射数据
                tables = self.reader.read(filepath)
                bian_result = self.bian_mapper.map_tables(tables, source_file=filepath)

                # 1.1 准备“解释器表结构 + BIAN 映射结果”给前端可展开面板
                # 注意：这里不做额外计算，仅序列化现有中间结果。
                interpreter_tables = [_serialize_table(t) for t in (tables or [])]
                bian_mapping = _serialize_bian_mapping(bian_result)

                # 2. 转换为Data Vault
                granularity = (analysis or {}).get("granularity", "balanced")
                dv_model = self.dv_transformer.transform(bian_result, granularity=granularity)

                # 3. 生成定制图表
                raw_diagram = self.diagram_generator.generate_custom_diagram(dv_model, analysis, "raw")
                business_diagram = self.diagram_generator.generate_custom_diagram(dv_model, analysis, "business")

                # 4. 生成数据字典
                dictionary = self.dv_transformer.generate_data_dictionary(dv_model)

                # 构建详细的图表元数据
                diagram_metadata = {
                    'show_relationship_arrows': analysis.get('diagram_customization', {}).get('show_relationship_arrows', True),
                    'detailed_connections': analysis.get('diagram_customization', {}).get('detailed_connections', False),
                    'entity_count': len(dv_model.raw_vault) + len(dv_model.business_vault),
                    'relationship_count': len(dv_model.relationships),
                    'vault_type': analysis.get('output_priorities', {}).get('primary_vault_type', 'raw'),
                    'compliance_focused': analysis.get('compliance_requirements', {}).get('risk_management', False)
                }

                return jsonify({
                    'success': True,
                    'diagrams': {
                        'raw_vault': raw_diagram,
                        'business_vault': business_diagram,
                        'metadata': diagram_metadata
                    },
                    'dictionary': dictionary,
                    'interpreter_tables': interpreter_tables,
                    'bian_mapping': bian_mapping,
                    'summary': {
                        'tables_processed': len(bian_result.table_mappings),
                        'raw_entities': len(dv_model.raw_vault),
                        'business_entities': len(dv_model.business_vault),
                        'relationships': len(dv_model.relationships),
                        'granularity': granularity
                    }
                })

            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                print(f"生成失败: {error_details}")
                return jsonify({'success': False, 'error': str(e), 'details': error_details}), 500

        @self.app.route('/lineage')
        def lineage_page():
            """跳转到最新静态血缘快照"""
            return redirect(url_for('static', filename='lineage/latest.html'))

        @self.app.route('/api/lineage/build', methods=['POST'])
        def build_lineage():
            """构建血缘图数据"""
            try:
                data = request.json or {}
                filepath = data.get('filepath')
                granularity = data.get('granularity', 'balanced')
                user_requirement = data.get('requirement', '')

                if not filepath:
                    return jsonify({'success': False, 'error': 'Missing file path'}), 400

                tables = self.reader.read(filepath)
                bian_result = self.bian_mapper.map_tables(tables, source_file=filepath)

                analysis = {'granularity': granularity}
                analysis = self._ensure_granularity(analysis, user_requirement)
                final_granularity = analysis.get('granularity', 'balanced')

                dv_model = self.dv_transformer.transform(bian_result, granularity=final_granularity)
                lineage_payload = self._build_lineage_payload(dv_model, final_granularity)

                return jsonify({
                    'success': True,
                    'filepath': filepath,
                    'granularity': final_granularity,
                    'lineage': lineage_payload
                })
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                return jsonify({'success': False, 'error': str(e), 'details': error_details}), 500

    def allowed_file(self, filename):
        """检查文件扩展名"""
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'csv', 'xlsx', 'xls'}

    def run(self, host='0.0.0.0', port=5003, debug=True):
        """运行应用"""
        print("=" * 60)
        print("🤖 AI DataVault Generator")
        print("=" * 60)
        print("基于DeepSeek AI解读用户需求，自动生成Data Vault模型")
        print("\n功能特性:")
        print("* BIAN Party Mapping")
        print("* Data Vault 2.0 Transformation (Raw Vault + Business Vault)")
        print("* SVG Diagram Generation")
        print("* Data Dictionary Generation")
        print(f"\n访问地址: http://{host}:{port}")
        print("按 Ctrl+C 停止服务器\n")

        self.app.run(host=host, port=port, debug=debug)


def main():
    """主函数"""
    # 从环境变量获取API Key
    deepseek_api_key = os.getenv('DEEPSEEK_API_KEY')

    if not deepseek_api_key:
        print("错误: 未设置 DEEPSEEK_API_KEY 环境变量")
        print("请先设置环境变量：")
        print("- 方法1: python setup_env.bat")
        print("- 方法2: set DEEPSEEK_API_KEY=your_api_key_here")
        input("\n按Enter键退出...")
        sys.exit(1)

    # 创建并运行应用
    app = AI_DataVault_WebApp(deepseek_api_key)
    app.run()


if __name__ == "__main__":
    main()