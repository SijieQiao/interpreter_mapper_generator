"""
BIAN Party 映射规则引擎

基于配置文件驱动，执行字段/表到 BIAN 标准实体的映射规则。
支持：表映射、字段映射、枚举值转换、条件规则、模糊匹配。
"""
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import yaml


class MappingRuleEngine:
    """配置驱动的映射规则引擎"""

    def __init__(self, config_dir: Optional[Path] = None):
        self.config_dir = config_dir or Path(__file__).parent / "config"
        self._table_mapping: Dict[str, Dict] = {}
        self._column_mapping: Dict[str, Dict] = {}
        self._enums: Dict[str, Dict[str, str]] = {}
        self._rules: Dict[str, Dict] = {}
        self._table_aliases: Dict[str, List[str]] = {}
        self._load_configs()

    def _load_configs(self) -> None:
        """加载所有配置文件"""
        mapping_file = self.config_dir / "bian_party_mapping.yaml"
        enums_file = self.config_dir / "bian_enums.yaml"
        rules_file = self.config_dir / "rule_definitions.yaml"

        if mapping_file.exists():
            with open(mapping_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                self._table_mapping = data.get("table_mapping", {})
                self._column_mapping = data.get("column_mapping", {})

        if enums_file.exists():
            with open(enums_file, "r", encoding="utf-8") as f:
                self._enums = yaml.safe_load(f) or {}

        if rules_file.exists():
            with open(rules_file, "r", encoding="utf-8") as f:
                rules_data = yaml.safe_load(f)
                self._rules = rules_data.get("rules", {})
                if "table_alias" in self._rules:
                    self._table_aliases = self._rules["table_alias"].get("aliases", {})

    def resolve_table_name(self, legacy_table: str) -> Optional[str]:
        """
        解析遗留表名到标准表名（支持别名）
        例如: customer -> party, 主体 -> party
        """
        key = legacy_table.strip().lower()
        if key in self._table_mapping:
            return legacy_table

        for canonical, aliases in self._table_aliases.items():
            if key in [a.lower() for a in aliases]:
                return canonical
        return None

    def get_table_mapping(self, legacy_table: str) -> Optional[Dict]:
        """获取表级 BIAN 映射"""
        resolved = self.resolve_table_name(legacy_table)
        if resolved:
            return self._table_mapping.get(resolved)
        return self._table_mapping.get(legacy_table.strip().lower())

    def get_column_mapping(self, legacy_table: str, legacy_column: str) -> Optional[Dict]:
        """获取字段级 BIAN 映射"""
        resolved_table = self.resolve_table_name(legacy_table) or legacy_table.strip().lower()
        key = f"{resolved_table}.{legacy_column.strip().lower()}"
        key_orig = f"{legacy_table.strip().lower()}.{legacy_column.strip().lower()}"
        return self._column_mapping.get(key) or self._column_mapping.get(key_orig)

    def try_fuzzy_column_mapping(
        self, legacy_table: str, legacy_column: str
    ) -> Optional[Dict]:
        """
        模糊匹配字段映射（表名已标准化后）
        支持: party.customer_id -> 查找 party.*_id 模式
        """
        resolved = self.resolve_table_name(legacy_table) or legacy_table.strip().lower()
        col_lower = legacy_column.strip().lower()

        # 精确匹配
        exact = self.get_column_mapping(legacy_table, legacy_column)
        if exact:
            return exact

        # 遍历已配置的映射，查找相似字段
        prefix = f"{resolved}."
        for map_key, map_val in self._column_mapping.items():
            if map_key.startswith(prefix):
                config_col = map_key[len(prefix) :]
                if config_col.lower() == col_lower:
                    return map_val
                # 简单同义词：id <-> _id
                if config_col.lower().replace("_", "") == col_lower.replace("_", ""):
                    return map_val
        return None

    def apply_enum_mapping(
        self, value: Any, mapping_ref: str
    ) -> Optional[str]:
        """
        应用枚举值映射
        mapping_ref: 如 party_type, address_type, relationship_type
        """
        if value is None or mapping_ref not in self._enums:
            return None
        val_str = str(value).strip()
        mapping = self._enums.get(mapping_ref, {})
        return mapping.get(val_str) or mapping.get(val_str.lower()) or mapping.get(val_str.title())

    def evaluate_condition(
        self, rule_ref: str, context: Dict[str, Any]
    ) -> bool:
        """
        评估条件规则
        context: 如 {"party.party_type": "Individual", "table": "party"}
        """
        if rule_ref not in self._rules:
            return True  # 无规则则默认通过
        rule = self._rules[rule_ref]
        if rule.get("type") != "condition":
            return True
        cond = rule.get("condition")
        if not cond:
            return True
        depends_on = cond.get("depends_on")
        operator = cond.get("operator")
        values = cond.get("values", [])
        actual = context.get(depends_on)
        if actual is None:
            return False
        actual_str = str(actual).strip()
        if operator == "in":
            return any(
                str(v).strip().lower() == actual_str.lower() for v in values
            )
        return True

    def map_table_to_bian(
        self, legacy_table: str
    ) -> Optional[Dict[str, Any]]:
        """
        将遗留表映射到 BIAN 实体
        Returns: {bian_entity, description, subtype, parent_entity} 或 None
        """
        mapping = self.get_table_mapping(legacy_table)
        if not mapping:
            return None
        return {
            "bian_entity": mapping.get("bian_entity"),
            "description": mapping.get("description"),
            "subtype": mapping.get("subtype"),
            "parent_entity": mapping.get("parent_entity"),
        }

    def map_column_to_bian(
        self,
        legacy_table: str,
        legacy_column: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        将遗留字段映射到 BIAN 属性
        context: 用于条件规则，如从 party 表获取 party_type 用于 person_only 判断
        """
        mapping = self.get_column_mapping(legacy_table, legacy_column)
        if not mapping:
            mapping = self.try_fuzzy_column_mapping(legacy_table, legacy_column)
        if not mapping:
            return None

        # 条件规则评估
        rule_ref = mapping.get("rule_ref")
        if rule_ref and context:
            if not self.evaluate_condition(rule_ref, context):
                return None

        result = {
            "bian_attribute": mapping.get("bian_attribute"),
            "bian_entity": mapping.get("bian_entity"),
            "data_type": mapping.get("data_type"),
            "sub_attribute": mapping.get("sub_attribute"),
            "party_identification_type": mapping.get("party_identification_type"),
            "party_location_type": mapping.get("party_location_type"),
            "party_profile_type": mapping.get("party_profile_type"),
            "value_mapping_ref": mapping.get("value_mapping_ref"),
            "references": mapping.get("references"),
        }
        return {k: v for k, v in result.items() if v is not None}

    def get_all_bian_entities(self) -> List[str]:
        """返回所有已配置的 BIAN 实体名"""
        entities = set()
        for m in self._table_mapping.values():
            if m.get("bian_entity"):
                entities.add(m["bian_entity"])
        for m in self._column_mapping.values():
            if m.get("bian_entity"):
                entities.add(m["bian_entity"])
        return sorted(entities)