"""
BIAN Party 映射层

在 LegacyModelReader 解释器之上，将解析出的表/字段结构映射到 BIAN Party 标准。
使用规则引擎和配置文件完成映射。
"""
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from interpreter.legacy_model_reader_standalone import (
    TableMetadata,
    LegacyModelReader,
)
from .bian_mapping_engine import MappingRuleEngine


# ========== 映射结果模型 ==========


class BIANColumnMapping(BaseModel):
    """BIAN 映射后的字段信息"""
    legacy_table: str = Field(..., description="遗留表名")
    legacy_column: str = Field(..., description="遗留字段名")
    legacy_data_type: Optional[str] = Field(None, description="遗留数据类型")
    bian_entity: str = Field(..., description="BIAN 业务实体")
    bian_attribute: str = Field(..., description="BIAN 属性名")
    sub_attribute: Optional[str] = Field(None, description="BIAN 子属性")
    data_type: Optional[str] = Field(None, description="BIAN 数据类型")
    value_mapping_ref: Optional[str] = Field(None, description="枚举映射引用")
    mapped: bool = Field(True, description="是否成功映射")


class BIANTableMapping(BaseModel):
    """BIAN 映射后的表信息"""
    legacy_table: str = Field(..., description="遗留表名")
    bian_entity: str = Field(..., description="BIAN 业务实体")
    description: Optional[str] = Field(None, description="实体描述")
    subtype: Optional[str] = Field(None, description="实体子类型")
    parent_entity: Optional[str] = Field(None, description="父实体")
    columns: List[BIANColumnMapping] = Field(default_factory=list)
    unmapped_columns: List[str] = Field(default_factory=list, description="未映射的遗留字段")


class BIANMappingResult(BaseModel):
    """BIAN 映射层完整输出"""
    source_file: Optional[str] = Field(None, description="源文件路径")
    domain: str = Field(default="Party", description="BIAN 领域")
    table_mappings: List[BIANTableMapping] = Field(default_factory=list)
    unmapped_tables: List[str] = Field(default_factory=list, description="未映射的遗留表")
    summary: Dict[str, Any] = Field(default_factory=dict, description="映射统计摘要")


# ========== 映射层 ==========


class BIANMappingLayer:
    """
    BIAN Party 映射层

    在解释器输出之上，应用规则引擎和配置文件，输出 BIAN 标准结构。
    """

    def __init__(self, config_dir: Optional[Path] = None):
        self.engine = MappingRuleEngine(config_dir)
        self.reader = LegacyModelReader()

    def read_and_map(self, file_path: str) -> BIANMappingResult:
        """
        读取数据字典并执行 BIAN 映射（一站式接口）

        Args:
            file_path: 数据字典文件路径 (CSV/Excel)

        Returns:
            BIANMappingResult: 映射结果
        """
        tables = self.reader.read(file_path)
        return self.map_tables(tables, source_file=file_path)

    def map_tables(
        self,
        tables: List[TableMetadata],
        source_file: Optional[str] = None,
    ) -> BIANMappingResult:
        """
        将解释器输出的表元数据映射到 BIAN 标准

        Args:
            tables: LegacyModelReader 解析出的表列表
            source_file: 可选，源文件路径

        Returns:
            BIANMappingResult: 映射结果
        """
        table_mappings: List[BIANTableMapping] = []
        unmapped_tables: List[str] = []
        total_mapped = 0
        total_unmapped = 0

        # 构建 context：用于条件规则（如 person_only）
        # 简化处理：从 party 表取 party_type 作为上下文
        context = self._build_context(tables)

        for table in tables:
            legacy_name = table.name.strip()
            table_map = self.engine.map_table_to_bian(legacy_name)

            if not table_map:
                unmapped_tables.append(legacy_name)
                continue

            column_mappings: List[BIANColumnMapping] = []
            unmapped_cols: List[str] = []

            for col in table.columns:
                col_context = {**context, f"{legacy_name}.{col.name}": col.comment}
                col_map = self.engine.map_column_to_bian(
                    legacy_name, col.name, context=col_context
                )

                if col_map:
                    bic = BIANColumnMapping(
                        legacy_table=legacy_name,
                        legacy_column=col.name,
                        legacy_data_type=str(col.data_type.value) if hasattr(col.data_type, "value") else str(col.data_type),
                        bian_entity=col_map.get("bian_entity", table_map["bian_entity"]),
                        bian_attribute=col_map["bian_attribute"],
                        sub_attribute=col_map.get("sub_attribute"),
                        data_type=col_map.get("data_type"),
                        value_mapping_ref=col_map.get("value_mapping_ref"),
                        mapped=True,
                    )
                    column_mappings.append(bic)
                    total_mapped += 1
                else:
                    unmapped_cols.append(col.name)
                    total_unmapped += 1

            btm = BIANTableMapping(
                legacy_table=legacy_name,
                bian_entity=table_map["bian_entity"],
                description=table_map.get("description"),
                subtype=table_map.get("subtype"),
                parent_entity=table_map.get("parent_entity"),
                columns=column_mappings,
                unmapped_columns=unmapped_cols,
            )
            table_mappings.append(btm)

        summary = {
            "total_tables": len(tables),
            "mapped_tables": len(table_mappings),
            "unmapped_tables_count": len(unmapped_tables),
            "total_columns_mapped": total_mapped,
            "total_columns_unmapped": total_unmapped,
        }

        return BIANMappingResult(
            source_file=source_file,
            domain="Party",
            table_mappings=table_mappings,
            unmapped_tables=unmapped_tables,
            summary=summary,
        )

    def _build_context(self, tables: List[TableMetadata]) -> Dict[str, Any]:
        """从表数据构建规则上下文（如 party_type 用于 person_only 等）"""
        context: Dict[str, Any] = {}
        for t in tables:
            if t.name.strip().lower() == "party":
                for c in t.columns:
                    if c.name.strip().lower() == "party_type":
                        # 无法从元数据获知具体值，这里仅提供占位
                        context["party.party_type"] = None
                        break
        return context

    def get_mapping_config_summary(self) -> Dict[str, Any]:
        """返回当前加载的映射配置摘要（用于调试或展示）"""
        return {
            "tables_configured": list(self.engine._table_mapping.keys()),
            "columns_configured": len(self.engine._column_mapping),
            "enum_mappings": list(self.engine._enums.keys()),
            "bian_entities": self.engine.get_all_bian_entities(),
        }


# ========== 便捷函数 ==========


def map_legacy_to_bian(file_path: str) -> BIANMappingResult:
    """
    便捷函数：读取数据字典并映射到 BIAN Party 标准

    Usage:
        from bian_mapping_layer import map_legacy_to_bian
        result = map_legacy_to_bian('data/input/uploads/sample_data_dic2.xlsx')
        for tm in result.table_mappings:
            print(f"{tm.legacy_table} -> {tm.bian_entity}")
    """
    layer = BIANMappingLayer()
    return layer.read_and_map(file_path)