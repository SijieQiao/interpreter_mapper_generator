"""
Data Vault 2.0 转换层

将 BIAN 映射结果转换为符合 Data Vault 2.0 标准的模型结构。
支持 Raw Vault (Hubs, Links, Satellites) 和 Business Vault (PIT, Bridge, Historic tables) 的生成。
"""
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass
from enum import Enum
import hashlib
import json

from pydantic import BaseModel, Field

from .bian_mapping_layer import BIANMappingResult, BIANTableMapping, BIANColumnMapping


# ========== Data Vault 2.0 数据结构 ==========

class DVEntityType(Enum):
    """Data Vault 实体类型"""
    HUB = "hub"
    LINK = "link"
    SATELLITE = "satellite"
    PIT = "pit"
    BRIDGE = "bridge"
    HISTORIC = "historic"


class DVColumnType(Enum):
    """Data Vault 列类型"""
    BUSINESS_KEY = "business_key"
    HASH_KEY = "hash_key"
    HASH_DIFF = "hash_diff"
    LOAD_DATE = "load_date"
    LOAD_END_DATE = "load_end_date"
    RECORD_SOURCE = "record_source"
    ATTRIBUTE = "attribute"
    REFERENCE = "reference"


@dataclass
class DVColumn:
    """Data Vault 列定义"""
    name: str
    data_type: str
    column_type: DVColumnType
    nullable: bool = False
    description: str = ""
    source_column: Optional[str] = None
    source_table: Optional[str] = None


@dataclass
class DVRelationship:
    """Data Vault 关系定义"""
    from_entity: str
    to_entity: str
    from_column: str
    to_column: str
    relationship_type: str = "FK"  # FK, Ref


@dataclass
class DVEntity:
    """Data Vault 实体定义"""
    name: str
    entity_type: DVEntityType
    description: str
    columns: List[DVColumn]
    relationships: List[DVRelationship] = None
    source_tables: List[str] = None

    def __post_init__(self):
        if self.relationships is None:
            self.relationships = []
        if self.source_tables is None:
            self.source_tables = []


class DataVaultModel(BaseModel):
    """完整 Data Vault 模型"""
    raw_vault: List[DVEntity] = Field(default_factory=list, description="Raw Vault 实体")
    business_vault: List[DVEntity] = Field(default_factory=list, description="Business Vault 实体")
    relationships: List[DVRelationship] = Field(default_factory=list, description="所有关系")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="元数据")


# ========== Data Vault 2.0 转换器 ==========

class DataVaultTransformer:
    """
    Data Vault 2.0 转换器

    将 BIAN 映射结果转换为 Data Vault 2.0 结构
    """

    def __init__(self):
        self.hash_salt = "DV2_BIAN_PARTY_2024"  # 用于生成哈希键的盐值
        self.granularity = "balanced"

    def transform(self, bian_result: BIANMappingResult, granularity: str = "balanced") -> DataVaultModel:
        """
        将 BIAN 映射结果转换为 Data Vault 2.0 模型

        Args:
            bian_result: BIAN 映射结果
            granularity: 粒度控制（coarse | balanced | fine）

        Returns:
            DataVaultModel: 完整的 Data Vault 模型
        """
        try:
            self.granularity = granularity or "balanced"
            print(f"Starting Data Vault transformation for {len(bian_result.table_mappings)} tables (granularity: {self.granularity})")

            model = DataVaultModel()

            # 1. 生成 Raw Vault
            print("Generating Raw Vault...")
            self._generate_raw_vault(bian_result, model)
            print(f"Raw Vault: {len(model.raw_vault)} entities created")

            # 2. 生成 Business Vault
            print("Generating Business Vault...")
            self._generate_business_vault(bian_result, model)
            print(f"Business Vault: {len(model.business_vault)} entities created")

            # 3. 生成关系
            print("Generating relationships...")
            self._generate_relationships(model)
            print(f"Relationships: {len(model.relationships)} relationships created")

            # 4. 设置元数据
            model.metadata = {
                "source_domain": "BIAN Party",
                "transformation_standard": "Data Vault 2.0",
                "total_raw_entities": len(model.raw_vault),
                "total_business_entities": len(model.business_vault),
                "total_relationships": len(model.relationships)
            }

            print("Data Vault transformation completed successfully")
            return model

        except Exception as e:
            print(f"Error during Data Vault transformation: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def _generate_raw_vault(self, bian_result: BIANMappingResult, model: DataVaultModel):
        """生成 Raw Vault (Hubs, Links, Satellites)"""

        # 分析 BIAN 实体间的关系
        entity_relationships = self._analyze_entity_relationships(bian_result)

        for table_mapping in bian_result.table_mappings:
            bian_entity = table_mapping.bian_entity

            # 生成 Hub
            hub = self._create_hub(table_mapping)
            model.raw_vault.append(hub)

            # 生成 Satellite (基于属性分组)
            satellites = self._create_satellites(table_mapping, hub.name)
            model.raw_vault.extend(satellites)

        # 生成 Links (基于实体间关系)
        links = self._create_links(bian_result, entity_relationships)
        model.raw_vault.extend(links)

    def _create_hub(self, table_mapping: BIANTableMapping) -> DVEntity:
        """创建 Hub 实体"""

        entity_key = table_mapping.bian_entity.lower().replace(' ', '_')
        hub_name = f"hub_{entity_key}"

        columns = []

        # Hash Key (主键)
        hash_key_col = DVColumn(
            name=f"{table_mapping.bian_entity.lower()}_hk",
            data_type="VARCHAR(32)",
            column_type=DVColumnType.HASH_KEY,
            nullable=False,
            description=f"{table_mapping.bian_entity} Hash Key"
        )
        columns.append(hash_key_col)

        # Business Key (业务主键)
        business_key_found = False
        for col in table_mapping.columns:
            if self._is_business_key(col):
                bk_col = DVColumn(
                    name=f"{col.legacy_column.lower()}_bk",
                    data_type="VARCHAR(255)",
                    column_type=DVColumnType.BUSINESS_KEY,
                    nullable=False,
                    description=f"{col.bian_attribute} Business Key",
                    source_column=col.legacy_column,
                    source_table=table_mapping.legacy_table
                )
                columns.append(bk_col)
                business_key_found = True
                break

        # 如果没找到业务主键，使用默认的 ID 字段
        if not business_key_found:
            for col in table_mapping.columns:
                if "id" in col.legacy_column.lower() and col.legacy_column.lower().endswith("_id"):
                    bk_col = DVColumn(
                        name=f"{col.legacy_column.lower()}_bk",
                        data_type="VARCHAR(255)",
                        column_type=DVColumnType.BUSINESS_KEY,
                        nullable=False,
                        description=f"{col.bian_attribute} Business Key",
                        source_column=col.legacy_column,
                        source_table=table_mapping.legacy_table
                    )
                    columns.append(bk_col)
                    business_key_found = True
                    break

        # 如果仍然没有找到业务键，创建一个默认的业务键
        if not business_key_found:
            bk_col = DVColumn(
                name=f"{table_mapping.bian_entity.lower().replace(' ', '_')}_id_bk",
                data_type="VARCHAR(255)",
                column_type=DVColumnType.BUSINESS_KEY,
                nullable=False,
                description=f"{table_mapping.bian_entity} Default Business Key",
                source_column="generated",
                source_table=table_mapping.legacy_table
            )
            columns.append(bk_col)

        # Load Date
        load_date_col = DVColumn(
            name="load_date",
            data_type="TIMESTAMP",
            column_type=DVColumnType.LOAD_DATE,
            nullable=False,
            description="Record Load Date"
        )
        columns.append(load_date_col)

        # Record Source
        record_source_col = DVColumn(
            name="record_source",
            data_type="VARCHAR(255)",
            column_type=DVColumnType.RECORD_SOURCE,
            nullable=False,
            description="Source System Reference"
        )
        columns.append(record_source_col)

        return DVEntity(
            name=hub_name,
            entity_type=DVEntityType.HUB,
            description=f"Hub for {table_mapping.bian_entity}",
            columns=columns,
            source_tables=[table_mapping.legacy_table]
        )

    def _create_satellites(self, table_mapping: BIANTableMapping, hub_name: str) -> List[DVEntity]:
        """创建 Satellite 实体"""

        satellites = []

        # 按属性分类分组
        attribute_groups = self._group_attributes_by_category(table_mapping.columns)
        attribute_groups = self._apply_granularity_to_groups(attribute_groups, table_mapping)

        entity_key = table_mapping.bian_entity.lower().replace(' ', '_')

        for group_name, columns in attribute_groups.items():
            if not columns:
                continue

            sat_name = f"sat_{entity_key}_{group_name}"

            sat_columns = []

            # Hash Key (引用 Hub)
            hk_col = DVColumn(
                name=f"{entity_key}_hk",
                data_type="VARCHAR(32)",
                column_type=DVColumnType.HASH_KEY,
                nullable=False,
                description=f"Reference to {hub_name}"
            )
            sat_columns.append(hk_col)

            # Hash Diff (用于检测变化)
            hash_diff_col = DVColumn(
                name="hash_diff",
                data_type="VARCHAR(32)",
                column_type=DVColumnType.HASH_DIFF,
                nullable=False,
                description="Hash of all descriptive attributes"
            )
            sat_columns.append(hash_diff_col)

            # Load Date
            load_date_col = DVColumn(
                name="load_date",
                data_type="TIMESTAMP",
                column_type=DVColumnType.LOAD_DATE,
                nullable=False,
                description="Record Load Date"
            )
            sat_columns.append(load_date_col)

            # Load End Date
            load_end_date_col = DVColumn(
                name="load_end_date",
                data_type="TIMESTAMP",
                column_type=DVColumnType.LOAD_END_DATE,
                nullable=True,
                description="Record Load End Date"
            )
            sat_columns.append(load_end_date_col)

            # Record Source
            record_source_col = DVColumn(
                name="record_source",
                data_type="VARCHAR(255)",
                column_type=DVColumnType.RECORD_SOURCE,
                nullable=False,
                description="Source System Reference"
            )
            sat_columns.append(record_source_col)

            # 属性列
            for col in columns:
                attr_col = DVColumn(
                    name=col.legacy_column.lower(),
                    data_type=self._map_data_type(col.data_type or "VARCHAR(255)"),
                    column_type=DVColumnType.ATTRIBUTE,
                    nullable=True,
                    description=col.bian_attribute or col.legacy_column,
                    source_column=col.legacy_column,
                    source_table=table_mapping.legacy_table
                )
                sat_columns.append(attr_col)

            satellite = DVEntity(
                name=sat_name,
                entity_type=DVEntityType.SATELLITE,
                description=f"Satellite for {table_mapping.bian_entity} {group_name} attributes",
                columns=sat_columns,
                source_tables=[table_mapping.legacy_table]
            )
            satellites.append(satellite)

        return satellites

    def _create_links(self, bian_result: BIANMappingResult, relationships: Dict[str, List[str]]) -> List[DVEntity]:
        """创建 Link 实体"""

        links = []

        # 分析关系并创建 Links
        processed_relationships = set()

        for table_mapping in bian_result.table_mappings:
            entity = table_mapping.bian_entity

            if entity in relationships:
                for related_entity in relationships[entity]:
                    # 避免重复创建 Link
                    rel_key = tuple(sorted([entity, related_entity]))
                    if rel_key in processed_relationships:
                        continue
                    processed_relationships.add(rel_key)

                    # 创建 Link
                    link_name = f"link_{entity.lower().replace(' ', '_')}_{related_entity.lower().replace(' ', '_')}"

                    link_columns = []

                    # Link Hash Key
                    link_hk_col = DVColumn(
                        name=f"{entity.lower()}_{related_entity.lower()}_lk",
                        data_type="VARCHAR(32)",
                        column_type=DVColumnType.HASH_KEY,
                        nullable=False,
                        description=f"Link Hash Key for {entity} to {related_entity}"
                    )
                    link_columns.append(link_hk_col)

                    # Hub Hash Keys
                    entity_hk_col = DVColumn(
                        name=f"{entity.lower()}_hk",
                        data_type="VARCHAR(32)",
                        column_type=DVColumnType.HASH_KEY,
                        nullable=False,
                        description=f"Reference to {entity} Hub"
                    )
                    link_columns.append(entity_hk_col)

                    related_hk_col = DVColumn(
                        name=f"{related_entity.lower()}_hk",
                        data_type="VARCHAR(32)",
                        column_type=DVColumnType.HASH_KEY,
                        nullable=False,
                        description=f"Reference to {related_entity} Hub"
                    )
                    link_columns.append(related_hk_col)

                    # Load Date
                    load_date_col = DVColumn(
                        name="load_date",
                        data_type="TIMESTAMP",
                        column_type=DVColumnType.LOAD_DATE,
                        nullable=False,
                        description="Record Load Date"
                    )
                    link_columns.append(load_date_col)

                    # Record Source
                    record_source_col = DVColumn(
                        name="record_source",
                        data_type="VARCHAR(255)",
                        column_type=DVColumnType.RECORD_SOURCE,
                        nullable=False,
                        description="Source System Reference"
                    )
                    link_columns.append(record_source_col)

                    link = DVEntity(
                        name=link_name,
                        entity_type=DVEntityType.LINK,
                        description=f"Link between {entity} and {related_entity}",
                        columns=link_columns,
                        source_tables=[table_mapping.legacy_table]
                    )
                    links.append(link)

        return links

    def _generate_business_vault(self, bian_result: BIANMappingResult, model: DataVaultModel):
        """生成 Business Vault (PIT, Bridge, Historic tables)"""

        # 为每个 Hub 生成 PIT table
        for entity in model.raw_vault:
            if entity.entity_type == DVEntityType.HUB:
                pit = self._create_pit_table(entity, model.raw_vault)
                if pit:
                    model.business_vault.append(pit)

        # 为复杂关系生成 Bridge tables
        bridges = self._create_bridge_tables(bian_result, model.raw_vault)
        model.business_vault.extend(bridges)

        # 生成 Historic tables (基于卫星的缓慢变化维度)
        for entity in model.raw_vault:
            if entity.entity_type == DVEntityType.SATELLITE:
                historic = self._create_historic_table(entity)
                model.business_vault.append(historic)

    def _create_pit_table(self, hub: DVEntity, all_entities: List[DVEntity]) -> Optional[DVEntity]:
        """创建 Point-in-Time table"""

        # 找到与该 Hub 相关的所有 Satellites
        related_satellites = []
        hub_entity_name = hub.name.replace("hub_", "").replace("_", " ").title()

        for entity in all_entities:
            if entity.entity_type == DVEntityType.SATELLITE:
                # 检查是否与当前 Hub 相关
                hk_ref = None
                for col in entity.columns:
                    if col.column_type == DVColumnType.HASH_KEY and f"{hub_entity_name.lower().replace(' ', '_')}_hk" in col.name:
                        hk_ref = col
                        break

                if hk_ref:
                    related_satellites.append(entity)

        if not related_satellites:
            return None

        pit_name = f"pit_{hub.name.replace('hub_', '')}"

        pit_columns = []

        # Hub Hash Key
        hk_col = DVColumn(
            name=f"{hub_entity_name.lower().replace(' ', '_')}_hk",
            data_type="VARCHAR(32)",
            column_type=DVColumnType.HASH_KEY,
            nullable=False,
            description=f"Reference to {hub.name}"
        )
        pit_columns.append(hk_col)

        # As-of Date (PIT 时间点)
        as_of_date_col = DVColumn(
            name="as_of_date",
            data_type="TIMESTAMP",
            column_type=DVColumnType.ATTRIBUTE,
            nullable=False,
            description="Point-in-Time Date"
        )
        pit_columns.append(as_of_date_col)

        # 为每个 Satellite 添加 Load Date 引用
        for sat in related_satellites:
            sat_load_date_col = DVColumn(
                name=f"{sat.name}_load_date",
                data_type="TIMESTAMP",
                column_type=DVColumnType.ATTRIBUTE,
                nullable=True,
                description=f"Load Date from {sat.name}"
            )
            pit_columns.append(sat_load_date_col)

        return DVEntity(
            name=pit_name,
            entity_type=DVEntityType.PIT,
            description=f"Point-in-Time table for {hub_entity_name}",
            columns=pit_columns,
            source_tables=hub.source_tables
        )

    def _create_bridge_tables(self, bian_result: BIANMappingResult, raw_entities: List[DVEntity]) -> List[DVEntity]:
        """创建 Bridge tables 处理多对多关系"""

        bridges = []

        # 分析多对多关系（特别是 Party Relationship）
        for table_mapping in bian_result.table_mappings:
            if "relationship" in table_mapping.legacy_table.lower():
                # 为关系表创建 Bridge
                bridge_name = f"bridge_{table_mapping.bian_entity.lower().replace(' ', '_')}"

                bridge_columns = []

                # Bridge Hash Key
                bridge_hk_col = DVColumn(
                    name=f"{table_mapping.bian_entity.lower().replace(' ', '_')}_bk",
                    data_type="VARCHAR(32)",
                    column_type=DVColumnType.HASH_KEY,
                    nullable=False,
                    description=f"Bridge Key for {table_mapping.bian_entity}"
                )
                bridge_columns.append(bridge_hk_col)

                # 关系参与者 Hash Keys
                for col in table_mapping.columns:
                    if "_id" in col.legacy_column.lower() and col.legacy_column.lower().endswith("_id"):
                        if "from" in col.legacy_column.lower() or "to" in col.legacy_column.lower():
                            hk_col = DVColumn(
                                name=f"{col.legacy_column.lower().replace('_id', '_hk')}",
                                data_type="VARCHAR(32)",
                                column_type=DVColumnType.REFERENCE,
                                nullable=False,
                                description=f"Reference to {col.bian_attribute}",
                                source_column=col.legacy_column,
                                source_table=table_mapping.legacy_table
                            )
                            bridge_columns.append(hk_col)

                # Effectivity Satellite 字段
                load_date_col = DVColumn(
                    name="load_date",
                    data_type="TIMESTAMP",
                    column_type=DVColumnType.LOAD_DATE,
                    nullable=False,
                    description="Record Load Date"
                )
                bridge_columns.append(load_date_col)

                load_end_date_col = DVColumn(
                    name="load_end_date",
                    data_type="TIMESTAMP",
                    column_type=DVColumnType.LOAD_END_DATE,
                    nullable=True,
                    description="Record Load End Date"
                )
                bridge_columns.append(load_end_date_col)

                # 关系属性
                for col in table_mapping.columns:
                    if not "_id" in col.legacy_column.lower():
                        attr_col = DVColumn(
                            name=col.legacy_column.lower(),
                            data_type=self._map_data_type(col.data_type or "VARCHAR(255)"),
                            column_type=DVColumnType.ATTRIBUTE,
                            nullable=True,
                            description=col.bian_attribute or col.legacy_column,
                            source_column=col.legacy_column,
                            source_table=table_mapping.legacy_table
                        )
                        bridge_columns.append(attr_col)

                bridge = DVEntity(
                    name=bridge_name,
                    entity_type=DVEntityType.BRIDGE,
                    description=f"Bridge table for {table_mapping.bian_entity} relationships",
                    columns=bridge_columns,
                    source_tables=[table_mapping.legacy_table]
                )
                bridges.append(bridge)

        return bridges

    def _create_historic_table(self, satellite: DVEntity) -> DVEntity:
        """创建 Historic table (基于 Satellite 的缓慢变化维度)"""

        historic_name = f"hist_{satellite.name.replace('sat_', '')}"

        historic_columns = []

        # 复制 Satellite 的所有列
        for col in satellite.columns:
            hist_col = DVColumn(
                name=col.name,
                data_type=col.data_type,
                column_type=col.column_type,
                nullable=col.nullable,
                description=f"Historic {col.description}",
                source_column=col.source_column,
                source_table=col.source_table
            )
            historic_columns.append(hist_col)

        # 添加版本控制字段
        version_col = DVColumn(
            name="version_number",
            data_type="INT",
            column_type=DVColumnType.ATTRIBUTE,
            nullable=False,
            description="Version Number for SCD"
        )
        historic_columns.append(version_col)

        is_current_col = DVColumn(
            name="is_current",
            data_type="BOOLEAN",
            column_type=DVColumnType.ATTRIBUTE,
            nullable=False,
            description="Flag indicating if this is the current version"
        )
        historic_columns.append(is_current_col)

        valid_from_col = DVColumn(
            name="valid_from_date",
            data_type="TIMESTAMP",
            column_type=DVColumnType.ATTRIBUTE,
            nullable=False,
            description="Valid From Date"
        )
        historic_columns.append(valid_from_col)

        valid_to_col = DVColumn(
            name="valid_to_date",
            data_type="TIMESTAMP",
            column_type=DVColumnType.ATTRIBUTE,
            nullable=True,
            description="Valid To Date"
        )
        historic_columns.append(valid_to_col)

        return DVEntity(
            name=historic_name,
            entity_type=DVEntityType.HISTORIC,
            description=f"Historic table for {satellite.name} (SCD Type 2)",
            columns=historic_columns,
            source_tables=satellite.source_tables
        )

    def _analyze_entity_relationships(self, bian_result: BIANMappingResult) -> Dict[str, List[str]]:
        """分析 BIAN 实体间的关系"""

        relationships = {}

        # 基于表间关系推断实体关系
        for table_mapping in bian_result.table_mappings:
            entity = table_mapping.bian_entity
            related_entities = []

            # 检查外键关系
            for col in table_mapping.columns:
                if "party_id" in col.legacy_column.lower():
                    if entity != "Party":
                        if "Party" not in related_entities:
                            related_entities.append("Party")

            # 检查关系表
            if "relationship" in table_mapping.legacy_table.lower():
                # 关系表通常连接多个实体
                if "Party" not in related_entities:
                    related_entities.append("Party")
                # 可以扩展为识别更多关系

            relationships[entity] = related_entities

        return relationships

    def _group_attributes_by_category(self, columns: List[BIANColumnMapping]) -> Dict[str, List[BIANColumnMapping]]:
        """按类别分组属性"""

        groups = {
            "basic": [],      # 基本信息
            "contact": [],    # 联系信息
            "address": [],    # 地址信息
            "profile": [],    # 档案信息
            "role": [],       # 角色信息
            "other": []       # 其他
        }

        for col in columns:
            col_name = col.legacy_column.lower()

            # 跳过 ID 和时间戳字段（这些在 Hub 中）
            if any(keyword in col_name for keyword in ["id", "created", "updated", "date", "time"]):
                continue

            # 按字段名分类
            if any(keyword in col_name for keyword in ["name", "type", "status", "gender", "birth"]):
                groups["basic"].append(col)
            elif any(keyword in col_name for keyword in ["email", "phone", "contact", "mobile", "fax"]):
                groups["contact"].append(col)
            elif any(keyword in col_name for keyword in ["address", "city", "country", "postal", "location"]):
                groups["address"].append(col)
            elif any(keyword in col_name for keyword in ["risk", "profile", "compliance", "source"]):
                groups["profile"].append(col)
            elif any(keyword in col_name for keyword in ["role", "authorization", "position", "department"]):
                groups["role"].append(col)
            else:
                groups["other"].append(col)

        # 移除空组
        return {k: v for k, v in groups.items() if v}

    def _apply_granularity_to_groups(self, groups: Dict[str, List[BIANColumnMapping]], table_mapping: BIANTableMapping) -> Dict[str, List[BIANColumnMapping]]:
        """根据粒度调整 Satellite 分组"""
        granularity = (self.granularity or "balanced").lower()

        ordered_keys = ["basic", "profile", "role", "contact", "address", "other"]
        ordered_groups = [(k, groups[k]) for k in ordered_keys if k in groups]

        if granularity == "coarse":
            total_cols = sum(len(cols) for cols in groups.values())
            target = 1 if total_cols <= 10 else 2
            if target == 1:
                merged = []
                for _, cols in ordered_groups:
                    merged.extend(cols)
                return {"all": merged} if merged else {}

            core_cols = []
            rest_cols = []
            core_keys = {"basic", "profile"}
            for key, cols in ordered_groups:
                if key in core_keys:
                    core_cols.extend(cols)
                else:
                    rest_cols.extend(cols)
            merged = {}
            if core_cols:
                merged["core"] = core_cols
            if rest_cols:
                merged["extended"] = rest_cols
            return merged if merged else {}

        if granularity == "fine":
            fine_groups = dict(groups)
            audit_fields = []
            compliance_fields = []

            for cols in groups.values():
                for col in cols:
                    name = col.legacy_column.lower()
                    if any(k in name for k in ["risk", "audit", "compliance", "score", "rating", "status", "flag"]):
                        compliance_fields.append(col)
                    elif any(k in name for k in ["updated", "created", "changed", "valid", "effective"]):
                        audit_fields.append(col)

            if compliance_fields:
                fine_groups["compliance"] = compliance_fields
            if audit_fields:
                fine_groups["audit"] = audit_fields

            # 移除被单独拆分的字段，避免重复
            if compliance_fields or audit_fields:
                filtered = {}
                for name, cols in fine_groups.items():
                    if name in {"compliance", "audit"}:
                        filtered[name] = cols
                        continue
                    filtered[name] = [c for c in cols if c not in compliance_fields and c not in audit_fields]
                fine_groups = {k: v for k, v in filtered.items() if v}

            total_cols = sum(len(cols) for cols in fine_groups.values())
            target = 4 if total_cols <= 20 else 5
            return self._merge_groups_to_target(fine_groups, target)

        # balanced
        total_cols = sum(len(cols) for cols in groups.values())
        target = 2 if total_cols <= 10 else 3
        return self._merge_groups_to_target(groups, target)

    def _merge_groups_to_target(self, groups: Dict[str, List[BIANColumnMapping]], target: int) -> Dict[str, List[BIANColumnMapping]]:
        """合并分组直到满足目标数量"""
        if not groups:
            return {}

        if len(groups) <= target:
            return {k: v for k, v in groups.items() if v}

        merged = {k: list(v) for k, v in groups.items() if v}

        while len(merged) > target:
            smallest_key = min(merged, key=lambda k: len(merged[k]))
            smallest_cols = merged.pop(smallest_key)
            if not merged:
                merged[smallest_key] = smallest_cols
                break
            target_key = min(merged, key=lambda k: len(merged[k]))
            merged[target_key].extend(smallest_cols)

        return merged

    def _is_business_key(self, col: BIANColumnMapping) -> bool:
        """判断是否为业务主键"""

        col_name = col.legacy_column.lower()

        # 业务主键候选字段
        business_key_indicators = [
            "reference", "number", "code", "identifier",
            "national_id", "tax_id", "registration"
        ]

        return any(indicator in col_name for indicator in business_key_indicators)

    def _map_data_type(self, data_type: str) -> str:
        """映射数据类型"""

        if not data_type:
            return "VARCHAR(255)"

        type_mapping = {
            "string": "VARCHAR(255)",
            "int": "INT",
            "integer": "INT",
            "bigint": "BIGINT",
            "float": "FLOAT",
            "double": "DOUBLE PRECISION",
            "decimal": "DECIMAL(18,2)",
            "boolean": "BOOLEAN",
            "bool": "BOOLEAN",
            "date": "DATE",
            "datetime": "TIMESTAMP",
            "timestamp": "TIMESTAMP",
        }

        return type_mapping.get(data_type.lower(), "VARCHAR(255)")

    def _generate_relationships(self, model: DataVaultModel):
        """生成实体间关系"""

        relationships = []

        # 为所有实体生成关系
        for entity in model.raw_vault + model.business_vault:
            for col in entity.columns:
                if col.column_type == DVColumnType.HASH_KEY and col.name != f"{entity.name.replace('hub_', '').replace('link_', '').replace('sat_', '').replace('pit_', '').replace('bridge_', '').replace('hist_', '')}_hk":
                    # 这是一个引用其他实体的 Hash Key
                    if col.name.endswith("_hk"):
                        # 推断被引用实体
                        if "hub_" in entity.name or "sat_" in entity.name:
                            referenced_entity = f"hub_{col.name.replace('_hk', '')}"
                        elif "link_" in entity.name:
                            # Link 引用多个 Hubs
                            referenced_entity = f"hub_{col.name.replace('_hk', '')}"
                        else:
                            continue

                        rel = DVRelationship(
                            from_entity=entity.name,
                            to_entity=referenced_entity,
                            from_column=col.name,
                            to_column=col.name,
                            relationship_type="FK"
                        )
                        relationships.append(rel)

        model.relationships = relationships

    def generate_svg_diagram(self, model: DataVaultModel, vault_type: str = "raw") -> str:
        """生成 SVG 格式的 Data Vault 图表"""

        if vault_type == "raw":
            entities = model.raw_vault
            title = "Raw Data Vault 2.0 Model"
        else:
            entities = model.business_vault
            title = "Business Data Vault 2.0 Model"

        # 计算布局
        hubs = [e for e in entities if e.entity_type == DVEntityType.HUB]
        links = [e for e in entities if e.entity_type == DVEntityType.LINK]
        satellites = [e for e in entities if e.entity_type == DVEntityType.SATELLITE]
        pits = [e for e in entities if e.entity_type == DVEntityType.PIT]
        bridges = [e for e in entities if e.entity_type == DVEntityType.BRIDGE]
        historics = [e for e in entities if e.entity_type == DVEntityType.HISTORIC]

        # SVG 尺寸
        width = 1200
        height = max(800, len(entities) * 100)

        svg_content = f'''<?xml version="1.0" encoding="UTF-8"?>
<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <style type="text/css">
      .title {{ font-family: Arial, sans-serif; font-size: 24px; font-weight: bold; text-anchor: middle; }}
      .entity {{ font-family: Arial, sans-serif; font-size: 12px; }}
      .hub {{ fill: #e3f2fd; stroke: #1976d2; stroke-width: 2; }}
      .link {{ fill: #f3e5f5; stroke: #7b1fa2; stroke-width: 2; }}
      .satellite {{ fill: #e8f5e8; stroke: #388e3c; stroke-width: 2; }}
      .pit {{ fill: #fff3e0; stroke: #f57c00; stroke-width: 2; }}
      .bridge {{ fill: #fce4ec; stroke: #c2185b; stroke-width: 2; }}
      .historic {{ fill: #f3e5f5; stroke: #6a1b9a; stroke-width: 2; }}
      .column {{ font-size: 10px; }}
      .pk {{ font-weight: bold; fill: #d32f2f; }}
      .fk {{ font-style: italic; fill: #1976d2; }}
    </style>
  </defs>

  <!-- 标题 -->
  <text x="{width//2}" y="30" class="title">{title}</text>

  <!-- 图例 -->
  <g transform="translate(50, 50)">
    <rect x="0" y="0" width="20" height="15" class="hub"/>
    <text x="30" y="12" class="entity">Hub</text>

    <rect x="80" y="0" width="20" height="15" class="link"/>
    <text x="110" y="12" class="entity">Link</text>

    <rect x="160" y="0" width="20" height="15" class="satellite"/>
    <text x="190" y="12" class="entity">Satellite</text>

    <rect x="260" y="0" width="20" height="15" class="pit"/>
    <text x="290" y="12" class="entity">PIT</text>

    <rect x="320" y="0" width="20" height="15" class="bridge"/>
    <text x="350" y="12" class="entity">Bridge</text>

    <rect x="410" y="0" width="20" height="15" class="historic"/>
    <text x="440" y="12" class="entity">Historic</text>
  </g>
'''

        y_offset = 120

        # 绘制实体
        for i, entity in enumerate(entities):
            x = 50 + (i % 3) * 350
            y = y_offset + (i // 3) * 200

            # 实体框
            entity_class = entity.entity_type.value
            svg_content += f'''
  <g transform="translate({x}, {y})">
    <rect x="0" y="0" width="300" height="{20 + len(entity.columns) * 15}" class="{entity_class}" rx="5"/>
    <text x="150" y="15" class="entity" text-anchor="middle" font-weight="bold">{entity.name}</text>
'''

            # 列
            for j, col in enumerate(entity.columns):
                y_pos = 35 + j * 15
                col_class = ""
                if col.column_type == DVColumnType.HASH_KEY:
                    col_class = "pk"
                elif col.column_type == DVColumnType.REFERENCE:
                    col_class = "fk"

                svg_content += f'''
    <text x="10" y="{y_pos}" class="column {col_class}">{col.name}: {col.data_type}</text>'''

            svg_content += '''
  </g>'''

        # 绘制关系线（简化版）
        for rel in model.relationships[:10]:  # 限制关系数量避免图表过于复杂
            svg_content += f'''
  <line x1="200" y1="100" x2="400" y2="100" stroke="#666" stroke-width="1" marker-end="url(#arrow)"/>
  <defs>
    <marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L0,6 L9,3 z" fill="#666"/>
    </marker>
  </defs>'''

        svg_content += '''
</svg>'''

        return svg_content

    def generate_data_dictionary(self, model: DataVaultModel) -> str:
        """生成数据字典（Visual Paradigm 兼容格式）"""

        output = []
        output.append("# Data Vault 2.0 Data Dictionary")
        output.append(f"Generated for: BIAN Party Domain")
        output.append(f"Standard: Data Vault 2.0")
        output.append("")

        # Raw Vault 部分
        output.append("## Raw Data Vault")
        output.append("")

        for entity in model.raw_vault:
            output.append(f"### {entity.name}")
            output.append(f"**Type:** {entity.entity_type.value.upper()}")
            output.append(f"**Description:** {entity.description}")
            output.append("")
            output.append("| Column Name | Data Type | Key Type | Nullable | Description |")
            output.append("|-------------|-----------|----------|----------|-------------|")

            for col in entity.columns:
                key_type = col.column_type.value.replace("_", " ").title()
                nullable = "Yes" if col.nullable else "No"
                desc = col.description or ""
                output.append(f"| {col.name} | {col.data_type} | {key_type} | {nullable} | {desc} |")

            output.append("")

        # Business Vault 部分
        output.append("## Business Data Vault")
        output.append("")

        for entity in model.business_vault:
            output.append(f"### {entity.name}")
            output.append(f"**Type:** {entity.entity_type.value.upper()}")
            output.append(f"**Description:** {entity.description}")
            output.append("")
            output.append("| Column Name | Data Type | Key Type | Nullable | Description |")
            output.append("|-------------|-----------|----------|----------|-------------|")

            for col in entity.columns:
                key_type = col.column_type.value.replace("_", " ").title()
                nullable = "Yes" if col.nullable else "No"
                desc = col.description or ""
                output.append(f"| {col.name} | {col.data_type} | {key_type} | {nullable} | {desc} |")

            output.append("")

        # 关系部分
        output.append("## Relationships")
        output.append("")
        output.append("| From Entity | To Entity | From Column | To Column | Type |")
        output.append("|-------------|-----------|-------------|----------|------|")

        for rel in model.relationships:
            output.append(f"| {rel.from_entity} | {rel.to_entity} | {rel.from_column} | {rel.to_column} | {rel.relationship_type} |")

        return "\n".join(output)


# ========== 便捷函数 ==========

def transform_bian_to_data_vault(bian_result: BIANMappingResult) -> DataVaultModel:
    """
    便捷函数：将 BIAN 映射结果转换为 Data Vault 2.0 模型

    Usage:
        from data_vault_layer import transform_bian_to_data_vault
        dv_model = transform_bian_to_data_vault(bian_result)
    """
    transformer = DataVaultTransformer()
    return transformer.transform(bian_result)