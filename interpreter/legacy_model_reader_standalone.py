"""
老旧模型数据字典读取器（独立版本，可直接复制使用）

使用方法：
    from legacy_model_reader_standalone import LegacyModelReader
    
    reader = LegacyModelReader()
    tables = reader.read('your_file.xlsx')  # 或 .csv
    for table in tables:
        print(f"表: {table.name}, 字段数: {len(table.columns)}")

依赖：
    pip install pandas openpyxl pydantic
"""
import pandas as pd
from pathlib import Path
from typing import List, Optional
from enum import Enum
from pydantic import BaseModel, Field


# ========== 数据模型定义 ==========

class DataType(str, Enum):
    """数据类型枚举"""
    VARCHAR = "VARCHAR"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"
    TEXT = "TEXT"
    UUID = "UUID"


class Column(BaseModel):
    """字段定义"""
    name: str = Field(..., description="字段名")
    data_type: DataType = Field(..., description="数据类型")
    length: Optional[int] = Field(None, description="长度（如VARCHAR的长度）")
    precision: Optional[int] = Field(None, description="精度（如DECIMAL的精度）")
    scale: Optional[int] = Field(None, description="小数位数")
    nullable: bool = Field(True, description="是否可为空")
    default_value: Optional[str] = Field(None, description="默认值")
    comment: Optional[str] = Field(None, description="字段注释")
    is_primary_key: bool = Field(False, description="是否为主键")
    is_foreign_key: bool = Field(False, description="是否为外键")
    referenced_table: Optional[str] = Field(None, description="外键引用表")
    referenced_column: Optional[str] = Field(None, description="外键引用字段")


class ForeignKey(BaseModel):
    """外键定义"""
    name: Optional[str] = Field(None, description="外键约束名")
    columns: List[str] = Field(..., description="外键字段列表")
    referenced_table: str = Field(..., description="引用表名")
    referenced_columns: List[str] = Field(..., description="引用字段列表")


class TableMetadata(BaseModel):
    """表元数据"""
    name: str = Field(..., description="表名")
    schema_name: Optional[str] = Field(None, description="模式名")
    comment: Optional[str] = Field(None, description="表注释")
    columns: List[Column] = Field(default_factory=list, description="字段列表")
    primary_keys: List[str] = Field(default_factory=list, description="主键字段列表")
    foreign_keys: List[ForeignKey] = Field(default_factory=list, description="外键列表")


# ========== 解释器类 ==========

class LegacyModelReader:
    """
    老旧模型数据字典读取器
    
    支持格式：
    - CSV (.csv)
    - Excel (.xlsx, .xls)
    
    数据字典格式要求：
    - 必需列：表名（table_name / 表名 / attribute_name）、字段名（column_name / 字段名）
    - 可选列：数据类型、长度、精度、主键/外键标识、引用表/字段、注释等
    
    支持的列名映射（自动识别）：
    - 表名：table_name, 表名, table, 表
    - 字段名：column_name, attribute_name, 字段名, 字段, column, 列名
    - 主键：is_pk, isPK, ispk, 主键, primary_key, pk
    - 外键：is_fk, isFK, isfk, 外键, foreign_key, fk
    - 注释：column_comment, attribute_desc, 字段注释, 注释, comment
    """
    
    # 列名映射（支持中文、英文及 sample 常见列名）
    COLUMN_MAPPING = {
        '表名': 'table_name',
        'table_name': 'table_name',
        '表': 'table_name',
        'table': 'table_name',
        '字段名': 'column_name',
        'column_name': 'column_name',
        'attribute_name': 'column_name',
        '字段': 'column_name',
        'column': 'column_name',
        '列名': 'column_name',
        '数据类型': 'data_type',
        'data_type': 'data_type',
        '类型': 'data_type',
        'type': 'data_type',
        '长度': 'length',
        'length': 'length',
        'len': 'length',
        '精度': 'precision',
        'precision': 'precision',
        '小数位': 'scale',
        'scale': 'scale',
        '主键': 'is_pk',
        'is_pk': 'is_pk',
        'ispk': 'is_pk',
        'isPK': 'is_pk',
        'primary_key': 'is_pk',
        'pk': 'is_pk',
        '外键': 'is_fk',
        'is_fk': 'is_fk',
        'isfk': 'is_fk',
        'isFK': 'is_fk',
        'foreign_key': 'is_fk',
        'fk': 'is_fk',
        '引用表': 'ref_table',
        'ref_table': 'ref_table',
        'referenced_table': 'ref_table',
        '引用字段': 'ref_column',
        'ref_column': 'ref_column',
        'referenced_column': 'ref_column',
        '字段注释': 'column_comment',
        'column_comment': 'column_comment',
        'attribute_desc': 'column_comment',
        '注释': 'column_comment',
        'comment': 'comment',
        '表注释': 'table_comment',
        'table_comment': 'table_comment',
        '是否为空': 'nullable',
        'nullable': 'nullable',
        'null': 'nullable',
        '允许空': 'nullable',
    }
    
    def read(self, file_path: str) -> List[TableMetadata]:
        """
        读取老旧模型数据字典（支持 CSV 和 Excel）
        
        Args:
            file_path: 文件路径
            
        Returns:
            表元数据列表
            
        Raises:
            FileNotFoundError: 文件不存在
            ValueError: 文件格式不支持或缺少必需列
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        # 根据文件扩展名选择读取方式
        suffix = file_path.suffix.lower()
        if suffix == '.csv':
            df = self._read_csv(file_path)
        elif suffix in ['.xlsx', '.xls']:
            df = self._read_excel(file_path)
        else:
            raise ValueError(f"不支持的文件格式: {suffix}。支持格式: .csv, .xlsx, .xls")
        
        # 标准化列名
        normalized_columns = []
        for col in df.columns:
            col_stripped = col.strip()
            if col_stripped in self.COLUMN_MAPPING:
                normalized_columns.append(self.COLUMN_MAPPING[col_stripped])
            else:
                col_lower = col_stripped.lower()
                if col_lower in self.COLUMN_MAPPING:
                    normalized_columns.append(self.COLUMN_MAPPING[col_lower])
                else:
                    normalized_columns.append(col_lower)
        df.columns = normalized_columns
        
        # 解析为表元数据
        return self._parse_to_tables(df)
    
    def _read_csv(self, file_path: Path) -> pd.DataFrame:
        """读取 CSV 文件（支持多种编码）"""
        encodings = ['utf-8-sig', 'utf-8', 'gbk', 'gb2312']
        for encoding in encodings:
            try:
                return pd.read_csv(file_path, encoding=encoding)
            except UnicodeDecodeError:
                continue
        raise ValueError(f"无法读取 CSV 文件，尝试的编码: {encodings}")
    
    def _read_excel(self, file_path: Path) -> pd.DataFrame:
        """读取 Excel 文件（自动识别'数据字典'工作表）"""
        try:
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            if '数据字典' in sheet_names:
                return pd.read_excel(file_path, sheet_name='数据字典')
            for sheet in sheet_names:
                if sheet.lower() == '数据字典':
                    return pd.read_excel(file_path, sheet_name=sheet)
            return pd.read_excel(file_path, sheet_name=0)
        except Exception as e:
            raise ValueError(f"无法读取 Excel 文件: {e}")
    
    def _parse_to_tables(self, df: pd.DataFrame) -> List[TableMetadata]:
        """将 DataFrame 解析为表元数据列表"""
        if 'table_name' not in df.columns:
            raise ValueError("数据字典缺少必需列: 表名 (table_name) 或 table_name")
        if 'column_name' not in df.columns:
            raise ValueError("数据字典缺少必需列: 字段名 (column_name) 或 attribute_name")
        
        tables = []
        for table_name, group in df.groupby('table_name'):
            # 获取表注释（取第一行的表注释，如果存在）
            table_comment = None
            if 'table_comment' in group.columns:
                table_comment = group['table_comment'].iloc[0]
                if pd.notna(table_comment):
                    table_comment = str(table_comment).strip()
                    if not table_comment:
                        table_comment = None
            
            columns = []
            primary_keys = []
            foreign_keys = []
            
            for _, row in group.iterrows():
                data_type_str = str(row.get('data_type', 'VARCHAR')).upper().strip()
                data_type = self._parse_data_type(data_type_str)
                column_comment = self._safe_str(row.get('column_comment'))
                comment = self._safe_str(row.get('comment'))
                final_comment = column_comment or comment
                is_fk = self._parse_bool(row.get('is_fk'))
                ref_table = self._safe_str(row.get('ref_table'))
                ref_column = self._safe_str(row.get('ref_column'))
                if is_fk and not ref_table:
                    if comment:
                        parsed_table, parsed_column = self._parse_fk_reference_from_comment(comment)
                        if parsed_table:
                            ref_table, ref_column = parsed_table, parsed_column
                    if not ref_table and column_comment:
                        parsed_table, parsed_column = self._parse_fk_reference_from_comment(column_comment)
                        if parsed_table:
                            ref_table, ref_column = parsed_table, parsed_column
                column = Column(
                    name=str(row.get('column_name', '')).strip(),
                    data_type=data_type,
                    length=self._safe_int(row.get('length')),
                    precision=self._safe_int(row.get('precision')),
                    scale=self._safe_int(row.get('scale')),
                    nullable=self._parse_nullable(row.get('nullable')),
                    comment=final_comment,
                    is_primary_key=self._parse_bool(row.get('is_pk')),
                    is_foreign_key=is_fk,
                    referenced_table=ref_table,
                    referenced_column=ref_column
                )
                
                columns.append(column)
                
                # 收集主键
                if column.is_primary_key:
                    primary_keys.append(column.name)
                
                # 收集外键
                if column.is_foreign_key and column.referenced_table:
                    foreign_keys.append(
                        ForeignKey(
                            columns=[column.name],
                            referenced_table=column.referenced_table,
                            referenced_columns=[column.referenced_column] if column.referenced_column else []
                        )
                    )
            
            table = TableMetadata(
                name=str(table_name).strip(),
                comment=table_comment,
                columns=columns,
                primary_keys=primary_keys,
                foreign_keys=foreign_keys
            )
            
            tables.append(table)
        
        return tables
    
    def _parse_data_type(self, data_type_str: str) -> DataType:
        """解析数据类型字符串"""
        if not data_type_str:
            return DataType.VARCHAR
        
        data_type_str = data_type_str.upper().strip()
        
        type_mapping = {
            'VARCHAR': DataType.VARCHAR,
            'CHAR': DataType.VARCHAR,
            'STRING': DataType.VARCHAR,
            'TEXT': DataType.TEXT,
            'INT': DataType.INTEGER,
            'INTEGER': DataType.INTEGER,
            'INT4': DataType.INTEGER,
            'BIGINT': DataType.BIGINT,
            'INT8': DataType.BIGINT,
            'DECIMAL': DataType.DECIMAL,
            'NUMERIC': DataType.DECIMAL,
            'FLOAT': DataType.DECIMAL,
            'DOUBLE': DataType.DECIMAL,
            'DATE': DataType.DATE,
            'TIMESTAMP': DataType.TIMESTAMP,
            'DATETIME': DataType.TIMESTAMP,
            'TIME': DataType.TIMESTAMP,
            'BOOLEAN': DataType.BOOLEAN,
            'BOOL': DataType.BOOLEAN,
            'UUID': DataType.UUID,
            'BLOB': DataType.TEXT,
            'CLOB': DataType.TEXT,
        }
        
        # 处理带长度的类型，如 VARCHAR(255) 或 VARCHAR2(100)
        base_type = data_type_str.split('(')[0]
        return type_mapping.get(base_type, DataType.VARCHAR)
    
    def _safe_int(self, value) -> Optional[int]:
        """安全转换为整数"""
        if pd.isna(value):
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def _safe_str(self, value) -> Optional[str]:
        """安全转换为字符串"""
        if pd.isna(value):
            return None
        s = str(value).strip()
        return s if s else None
    
    def _parse_bool(self, value) -> bool:
        """解析布尔值（支持多种格式：是/否、Y/N、YES/NO、1/0、True/False）"""
        if pd.isna(value):
            return False
        
        if isinstance(value, bool):
            return value
        
        s = str(value).strip().upper()
        true_values = {'是', 'Y', 'YES', '1', 'TRUE', 'T', '真'}
        return s in true_values
    
    def _parse_nullable(self, value) -> bool:
        """解析是否可空（默认 True）"""
        if pd.isna(value):
            return True
        
        if isinstance(value, bool):
            return value
        
        s = str(value).strip().upper()
        false_values = {'否', 'N', 'NO', '0', 'FALSE', 'F', '假', 'NOT NULL', 'NOT_NULL'}
        return s not in false_values
    
    def _parse_fk_reference_from_comment(self, comment: Optional[str]) -> tuple:
        """从 comment 中解析外键引用信息。支持：外键，关联party.party_id；FK: party.party_id 等。"""
        if not comment:
            return None, None
        import re
        s = str(comment).strip()
        if not s:
            return None, None
        m = re.search(r'外键[，,]\s*关联\s*([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)', s, re.I)
        if m:
            return m.group(1), m.group(2)
        m = re.search(r'FK\s*:\s*([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)', s, re.I)
        if m:
            return m.group(1), m.group(2)
        m = re.search(r'foreign\s+key\s+to\s+([a-zA-Z_][a-zA-Z0-9_]*)[(\.]([a-zA-Z_][a-zA-Z0-9_]*)[).]?', s, re.I)
        if m:
            return m.group(1), m.group(2)
        m = re.search(r'references\s+([a-zA-Z_][a-zA-Z0-9_]*)[(\.]([a-zA-Z_][a-zA-Z0-9_]*)[).]?', s, re.I)
        if m:
            return m.group(1), m.group(2)
        if '外键' in s or 'FK' in s.upper() or 'foreign' in s.lower():
            m = re.search(r'([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)', s)
            if m:
                return m.group(1), m.group(2)
        return None, None
    
    def validate(self, file_path: str) -> bool:
        """验证文件格式"""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                return False
            
            suffix = file_path.suffix.lower()
            if suffix == '.csv':
                df = pd.read_csv(file_path, nrows=1, encoding='utf-8-sig')
                return len(df.columns) > 0
            elif suffix in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path, nrows=1)
                return len(df.columns) > 0
            else:
                return False
        except Exception:
            return False