"""
映射层 Web应用

专注于显示 BIAN Party 映射结果。
先调用解释器读取表格，然后进行映射，最终显示结果。
"""
import os
import sys
from pathlib import Path
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename

# 添加项目根目录到路径，确保能导入其他模块
current_dir = Path(__file__).parent
project_root = current_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from interpreter.legacy_model_reader_standalone import LegacyModelReader
from mapper.bian_mapping_layer import BIANMappingLayer, BIANMappingResult
from mapper.data_vault_layer import DataVaultTransformer, DataVaultModel

app = Flask(__name__, template_folder='templates', static_folder='static')
CORS(app)

# 配置
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB 最大文件大小

# 相对于mapper目录的路径
PROJECT_ROOT = Path(__file__).parent.parent
UPLOAD_FOLDER = PROJECT_ROOT / 'data' / 'input' / 'uploads'

UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

def allowed_file(filename):
    """检查文件扩展名是否允许"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def _serialize_bian_mapping(result: BIANMappingResult):
    """将 BIAN 映射结果序列化为 JSON"""
    return {
        'source_file': result.source_file,
        'domain': result.domain,
        'summary': result.summary,
        'table_mappings': [
            {
                'legacy_table': tm.legacy_table,
                'bian_entity': tm.bian_entity,
                'description': tm.description,
                'subtype': tm.subtype,
                'parent_entity': tm.parent_entity,
                'columns': [
                    {
                        'legacy_column': c.legacy_column,
                        'bian_attribute': c.bian_attribute,
                        'bian_entity': c.bian_entity,
                        'sub_attribute': c.sub_attribute,
                        'data_type': c.data_type,
                        'value_mapping_ref': c.value_mapping_ref,
                    }
                    for c in tm.columns
                ],
                'unmapped_columns': tm.unmapped_columns,
            }
            for tm in result.table_mappings
        ],
        'unmapped_tables': result.unmapped_tables,
    }

@app.route('/')
def index():
    """主页 - 重定向到映射页面"""
    return render_template('mapper.html')

@app.route('/mapper')
def mapper_page():
    """映射器测试页面"""
    return render_template('mapper.html')

@app.route('/test')
def test_page():
    """API测试页面"""
    return render_template('test_api.html')

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """上传文件接口"""
    if 'file' not in request.files:
        return jsonify({'error': '没有文件'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': '未选择文件'}), 400

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = UPLOAD_FOLDER / filename
        file.save(str(filepath))

        return jsonify({
            'success': True,
            'filename': filename,
            'filepath': str(filepath),
        })

    return jsonify({'error': '不支持的文件类型'}), 400

@app.route('/api/mapper/run', methods=['POST'])
def run_mapper():
    """运行映射器：先解释器解析，然后 BIAN 映射"""
    try:
        data = request.json or {}
        filepath = data.get('filepath')

        if not filepath:
            return jsonify({'success': False, 'error': '缺少 filepath'}), 400

        path = Path(filepath)
        if not path.exists():
            return jsonify({'success': False, 'error': f'文件不存在: {filepath}'}), 400

        # 1. 先用解释器读取表格
        reader = LegacyModelReader()
        if not reader.validate(str(path)):
            return jsonify({'success': False, 'error': '无效的数据字典文件（支持 .csv, .xlsx, .xls）'}), 400

        tables = reader.read(str(path))

        # 2. 应用 BIAN 映射
        layer = BIANMappingLayer()
        bian_result = layer.map_tables(tables, source_file=str(path))

        # 3. 返回映射结果
        label = 'BIAN Party 映射器（基于解释器结果）'

        return jsonify({
            'success': True,
            'mapper': 'bian_party',
            'label': label,
            'filepath': str(path),
            'bian_mapping': _serialize_bian_mapping(bian_result),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/mapper/config', methods=['GET'])
def get_mapper_config():
    """获取映射器配置摘要"""
    try:
        layer = BIANMappingLayer()
        return jsonify({
            'success': True,
            'config': layer.get_mapping_config_summary(),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ========== Data Vault 2.0 API 端点 ==========

@app.route('/api/datavault/transform', methods=['POST'])
def transform_to_data_vault():
    """将 BIAN 映射结果转换为 Data Vault 2.0 模型"""
    try:
        data = request.json or {}
        filepath = data.get('filepath')

        if not filepath:
            return jsonify({'success': False, 'error': '缺少 filepath'}), 400

        path = Path(filepath)
        if not path.exists():
            return jsonify({'success': False, 'error': f'文件不存在: {filepath}'}), 400

        # 1. 先进行 BIAN 映射
        reader = LegacyModelReader()
        if not reader.validate(str(path)):
            return jsonify({'success': False, 'error': '无效的数据字典文件'}), 400

        tables = reader.read(str(path))

        print(f"Processing file: {filepath}")

        bian_layer = BIANMappingLayer()
        bian_result = bian_layer.map_tables(tables, source_file=str(path))
        print(f"BIAN mapping completed: {len(bian_result.table_mappings)} tables mapped")

        # 2. 转换为 Data Vault 2.0
        dv_transformer = DataVaultTransformer()
        dv_model = dv_transformer.transform(bian_result)
        print(f"Data Vault transformation completed: {len(dv_model.raw_vault)} raw entities, {len(dv_model.business_vault)} business entities")

        # 3. 返回结果
        return jsonify({
            'success': True,
            'filepath': str(path),
            'bian_summary': _serialize_bian_mapping(bian_result),
            'data_vault': {
                'raw_vault_entities': len(dv_model.raw_vault),
                'business_vault_entities': len(dv_model.business_vault),
                'total_relationships': len(dv_model.relationships),
                'metadata': dv_model.metadata,
                'raw_entities': [
                    {
                        'name': entity.name,
                        'type': entity.entity_type.value,
                        'description': entity.description,
                        'columns': [
                            {
                                'name': col.name,
                                'data_type': col.data_type,
                                'column_type': col.column_type.value,
                                'nullable': col.nullable,
                                'description': col.description,
                                'source_column': col.source_column,
                                'source_table': col.source_table
                            }
                            for col in entity.columns
                        ],
                        'source_tables': entity.source_tables
                    }
                    for entity in dv_model.raw_vault
                ],
                'business_entities': [
                    {
                        'name': entity.name,
                        'type': entity.entity_type.value,
                        'description': entity.description,
                        'columns': [
                            {
                                'name': col.name,
                                'data_type': col.data_type,
                                'column_type': col.column_type.value,
                                'nullable': col.nullable,
                                'description': col.description,
                                'source_column': col.source_column,
                                'source_table': col.source_table
                            }
                            for col in entity.columns
                        ],
                        'source_tables': entity.source_tables
                    }
                    for entity in dv_model.business_vault
                ],
                'relationships': [
                    {
                        'from_entity': rel.from_entity,
                        'to_entity': rel.to_entity,
                        'from_column': rel.from_column,
                        'to_column': rel.to_column,
                        'relationship_type': rel.relationship_type
                    }
                    for rel in dv_model.relationships
                ]
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/datavault/diagram/<vault_type>', methods=['POST'])
def generate_diagram(vault_type):
    """生成 Data Vault 图表 (SVG 格式)"""
    try:
        if vault_type not in ['raw', 'business']:
            return jsonify({'success': False, 'error': '无效的 vault 类型 (raw/business)'}), 400

        data = request.json or {}
        filepath = data.get('filepath')

        if not filepath:
            return jsonify({'success': False, 'error': '缺少 filepath'}), 400

        # 重新生成 Data Vault 模型
        reader = LegacyModelReader()
        tables = reader.read(filepath)

        bian_layer = BIANMappingLayer()
        bian_result = bian_layer.map_tables(tables, source_file=filepath)

        dv_transformer = DataVaultTransformer()
        dv_model = dv_transformer.transform(bian_result)

        # 生成 SVG
        svg_content = dv_transformer.generate_svg_diagram(dv_model, vault_type)

        return jsonify({
            'success': True,
            'vault_type': vault_type,
            'svg_content': svg_content
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/datavault/dictionary', methods=['POST'])
def generate_data_dictionary():
    """生成数据字典"""
    try:
        data = request.json or {}
        filepath = data.get('filepath')

        if not filepath:
            return jsonify({'success': False, 'error': '缺少 filepath'}), 400

        # 重新生成 Data Vault 模型
        reader = LegacyModelReader()
        tables = reader.read(filepath)

        bian_layer = BIANMappingLayer()
        bian_result = bian_layer.map_tables(tables, source_file=filepath)

        dv_transformer = DataVaultTransformer()
        dv_model = dv_transformer.transform(bian_result)

        # 生成数据字典
        dictionary_content = dv_transformer.generate_data_dictionary(dv_model)

        return jsonify({
            'success': True,
            'dictionary_content': dictionary_content,
            'format': 'markdown'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500



if __name__ == '__main__':
    print("=" * 60)
    print("Data Vault 2.0 Converter - Banking Industry BIAN Standards")
    print("=" * 60)
    print("\nSupported Features:")
    print("* BIAN Party Mapping")
    print("* Data Vault 2.0 Transformation (Raw Vault + Business Vault)")
    print("* SVG Diagram Generation")
    print("* Data Dictionary Generation")
    print("\nAccess URLs:")
    print("Main App: http://localhost:5002")
    print("API Test: http://localhost:5002/test")
    print("Press Ctrl+C to stop the server\n")
    app.run(debug=True, host='0.0.0.0', port=5002)