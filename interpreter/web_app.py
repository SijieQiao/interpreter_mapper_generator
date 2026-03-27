"""
解释器层 Web应用

专注于显示遗留数据字典的解析结果。
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

app = Flask(__name__, template_folder='templates', static_folder='static')
CORS(app)

# 配置
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB 最大文件大小

# 相对于interpreter目录的路径
PROJECT_ROOT = Path(__file__).parent.parent
UPLOAD_FOLDER = PROJECT_ROOT / 'data' / 'input' / 'uploads'

UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

def allowed_file(filename):
    """检查文件扩展名是否允许"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def _serialize_table(table):
    """将 TableMetadata 转换为 JSON 可序列化的字典"""
    return {
        'name': table.name,
        'comment': table.comment,
        'columns': [
            {
                'name': col.name,
                'data_type': str(col.data_type.value) if hasattr(col.data_type, 'value') else str(col.data_type),
                'length': col.length,
                'precision': col.precision,
                'scale': col.scale,
                'nullable': col.nullable,
                'comment': col.comment,
                'is_primary_key': col.is_primary_key,
                'is_foreign_key': col.is_foreign_key,
                'referenced_table': col.referenced_table,
                'referenced_column': col.referenced_column,
            }
            for col in table.columns
        ],
        'primary_keys': table.primary_keys,
        'foreign_keys': [
            {
                'name': fk.name,
                'columns': fk.columns,
                'referenced_table': fk.referenced_table,
                'referenced_columns': fk.referenced_columns,
            }
            for fk in table.foreign_keys
        ],
    }

@app.route('/')
def index():
    """主页 - 重定向到解释器页面"""
    return render_template('interpreter.html')

@app.route('/interpreter')
def interpreter_page():
    """解释器测试页面"""
    return render_template('interpreter.html')

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

@app.route('/api/interpreter/run', methods=['POST'])
def run_interpreter():
    """运行解释器并返回解析结果"""
    try:
        data = request.json or {}
        filepath = data.get('filepath')

        if not filepath:
            return jsonify({'success': False, 'error': '缺少 filepath'}), 400

        path = Path(filepath)
        if not path.exists():
            return jsonify({'success': False, 'error': f'文件不存在: {filepath}'}), 400

        reader = LegacyModelReader()
        label = 'LegacyModelReader（遗留模型解释器 · CSV/Excel）'

        if not reader.validate(str(path)):
            return jsonify({'success': False, 'error': '无效的数据字典文件（支持 .csv, .xlsx, .xls）'}), 400

        tables = reader.read(str(path))
        out = [_serialize_table(t) for t in tables]

        return jsonify({
            'success': True,
            'interpreter': 'legacy',
            'label': label,
            'filepath': str(path),
            'table_count': len(tables),
            'tables': out,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    print("=" * 60)
    print("遗留模型解释器 - Web界面")
    print("=" * 60)
    print("\n访问地址: http://localhost:5000")
    print("按 Ctrl+C 停止服务器\n")
    app.run(debug=True, host='0.0.0.0', port=5000)