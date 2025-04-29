from flask import Flask, request, jsonify
import pandas as pd
import io
import excel_reader
import os
import tempfile
import db_processor
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "Welcome to the API"})

@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    try:
        # Save the uploaded file to a temporary location
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, file.filename)
        file.save(temp_file_path)
        
        # Process the file using ExcelReader
        reader = excel_reader.ExcelReader()
        
        # 파일 확장자에 따라 처리
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext == '.csv':
            result = reader.case_data_to_json(temp_file_path)  # 수정된 read_excel_file 메서드가 CSV도 처리
        else:
            result = reader.case_data_to_json(temp_file_path)
            
        db_processor.DataProcessor().process_case_sheet_data(result)
        
        # Clean up the temporary file
        os.remove(temp_file_path)
        
        # Return the processed data
        return jsonify({
            "message": "File successfully processed",
            "data": result
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Vercel에서는 이 부분이 실행되지 않지만, 로컬 개발용으로 유지
if __name__ == '__main__':
    app.run(debug=True)