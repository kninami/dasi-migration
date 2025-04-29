from flask import Flask, request, jsonify
import pandas as pd
import io
import excel_reader
import os
import tempfile
import db_processor
from dotenv import load_dotenv
import logging

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "Welcome to the API"})

@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    logger.info("Upload endpoint called")
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
        
        logger.info(f"Processing file: {file.filename}")
        
        # Return the processed data
        return jsonify({
            "message": "File successfully processed",
            "data": result
        }), 200

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)