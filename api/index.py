from flask import Flask, request, jsonify
import pandas as pd
import io
import excel_reader
import os
import tempfile
import db_processor
from dotenv import load_dotenv
import logging
from functools import wraps

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 토큰 검증 데코레이터 함수
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        
        if not token:
            logger.warning("Token is missing")
            return jsonify({"error": "Token is missing"}), 401
        
        expected_token = os.getenv('RETOOL_TOKEN')
        if token != f"Bearer {expected_token}":
            logger.warning("Invalid token")
            return jsonify({"error": "Invalid token"}), 401
            
        return f(*args, **kwargs)
    return decorated

@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "Welcome to the API"})

@app.route('/upload-csv', methods=['POST'])
@token_required
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
        
        result = reader.case_data_from_csv(temp_file_path)
        logger.info(f"Uploaded Result: {result}")
        try:
            db_processor.DataProcessor().process_case_sheet_data(result)
        except Exception as e:
            logger.error(f"Error processing case sheet data: {str(e)}")
        
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