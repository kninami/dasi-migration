from flask import Flask, request, jsonify
import pandas as pd
import io
import excel_reader
import os
import tempfile
import db_processor
app = Flask(__name__)

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

if __name__ == '__main__':
    app.run(debug=True)