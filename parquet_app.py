import os
import sys
import uuid
import logging
import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime
from werkzeug.utils import secure_filename
from flask import Flask, render_template, request, jsonify

# Configure logging
log_filename = os.path.join(os.path.dirname(__file__), 'celonis_data_push_parquet.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')
app.config['MAX_CONTENT_LENGTH'] = 1000 * 1024 * 1024  # 1GB max file size

# Supported file formats for conversion
SUPPORTED_FORMATS = {'csv', 'txt', 'tsv', 'xls', 'xlsx'}

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in SUPPORTED_FORMATS

def convert_to_parquet(file_path, original_filename):
    """
    Converts the input file to Parquet format.
    Returns the path to the new parquet file and the dataframe row count.
    """
    try:
        file_ext = original_filename.rsplit('.', 1)[1].lower()
        
        if file_ext == 'csv':
            df = pd.read_csv(file_path)
        elif file_ext == 'tsv':
            df = pd.read_csv(file_path, sep='\t')
        elif file_ext == 'txt':
            df = pd.read_csv(file_path, sep='\t') # Assuming txt is tab-separated or similar, default to csv/tsv logic
        elif file_ext in ['xls', 'xlsx']:
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file extension: {file_ext}")

        # Define parquet filename (source file name as parquet file name)
        base_name = original_filename.rsplit('.', 1)[0]
        parquet_filename = f"{base_name}.parquet"
        parquet_path = os.path.join(app.config['UPLOAD_FOLDER'], parquet_filename)
        
        # Save as parquet
        df.to_parquet(parquet_path, index=False)
        
        return parquet_path, parquet_filename, len(df)
    except Exception as e:
        logger.error(f"Error converting to parquet: {str(e)}")
        raise

def push_to_celonis(parquet_paths, target_name, pool_id, api_key, base_url):
    """
    Pushes the parquet file(s) to Celonis using the logic from test.py.
    Accepts a single path or a list of paths.
    """
    try:
        if isinstance(parquet_paths, str):
            parquet_paths = [parquet_paths]

        # Ensure base_url has no trailing slash and correct scheme
        if not base_url.startswith('http'):
            base_url = f"https://{base_url}"
        base_url = base_url.rstrip('/')
        
        # 1. Create Data Push Job
        url = f"{base_url}/integration/api/v1/data-push/{pool_id}/jobs/"
        
        body = {
            'type': 'DELTA', 
            'fileType': 'PARQUET', 
            'targetName': target_name, 
            'dataPoolId': pool_id
        }
        headers = {
            'Authorization': f'Bearer {api_key}', 
            'Content-Type': 'application/json'
        }
        
        logger.info(f"Creating job at {url} with target {target_name}")
        response = requests.post(url=url, json=body, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Failed to create job. Status: {response.status_code}, Response: {response.text}")
            
        data_push_job = response.json()
        job_id = data_push_job['id']
        logger.info(f"Job created: {job_id}")
        
        # 2. Upload Chunks (Loop)
        push_chunk_url = f"{url}{job_id}/chunks/upserted"
        
        for i, path in enumerate(parquet_paths):
            logger.info(f"Uploading chunk {i+1}/{len(parquet_paths)} from {path} to {push_chunk_url}")
            
            with open(path, 'rb') as f:
                files = {'file': f}
                chunk_headers = {'Authorization': f'Bearer {api_key}'}
                response = requests.post(url=push_chunk_url, files=files, headers=chunk_headers)
                
            if response.status_code != 200:
                raise Exception(f"Failed to upload chunk {i+1}. Status: {response.status_code}, Response: {response.text}")
                
            logger.info(f"Chunk {i+1} uploaded successfully")
            
            # Wait for 3 seconds after each chunk upload
            if i < len(parquet_paths) - 1: # Optional: Don't wait after the last one, or do? User said "after each". Let's do after each to be safe or just simple loop.
                logger.info("Waiting 3 seconds...")
                time.sleep(3)
        
        # 3. Execute Job
        execute_url = f"{url}{job_id}"
        logger.info(f"Executing job at {execute_url}")
        response = requests.post(url=execute_url, json={}, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Failed to execute job. Status: {response.status_code}, Response: {response.text}")
            
        logger.info("Job executed successfully")
        return job_id
        
    except Exception as e:
        logger.error(f"Celonis Push Error: {str(e)}")
        raise

@app.route('/')
def index():
    return render_template('parquet_upload.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        base_url = request.form.get('base_url')
        pool_id = request.form.get('pool_id')
        api_key = request.form.get('api_key')
        file = request.files.get('file')
        
        if not all([base_url, pool_id, api_key, file]):
            return jsonify({'error': 'Missing required fields'}), 400
            
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file format'}), 400
            
        # Save uploaded file
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        # Convert to Parquet
        parquet_path, parquet_filename, row_count = convert_to_parquet(file_path, filename)
        
        # Log Data
        # "log all data like base url , pool id , parquest file name , row count & date time"
        log_entry = f"Base URL: {base_url} | Pool ID: {pool_id} | Parquet File: {parquet_filename} | Rows: {row_count} | Time: {datetime.now()}"
        logger.info(f"TRANSACTION: {log_entry}")
        
        # Push to Celonis
        # "under target name should show parquet file name" -> So we pass parquet_filename as target_name
        # Check file size (1GB = 1024^3 bytes)
        file_size = os.path.getsize(parquet_path)
        files_to_push = [parquet_path]
        
        if file_size > 1024 * 1024 * 1024:
            logger.info(f"File size {file_size} bytes > 1GB. Splitting into 10 chunks.")
            
            # Read parquet back
            df_full = pd.read_parquet(parquet_path)
            
            # Split into 10 chunks
            chunks = np.array_split(df_full, 10)
            files_to_push = []
            
            base_name = parquet_filename.rsplit('.', 1)[0]
            
            for i, chunk in enumerate(chunks):
                chunk_filename = f"{base_name}_part_{i+1}.parquet"
                chunk_path = os.path.join(app.config['UPLOAD_FOLDER'], chunk_filename)
                chunk.to_parquet(chunk_path, index=False)
                files_to_push.append(chunk_path)
                logger.info(f"Created chunk {i+1}: {chunk_path}")
        
        # Push to Celonis
        # "under target name should show parquet file name" -> So we pass parquet_filename as target_name
        push_to_celonis(files_to_push, parquet_filename, pool_id, api_key, base_url)
        
        return jsonify({
            'success': True,
            'message': 'File uploaded and pushed successfully',
            'target_name': parquet_filename,
            'row_count': row_count
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001) # Running on 5001 to avoid conflict if app.py is running
