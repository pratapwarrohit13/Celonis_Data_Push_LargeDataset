# Celonis Data Push & Parquet Converter

A Python Flask web application that simplifies the process of uploading data to Celonis. It accepts various file formats (CSV, Excel, etc.), converts them to Parquet format automatically, and pushes them to a specified Celonis Data Pool using the Data Push API.

> 📘 **User Friendly Guide**: Check out the [USER_GUIDE.md](USER_GUIDE.md) for a step-by-step walkthrough!

## Features

- **File Support**: Upload CSV, TSV, TXT, XLS, and XLSX files.
- **Auto-Conversion**: Automatically converts uploaded files to `.parquet` format.
- **Large File Handling**: Automatically splits files >1GB into chunks and uploads them sequentially with safety delays.
- **Celonis Integration**: Pushes data directly to Celonis Data Integration pools.
- **Real-time Logging**: Logs all transactions including Base URL, Pool ID, Filename, Row Count, and Timestamp.
- **User Interface**: Simple web interface for easy file uploads.

## Prerequisites

- Python 3.8+
- pip (Python package manager)

## Installation

1. Clone the repository or download the source code.
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

1. Start the application:

```bash
python parquet_app.py
```

2. Open your web browser and navigate to:
   `http://localhost:5001`

3. Fill in the form:
   - **Base URL**: Your Celonis Team URL (e.g., `https://your-team.celonis.cloud`)
   - **Pool ID**: The ID of the Data Pool where you want to push data.
   - **API Key**: Your Celonis API Key (User or App Key).
   - **File**: Select the file you want to upload.

4. Click **Upload & Push**.

## Logging

The application logs all activities to `celonis_data_push_parquet.log` in the project directory.
Log format: `Base URL | Pool ID | Parquet File | Rows | Time`

## Project Structure

- `parquet_app.py`: Main Flask application.
- `templates/parquet_upload.html`: Frontend HTML template.
- `uploads/`: Directory where converted parquet files are stored.
- `requirements.txt`: Python dependencies.
