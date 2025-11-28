# 📘 Celonis Data Push App - User Guide

Welcome to the **Celonis Data Push Application**! This guide will walk you through everything you need to know to set up and use the tool to upload your data to Celonis easily.

---

## 🚀 What does this tool do?

This tool helps you upload data files (like Excel or CSV) directly into your Celonis Data Pool. It handles the heavy lifting for you:
1.  **Accepts your file**: You can upload CSV, Excel, Text, or TSV files.
2.  **Converts it**: It automatically transforms your file into a high-performance format called **Parquet**.
3.  **Smart Upload**: 
    *   **Auto-Chunking**: If your file is larger than **1 GB**, the app automatically splits it into 10 smaller chunks to ensure a stable upload.
    *   **Safe Pacing**: It waits 3 seconds between uploading each chunk to prevent overwhelming the server.
4.  **Uploads it**: It securely pushes the data to your Celonis environment.

---

## 🛠️ Prerequisites

Before you start, make sure you have the following:

1.  **Python Installed**: You need Python (version 3.8 or newer) on your computer.
2.  **Celonis Access**:
    *   **Base URL**: The web address of your Celonis team (e.g., `https://your-team.celonis.cloud`).
    *   **Pool ID**: The ID of the Data Pool where you want the data to go.
    *   **API Key**: A key to authorize the upload. (Ask your Celonis admin if you don't have one).

---

## ⚙️ Setup Instructions

Follow these simple steps to get the app running on your computer.

### Step 1: Prepare the Folder
Make sure you have all the project files in a folder on your computer.

### Step 2: Install Dependencies
Open your terminal or command prompt in the project folder and run this command to install the necessary "parts" for the app:

```bash
pip install -r requirements.txt
```

### Step 3: Start the App
Run this command to start the application:

```bash
python parquet_app.py
```

You should see a message saying the app is running (usually on `http://127.0.0.1:5001`).

---

## 📝 How to Use

1.  **Open the App**: Open your web browser (Chrome, Edge, Firefox) and go to:
    👉 **http://localhost:5001**

2.  **Fill in the Details**:
    *   **Base URL**: Paste your Celonis URL here.
    *   **Pool ID**: Paste your Data Pool ID.
    *   **API Key**: Paste your API Key.
    *   **Select File**: Click the button to choose the file you want to upload from your computer.

3.  **Upload**:
    *   Click the blue **Upload & Push** button.
    *   Wait a moment while the app processes your file. You'll see a "Processing..." message.
    *   *Note: For very large files (>1GB), this may take a few minutes as it splits and uploads chunks one by one.*

4.  **Success!**:
    *   Once done, you will see a green success message.
    *   It will show you the **Target Name** (the name of the file in Celonis) and how many **Rows** were processed.

---

## ❓ Troubleshooting & Common Errors

Here are some common issues you might face and how to fix them:

### 1. "Invalid file format"
*   **Problem**: You tried to upload a file type that isn't supported.
*   **Solution**: Ensure your file ends with `.csv`, `.xlsx`, `.xls`, `.txt`, or `.tsv`.

### 2. "401 Unauthorized"
*   **Problem**: The application was denied access to Celonis.
*   **Solution**: Check your **API Key**. It might be incorrect, copied with extra spaces, or expired. Generate a new one in your Celonis Profile settings.

### 3. "404 Not Found"
*   **Problem**: The App couldn't find the Data Pool or the Celonis instance.
*   **Solution**: 
    *   Check your **Base URL**. It should look like `https://your-team.celonis.cloud`.
    *   Check your **Pool ID**. Make sure it is the exact UUID of your Data Pool.

### 4. "Connection Error" or "Timeout"
*   **Problem**: The app couldn't reach the internet or Celonis server.
*   **Solution**: Check your internet connection. If you are on a VPN, try disconnecting or ensuring it allows access to Celonis.

### 5. "Memory Error" (during conversion)
*   **Problem**: The file is too massive for your computer's RAM to convert at once.
*   **Solution**: Try splitting your source CSV into smaller files before uploading, or run the app on a machine with more RAM.

### 6. "Job failed" (in logs)
*   **Problem**: Celonis rejected the data.
*   **Solution**: Check the `celonis_data_push_parquet.log` file. It often contains a specific error message from Celonis (e.g., "Schema mismatch" or "Column type error").

---

## 📞 Support

If you encounter issues not listed here, please contact your technical support team and provide them with the log file (`celonis_data_push_parquet.log`) located in the project folder.
