# DBF Uploader GUI Application

## Overview
This GUI application provides a user-friendly interface for the DBF uploader that uses local record count tracking to optimize the upload process. Instead of making many API requests to check for changes, it uses a local JSON file to track the number of records in the DBF file and only uploads when there are new records.

## Features
- **Graphical User Interface**: Easy-to-use interface built with PyQt5
- **Local Record Count Tracking**: Uses `record_count.json` to store the previous count of records in the DBF file
- **Efficient Comparison**: Compares the current DBF record count with the stored count to determine if new records exist
- **Real-time Logging**: Displays detailed logs of the upload process
- **Progress Tracking**: Shows progress bar during upload operations
- **Auto-refresh Option**: Can automatically check for changes at regular intervals
- **Stop Functionality**: Ability to stop ongoing uploads

## Key Components
- **Status Panel**: Shows current and previous record counts, file path, and current status
- **Control Buttons**: 
  - Upload Now: Start the upload process immediately
  - Check for Changes: Check if there are new records without uploading
  - Refresh Status: Update the status information
  - Auto-Refresh: Toggle automatic checking for changes
- **Progress Bar**: Visual indication of upload progress
- **Log Output**: Detailed logging of all operations
- **Stop Button**: Appears during uploads to stop the process

## How It Works
1. The application reads the current count of records in the `opro.dbf` file
2. Compares this count to the previously stored count in `record_count.json`
3. If there are more records than before, it processes and uploads the new records
4. After successful upload, it updates the stored count to the current count

## Setup
1. Make sure PyQt5 is installed: `pip install PyQt5`
2. Ensure you have the required dependencies: `requests`, `dbfread`
3. The application will automatically create the `record_count.json` file with an initial count of 0
4. You can manually update this file if needed with the current number of records in the DBF file

## Usage
Run the application:
```bash
python gui_dbf_uploader.py
```

The GUI will display:
- Current and previous record counts
- Status of the last operation
- Detailed log output
- Progress during uploads
- Control buttons for manual operation

## Benefits
- Significantly reduces the number of API requests
- Maintains the same functionality and reliability
- Faster execution since it only processes when there are new records
- User-friendly interface with real-time feedback
- Eliminates the need for database connections