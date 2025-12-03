# DBF Uploader with Local Record Count Tracking

## Overview
This script uploads DBF records to the production orders API. Instead of making many API requests to check for changes, it now uses a local JSON file to track the number of records in the DBF file. The script only uploads records when there are new ones compared to the last processed count.

## Key Changes
- **Local Record Count Storage**: Uses `record_count.json` to store the previous count of records in the DBF file
- **Efficient Comparison**: Compares the current DBF record count with the stored count to determine if new records exist
- **Reduced API Calls**: Only uploads when there are actually new records to process
- **Backward Compatible**: Maintains all existing functionality while optimizing for efficiency

## How It Works
1. The script reads the current count of records in the `opro.dbf` file
2. It compares this count to the previously stored count in `record_count.json`
3. If there are more records than before, it processes and uploads the new records
4. After successful upload, it updates the stored count to the current count

## Files Created/Modified
- `new_script_dbf_uploader.py`: The new implementation with local record count tracking
- `record_count.json`: Local storage for the previous record count (created automatically)

## Setup
1. The script will automatically create the `record_count.json` file with an initial count of 0
2. You can manually update this file if needed with the current number of records in the DBF file
3. The first run will process all records in the DBF file and store that count as the baseline

## Environment
The script includes the same configuration as before, including:
- API endpoint settings
- Batch processing
- Logging
- Error handling
- Force send and state clearing options

## Usage
Run the script normally:
```bash
python new_script_dbf_uploader.py
```

## Benefits
- Significantly reduces the number of API requests
- Maintains the same functionality and reliability
- Faster execution since it only processes when there are new records
- Eliminates the need for database connections