from flask import Flask, jsonify, request
import requests
import time
from datetime import datetime
from dbfread import DBF
import os
import json
import hashlib
import sys

app = Flask(__name__)

# API configuration
API_BASE_URL = "https://wmsys.fly.dev/api/production_orders"  # Production URL

# Batch configuration
BATCH_SIZE = 50  # Number of records to send in each batch

# Rutas de los archivos .dbf
DBF_PATHS = [
    'C:\\\\ALPHAERP\\\\Empresas\\\\FLEXIEMP\\\\ipedidoc.dbf',
    'C:\\\\ALPHAERP\\\\Empresas\\\\FLEXIEMP\\\\ipedidod.dbf',
    'C:\\\\ALPHAERP\\\\Empresas\\\\FLEXIEMP\\\\oprod.dbf',
    'C:\\\\ALPHAERP\\\\Empresas\\\\FLEXIEMP\\\\opro.dbf',
    'C:\\\\ALPHAERP\\\\Empresas\\\\FLEXIEMP\\\\ordproc.dbf',
    'C:\\\\ALPHAERP\\\\Empresas\\\\FLEXIEMP\\\\PEDIENTR.dbf',
    'C:\\\\ALPHAERP\\\\Empresas\\\\FLEXIEMP\\\\remc.dbf',
    'C:\\\\ALPHAERP\\\\Empresas\\\\FLEXIEMP\\\\remd.dbf',
]

# File to store last processed state
STATE_FILE = "dbf_state.json"

def load_state():
    """Load the last processed state from file"""
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load state file: {e}")
    return {}

def save_state(state):
    """Save the current state to file"""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f)
    except Exception as e:
        print(f"Error saving state: {e}")

def get_file_hash(filepath):
    """Get file modification time and size for change detection"""
    try:
        stat = os.stat(filepath)
        return {
            'mtime': stat.st_mtime,
            'size': stat.st_size
        }
    except Exception as e:
        print(f"Error getting file hash for {filepath}: {e}")
        return None

def create_record_hash(record):
    """Create a hash of a record to detect changes"""
    try:
        record_str = json.dumps(record, sort_keys=True, default=str)
        return hashlib.md5(record_str.encode()).hexdigest()
    except Exception as e:
        print(f"Error creating record hash: {e}")
        return None

def send_batch_to_api(batch_data):
    """Send a batch of records to the API endpoint"""
    try:
        print(f"Sending batch of {len(batch_data)} records to API...")
        
        payload = {
            "company_name": "Flexiempaques",
            "production_orders": batch_data
        }
        
        response = requests.post(
            API_BASE_URL + "/batch",
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Successfully sent batch of {len(batch_data)} records to API")
            return result
        else:
            print(f"✗ Failed to send batch to API: {response.status_code} - {response.text}")
            return {"error": f"API error: {response.status_code}", "status_code": response.status_code}
            
    except Exception as e:
        print(f"✗ Error sending batch to API: {str(e)}")
        return {"error": str(e)}

def process_dbf_files_direct():
    """Process DBF files and send only new/modified records to API in batches"""
    try:
        print("=" * 50)
        print("STARTING DBF PROCESSING")
        print("=" * 50)
        
        state = load_state()
        print(f"Loaded state with {len(state)} entries")
        
        start_time = time.time()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'Checking for changes at: {timestamp}')
        
        changes_found = False
        all_new_records = []
        
        print(f"Checking {len(DBF_PATHS)} DBF files:")
        for dbf_path in DBF_PATHS:
            try:
                print(f"\\n--- Processing file: {dbf_path} ---")
                if os.path.exists(dbf_path):
                    base_name = os.path.splitext(os.path.basename(dbf_path))[0]
                    print(f"File exists: {base_name}")
                    
                    file_info = get_file_hash(dbf_path)
                    if not file_info:
                        print(f"Could not get file info for {dbf_path}")
                        continue
                        
                    prev_file_info = state.get(dbf_path, {})
                    print(f"Current file info: mtime={file_info['mtime']}, size={file_info['size']}")
                    print(f"Previous file info: mtime={prev_file_info.get('mtime', 0)}, size={prev_file_info.get('size', 0)}")
                    
                    if (file_info['mtime'] != prev_file_info.get('mtime', 0) or 
                        file_info['size'] != prev_file_info.get('size', 0)):
                        
                        print(f"✓ File {base_name} has been modified - processing...")
                        changes_found = True
                        
                        dbf = DBF(dbf_path, ignore_missing_memofile=True)
                        
                        processed_records = state.get(f"{dbf_path}_records", {})
                        new_processed_records = {}
                        
                        record_count = 0
                        new_records = 0
                        updated_records = 0
                        
                        print("Processing records...")
                        for record in dbf:
                            record_count += 1
                            record_dict = dict(record)
                            
                            record_hash = create_record_hash(record_dict)
                            if not record_hash:
                                continue
                            
                            if record_hash not in processed_records:
                                print(f"  New record #{record_count}")
                                all_new_records.append(record_dict)
                                new_records += 1
                            elif processed_records[record_hash] != record_hash:
                                print(f"  Modified record #{record_count}")
                                all_new_records.append(record_dict)
                                updated_records += 1
                                
                            new_processed_records[record_hash] = record_hash
                        
                        print(f"  Summary for {base_name}: {record_count} total, {new_records} new, {updated_records} updated")
                        state[f"{dbf_path}_records"] = new_processed_records
                        state[dbf_path] = file_info
                    else:
                        print(f"- No changes in {base_name}")
                else:
                    print(f"✗ File not found: {dbf_path}")
                    
            except Exception as e:
                print(f"✗ Error processing {dbf_path}: {str(e)}")
                import traceback
                traceback.print_exc()
        
        print(f"\\nTotal new/modified records found: {len(all_new_records)}")
        
        results = []
        if all_new_records:
            print(f"Sending {len(all_new_records)} records in batches...")
            
            for i in range(0, len(all_new_records), BATCH_SIZE):
                batch = all_new_records[i:i + BATCH_SIZE]
                print(f"Sending batch {i//BATCH_SIZE + 1}/{(len(all_new_records)-1)//BATCH_SIZE + 1} ({len(batch)} records)")
                batch_result = send_batch_to_api(batch)
                results.append({
                    "batch_index": i // BATCH_SIZE,
                    "batch_size": len(batch),
                    "result": batch_result
                })
                time.sleep(0.1)
            
            save_state(state)
            print("State saved")
        else:
            print("No new or modified records to send")
        
        elapsed_time = time.time() - start_time
        print(f'Check completed in: {elapsed_time:.2f} seconds')
        
        response_data = {
            "status": "completed",
            "changes_found": changes_found,
            "records_processed": len(all_new_records),
            "batches_sent": len(results),
            "processing_time": elapsed_time
        }
        
        print("=" * 50)
        print("PROCESSING COMPLETED")
        print("=" * 50)
        
        return response_data
        
    except Exception as e:
        print(f"✗ Critical error in process_dbf_files: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

# Endpoint que acepta tanto GET como POST
@app.route('/process', methods=['GET', 'POST'])
def process_endpoint():
    """Process DBF files via API endpoint"""
    return jsonify(process_dbf_files_direct())

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/', methods=['GET'])
def home():
    """Home endpoint with instructions"""
    return jsonify({
        "message": "DBF Uploader Service",
        "endpoints": {
            "GET /": "This help message",
            "GET/POST /process": "Process DBF files and upload to API",
            "GET /health": "Health check"
        },
        "status": "running"
    })

if __name__ == '__main__':
    # Si se llama con argumento "run", procesa directamente
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        print("Running direct processing mode...")
        result = process_dbf_files_direct()
        print("Result:", json.dumps(result, indent=2))
        sys.exit(0)
    
    # Modo normal: servidor Flask
    print("Starting Flask server...")
    print("Access endpoints:")
    print("  http://localhost:5000/ - Home/Help")
    print("  http://localhost:5000/process - Process DBF files")
    print("  http://localhost:5000/health - Health check")
    print("")
    print("Or run directly with: python flask_dbf_uploader.py run")
    
    app.run(host='0.0.0.0', port=5000, debug=True)