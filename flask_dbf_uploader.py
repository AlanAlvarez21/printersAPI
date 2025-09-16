import requests
import time
from datetime import datetime
from dbfread import DBF
import os
import json
import hashlib

# API configuration
API_BASE_URL = "https://wmsys.fly.dev/api/production_orders"  # Production URL

# Configuration
CHECK_INTERVAL = 60  # Check every 60 seconds
BATCH_SIZE = 50  # Number of records to send in each batch

# Ruta del archivo opro.dbf (ajusta según tu sistema)
OPRO_DBF_PATH = 'C:\\ALPHAERP\\Empresas\\FLEXIEMP\\opro.dbf'

# File to store last processed state
STATE_FILE = "opro_state.json"

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

def clean_value(value):
    """Clean and convert value to appropriate type"""
    if value is None or str(value).lower() in ['nan', 'none', '']:
        return ''
    return str(value)

def map_opro_record_to_api(record):
    """Map opro.dbf record to exact API format with comprehensive field mapping"""
    try:
        # Convert all values to strings and clean them
        cleaned_record = {k: clean_value(v) for k, v in record.items()}
        
        # Mapeo completo de campos importantes
        mapped_record = {
            # Campos requeridos por la API
            "product_id": "9ef77e17-4a1f-435f-99e3-21dbff7c68ee",
            "quantity_requested": int(float(cleaned_record.get('CANT_LIQ', '0') or '0')),
            "warehouse_id": "45c4bbc8-2950-434c-b710-2ae0e080bfd1",
            
            # Campos de identificación
            "no_opro": cleaned_record.get('NO_OPRO', ''),
            "priority": determine_priority(cleaned_record),
            
            # Notas y observaciones
            "notes": cleaned_record.get('OBSERVAT', ''),
            
            # Campos adicionales del schema
            "lote_referencia": cleaned_record.get('LOTE', ''),
            "stat_opro": cleaned_record.get('OPROSTAT', ''),
            "ren_orp": cleaned_record.get('REFERENC', ''),
            
            # Información adicional útil
            "creation_date": cleaned_record.get('FEC_OPRO', ''),
            "carga_copr": float(cleaned_record.get('CARGA', '0') or '0'),
            "trans_liq": cleaned_record.get('TRANS_LIQ', ''),
            "alm_tran": cleaned_record.get('ALM_TRAN', ''),
            "hora_opr": cleaned_record.get('HORA_OPR', ''),
            "master_prod": cleaned_record.get('MASTERPROD', ''),
            "nivel_ord": cleaned_record.get('NIVELORD', ''),
            "suc_sup": cleaned_record.get('SUCSUP', ''),
            "pedido_no": cleaned_record.get('NO_PED', ''),
            "super_master": cleaned_record.get('SUPERMASTER', ''),
            "turno": cleaned_record.get('TURNO', ''),
            "part_sup": cleaned_record.get('PARTSUP', ''),
        }
        
        # Asegurar valores válidos para campos numéricos
        if mapped_record["quantity_requested"] <= 0:
            mapped_record["quantity_requested"] = 1000
            
        return mapped_record
        
    except Exception as e:
        print(f"Error mapping record: {e}")
        return None

def determine_priority(record):
    """Determinar prioridad basada en el estado"""
    status = record.get('OPROSTAT', '').lower()
    
    if 'terminada' in status:
        return 'high'
    elif 'cancelada' in status:
        return 'low'
    elif 'urgente' in status or 'alta' in status:
        return 'high'
    elif 'baja' in status:
        return 'low'
    else:
        return 'medium'

def send_batch_to_api(batch_data):
    """Send a batch of records to the API endpoint"""
    try:
        print(f"Sending batch of {len(batch_data)} records to API...")
        
        # Formato exacto que espera la API
        payload = {
            "company_name": "Flexiempaques",
            "production_orders": batch_data
        }
        
        print(f"Payload sample keys: {list(batch_data[0].keys()) if batch_data else 'No data'}")
        
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
            return {"error": f"API error: {response.status_code}", "status_code": response.status_code, "response_text": response.text}
            
    except Exception as e:
        print(f"✗ Error sending batch to API: {str(e)}")
        return {"error": str(e)}

def process_opro_file():
    """Process opro.dbf file and send records to API"""
    try:
        print("=" * 60)
        print(f"PROCESSING OPRO.DBF - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        state = load_state()
        print(f"Loaded state with {len(state)} entries")
        
        start_time = time.time()
        
        # Check if file exists
        if not os.path.exists(OPRO_DBF_PATH):
            print(f"✗ File not found: {OPRO_DBF_PATH}")
            return {"status": "error", "message": "File not found"}
            
        # Check if file has changed
        file_info = get_file_hash(OPRO_DBF_PATH)
        if not file_info:
            print(f"✗ Could not get file info for {OPRO_DBF_PATH}")
            return {"status": "error", "message": "Could not get file info"}
            
        prev_file_info = state.get('file_info', {})
        print(f"Current file info: mtime={file_info['mtime']}, size={file_info['size']}")
        print(f"Previous file info: mtime={prev_file_info.get('mtime', 'None')}, size={prev_file_info.get('size', 'None')}")
        
        # If file hasn't changed, skip processing
        if (file_info['mtime'] == prev_file_info.get('mtime', 0) and 
            file_info['size'] == prev_file_info.get('size', 0)):
            print("No changes in opro.dbf file")
            print("=" * 60)
            print("WAITING FOR NEXT CHECK...")
            print("=" * 60)
            return {"status": "completed", "changes_found": False, "records_processed": 0}
            
        print("✓ File has been modified - processing...")
        
        # Open the DBF file
        print(f"Opening DBF file: {OPRO_DBF_PATH}")
        dbf = DBF(OPRO_DBF_PATH, ignore_missing_memofile=True)
        
        # Get processed records state
        processed_records = state.get('processed_records', {})
        new_processed_records = {}
        
        # Process records
        all_records = []
        record_count = 0
        new_records = 0
        updated_records = 0
        
        print("Processing records...")
        for record in dbf:
            record_count += 1
            record_dict = dict(record)
            
            # Create record hash
            record_hash = create_record_hash(record_dict)
            if not record_hash:
                continue
                
            # Map record to API format
            mapped_record = map_opro_record_to_api(record_dict)
            if not mapped_record:
                continue
                
            # Check if this is a new or modified record
            if record_hash not in processed_records:
                # New record
                print(f"  New record #{record_count}: {mapped_record.get('no_opro', 'Unknown')}")
                all_records.append(mapped_record)
                new_records += 1
            elif processed_records[record_hash] != record_hash:
                # Modified record
                print(f"  Modified record #{record_count}: {mapped_record.get('no_opro', 'Unknown')}")
                all_records.append(mapped_record)
                updated_records += 1
                
            # Store record hash
            new_processed_records[record_hash] = record_hash
            
            # Show progress every 50 records
            if record_count % 50 == 0:
                print(f"  Processed {record_count} records...")
        
        print(f"  Summary: {record_count} total, {new_records} new, {updated_records} updated")
        
        # Send records in batches
        results = []
        if all_records:
            print(f"Sending {len(all_records)} records in {((len(all_records)-1)//BATCH_SIZE)+1} batches...")
            
            for i in range(0, len(all_records), BATCH_SIZE):
                batch = all_records[i:i + BATCH_SIZE]
                print(f"Sending batch {i//BATCH_SIZE + 1}/{((len(all_records)-1)//BATCH_SIZE)+1} ({len(batch)} records)")
                batch_result = send_batch_to_api(batch)
                results.append({
                    "batch_index": i // BATCH_SIZE,
                    "batch_size": len(batch),
                    "result": batch_result
                })
                time.sleep(0.1)  # Small delay between batches
            
            # Update state
            state['processed_records'] = new_processed_records
            state['file_info'] = file_info
            save_state(state)
            print("State saved")
        else:
            print("No new or modified records to send")
            # Still update file info to avoid reprocessing
            state['file_info'] = file_info
            save_state(state)
            print("File info updated")
        
        elapsed_time = time.time() - start_time
        print(f'Processing completed in: {elapsed_time:.2f} seconds')
        
        print("=" * 60)
        print("WAITING FOR NEXT CHECK...")
        print("=" * 60)
        
        return {
            "status": "completed",
            "changes_found": True,
            "records_processed": len(all_records),
            "new_records": new_records,
            "updated_records": updated_records,
            "batches_sent": len(results),
            "processing_time": elapsed_time,
            "total_records": record_count
        }
        
    except Exception as e:
        print(f"✗ Critical error in process_opro_file: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

def automatic_processing_loop():
    """Main loop that runs processing automatically"""
    print("Starting automatic OPRO processing service...")
    print(f"Configuration:")
    print(f"  - API URL: {API_BASE_URL}")
    print(f"  - Check interval: {CHECK_INTERVAL} seconds")
    print(f"  - Batch size: {BATCH_SIZE} records")
    print(f"  - DBF file: {OPRO_DBF_PATH}")
    print("")
    
    while True:
        try:
            # Process files
            result = process_opro_file()
            
            # Show summary
            if result.get("status") == "completed" and result.get("changes_found"):
                print(f"Summary: {result.get('total_records', 0)} total records, "
                      f"{result.get('new_records', 0)} new, "
                      f"{result.get('updated_records', 0)} updated, "
                      f"{result.get('batches_sent', 0)} batches sent")
            
            # Wait for next check
            print(f"Sleeping for {CHECK_INTERVAL} seconds...")
            time.sleep(CHECK_INTERVAL)
            
        except KeyboardInterrupt:
            print("\nStopping automatic processing service...")
            break
        except Exception as e:
            print(f"Error in main loop: {e}")
            print("Continuing...")
            time.sleep(CHECK_INTERVAL)

if __name__ == '__main__':
    automatic_processing_loop()