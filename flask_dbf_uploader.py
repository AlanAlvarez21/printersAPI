import requests
import time
import logging
from datetime import datetime
from dbfread import DBF
import os
import json
import hashlib
import sys
from typing import Dict, List, Optional, Any
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dbf_uploader.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('DBFUploader')

# API configuration
API_BASE_URL = "https://wmsys.fly.dev/api/production_orders"  # Production URL
API_TIMEOUT = 30  # seconds
MAX_RETRIES = 3

# Configuration
CHECK_INTERVAL = 60  # Check every 60 seconds
BATCH_SIZE = 50  # Number of records to send in each batch

# Ruta del archivo opro.dbf (ajusta según tu sistema)
OPRO_DBF_PATH = 'C:\\ALPHAERP\\Empresas\\FLEXIEMP\\opro.dbf'

# File to store last processed state
STATE_FILE = "dbf_state.json"

class DBFUploader:
    def __init__(self):
        self.state = self.load_state()
        self.session = requests.Session()
        
    def load_state(self) -> Dict:
        """Load the last processed state from file"""
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, 'r') as f:
                    state = json.load(f)
                    logger.info(f"Loaded state with {len(state)} entries")
                    return state
        except Exception as e:
            logger.warning(f"Could not load state file: {e}")
        return {}

    def save_state(self) -> bool:
        """Save the current state to file"""
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump(self.state, f, indent=2)
            logger.debug("State saved successfully")
            return True
        except Exception as e:
            logger.error(f"Error saving state: {e}")
            return False

    def get_file_hash(self, filepath: str) -> Optional[Dict]:
        """Get file modification time and size for change detection"""
        try:
            stat = os.stat(filepath)
            return {
                'mtime': stat.st_mtime,
                'size': stat.st_size
            }
        except Exception as e:
            logger.error(f"Error getting file hash for {filepath}: {e}")
            return None

    def create_record_hash(self, record: Dict) -> Optional[str]:
        """Create a hash of a record to detect changes"""
        try:
            record_str = json.dumps(record, sort_keys=True, default=str)
            return hashlib.md5(record_str.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Error creating record hash: {e}")
            return None

    def clean_value(self, value: Any) -> str:
        """Clean and convert value to appropriate type"""
        if value is None or str(value).lower() in ['nan', 'none', '']:
            return ''
        return str(value)

    def determine_priority(self, record: Dict) -> str:
        """Determine priority based on status"""
        status = record.get('OPROSTAT', '').lower()
        
        if 'terminada' in status or 'completada' in status:
            return 'high'
        elif 'cancelada' in status or 'rechazada' in status:
            return 'low'
        elif 'urgente' in status or 'alta' in status:
            return 'high'
        elif 'baja' in status:
            return 'low'
        else:
            return 'medium'

    def map_opro_record_to_api(self, record: Dict) -> Optional[Dict]:
        """Map opro.dbf record to exact API schema format - ONLY required fields"""
        try:
            # Convert all values to strings and clean them
            cleaned_record = {k: self.clean_value(v) for k, v in record.items()}
            
            # Extract only the fields that match the API schema
            mapped_record = {
                # Required fields from API schema
                "product_id": "9ef77e17-4a1f-435f-99e3-21dbff7c68ee",  # Default product ID
                "quantity_requested": max(1, int(float(cleaned_record.get('CANT_LIQ', '0') or '0'))),
                "warehouse_id": "45c4bbc8-2950-434c-b710-2ae0e080bfd1",  # Default warehouse ID
                
                # Optional but useful fields from schema
                "no_opro": cleaned_record.get('NO_OPRO', ''),
                "priority": self.determine_priority(cleaned_record),
                "notes": cleaned_record.get('OBSERVAT', ''),  # This maps to your notes field
                
                # Additional fields that are part of the schema
                "lote_referencia": cleaned_record.get('LOTE', ''),
                "stat_opro": cleaned_record.get('OPROSTAT', ''),
            }
            
            return mapped_record
            
        except Exception as e:
            logger.error(f"Error mapping record to API schema: {e}")
            return None

    def send_batch_to_api(self, batch_data: List[Dict]) -> Dict:
        """Send a batch of records to the API endpoint with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Sending batch of {len(batch_data)} records to API (attempt {attempt + 1})")
                
                payload = {
                    "company_name": "Flexiempaques",
                    "production_orders": batch_data
                }
                
                response = self.session.post(
                    API_BASE_URL + "/batch",
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=API_TIMEOUT
                )
                
                if response.status_code == 200:
                    result = response.json()
                    success_count = sum(1 for r in result.get('results', []) if r.get('status') == 'success')
                    logger.info(f"✓ Successfully sent batch: {success_count}/{len(batch_data)} records processed")
                    return {"success": True, "data": result}
                else:
                    logger.warning(f"API returned {response.status_code}: {response.text}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                        
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error on attempt {attempt + 1}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
                    
        logger.error("✗ Failed to send batch after all retries")
        return {"success": False, "error": "Failed after retries"}

    def process_opro_file(self) -> Dict:
        """Process opro.dbf file and send records to API - ONLY schema matching data"""
        try:
            logger.info("=" * 60)
            logger.info(f"PROCESSING OPRO.DBF - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)
            
            start_time = time.time()
            
            # Check if file exists
            if not os.path.exists(OPRO_DBF_PATH):
                logger.error(f"File not found: {OPRO_DBF_PATH}")
                return {"status": "error", "message": "File not found"}
                
            # Check if file has changed
            file_info = self.get_file_hash(OPRO_DBF_PATH)
            if not file_info:
                logger.error(f"Could not get file info for {OPRO_DBF_PATH}")
                return {"status": "error", "message": "Could not get file info"}
                
            prev_file_info = self.state.get('file_info', {})
            logger.info(f"Current file info: mtime={file_info['mtime']}, size={file_info['size']}")
            logger.info(f"Previous file info: mtime={prev_file_info.get('mtime', 'None')}, size={prev_file_info.get('size', 'None')}")
            
            # If file hasn't changed, skip processing
            if (file_info['mtime'] == prev_file_info.get('mtime', 0) and 
                file_info['size'] == prev_file_info.get('size', 0)):
                logger.info("No changes in opro.dbf file")
                logger.info("=" * 60)
                logger.info("WAITING FOR NEXT CHECK...")
                logger.info("=" * 60)
                return {"status": "completed", "changes_found": False, "records_processed": 0}
                
            logger.info("✓ File has been modified - processing...")
            
            # Open the DBF file
            logger.info(f"Opening DBF file: {OPRO_DBF_PATH}")
            dbf = DBF(OPRO_DBF_PATH, ignore_missing_memofile=True)
            
            # Get processed records state
            processed_records = self.state.get('processed_records', {})
            new_processed_records = {}
            
            # Process records - ONLY schema matching data
            all_records = []
            record_count = 0
            new_records = 0
            updated_records = 0
            
            logger.info("Processing records (schema matching only)...")
            for record in dbf:
                record_count += 1
                record_dict = dict(record)
                
                # Create record hash
                record_hash = self.create_record_hash(record_dict)
                if not record_hash:
                    continue
                    
                # Map record to API format - ONLY schema fields
                mapped_record = self.map_opro_record_to_api(record_dict)
                if not mapped_record:
                    continue
                    
                # Check if this is a new or modified record
                if record_hash not in processed_records:
                    # New record
                    logger.debug(f"  New record #{record_count}: {mapped_record.get('no_opro', 'Unknown')}")
                    all_records.append(mapped_record)
                    new_records += 1
                elif processed_records[record_hash] != record_hash:
                    # Modified record
                    logger.debug(f"  Modified record #{record_count}: {mapped_record.get('no_opro', 'Unknown')}")
                    all_records.append(mapped_record)
                    updated_records += 1
                    
                # Store record hash
                new_processed_records[record_hash] = record_hash
                
                # Show progress every 100 records
                if record_count % 100 == 0:
                    logger.info(f"  Processed {record_count} records...")
            
            logger.info(f"  Summary: {record_count} total, {new_records} new, {updated_records} updated")
            
            # Send records in batches
            results = []
            if all_records:
                total_batches = ((len(all_records)-1) // BATCH_SIZE) + 1
                logger.info(f"Sending {len(all_records)} schema-matching records in {total_batches} batches...")
                
                for i in range(0, len(all_records), BATCH_SIZE):
                    batch = all_records[i:i + BATCH_SIZE]
                    batch_index = i // BATCH_SIZE + 1
                    logger.info(f"Sending batch {batch_index}/{total_batches} ({len(batch)} records)")
                    
                    batch_result = self.send_batch_to_api(batch)
                    results.append({
                        "batch_index": batch_index,
                        "batch_size": len(batch),
                        "result": batch_result
                    })
                    
                    # Small delay between batches
                    time.sleep(0.1)
                
                # Update state
                self.state['processed_records'] = new_processed_records
                self.state['file_info'] = file_info
                if self.save_state():
                    logger.info("State saved")
                else:
                    logger.error("Failed to save state")
            else:
                logger.info("No new or modified schema-matching records to send")
                # Still update file info to avoid reprocessing
                self.state['file_info'] = file_info
                if self.save_state():
                    logger.info("File info updated")
                else:
                    logger.error("Failed to update file info")
            
            elapsed_time = time.time() - start_time
            logger.info(f'Processing completed in: {elapsed_time:.2f} seconds')
            
            logger.info("=" * 60)
            logger.info("WAITING FOR NEXT CHECK...")
            logger.info("=" * 60)
            
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
            logger.error(f"Critical error in process_opro_file: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": str(e)}

    def run_once(self) -> bool:
        """Run processing once and return success status"""
        try:
            result = self.process_opro_file()
            if result.get("status") == "completed":
                if result.get("changes_found"):
                    logger.info(f"Summary: {result.get('total_records', 0)} total records, "
                              f"{result.get('new_records', 0)} new, "
                              f"{result.get('updated_records', 0)} updated, "
                              f"{result.get('batches_sent', 0)} batches sent")
                return True
            else:
                logger.error(f"Processing failed: {result.get('message', 'Unknown error')}")
                return False
        except Exception as e:
            logger.error(f"Error in run_once: {e}")
            logger.error(traceback.format_exc())
            return False

    def run_continuous(self) -> None:
        """Main loop that runs processing continuously"""
        logger.info("Starting SCHEMA-ONLY OPRO processing service...")
        logger.info(f"Configuration:")
        logger.info(f"  - API URL: {API_BASE_URL}")
        logger.info(f"  - Check interval: {CHECK_INTERVAL} seconds")
        logger.info(f"  - Batch size: {BATCH_SIZE} records")
        logger.info(f"  - DBF file: {OPRO_DBF_PATH}")
        logger.info(f"  - Mode: Schema matching only")
        logger.info("")
        
        while True:
            try:
                # Process files
                success = self.run_once()
                
                # Wait for next check
                logger.info(f"Sleeping for {CHECK_INTERVAL} seconds...")
                time.sleep(CHECK_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("Stopping automatic processing service...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                logger.error(traceback.format_exc())
                logger.info("Continuing...")
                time.sleep(CHECK_INTERVAL)

def main():
    """Main function"""
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        # Run once and exit
        uploader = DBFUploader()
        success = uploader.run_once()
        sys.exit(0 if success else 1)
    else:
        # Run continuously
        uploader = DBFUploader()
        uploader.run_continuous()

if __name__ == '__main__':
    main()