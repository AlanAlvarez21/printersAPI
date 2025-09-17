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
        logging.FileHandler('dbf_final_uploader.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('DBFUploader')

# API configuration - UPDATED URL
API_BASE_URL = "https://wmsys.fly.dev"
API_ENDPOINT = "/api/production_orders"
API_TIMEOUT = 30
MAX_RETRIES = 3

# Configuration
CHECK_INTERVAL = 60
BATCH_SIZE = 50
MAX_RECORDS_TO_PROCESS = 100  # Limit for testing

# Ruta del archivo opro.dbf
OPRO_DBF_PATH = 'C:\\\\ALPHAERP\\\\Empresas\\\\FLEXIEMP\\\\opro.dbf'

# File to store last processed state
STATE_FILE = "final_dbf_state.json"

class DBFUploader:
    def __init__(self):
        self.state = self.load_state()
        self.session = requests.Session()
        
    def load_state(self) -> Dict:
        """Load the last processed state from file"""
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    logger.info(f"Loaded state with {len(state)} entries")
                    return state
        except Exception as e:
            logger.warning(f"Could not load state file: {e}")
        return {}

    def save_state(self) -> bool:
        """Save the current state to file"""
        try:
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
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
            record_str = json.dumps(record, sort_keys=True, default=str, ensure_ascii=False)
            return hashlib.md5(record_str.encode('utf-8')).hexdigest()
        except Exception as e:
            logger.error(f"Error creating record hash: {e}")
            return None

    def clean_value(self, value: Any) -> str:
        """Clean and convert value to appropriate type"""
        if value is None or str(value).lower() in ['nan', 'none', '']:
            return ''
        return str(value).strip()

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
        """Map opro.dbf record to exact API schema format"""
        try:
            # Convert all values to strings and clean them
            cleaned_record = {k: self.clean_value(v) for k, v in record.items()}
            
            # Extract the fields that match the API schema
            mapped_record = {
                "product_id": "9ef77e17-4a1f-435f-99e3-21dbff7c68ee",
                "quantity_requested": max(1, int(float(cleaned_record.get('CANT_LIQ', '0') or '0'))),
                "warehouse_id": "1ac67bd3-d5b1-4bbb-9f33-31d4a71af536",
                "no_opro": cleaned_record.get('NO_OPRO', ''),
                "priority": self.determine_priority(cleaned_record),
                "notes": cleaned_record.get('OBSERVAT', ''),
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
                
                # Log first record for debugging
                if batch_data:
                    first_record = batch_data[0]
                    logger.info(f"First record - NO_OPRO: {first_record.get('no_opro', 'N/A')}, "
                               f"Quantity: {first_record.get('quantity_requested', 'N/A')}")
                
                response = self.session.post(
                    API_BASE_URL + API_ENDPOINT + "/batch",
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=API_TIMEOUT
                )
                
                logger.info(f"API Response Status: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        result = response.json()
                        logger.debug(f"API Response: {json.dumps(result, indent=2)[:500]}...")
                        
                        # Try to get success count from different possible formats
                        success_count = 0
                        total_count = len(batch_data)
                        
                        if 'results' in result:
                            success_count = sum(1 for r in result['results'] if r.get('status') == 'success')
                            logger.info(f"API processed batch: {success_count}/{total_count} records successful")
                        elif 'success_count' in result:
                            success_count = result.get('success_count', 0)
                            logger.info(f"API processed batch: {success_count}/{total_count} records successful")
                        else:
                            # If we got 200 OK, assume all were processed
                            success_count = total_count
                            logger.info(f"API processed batch: {success_count}/{total_count} records successful (assumed)")
                        
                        return {"success": True, "data": result, "success_count": success_count, "total_count": total_count}
                        
                    except Exception as json_error:
                        logger.error(f"Error parsing JSON response: {json_error}")
                        logger.error(f"Response text: {response.text[:500]}...")
                        return {"success": True, "data": {"message": "Success but parsing error"}, "success_count": len(batch_data), "total_count": len(batch_data)}
                        
                else:
                    logger.warning(f"API returned {response.status_code}: {response.text}")
                    if response.status_code == 422:
                        logger.error("Validation error - check payload format")
                        logger.error(f"Payload sample: {json.dumps(payload, indent=2)[:500]}...")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)
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
                logger.error(traceback.format_exc())
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
                    
        logger.error("Failed to send batch after all retries")
        return {"success": False, "error": "Failed after retries"}

    def process_opro_file(self) -> Dict:
        """Process opro.dbf file and send records to API"""
        try:
            logger.info("=" * 60)
            logger.info(f"FINAL DBF PROCESSING - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
                
            logger.info("File has been modified - processing...")
            
            # Open the DBF file
            logger.info(f"Opening DBF file: {OPRO_DBF_PATH}")
            dbf = DBF(OPRO_DBF_PATH, ignore_missing_memofile=True)
            
            # Process records with limit for testing
            all_records = []
            record_count = 0
            
            logger.info(f"Processing up to {MAX_RECORDS_TO_PROCESS} records...")
            for record in dbf:
                record_count += 1
                if record_count > MAX_RECORDS_TO_PROCESS:
                    logger.info(f"Reached limit of {MAX_RECORDS_TO_PROCESS} records, stopping...")
                    break
                    
                record_dict = dict(record)
                
                # Map record to API format
                mapped_record = self.map_opro_record_to_api(record_dict)
                if mapped_record:
                    all_records.append(mapped_record)
                
                # Show progress
                if record_count % 50 == 0:
                    logger.info(f"Processed {record_count} records...")
            
            logger.info(f"Prepared {len(all_records)} records for sending")
            
            # Send records in batches
            if all_records:
                total_batches = min(((len(all_records)-1) // BATCH_SIZE) + 1, 2)  # Limit to 2 batches for testing
                logger.info(f"Sending {len(all_records)} records in {total_batches} batches...")
                
                successful_sends = 0
                total_sent = 0
                
                for i in range(0, len(all_records), BATCH_SIZE):
                    if i >= BATCH_SIZE * 2:  # Limit to 2 batches for testing
                        break
                        
                    batch = all_records[i:i + BATCH_SIZE]
                    batch_index = i // BATCH_SIZE + 1
                    logger.info(f"Sending batch {batch_index}/{total_batches} ({len(batch)} records)")
                    
                    batch_result = self.send_batch_to_api(batch)
                    
                    if batch_result.get("success"):
                        successful_sends += batch_result.get("success_count", 0)
                        total_sent += batch_result.get("total_count", len(batch))
                        logger.info(f"Batch {batch_index} completed: {batch_result.get('success_count', 0)}/{batch_result.get('total_count', len(batch))} successful")
                    else:
                        logger.error(f"Batch {batch_index} failed: {batch_result.get('error', 'Unknown error')}")
                    
                    # Delay between batches
                    time.sleep(1)
                
                logger.info(f"Overall result: {successful_sends}/{total_sent} records sent successfully")
                
                # Update state
                self.state['file_info'] = file_info
                if self.save_state():
                    logger.info("State saved")
                else:
                    logger.error("Failed to save state")
            else:
                logger.info("No valid records to send")
                # Still update file info
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
                "records_processed": len(all_records),
                "processing_time": elapsed_time
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
                logger.info(f"Summary: {result.get('records_processed', 0)} records processed")
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
        logger.info("Starting FINAL DBF processing service...")
        logger.info("Configuration:")
        logger.info(f"  - API URL: {API_BASE_URL}{API_ENDPOINT}")
        logger.info(f"  - Check interval: {CHECK_INTERVAL} seconds")
        logger.info(f"  - Batch size: {BATCH_SIZE} records")
        logger.info(f"  - Max records to process: {MAX_RECORDS_TO_PROCESS}")
        logger.info(f"  - DBF file: {OPRO_DBF_PATH}")
        logger.info("")
        
        while True:
            try:
                success = self.run_once()
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
        uploader = DBFUploader()
        success = uploader.run_once()
        sys.exit(0 if success else 1)
    else:
        uploader = DBFUploader()
        uploader.run_continuous()

if __name__ == '__main__':
    main()