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
        logging.FileHandler('ordprod_inventory_uploader.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('OrdProdInventoryUploader')

# API configuration - Using the correct endpoint from your example
API_BASE_URL = "https://wmsys.fly.dev"  # Local development URL like in your example
INVENTORY_CODES_ENDPOINT = "/api/inventory_codes"
API_TIMEOUT = 30
MAX_RETRIES = 3

# Configuration - Remove the artificial limit
CHECK_INTERVAL = 60
BATCH_SIZE = 50
MAX_RECORDS_TO_PROCESS = float('inf')  # No limit - process all records

# Ruta del archivo ordprod.dbf
ORDPROD_DBF_PATH = 'ordprod.dbf'

# File to store last processed state
STATE_FILE = "ordprod_inventory_state.json"

# Flag to force processing even if file hasn't changed (for testing)
FORCE_PROCESSING = "--force" in sys.argv

class OrdProdInventoryUploader:
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

    def clean_value(self, value: Any) -> str:
        """Clean and convert value to appropriate type"""
        if value is None or str(value).lower() in ['nan', 'none', '', 'null']:
            return ''
        return str(value).strip()

    def map_ordprod_record_to_inventory_code(self, record: Dict) -> Optional[Dict]:
        """Map ordprod.dbf record to inventory code API format"""
        try:
            # Convert all values to strings and clean them
            cleaned_record = {k: self.clean_value(v) for k, v in record.items()}
            
            # Map ordprod fields to inventory code fields based on your example
            mapped_record = {
                "no_ordp": cleaned_record.get('NO_ORDP', ''),
                "cve_copr": cleaned_record.get('CVE_COPR', ''),
                "cve_prod": cleaned_record.get('CVE_PROD', ''),
                "can_copr": float(cleaned_record.get('CAN_COPR', 0) or 0),
                "tip_copr": int(float(cleaned_record.get('TIP_COPR', 1) or 1)),
                "costo": float(cleaned_record.get('COSTO', 0) or 0),
                "fecha": cleaned_record.get('FECH_CTO', datetime.now().strftime('%Y-%m-%d')),
                "cve_suc": cleaned_record.get('CVE_SUC', ''),
                "trans": int(float(cleaned_record.get('TRANS', 0) or 0)),
                "lote": cleaned_record.get('LOTE', ''),
                "new_med": cleaned_record.get('NEW_MED', ''),
                "new_copr": cleaned_record.get('NEW_COPR', ''),
                "costo_rep": float(cleaned_record.get('COSTO_REP', 0) or 0),
                "partresp": int(float(cleaned_record.get('PARTRESP', 0) or 0)),
                "dmov": cleaned_record.get('DMOV', ''),
                "partop": int(float(cleaned_record.get('PARTOP', 0) or 0)),
                "fcdres": float(cleaned_record.get('FCDRES', 0) or 0),
                "undres": cleaned_record.get('UNDRES', ''),
            }
            
            # Remove empty fields to keep payload clean
            mapped_record = {k: v for k, v in mapped_record.items() 
                           if v not in [None, '', 0] or k in ['can_copr', 'costo', 'tip_copr', 'trans', 'partresp', 'partop', 'fcdres']}
            
            return mapped_record
            
        except Exception as e:
            logger.error(f"Error mapping record to inventory code schema: {e}")
            logger.error(f"Record data: {record}")
            return None

    def send_inventory_code_to_api(self, inventory_code_data: Dict) -> Dict:
        """Send a single inventory code to the API endpoint with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Sending inventory code NO_ORDP: {inventory_code_data.get('no_ordp', 'N/A')} to API (attempt {attempt + 1})")
                
                # Wrap the inventory code data in the expected format (as per your example)
                payload = {
                    "inventory_code": inventory_code_data
                }
                
                logger.debug(f"Payload: {json.dumps(payload, indent=2, ensure_ascii=False)[:500]}...")
                
                response = self.session.post(
                    API_BASE_URL + INVENTORY_CODES_ENDPOINT,
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=API_TIMEOUT
                )
                
                logger.info(f"API Response Status: {response.status_code}")
                
                if response.status_code in [200, 201]:
                    try:
                        result = response.json()
                        logger.debug(f"API Response: {json.dumps(result, indent=2, ensure_ascii=False)[:500]}...")
                        logger.info(f"Inventory code sent successfully")
                        return {"success": True, "data": result}
                    except Exception as json_error:
                        logger.error(f"Error parsing JSON response: {json_error}")
                        logger.error(f"Response text: {response.text[:500]}...")
                        return {"success": True, "data": {"message": "Success but parsing error"}}
                elif response.status_code == 409:
                    # Conflict - inventory code already exists
                    logger.warning(f"Inventory code already exists: {inventory_code_data.get('no_ordp', 'N/A')}")
                    return {"success": True, "data": {"message": "Already exists"}}
                else:
                    logger.warning(f"API returned {response.status_code}: {response.text}")
                    if response.status_code == 422:
                        logger.error("VALIDATION ERROR - Check payload format")
                        logger.error(f"Full payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")
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
                    
        logger.error("Failed to send inventory code after all retries")
        return {"success": False, "error": "Failed after retries"}

    def process_ordprod_file(self) -> Dict:
        """Process ordprod.dbf file and send records as inventory codes to API"""
        try:
            logger.info("=" * 60)
            logger.info(f"ORDPROD INVENTORY CODES PROCESSING - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)
            
            start_time = time.time()
            
            # Check if file exists
            if not os.path.exists(ORDPROD_DBF_PATH):
                logger.error(f"File not found: {ORDPROD_DBF_PATH}")
                return {"status": "error", "message": "File not found"}
                
            # Check if file has changed
            file_info = self.get_file_hash(ORDPROD_DBF_PATH)
            if not file_info:
                logger.error(f"Could not get file info for {ORDPROD_DBF_PATH}")
                return {"status": "error", "message": "Could not get file info"}
                
            prev_file_info = self.state.get('file_info', {})
            logger.info(f"Current file info: mtime={file_info['mtime']}, size={file_info['size']}")
            logger.info(f"Previous file info: mtime={prev_file_info.get('mtime', 'None')}, size={prev_file_info.get('size', 'None')}")
            
            # If file hasn't changed, skip processing (unless force processing is enabled)
            if (not FORCE_PROCESSING and 
                file_info['mtime'] == prev_file_info.get('mtime', 0) and 
                file_info['size'] == prev_file_info.get('size', 0)):
                logger.info("No changes in ordprod.dbf file")
                logger.info("=" * 60)
                logger.info("WAITING FOR NEXT CHECK...")
                logger.info("=" * 60)
                return {"status": "completed", "changes_found": False, "records_processed": 0}
                
            if FORCE_PROCESSING:
                logger.info("Force processing enabled - processing file regardless of changes...")
            else:
                logger.info("File has been modified - processing...")
            
            # Open the DBF file
            logger.info(f"Opening DBF file: {ORDPROD_DBF_PATH}")
            dbf = DBF(ORDPROD_DBF_PATH, ignore_missing_memofile=True)
            
            # Process ALL records (no artificial limit)
            record_count = 0
            successful_sends = 0
            total_sent = 0
            
            logger.info(f"Processing ALL records from {ORDPROD_DBF_PATH}...")
            logger.info(f"Total records in file: {len(dbf)}")
            
            for record in dbf:
                record_count += 1
                
                record_dict = dict(record)
                
                # Map record to inventory code format
                mapped_record = self.map_ordprod_record_to_inventory_code(record_dict)
                if mapped_record:
                    # Send each record individually
                    result = self.send_inventory_code_to_api(mapped_record)
                    total_sent += 1
                    
                    if result.get("success"):
                        successful_sends += 1
                        logger.info(f"Record {record_count} sent successfully")
                    else:
                        logger.error(f"Record {record_count} failed: {result.get('error', 'Unknown error')}")
                else:
                    logger.warning(f"Record {record_count} could not be mapped")
                
                # Show progress every 50 records
                if record_count % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = record_count / elapsed if elapsed > 0 else 0
                    logger.info(f"Processed {record_count} records ({rate:.1f} records/sec)...")
            
            logger.info(f"Overall result: {successful_sends}/{total_sent} records sent successfully")
            
            # Update state
            self.state['file_info'] = file_info
            if self.save_state():
                logger.info("State saved")
            else:
                logger.error("Failed to save state")
            
            elapsed_time = time.time() - start_time
            logger.info(f'Processing completed in: {elapsed_time:.2f} seconds')
            
            logger.info("=" * 60)
            logger.info("WAITING FOR NEXT CHECK...")
            logger.info("=" * 60)
            
            return {
                "status": "completed",
                "records_processed": record_count,
                "successful_sends": successful_sends,
                "total_sent": total_sent,
                "processing_time": elapsed_time
            }
            
        except Exception as e:
            logger.error(f"Critical error in process_ordprod_file: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": str(e)}

    def run_once(self) -> bool:
        """Run processing once and return success status"""
        try:
            result = self.process_ordprod_file()
            if result.get("status") == "completed":
                logger.info(f"Summary: {result.get('records_processed', 0)} records processed")
                if 'successful_sends' in result:
                    logger.info(f"Successfully sent: {result.get('successful_sends', 0)}/{result.get('total_sent', 0)} records")
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
        logger.info("Starting ORDPROD INVENTORY CODES processing service...")
        logger.info("Configuration:")
        logger.info(f"  - API URL: {API_BASE_URL}{INVENTORY_CODES_ENDPOINT}")
        logger.info(f"  - Check interval: {CHECK_INTERVAL} seconds")
        logger.info(f"  - DBF file: {ORDPROD_DBF_PATH}")
        logger.info(f"  - Processing ALL records (no limit)")
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
        uploader = OrdProdInventoryUploader()
        success = uploader.run_once()
        sys.exit(0 if success else 1)
    else:
        uploader = OrdProdInventoryUploader()
        uploader.run_continuous()

if __name__ == '__main__':
    main()