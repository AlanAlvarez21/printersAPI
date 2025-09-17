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
        logging.FileHandler('dbf_single_uploader.log', encoding='utf-8'),
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
MAX_RECORDS_TO_SEND = 10  # Only send first 10 records for testing

# Ruta del archivo opro.dbf (ajusta según tu sistema)
OPRO_DBF_PATH = 'C:\\ALPHAERP\\Empresas\\FLEXIEMP\\opro.dbf'

# File to store last processed state
STATE_FILE = "dbf_single_state.json"

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
            
            # Log sample data for debugging
            no_opro = cleaned_record.get('NO_OPRO', 'N/A')
            cantidad = cleaned_record.get('CANT_LIQ', '0')
            estado = cleaned_record.get('OPROSTAT', 'N/A')
            logger.debug(f"Mapping record - NO_OPRO: {no_opro}, CANTIDAD: {cantidad}, ESTADO: {estado}")
            
            # Extract the fields that match the API schema
            mapped_record = {
                # Required fields from API schema
                "product_id": "9ef77e17-4a1f-435f-99e3-21dbff7c68ee",
                "quantity_requested": max(1, int(float(cleaned_record.get('CANT_LIQ', '0') or '0'))),
                "warehouse_id": "1ac67bd3-d5b1-4bbb-9f33-31d4a71af536",
                
                # Optional fields from schema
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

    def send_single_record_to_api(self, record_data: Dict) -> Dict:
        """Send a single record to the API endpoint with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Sending single record to API (attempt {attempt + 1})")
                logger.info(f"Record data: NO_OPRO={record_data.get('no_opro', 'N/A')}, "
                           f"Quantity={record_data.get('quantity_requested', 'N/A')}")
                
                payload = {
                    "company_name": "Flexiempaques",
                    "production_order": record_data  # Single record, not array
                }
                
                logger.debug(f"Single payload: {json.dumps(payload, indent=2)[:500]}...")
                
                response = self.session.post(
                    API_BASE_URL,  # Single endpoint, not batch
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=API_TIMEOUT
                )
                
                logger.info(f"API Response Status: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        result = response.json()
                        logger.debug(f"API Response Body: {json.dumps(result, indent=2)[:500]}...")
                        logger.info("Single record sent successfully")
                        return {"success": True, "data": result}
                    except Exception as json_error:
                        logger.error(f"Error parsing JSON response: {json_error}")
                        logger.error(f"Response text: {response.text[:500]}...")
                        return {"success": True, "data": {"message": "Success but parsing error"}}
                        
                else:
                    logger.warning(f"API returned {response.status_code}: {response.text}")
                    if attempt < MAX_RETRIES - 1:
                        logger.info(f"Retrying in {2 ** attempt} seconds...")
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
                    
        logger.error("Failed to send single record after all retries")
        return {"success": False, "error": "Failed after retries"}

    def process_opro_file(self) -> Dict:
        """Process opro.dbf file and send records to API one by one"""
        try:
            logger.info("=" * 60)
            logger.info(f"PROCESSING OPRO.DBF - SINGLE RECORD TEST - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
            
            # Process only first few records for testing
            records_sent = 0
            records_processed = 0
            
            logger.info(f"Processing first {MAX_RECORDS_TO_SEND} records for testing...")
            for record in dbf:
                records_processed += 1
                if records_processed > MAX_RECORDS_TO_SEND:
                    break
                    
                record_dict = dict(record)
                
                # Map record to API format
                mapped_record = self.map_opro_record_to_api(record_dict)
                if not mapped_record:
                    continue
                
                # Send single record
                logger.info(f"Sending record #{records_processed}")
                result = self.send_single_record_to_api(mapped_record)
                
                if result.get("success"):
                    records_sent += 1
                    logger.info(f"Record #{records_processed} sent successfully")
                else:
                    logger.error(f"Failed to send record #{records_processed}: {result.get('error', 'Unknown error')}")
                
                # Delay between records
                time.sleep(1)
            
            # Update file info to avoid reprocessing in next cycle
            self.state['file_info'] = file_info
            if self.save_state():
                logger.info("File info updated")
            else:
                logger.error("Failed to update file info")
            
            elapsed_time = time.time() - start_time
            logger.info(f'Processing completed in: {elapsed_time:.2f} seconds')
            logger.info(f'Records processed: {records_processed}, Records sent: {records_sent}')
            
            logger.info("=" * 60)
            logger.info("WAITING FOR NEXT CHECK...")
            logger.info("=" * 60)
            
            return {
                "status": "completed",
                "records_processed": records_processed,
                "records_sent": records_sent,
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
                logger.info(f"Summary: {result.get('records_processed', 0)} records processed, "
                          f"{result.get('records_sent', 0)} records sent")
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
        logger.info("Starting SINGLE RECORD TEST processing service...")
        logger.info("Configuration:")
        logger.info(f"  - API URL: {API_BASE_URL}")
        logger.info(f"  - Check interval: {CHECK_INTERVAL} seconds")
        logger.info(f"  - Max records to send: {MAX_RECORDS_TO_SEND}")
        logger.info(f"  - DBF file: {OPRO_DBF_PATH}")
        logger.info(f"  - Mode: Single record testing")
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