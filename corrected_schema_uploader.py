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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('corrected_schema_uploader.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('CorrectedSchemaUploader')

# API configuration
# API_BASE_URL = "https://wmsys.fly.dev"  # Production URL
API_BASE_URL = "http://localhost:3000"  # Local development URL
API_ENDPOINT = "/api/production_orders/batch"
API_TIMEOUT = 90
MAX_RETRIES = 3

# Configuration
BATCH_SIZE = 25
DBF_PATH = './opro.dbf'
STATE_FILE = "dbf_state_corrected.json"

class CorrectedSchemaUploader:
    def __init__(self):
        self.session = requests.Session()
        self.state = self.load_state()
        
    def load_state(self) -> Dict:
        """Load the last processed state from file"""
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load state file: {e}")
        return {}

    def save_state(self) -> bool:
        """Save the current state to file"""
        try:
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            logger.error(f"Error saving state: {e}")
            return False

    def clean_value(self, value: Any) -> str:
        """Clean and convert value to appropriate type"""
        if value is None or str(value).lower() in ['nan', 'none', '']:
            return ''
        return str(value).strip()

    def extract_quantity(self, record: Dict) -> int:
        """Extract meaningful quantity from various fields"""
        try:
            # Try different quantity fields in order of preference
            ren_opro = self.clean_value(record.get('REN_OPRO', '0'))
            carga_opro = self.clean_value(record.get('CARGA_OPRO', '0'))
            cant_liq = self.clean_value(record.get('CANT_LIQ', '0'))
            
            # Use the first valid non-zero value
            for value in [ren_opro, carga_opro, cant_liq]:
                if value and value.lower() not in ['nan', 'none', '', '0']:
                    try:
                        qty = float(value)
                        if qty > 0:
                            return max(1, int(qty))
                    except:
                        continue
                        
            # Default quantity if nothing found
            return 1000
        except:
            return 1000

    def extract_year(self, record: Dict) -> str:
        """Extract year from date field"""
        try:
            # Try different date fields
            fec_opro = self.clean_value(record.get('FEC_OPRO', ''))
            ano = self.clean_value(record.get('ANO', ''))
            
            # Try FEC_OPRO first
            if fec_opro:
                # Handle different date formats
                if '-' in fec_opro:
                    return fec_opro.split('-')[0]  # YYYY-MM-DD format
                elif '/' in fec_opro:
                    parts = fec_opro.split('/')
                    if len(parts) == 3:
                        # Assuming MM/DD/YYYY or DD/MM/YYYY, take the year part
                        return parts[2] if len(parts[2]) == 4 else ''
                elif len(fec_opro) >= 4:
                    # Direct year format
                    if fec_opro[:4].isdigit():
                        return fec_opro[:4]
            
            # Try ANO field
            if ano and ano.isdigit():
                return ano
                
            # Default to current year
            return str(datetime.now().year)
        except:
            return str(datetime.now().year)

    def map_record_to_api(self, record: Dict) -> Optional[Dict]:
        """Map DBF record to API format with CORRECT field mapping"""
        try:
            # Clean all values
            cleaned = {k: self.clean_value(v) for k, v in record.items()}
            
            # Extract year
            year = self.extract_year(cleaned)
            
            # Extract quantity
            quantity = self.extract_quantity(cleaned)
            
            # Get product key - this is the main identifier
            product_key = cleaned.get('CVE_PROP', '')
            
            # Validate required fields
            no_opro = cleaned.get('NO_OPRO', '')
            if not no_opro:
                logger.warning("Skipping record: NO_OPRO is empty")
                return None
                
            # Validate product key
            if not product_key:
                logger.warning(f"Record with NO_OPRO {no_opro} has empty CVE_PROP")
                # Still process it, but log the issue
                
            # CORRECT mapping based on your requirements:
            # Only include fields that are permitted by the API controller
            mapped = {
                # product_key is the external product identifier
                "product_key": product_key,
                
                # quantity from liquidated quantity
                "quantity_requested": quantity,
                
                # warehouse_id (use a valid warehouse ID)
                "warehouse_id": "45c4bbc8-2950-434c-b710-2ae0e080bfd1",  # Warehouse for Flexiempaques
                
                # priority based on status
                "priority": "medium",  # Default, can be adjusted
                
                # NO_OPRO (numero de orden de produccion)
                "no_opro": no_opro,
                
                # NOTES should ONLY contain OBSERVA data
                "notes": cleaned.get('OBSERVA', ''),
                
                # LOTE (lote del producto)
                "lote_referencia": cleaned.get('LOTE', ''),
                
                # Year field
                "ano": year,  # Using 'ano' instead of 'year' to match model field
                
                # Other fields that are permitted by the API
                "stat_opro": cleaned.get('STAT_OPRO', ''),
                # Note: We're not including 'referencia' as it's not a valid column in the model
                # Note: We're not including 'status' as it should be set by the controller to a default value
            }
            
            # Remove empty fields to keep payload clean, but keep 'notes' field even if empty
            mapped = {k: v for k, v in mapped.items() if v not in [None, 0] or k == 'notes'}
            
            # Log mapping for verification
            logger.debug(f"Mapped record - NO_OPRO: {mapped.get('no_opro')}, "
                        f"Product: {mapped.get('product_key')}, "
                        f"Quantity: {mapped.get('quantity_requested')}, "
                        f"Year: {mapped.get('ano')}, "
                        f"Notes: '{mapped.get('notes', '')}'")
            
            # Log the final mapped dict for debugging
            logger.debug(f"Final mapped dict: {mapped}")
            
            return mapped
            
        except Exception as e:
            logger.error(f"Error mapping record: {e}")
            return None

    def send_batch_to_api(self, batch_data: List[Dict]) -> Dict:
        """Send a batch of records to the API endpoint"""
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Sending batch of {len(batch_data)} records to API")
                
                # Log the first record for debugging
                if batch_data:
                    logger.debug(f"First record sample: {batch_data[0]}")
                
                payload = {
                    "company_name": "Flexiempaques",
                    "production_orders": batch_data
                }
                
                logger.debug(f"Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")
                
                # Log specifically the notes values in the payload
                for i, order in enumerate(payload.get('production_orders', [])):
                    if 'notes' in order:
                        logger.debug(f"Order {i} notes: '{order['notes']}'")
                    if 'status' in order:
                        logger.debug(f"Order {i} status: '{order['status']}'")
                
                # Remove any 'status' fields that are empty before sending
                for order in payload.get('production_orders', []):
                    if 'status' in order and not order['status']:
                        del order['status']
                        logger.debug(f"Removed empty status field from order {order.get('no_opro', 'unknown')}")
                
                response = self.session.post(
                    API_BASE_URL + API_ENDPOINT,
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=API_TIMEOUT
                )
                
                logger.info(f"API Response Status: {response.status_code}")
                
                # Log response content for debugging
                try:
                    response_content = response.json()
                    logger.debug(f"API Response Content: {json.dumps(response_content, indent=2)}")
                except:
                    logger.debug(f"API Response Text: {response.text}")
                
                if response.status_code == 200:
                    try:
                        result = response.json()
                        success_count = result.get('success_count', 0)
                        total_count = result.get('total_count', len(batch_data))
                        logger.info(f"API processed batch: {success_count}/{total_count} records successful")
                        
                        # Log individual results
                        for i, res in enumerate(result.get('results', [])):
                            if res.get('status') == 'error':
                                logger.warning(f"Record {i} failed: {res.get('errors', 'Unknown error')}")
                        
                        return {"success": True, "data": result}
                    except Exception as e:
                        logger.info(f"Batch sent successfully but error parsing response: {e}")
                        return {"success": True, "data": {}}
                else:
                    logger.warning(f"API error {response.status_code}: {response.text}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)
                        
            except Exception as e:
                logger.error(f"Error sending batch (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    
        return {"success": False, "error": "Failed after retries"}

    def process_dbf_file(self) -> bool:
        """Process DBF file with CORRECT schema mapping"""
        try:
            logger.info("=" * 60)
            logger.info("PROCESSING DBF WITH CORRECT SCHEMA MAPPING")
            logger.info("=" * 60)
            
            # Check if file exists
            if not os.path.exists(DBF_PATH):
                logger.error(f"File not found: {DBF_PATH}")
                return False
                
            # Open DBF file with memo support
            logger.info(f"Opening DBF file: {DBF_PATH}")
            dbf = DBF(DBF_PATH, ignore_missing_memofile=False)
            
            # Process records
            all_records = []
            processed_count = 0
            for record in dbf:
                record_dict = dict(record)
                mapped_record = self.map_record_to_api(record_dict)
                if mapped_record:
                    all_records.append(mapped_record)
                    processed_count += 1
                    
                    # Log progress every 100 records
                    if processed_count % 100 == 0:
                        logger.info(f"Processed {processed_count} records so far...")
            
            logger.info(f"Prepared {len(all_records)} valid records for sending out of {processed_count} total")
            
            if not all_records:
                logger.warning("No valid records found to send")
                return True
            
            # Send in batches
            successful_sends = 0
            total_records = len(all_records)
            
            for i in range(0, len(all_records), BATCH_SIZE):
                batch = all_records[i:i + BATCH_SIZE]
                logger.info(f"Processing batch {i//BATCH_SIZE + 1} ({len(batch)} records)")
                batch_result = self.send_batch_to_api(batch)
                if batch_result.get("success"):
                    result_data = batch_result.get("data", {})
                    success_count = result_data.get('success_count', len(batch))
                    successful_sends += success_count
                    logger.info(f"Batch {i//BATCH_SIZE + 1} sent: {success_count} records successful")
                else:
                    logger.error(f"Batch {i//BATCH_SIZE + 1} failed: {batch_result.get('error')}")
            
            logger.info(f"Total records sent: {successful_sends}/{total_records}")
            
            # Save state
            self.save_state()
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing DBF file: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

def main():
    """Main function"""
    logger.info("Starting CORRECTED SCHEMA DBF Uploader")
    
    uploader = CorrectedSchemaUploader()
    success = uploader.process_dbf_file()
    
    if success:
        logger.info("Upload completed successfully!")
    else:
        logger.error("Upload failed!")
    
    return success

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)