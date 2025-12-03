import sys
import os
import json
import logging
from datetime import datetime
from dbfread import DBF
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QTextEdit, QLabel, 
                             QProgressBar, QGroupBox, QFormLayout, QFileDialog,
                             QMessageBox)
from PyQt5.QtCore import QThread, pyqtSignal, QTimer, Qt
from PyQt5.QtGui import QFont, QPalette
import requests
import time
import hashlib
from typing import Dict, List, Optional, Any

# Configure logging
log_filename = 'gui_dbf_uploader.log'
log_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), log_filename)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filepath, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('GUI_DBF_Uploader')

# API configuration
API_BASE_URL = "https://wmsys.fly.dev"  # Production URL
API_ENDPOINT = "/api/production_orders/batch"
API_TIMEOUT = 90
MAX_RETRIES = 3

# Local record count file
RECORD_COUNT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "record_count.json")

# Configuration for file paths
BATCH_SIZE = 25

# For PyInstaller compatibility, we need to handle the _MEIPASS path
if getattr(sys, 'frozen', False):
    # Running as compiled executable
    application_path = sys._MEIPASS
    # For Windows executable, look for opro.dbf in the AlphaERP directory
    DBF_PATH = r"C:\ALPHAERP\Empresas\FLEXIEMP\opro.dbf"
else:
    # Running as script
    application_path = os.path.dirname(os.path.abspath(__file__))
    DBF_PATH = os.path.join(application_path, 'opro.dbf')

class DBFUploaderWorker(QThread):
    """Worker thread for DBF uploading to keep the GUI responsive"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    status_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(bool)

    def __init__(self):
        super().__init__()
        self.should_stop = False
        # Initialize previous record count
        self.previous_record_count = self.load_previous_record_count()

    def log_message(self, message):
        """Log a message and emit signal to update GUI"""
        logger.info(message)
        self.log_signal.emit(message)

    def load_previous_record_count(self) -> int:
        """Load the previous record count from the local JSON file"""
        try:
            if os.path.exists(RECORD_COUNT_FILE):
                with open(RECORD_COUNT_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    count = data.get('previous_record_count', 0)
                    self.log_message(f"Loaded previous record count: {count}")
                    return count
        except Exception as e:
            self.log_message(f"Could not load previous record count file: {e}")
        return 0

    def save_previous_record_count(self, count: int) -> bool:
        """Save the current record count to the local JSON file"""
        try:
            with open(RECORD_COUNT_FILE, 'w', encoding='utf-8') as f:
                json.dump({'previous_record_count': count}, f, indent=2, ensure_ascii=False)
            self.log_message(f"Saved new record count: {count}")
            return True
        except Exception as e:
            self.log_message(f"Error saving previous record count: {e}")
            return False

    def get_dbf_record_count(self) -> int:
        """Get the count of records in the DBF file"""
        try:
            if not os.path.exists(DBF_PATH):
                self.log_message(f"DBF file not found: {DBF_PATH}")
                return -1
                
            dbf = DBF(DBF_PATH, ignore_missing_memofile=False)
            record_count = len(list(dbf))
            self.log_message(f"DBF file has {record_count} records")
            return record_count
        except Exception as e:
            self.log_message(f"Failed to get record count from DBF file: {e}")
            return -1

    def records_changed(self) -> bool:
        """Check if the number of records in the DBF file differs from the previous count"""
        try:
            current_dbf_count = self.get_dbf_record_count()
            if current_dbf_count == -1:
                self.log_message("Could not get DBF record count, assuming changes exist")
                return True
            
            self.log_message(f"Comparing records: DBF has {current_dbf_count}, previously had {self.previous_record_count}")
            
            # If there are more records in the DBF than previously, there are new records to upload
            if current_dbf_count > self.previous_record_count:
                self.log_message(f"New records detected: {current_dbf_count - self.previous_record_count} new records to process")
                return True
            elif current_dbf_count < self.previous_record_count:
                self.log_message(f"DBF has fewer records than before: {self.previous_record_count - current_dbf_count} difference - this may indicate an issue")
                return True
            else:
                self.log_message("No new records detected")
                return False
        except Exception as e:
            self.log_message(f"Error checking if records changed: {e}")
            return True  # Assume changes exist to be safe

    def clean_value(self, value: Any) -> str:
        """Clean and convert value to appropriate type"""
        if value is None or str(value).lower() in ['nan', 'none', '']:
            return ''
        # Handle date objects by converting them to strings
        if hasattr(value, 'strftime'):  # This will catch date, datetime, etc.
            return value.isoformat()
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
                self.log_message("Skipping record: NO_OPRO is empty")
                return None
                
            # Validate product key
            if not product_key:
                self.log_message(f"Record with NO_OPRO {no_opro} has empty CVE_PROP")
                # Still process it, but log the issue
                
            # CORRECT mapping based on your requirements:
            # Only include fields that are permitted by the API controller
            mapped = {
                # product_key is the external product identifier
                "product_key": product_key,
                
                # quantity from liquidated quantity
                "quantity_requested": quantity,
                
                # warehouse_id (use a valid warehouse ID)
                # "warehouse_id": "45c4bbc8-2950-434c-b710-2ae0e080bfd1",  # local
                "warehouse_id": "1ac67bd3-d5b1-4bbb-9f33-31d4a71af536",  # Warehouse for Flexiempaques
                
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
            self.log_message(f"Mapped record - NO_OPRO: {mapped.get('no_opro')}, "
                           f"Product: {mapped.get('product_key')}, "
                           f"Quantity: {mapped.get('quantity_requested')}, "
                           f"Year: {mapped.get('ano')}")
            
            return mapped
            
        except Exception as e:
            self.log_message(f"Error mapping record: {e}")
            return None

    def send_batch_to_api(self, batch_data: List[Dict]) -> Dict:
        """Send a batch of records to the API endpoint"""
        for attempt in range(MAX_RETRIES):
            try:
                self.log_message(f"Sending batch of {len(batch_data)} records to API")
                
                payload = {
                    "company_name": "Flexiempaques",
                    "production_orders": batch_data
                }
                
                # Remove any 'status' fields that are empty before sending
                for order in payload.get('production_orders', []):
                    if 'status' in order and not order['status']:
                        del order['status']
                
                response = requests.post(
                    API_BASE_URL + API_ENDPOINT,
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=API_TIMEOUT
                )
                
                self.log_message(f"API Response Status: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        result = response.json()
                        success_count = result.get('success_count', 0)
                        total_count = result.get('total_count', len(batch_data))
                        self.log_message(f"API processed batch: {success_count}/{total_count} records successful")
                        
                        return {"success": True, "data": result}
                    except Exception as e:
                        self.log_message(f"Batch sent successfully but error parsing response: {e}")
                        return {"success": True, "data": {}}
                else:
                    self.log_message(f"API error {response.status_code}: {response.text}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)
                        
            except Exception as e:
                self.log_message(f"Error sending batch (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    
        return {"success": False, "error": "Failed after retries"}

    def run(self):
        """Main function that runs in the worker thread"""
        try:
            self.status_signal.emit("Checking for changes...")
            self.log_message("=" * 60)
            self.log_message("PROCESSING DBF WITH GUI DBF UPLOADER")
            self.log_message("=" * 60)
            
            # Check if file exists
            if not os.path.exists(DBF_PATH):
                self.log_message(f"File not found: {DBF_PATH}")
                self.finished_signal.emit(False)
                return
            
            # Check if there are new records by comparing DBF and previous counts
            if not self.records_changed():
                self.log_message("No new records detected, skipping upload")
                self.status_signal.emit("No new records detected")
                self.finished_signal.emit(True)
                return

            # Open DBF file with memo support
            self.log_message(f"Opening DBF file: {DBF_PATH}")
            dbf = DBF(DBF_PATH, ignore_missing_memofile=False)
            
            # Process records
            all_records = []
            processed_count = 0
            
            for record in dbf:
                if self.should_stop:
                    self.log_message("Upload process was stopped by user")
                    self.finished_signal.emit(False)
                    return
                
                record_dict = dict(record)
                no_opro = self.clean_value(record_dict.get('NO_OPRO', ''))
                
                # Skip records without NO_OPRO
                if not no_opro:
                    continue
                
                # Process all records since there are new ones
                mapped_record = self.map_record_to_api(record_dict)
                if mapped_record:
                    all_records.append(mapped_record)
                    processed_count += 1
                    
                    # Log progress every 100 records
                    if processed_count % 100 == 0:
                        self.log_message(f"Processed {processed_count} records so far...")
            
            self.log_message(f"Prepared {len(all_records)} records for sending")
            
            # Debug information
            self.log_message(f"Total records in DBF: {len(dbf)}")
            self.log_message(f"Records to send: {len(all_records)}")
            
            if not all_records:
                self.log_message("No valid records to send")
                self.status_signal.emit("No valid records to send")
                self.finished_signal.emit(True)
                return
            
            # Send in batches
            successful_sends = 0
            total_records = len(all_records)
            
            self.progress_signal.emit(0)  # Initialize progress
            
            for i in range(0, len(all_records), BATCH_SIZE):
                if self.should_stop:
                    self.log_message("Upload process was stopped by user")
                    self.finished_signal.emit(False)
                    return
                
                batch = all_records[i:i + BATCH_SIZE]
                self.status_signal.emit(f"Processing batch {i//BATCH_SIZE + 1} ({len(batch)} records)")
                batch_result = self.send_batch_to_api(batch)
                if batch_result.get("success"):
                    result_data = batch_result.get("data", {})
                    success_count = result_data.get('success_count', len(batch))
                    successful_sends += success_count
                    self.log_message(f"Batch {i//BATCH_SIZE + 1} sent: {success_count} records successful")
                else:
                    self.log_message(f"Batch {i//BATCH_SIZE + 1} failed: {batch_result.get('error')}")
                
                # Update progress bar
                progress = int(((i // BATCH_SIZE + 1) / (len(all_records) // BATCH_SIZE + 1)) * 100)
                self.progress_signal.emit(min(progress, 100))
            
            self.log_message(f"Total records sent: {successful_sends}/{total_records}")
            
            # Update and save the new record count
            current_dbf_count = self.get_dbf_record_count()
            if current_dbf_count != -1:
                self.previous_record_count = current_dbf_count
                self.save_previous_record_count(current_dbf_count)
            
            self.status_signal.emit("Upload completed successfully!")
            self.progress_signal.emit(100)
            self.finished_signal.emit(True)
            
        except Exception as e:
            self.log_message(f"Error processing DBF file: {e}")
            self.status_signal.emit(f"Error: {str(e)}")
            self.finished_signal.emit(False)

    def stop(self):
        """Set flag to stop the upload process"""
        self.should_stop = True


class DBFUploaderGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DBF Uploader with Local Record Count Tracking")
        self.setGeometry(100, 100, 800, 600)
        
        # Initialize worker
        self.worker = None
        self.thread = None
        
        # Create central widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        self.setup_ui(main_layout)
        
        # Set up timer for auto-refresh
        self.auto_refresh_timer = QTimer()
        self.auto_refresh_timer.timeout.connect(self.check_for_changes)
        
        # Initial status check
        self.update_status_labels()

    def setup_ui(self, main_layout):
        """Setup the user interface"""
        # Title
        title_label = QLabel("DBF Uploader with Local Record Count Tracking")
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(title_label)
        
        # Status group
        status_group = QGroupBox("Status")
        status_layout = QFormLayout()
        
        self.current_count_label = QLabel("Current DBF Records: Checking...")
        self.previous_count_label = QLabel("Previous DBF Records: Checking...")
        self.file_path_label = QLabel(f"DBF File: {DBF_PATH}")
        self.status_label = QLabel("Ready")
        
        status_layout.addRow("Current Records:", self.current_count_label)
        status_layout.addRow("Previous Records:", self.previous_count_label)
        status_layout.addRow("File Path:", self.file_path_label)
        status_layout.addRow("Status:", self.status_label)
        
        status_group.setLayout(status_layout)
        main_layout.addWidget(status_group)
        
        # Control buttons
        button_layout = QHBoxLayout()
        
        self.upload_button = QPushButton("Upload Now")
        self.upload_button.clicked.connect(self.start_upload)
        button_layout.addWidget(self.upload_button)
        
        self.check_button = QPushButton("Check for Changes")
        self.check_button.clicked.connect(self.check_for_changes)
        button_layout.addWidget(self.check_button)
        
        self.refresh_button = QPushButton("Refresh Status")
        self.refresh_button.clicked.connect(self.update_status_labels)
        button_layout.addWidget(self.refresh_button)
        
        self.auto_refresh_button = QPushButton("Auto-Refresh ON")
        self.auto_refresh_button.setCheckable(True)
        self.auto_refresh_button.clicked.connect(self.toggle_auto_refresh)
        button_layout.addWidget(self.auto_refresh_button)
        
        main_layout.addLayout(button_layout)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        main_layout.addWidget(self.progress_bar)
        
        # Log output
        log_group = QGroupBox("Log Output")
        log_layout = QVBoxLayout()
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        log_layout.addWidget(self.log_text)
        
        log_group.setLayout(log_layout)
        main_layout.addWidget(log_group)
        
        # Stop button (initially hidden)
        self.stop_button = QPushButton("Stop Upload")
        self.stop_button.clicked.connect(self.stop_upload)
        self.stop_button.setEnabled(False)
        main_layout.addWidget(self.stop_button)
        
        # Add stretch to push everything up
        main_layout.addStretch()

    def update_status_labels(self):
        """Update the status labels with current information"""
        try:
            # Get current DBF record count
            if os.path.exists(DBF_PATH):
                dbf = DBF(DBF_PATH, ignore_missing_memofile=False)
                current_count = len(list(dbf))
                self.current_count_label.setText(f"Current DBF Records: {current_count}")
            else:
                self.current_count_label.setText(f"Current DBF Records: File not found - {DBF_PATH}")
            
            # Get previous record count from JSON file
            try:
                if os.path.exists(RECORD_COUNT_FILE):
                    with open(RECORD_COUNT_FILE, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        prev_count = data.get('previous_record_count', 0)
                        self.previous_count_label.setText(f"Previous DBF Records: {prev_count}")
                else:
                    self.previous_count_label.setText("Previous DBF Records: 0 (first run)")
            except Exception as e:
                self.previous_count_label.setText(f"Previous DBF Records: Error reading - {e}")
        except Exception as e:
            self.current_count_label.setText(f"Current DBF Records: Error - {e}")

    def check_for_changes(self):
        """Check if there are new records without uploading"""
        try:
            # Create a temporary worker to check for changes
            temp_worker = DBFUploaderWorker()
            has_changes = temp_worker.records_changed()
            
            if has_changes:
                self.status_label.setText("Changes detected - Ready to upload")
                self.log_message("Changes detected in DBF file")
            else:
                self.status_label.setText("No changes detected")
                self.log_message("No changes detected in DBF file")
                
        except Exception as e:
            self.status_label.setText(f"Error checking for changes: {str(e)}")
            self.log_message(f"Error checking for changes: {str(e)}")

    def start_upload(self):
        """Start the upload process in a separate thread"""
        if self.worker is not None and self.thread is not None:
            # If there's an ongoing upload, stop it first
            self.stop_upload()
        
        # Disable buttons during upload
        self.upload_button.setEnabled(False)
        self.check_button.setEnabled(False)
        self.refresh_button.setEnabled(False)
        
        # Show stop button
        self.stop_button.setEnabled(True)
        
        # Create worker and thread
        self.worker = DBFUploaderWorker()
        self.thread = QThread()
        
        # Move worker to thread
        self.worker.moveToThread(self.thread)
        
        # Connect signals
        self.worker.log_signal.connect(self.log_message)
        self.worker.progress_signal.connect(self.progress_bar.setValue)
        self.worker.status_signal.connect(self.status_label.setText)
        self.worker.finished_signal.connect(self.upload_finished)
        
        # Start the thread
        self.thread.start()
        
        # Start the work
        self.worker.run()

    def stop_upload(self):
        """Stop the ongoing upload process"""
        if self.worker is not None:
            self.worker.stop()
            self.log_message("Stopping upload process...")
        
        self.stop_button.setEnabled(False)

    def upload_finished(self, success):
        """Handle upload completion"""
        # Clean up the thread
        if self.thread is not None:
            self.thread.quit()
            self.thread.wait()
            self.thread = None
            self.worker = None
        
        # Re-enable buttons
        self.upload_button.setEnabled(True)
        self.check_button.setEnabled(True)
        self.refresh_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        
        # Update status labels
        self.update_status_labels()
        
        if success:
            self.log_message("Upload process completed successfully!")
        else:
            self.log_message("Upload process completed with errors.")

    def toggle_auto_refresh(self):
        """Toggle auto-refresh functionality"""
        if self.auto_refresh_button.isChecked():
            self.auto_refresh_timer.start(30000)  # Refresh every 30 seconds
            self.auto_refresh_button.setText("Auto-Refresh ON")
            self.log_message("Auto-refresh enabled (every 30 seconds)")
        else:
            self.auto_refresh_timer.stop()
            self.auto_refresh_button.setText("Auto-Refresh OFF")
            self.log_message("Auto-refresh disabled")

    def log_message(self, message):
        """Add a message to the log text widget"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}"
        self.log_text.append(formatted_message)
        # Auto-scroll to the bottom
        self.log_text.moveCursor(self.log_text.textCursor().End)

    def closeEvent(self, event):
        """Handle the window close event"""
        # Stop the auto-refresh timer if it's running
        if self.auto_refresh_timer.isActive():
            self.auto_refresh_timer.stop()
        
        # Stop any ongoing upload
        if self.worker is not None:
            self.stop_upload()
        
        event.accept()


def main():
    app = QApplication(sys.argv)
    
    # Set application style
    app.setStyle('Fusion')
    
    # Create and show the GUI
    gui = DBFUploaderGUI()
    gui.show()
    
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()