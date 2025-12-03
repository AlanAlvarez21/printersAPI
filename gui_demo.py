#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GUI Demo Application for Printer and Scale Integration

This is a GUI application built with Tkinter that integrates:
- Printer functionality (TSC TX200)
- Scale functionality (serial communication)
- Status indicators for both devices
- Simple control interface
- Configuration persistence

To run this application:
1. Make sure Tkinter is installed (usually comes with Python)
2. Run with: python3 gui_demo.py

On macOS, if Tkinter is not available:
  brew install python-tk

On some systems, you might need to install tkinter separately:
  sudo apt-get install python3-tk (Ubuntu/Debian)
"""

try:
    import tkinter as tk
    from tkinter import ttk, messagebox, scrolledtext
    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False
    print("Tkinter is not available. Please install it to run this GUI application.")
    print("On macOS, you might need to install Python with Tkinter support:")
    print("  brew install python-tk")
    print("Or use your system Python which typically includes Tkinter.")

import threading
import time
import serial
import serial.tools.list_ports
import usb.core
import usb.util
from datetime import datetime
import logging
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration file path
CONFIG_FILE = "printer_scale_config.json"

class PrinterScaleGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Printer & Scale Control Panel")
        self.root.geometry("800x600")
        
        # Device states
        self.scale_connected = False
        self.printer_connected = False
        self.scale_thread = None
        self.scale_running = False
        self.scale_serial = None
        
        # Printer device
        self.printer_device = None
        self.endpoint_out = None
        self.endpoint_in = None
        
        # Configuration
        self.config = self.load_config()
        
        self.setup_ui()
        self.update_status()
        self.apply_config()
        
    def load_config(self):
        """Load configuration from file"""
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    logger.info("Configuration loaded successfully")
                    return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
        return {}
    
    def save_config(self):
        """Save current configuration to file"""
        try:
            # Save current values
            self.config['scale_port'] = self.scale_port_var.get()
            self.config['label_content'] = self.label_content_var.get()
            
            # Save connection states
            self.config['scale_connected'] = self.scale_connected
            self.config['printer_connected'] = self.printer_connected
            
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f, indent=2)
                logger.info("Configuration saved successfully")
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
    
    def apply_config(self):
        """Apply loaded configuration to UI elements"""
        if 'scale_port' in self.config:
            self.scale_port_var.set(self.config['scale_port'])
            
        if 'label_content' in self.config:
            self.label_content_var.set(self.config['label_content'])
            
        # Refresh port lists
        self.refresh_serial_ports()
            
        # Auto-connect devices if they were connected in previous session
        if self.config.get('scale_connected', False):
            self.root.after(1000, self.connect_scale)  # Delay to ensure UI is ready
            
        if self.config.get('printer_connected', False):
            self.root.after(1500, self.connect_printer)  # Delay to ensure UI is ready
        
    def setup_ui(self):
        # Main notebook for tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Status tab
        self.status_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.status_frame, text="Status")
        
        # Control tab
        self.control_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.control_frame, text="Control")
        
        # Setup status tab
        self.setup_status_tab()
        
        # Setup control tab
        self.setup_control_tab()
        
    def setup_status_tab(self):
        # Status indicators frame
        status_frame = ttk.LabelFrame(self.status_frame, text="Device Status", padding=10)
        status_frame.pack(fill='x', padx=10, pady=10)
        
        # Scale status
        scale_status_frame = ttk.Frame(status_frame)
        scale_status_frame.pack(fill='x', pady=5)
        
        ttk.Label(scale_status_frame, text="Scale:").pack(side='left')
        self.scale_status_indicator = tk.Label(scale_status_frame, text="DISCONNECTED", bg="red", fg="white", 
                                              width=15, anchor='center')
        self.scale_status_indicator.pack(side='left', padx=(10, 0))
        
        # Printer status
        printer_status_frame = ttk.Frame(status_frame)
        printer_status_frame.pack(fill='x', pady=5)
        
        ttk.Label(printer_status_frame, text="Printer:").pack(side='left')
        self.printer_status_indicator = tk.Label(printer_status_frame, text="DISCONNECTED", bg="red", fg="white", 
                                                width=15, anchor='center')
        self.printer_status_indicator.pack(side='left', padx=(10, 0))
        
        # Connection info frame
        info_frame = ttk.LabelFrame(self.status_frame, text="Connection Info", padding=10)
        info_frame.pack(fill='x', padx=10, pady=10)
        
        # Scale info
        scale_info_frame = ttk.Frame(info_frame)
        scale_info_frame.pack(fill='x', pady=5)
        
        ttk.Label(scale_info_frame, text="Scale Port:").pack(side='left')
        self.scale_port_label = ttk.Label(scale_info_frame, text="Not connected")
        self.scale_port_label.pack(side='left', padx=(10, 0))
        
        # Printer info
        printer_info_frame = ttk.Frame(info_frame)
        printer_info_frame.pack(fill='x', pady=5)
        
        ttk.Label(printer_info_frame, text="Printer:").pack(side='left')
        self.printer_info_label = ttk.Label(printer_info_frame, text="Not connected")
        self.printer_info_label.pack(side='left', padx=(10, 0))
        
        # Scale data display
        data_frame = ttk.LabelFrame(self.status_frame, text="Scale Data", padding=10)
        data_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        self.scale_data_display = tk.Text(data_frame, height=10, state='disabled')
        scrollbar = ttk.Scrollbar(data_frame, orient='vertical', command=self.scale_data_display.yview)
        self.scale_data_display.configure(yscrollcommand=scrollbar.set)
        
        self.scale_data_display.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
        
    def setup_control_tab(self):
        # Scale control frame
        scale_frame = ttk.LabelFrame(self.control_frame, text="Scale Control", padding=10)
        scale_frame.pack(fill='x', padx=10, pady=10)
        
        # Scale connection
        scale_conn_frame = ttk.Frame(scale_frame)
        scale_conn_frame.pack(fill='x', pady=5)
        
        ttk.Label(scale_conn_frame, text="Port:").pack(side='left')
        self.scale_port_var = tk.StringVar(value='COM3')
        self.scale_port_combo = ttk.Combobox(scale_conn_frame, textvariable=self.scale_port_var, width=15, state="readonly")
        self.scale_port_combo.pack(side='left', padx=(5, 10))
        
        self.connect_scale_btn = ttk.Button(scale_conn_frame, text="Connect Scale", command=self.connect_scale)
        self.connect_scale_btn.pack(side='left', padx=(0, 5))
        
        self.disconnect_scale_btn = ttk.Button(scale_conn_frame, text="Disconnect", command=self.disconnect_scale)
        self.disconnect_scale_btn.pack(side='left')
        
        # Scale reading controls
        scale_read_frame = ttk.Frame(scale_frame)
        scale_read_frame.pack(fill='x', pady=10)
        
        self.start_reading_btn = ttk.Button(scale_read_frame, text="Start Reading", command=self.start_scale_reading)
        self.start_reading_btn.pack(side='left', padx=(0, 5))
        
        self.stop_reading_btn = ttk.Button(scale_read_frame, text="Stop Reading", command=self.stop_scale_reading)
        self.stop_reading_btn.pack(side='left')
        
        # Printer control frame
        printer_frame = ttk.LabelFrame(self.control_frame, text="Printer Control", padding=10)
        printer_frame.pack(fill='x', padx=10, pady=10)
        
        # Printer connection
        printer_conn_frame = ttk.Frame(printer_frame)
        printer_conn_frame.pack(fill='x', pady=5)
        
        ttk.Label(printer_conn_frame, text="USB Device:").pack(side='left')
        self.printer_device_var = tk.StringVar()
        self.printer_device_combo = ttk.Combobox(printer_conn_frame, textvariable=self.printer_device_var, width=25, state="readonly")
        self.printer_device_combo.pack(side='left', padx=(5, 10))
        
        self.connect_printer_btn = ttk.Button(printer_conn_frame, text="Connect Printer", command=self.connect_printer)
        self.connect_printer_btn.pack(side='left', padx=(0, 5))
        
        self.disconnect_printer_btn = ttk.Button(printer_conn_frame, text="Disconnect", command=self.disconnect_printer)
        self.disconnect_printer_btn.pack(side='left')
        
        # Printer test
        printer_test_frame = ttk.Frame(printer_frame)
        printer_test_frame.pack(fill='x', pady=10)
        
        self.test_printer_btn = ttk.Button(printer_test_frame, text="Test Printer", command=self.test_printer)
        self.test_printer_btn.pack(side='left', padx=(0, 5))
        
        # Label content
        label_frame = ttk.LabelFrame(self.control_frame, text="Label Content", padding=10)
        label_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Label(label_frame, text="Content:").pack(anchor='w')
        self.label_content_var = tk.StringVar(value="TEST LABEL")
        self.label_content_entry = ttk.Entry(label_frame, textvariable=self.label_content_var, width=50)
        self.label_content_entry.pack(fill='x', pady=5)
        
        self.print_label_btn = ttk.Button(label_frame, text="Print Label", command=self.print_label)
        self.print_label_btn.pack(anchor='w', pady=(5, 0))
        
        # Serial ports list
        ports_frame = ttk.LabelFrame(self.control_frame, text="Available Serial Ports", padding=10)
        ports_frame.pack(fill='x', padx=10, pady=10)
        
        self.refresh_ports_btn = ttk.Button(ports_frame, text="Refresh Ports", command=self.refresh_serial_ports)
        self.refresh_ports_btn.pack(anchor='w', pady=(0, 10))
        
        self.ports_listbox = tk.Listbox(ports_frame, height=5)
        self.ports_listbox.pack(fill='x')
        
        self.refresh_serial_ports()
        
        # Bind listbox selection to update scale port entry
        self.ports_listbox.bind('<<ListboxSelect>>', self.on_port_select)
        
    def on_port_select(self, event):
        selection = self.ports_listbox.curselection()
        if selection:
            port = self.ports_listbox.get(selection[0]).split(' - ')[0]
            self.scale_port_var.set(port)
        
    def refresh_serial_ports(self):
        # Clear both comboboxes
        self.ports_listbox.delete(0, tk.END)
        self.scale_port_combo['values'] = []
        self.printer_device_combo['values'] = []
        
        # Get serial ports
        ports = serial.tools.list_ports.comports()
        port_list = []
        for port in ports:
            port_info = f"{port.device} - {port.description}"
            port_list.append(port.device)
            self.ports_listbox.insert(tk.END, port_info)
            
        # Set scale port combobox values
        self.scale_port_combo['values'] = port_list
        if port_list and not self.scale_port_var.get():
            self.scale_port_var.set(port_list[0])
            
        # Get USB printers
        printers = []
        try:
            # Look for TSC TX200 printers
            devices = usb.core.find(find_all=True, idVendor=0x1203)
            for device in devices:
                if device.idProduct == 0x0230:  # TSC TX200
                    printers.append(f"TSC TX200 - VID:0x1203 PID:0x0230")
                else:
                    printers.append(f"USB Printer - VID:0x{device.idVendor:04x} PID:0x{device.idProduct:04x}")
        except Exception as e:
            logger.warning(f"Error detecting USB printers: {e}")
            
        # Set printer device combobox values
        self.printer_device_combo['values'] = printers if printers else ["No USB printers found"]
        if printers and not self.printer_device_var.get():
            self.printer_device_var.set(printers[0])
        
    def update_status(self):
        # Update scale status indicator
        if self.scale_connected:
            self.scale_status_indicator.config(bg="green", text="CONNECTED")
        else:
            self.scale_status_indicator.config(bg="red", text="DISCONNECTED")
            
        # Update printer status indicator
        if self.printer_connected:
            self.printer_status_indicator.config(bg="green", text="CONNECTED")
        else:
            self.printer_status_indicator.config(bg="red", text="DISCONNECTED")
            
    def connect_scale(self):
        port = self.scale_port_var.get()
        try:
            self.scale_serial = serial.Serial(port, baudrate=115200, timeout=1)
            self.scale_connected = True
            self.scale_port_label.config(text=f"Connected to {port}")
            logger.info(f"Scale connected to {port}")
            messagebox.showinfo("Success", f"Scale connected to {port}")
            self.save_config()  # Save configuration when connecting
        except Exception as e:
            logger.error(f"Error connecting to scale: {e}")
            messagebox.showerror("Error", f"Failed to connect scale: {e}")
            
        self.update_status()
        
    def disconnect_scale(self):
        if self.scale_serial and self.scale_serial.is_open:
            self.stop_scale_reading()
            self.scale_serial.close()
            
        self.scale_connected = False
        self.scale_port_label.config(text="Not connected")
        logger.info("Scale disconnected")
        messagebox.showinfo("Disconnected", "Scale disconnected")
        self.update_status()
        self.save_config()  # Save configuration when disconnecting
        
    def start_scale_reading(self):
        if not self.scale_connected:
            messagebox.showerror("Error", "Scale not connected")
            return
            
        if self.scale_running:
            messagebox.showwarning("Warning", "Scale reading already running")
            return
            
        self.scale_running = True
        self.scale_thread = threading.Thread(target=self.scale_reading_loop, daemon=True)
        self.scale_thread.start()
        logger.info("Scale reading started")
        
    def stop_scale_reading(self):
        self.scale_running = False
        logger.info("Scale reading stopped")
        
    def scale_reading_loop(self):
        while self.scale_running and self.scale_serial and self.scale_serial.is_open:
            try:
                if self.scale_serial.in_waiting > 0:
                    data = self.scale_serial.readline().decode('utf-8', errors='ignore').strip()
                    if data:
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        display_text = f"[{timestamp}] {data}\n"
                        
                        # Update UI in thread-safe way
                        self.root.after(0, self.update_scale_display, display_text)
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error reading scale: {e}")
                break
                
    def update_scale_display(self, text):
        self.scale_data_display.config(state='normal')
        self.scale_data_display.insert(tk.END, text)
        self.scale_data_display.see(tk.END)  # Scroll to end
        self.scale_data_display.config(state='disabled')
        
    def connect_printer(self):
        try:
            # Buscar dispositivo TSC TX200 (Vendor ID: 0x1203, Product ID: 0x0230)
            self.printer_device = usb.core.find(idVendor=0x1203, idProduct=0x0230)
            
            if self.printer_device is None:
                # Try to find any printer with vendor ID 0x1203
                self.printer_device = usb.core.find(idVendor=0x1203)
                if self.printer_device is None:
                    raise Exception("No TSC printer found")
                
            # Configurar dispositivo
            if self.printer_device.is_kernel_driver_active(0):
                self.printer_device.detach_kernel_driver(0)
                
            self.printer_device.set_configuration()
            
            # Obtener interface y endpoints
            cfg = self.printer_device.get_active_configuration()
            intf = cfg[(0,0)]
            
            # Encontrar endpoints
            self.endpoint_out = usb.util.find_descriptor(
                intf,
                custom_match=lambda e: usb.util.endpoint_direction(e.bEndpointAddress) == usb.util.ENDPOINT_OUT
            )
            
            self.endpoint_in = usb.util.find_descriptor(
                intf,
                custom_match=lambda e: usb.util.endpoint_direction(e.bEndpointAddress) == usb.util.ENDPOINT_IN
            )
            
            if self.endpoint_out is None:
                raise Exception("Output endpoint not found")
                
            self.printer_connected = True
            device_info = self.printer_device_var.get() if self.printer_device_var.get() else "TSC Printer"
            self.printer_info_label.config(text=f"Connected: {device_info}")
            logger.info("Printer connected successfully")
            messagebox.showinfo("Success", "Printer connected successfully")
            self.save_config()  # Save configuration when connecting
            
        except Exception as e:
            logger.error(f"Error connecting printer: {e}")
            messagebox.showerror("Error", f"Failed to connect printer: {e}")
            
        self.update_status()
        
    def disconnect_printer(self):
        if self.printer_device:
            try:
                usb.util.dispose_resources(self.printer_device)
            except Exception as e:
                logger.warning(f"Error disconnecting printer: {e}")
                
        self.printer_device = None
        self.endpoint_out = None
        self.endpoint_in = None
        self.printer_connected = False
        self.printer_info_label.config(text="Not connected")
        logger.info("Printer disconnected")
        messagebox.showinfo("Disconnected", "Printer disconnected")
        self.update_status()
        self.save_config()  # Save configuration when disconnecting
        
    def enviar_comando(self, comando):
        if not self.printer_connected or not self.printer_device or not self.endpoint_out:
            raise Exception("Printer not connected")
            
        try:
            if isinstance(comando, str):
                comando = comando.encode('utf-8')
                
            bytes_escritos = self.endpoint_out.write(comando)
            logger.debug(f"Sent {bytes_escritos} bytes: {comando.decode('utf-8').strip()}")
            return True
        except Exception as e:
            logger.error(f"Error sending command: {e}")
            raise
            
    def test_printer(self):
        if not self.printer_connected:
            messagebox.showerror("Error", "Printer not connected")
            return
            
        try:
            # Comandos de test
            comandos_test = [
                "SIZE 80 mm, 50 mm\n",
                "GAP 2 mm, 0 mm\n",
                "DIRECTION 1,0\n",
                "REFERENCE 0,0\n",
                "OFFSET 0 mm\n",
                "SET PEEL OFF\n",
                "SET CUTTER OFF\n",
                "SET PARTIAL_CUTTER OFF\n",
                "SET TEAR ON\n",
                "CLS\n",
                "CODEPAGE 1252\n",
                "TEXT 160,75,\"4\",0,1,1,\"PRINTER TEST\"\n",
                "TEXT 160,125,\"3\",0,1,1,\"TSC TX200 OK\"\n",
                "BAR 120,225,400,2\n",
                f"TEXT 160,275,\"1\",0,1,1,\"{time.strftime('%Y-%m-%d %H:%M')}\"\n",
                "PRINT 1,1\n"
            ]
            
            for comando in comandos_test:
                self.enviar_comando(comando)
                time.sleep(0.1)
                
            logger.info("Printer test completed")
            messagebox.showinfo("Success", "Printer test completed")
            
        except Exception as e:
            logger.error(f"Error in printer test: {e}")
            messagebox.showerror("Error", f"Printer test failed: {e}")
            
    def print_label(self):
        if not self.printer_connected:
            messagebox.showerror("Error", "Printer not connected")
            return
            
        content = self.label_content_var.get()
        if not content:
            messagebox.showerror("Error", "Label content cannot be empty")
            return
            
        try:
            # Comandos para imprimir etiqueta
            comandos = [
                "SIZE 80 mm, 50 mm\n",
                "GAP 2 mm, 0 mm\n",
                "DIRECTION 1,0\n",
                "REFERENCE 0,0\n",
                "OFFSET 0 mm\n",
                "SET PEEL OFF\n",
                "SET CUTTER OFF\n",
                "SET PARTIAL_CUTTER OFF\n",
                "SET TEAR ON\n",
                "CLS\n",
                "CODEPAGE 1252\n",
                f"TEXT 160,75,\"4\",0,1,1,\"{content}\"\n",
                "TEXT 160,125,\"2\",0,1,1,\"Peso: --kg\"\n",
                "BAR 120,225,400,2\n",
                f"TEXT 160,275,\"1\",0,1,1,\"{time.strftime('%Y-%m-%d %H:%M')}\"\n",
                "PRINT 1,1\n"
            ]
            
            for comando in comandos:
                self.enviar_comando(comando)
                time.sleep(0.1)
                
            logger.info(f"Label printed: {content}")
            messagebox.showinfo("Success", f"Label printed: {content}")
            self.save_config()  # Save configuration when printing
            
        except Exception as e:
            logger.error(f"Error printing label: {e}")
            messagebox.showerror("Error", f"Failed to print label: {e}")

def main():
    if not TKINTER_AVAILABLE:
        print("Cannot run GUI application without Tkinter.")
        return
        
    root = tk.Tk()
    
    # Handle window closing
    def on_closing():
        app.save_config()
        root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    
    app = PrinterScaleGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()