# Printer & Scale Control Panel GUI

This is a GUI application built with Tkinter that integrates printer and scale functionality for industrial applications.

## Features

- **Printer Control**: Connect to and control TSC TX200 printers via USB
- **Scale Integration**: Connect to serial scales and read weight data
- **Status Indicators**: Visual indicators for device connection status
- **Label Printing**: Print custom labels with configurable content
- **Device Testing**: Built-in printer test functionality
- **Configuration Persistence**: Automatically saves and loads device settings

## Prerequisites

- Python 3.6 or higher
- Virtual environment (recommended)

## Installation

1. Create a virtual environment:
   ```bash
   python3 -m venv venv
   ```

2. Activate the virtual environment:
   ```bash
   # On macOS/Linux:
   source venv/bin/activate
   
   # On Windows:
   venv\Scripts\activate
   ```

3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the application with:
```bash
python3 gui_demo.py
```

Or use the launcher script:
```bash
./launch_gui.sh
```

## Application Interface

The application has two main tabs:

### Status Tab
- Shows connection status for both printer and scale
- Displays real-time scale data readings
- Shows connection information

### Control Tab
- Connect/disconnect printer and scale devices
- Start/stop scale reading
- Test printer functionality
- Print custom labels
- View available serial ports

## Configuration Persistence

The application automatically saves and loads configuration settings:

- Last used serial port for the scale
- Last entered label content
- Connection states of devices

Configuration is stored in `printer_scale_config.json` in the application directory.

## Device Configuration

### Scale Connection
1. Select the appropriate serial port from the list
2. Click "Connect Scale"
3. Use "Start Reading" to begin receiving weight data

### Printer Connection
1. Ensure the TSC TX200 printer is connected via USB
2. Click "Connect Printer"
3. Use "Test Printer" to verify the connection
4. Enter label content and click "Print Label" to print

## Troubleshooting

### Tkinter Issues
If you encounter Tkinter issues on macOS:
```bash
brew install python-tk
```

### Serial Port Permissions (Linux)
On Linux, you may need to add your user to the dialout group:
```bash
sudo usermod -a -G dialout $USER
```

### USB Permissions (Linux)
For USB printer access on Linux, you may need to add a udev rule:
```bash
echo 'SUBSYSTEM=="usb", ATTRS{idVendor}=="1203", ATTRS{idProduct}=="0230", MODE="0666"' | sudo tee /etc/udev/rules.d/99-tsc-printer.rules
sudo udevadm control --reload-rules
```

## Dependencies

- `pyserial`: For serial communication with scales
- `pyusb`: For USB communication with printers
- `tkinter`: For GUI interface (usually included with Python)

## License

This project is licensed under the MIT License.