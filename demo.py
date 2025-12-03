"""
Demo GUI Application with Color Indicator

This is a simple GUI application built with Tkinter that features:
- A color indicator panel that changes color
- Two buttons (Red and Green) that change the indicator color
- A Reset button to return to the default gray color

To run this application:
1. Make sure Tkinter is installed (usually comes with Python)
2. Run with: python3 demo.py

On macOS, if Tkinter is not available:
  brew install python-tk

On some systems, you might need to install tkinter separately:
  sudo apt-get install python3-tk (Ubuntu/Debian)
"""

try:
    import tkinter as tk
    from tkinter import ttk
    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False
    print("Tkinter is not available. Please install it to run this GUI application.")
    print("On macOS, you might need to install Python with Tkinter support:")
    print("  brew install python-tk")
    print("Or use your system Python which typically includes Tkinter.")

class ColorIndicatorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Color Indicator Demo")
        self.root.geometry("300x200")
        
        # Create a frame for the color indicator
        self.indicator_frame = ttk.Frame(root, height=100)
        self.indicator_frame.pack(pady=20, padx=20, fill='x')
        
        # Create the color indicator label
        self.indicator = tk.Label(
            self.indicator_frame, 
            text="COLOR INDICATOR", 
            bg="gray", 
            fg="white",
            font=("Arial", 16, "bold"),
            relief="raised",
            borderwidth=2
        )
        self.indicator.pack(fill='both', expand=True)
        
        # Create a frame for the buttons
        self.button_frame = ttk.Frame(root)
        self.button_frame.pack(pady=20)
        
        # Create the red button
        self.red_button = ttk.Button(
            self.button_frame, 
            text="Red", 
            command=self.set_red
        )
        self.red_button.pack(side='left', padx=10)
        
        # Create the green button
        self.green_button = ttk.Button(
            self.button_frame, 
            text="Green", 
            command=self.set_green
        )
        self.green_button.pack(side='left', padx=10)
        
        # Create the reset button
        self.reset_button = ttk.Button(
            self.button_frame, 
            text="Reset", 
            command=self.set_gray
        )
        self.reset_button.pack(side='left', padx=10)
        
        # Initialize with gray color
        self.current_color = "gray"
    
    def set_red(self):
        self.indicator.config(bg="red", text="RED")
        self.current_color = "red"
    
    def set_green(self):
        self.indicator.config(bg="green", text="GREEN")
        self.current_color = "green"
        
    def set_gray(self):
        self.indicator.config(bg="gray", text="COLOR INDICATOR")
        self.current_color = "gray"

def main():
    if not TKINTER_AVAILABLE:
        print("Cannot run GUI application without Tkinter.")
        return
    
    root = tk.Tk()
    app = ColorIndicatorApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()