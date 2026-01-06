import winreg
import serial.tools.list_ports

def get_windows_com_ports():
    """Obtiene puertos COM usando métodos específicos de Windows"""
    ports = []
    
    # Método 1: Usar serial.tools.list_ports (ya lo estamos usando)
    print("Método 1 - serial.tools.list_ports:")
    for port in serial.tools.list_ports.comports():
        print(f"  {port.device}: {port.description} (VID: {port.vid}, PID: {port.pid})")
        ports.append(port.device)
    
    # Método 2: Buscar en el registro de Windows
    print("\nMétodo 2 - Registro de Windows:")
    try:
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DEVICEMAP\SERIALCOMM")
        i = 0
        while True:
            try:
                name, value, _ = winreg.EnumValue(key, i)
                print(f"  {name}: {value}")
                if value not in ports:
                    ports.append(value)
                i += 1
            except WindowsError:
                break
        winreg.CloseKey(key)
    except Exception as e:
        print(f"  Error accediendo al registro: {e}")
    
    return ports

if __name__ == "__main__":
    print("Verificación de puertos COM en Windows")
    print("="*50)
    ports = get_windows_com_ports()
    print(f"\nPuertos encontrados: {ports}")
    
    # Verificar si podemos abrir cada puerto
    print("\nIntentando abrir cada puerto...")
    for port in ports:
        try:
            import serial
            s = serial.Serial(port, baudrate=9600, timeout=1)
            print(f"  ✓ {port} - ABIERTO EXITOSAMENTE")
            s.close()
        except Exception as e:
            print(f"  ✗ {port} - ERROR: {e}")