#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de diagnóstico para detectar puertos seriales disponibles
"""

import serial.tools.list_ports
import usb.core
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def diagnosticar_puertos():
    """Diagnóstico completo de puertos seriales y dispositivos USB"""
    print("=" * 60)
    print("DIAGNÓSTICO DE PUERTOS SERIALES Y DISPOSITIVOS USB")
    print("=" * 60)
    
    # 1. Listar todos los puertos seriales
    print("\n1. PUERTOS SERIALES DISPONIBLES:")
    print("-" * 40)
    
    ports = list(serial.tools.list_ports.comports())
    if not ports:
        print("   ✗ No se encontraron puertos seriales en el sistema")
        print("   Esto podría deberse a:")
        print("   - Impresora no conectada físicamente")
        print("   - Drivers no instalados correctamente")
        print("   - Problemas de permisos")
    else:
        for i, port in enumerate(ports, 1):
            print(f"   {i}. {port.device}")
            print(f"      Descripción: {port.description}")
            print(f"      HWID: {port.hwid}")
            print(f"      VID:PID: {port.vid}:{port.pid}")
            print()
    
    # 2. Buscar dispositivos USB específicos
    print("2. DISPOSITIVOS USB CON VID/PID CONOCIDOS:")
    print("-" * 40)
    
    # Buscar dispositivo TSC TX200 (Vendor ID: 0x1203, Product ID: 0x0230)
    try:
        tsc_device = usb.core.find(idVendor=0x1203, idProduct=0x0230)
        if tsc_device:
            print(f"   ✓ TSC TX200 encontrado:")
            print(f"     Vendor ID: 0x{tsc_device.idVendor:04x}")
            print(f"     Product ID: 0x{tsc_device.idProduct:04x}")
            print(f"     Device Bus: {tsc_device.bus}")
            print(f"     Device Address: {tsc_device.address}")
        else:
            print("   ✗ TSC TX200 no encontrado via USB")
    except Exception as e:
        print(f"   ✗ Error buscando dispositivo TSC via USB: {str(e)}")
    
    # 3. Buscar otros dispositivos USB comunes de impresoras
    print("\n3. OTROS DISPOSITIVOS USB POTENCIALMENTE RELACIONADOS:")
    print("-" * 40)
    
    # Lista de VID comunes para dispositivos de impresión
    printer_vids = [0x1203, 0x051d, 0x067b, 0x0403, 0x1a86]  # TSC, APC, Prolific, FTDI, etc.
    
    devices = usb.core.find(find_all=True)
    found_relevant = False
    for device in devices:
        if device.idVendor in printer_vids or device.idProduct in [0x0230]:  # TSC TX200
            print(f"   ✓ Dispositivo encontrado: 0x{device.idVendor:04x}:0x{device.idProduct:04x}")
            print(f"     Bus: {device.bus}, Address: {device.address}")
            found_relevant = True
    
    if not found_relevant:
        print("   No se encontraron dispositivos USB relevantes")
    
    # 4. Recomendaciones
    print("\n4. RECOMENDACIONES:")
    print("-" * 40)
    if not ports:
        print("   • Verifica que la impresora esté físicamente conectada")
        print("   • Asegúrate de que los drivers estén instalados")
        print("   • En Windows, revisa el Administrador de Dispositivos")
        print("   • En Linux/Mac, verifica permisos (puede necesitar sudo)")
    else:
        print("   • La impresora puede aparecer como un puerto COM (Windows) o /dev/tty* (Linux/Mac)")
        print("   • Busca puertos con descripciones como 'USB', 'Serial', 'TSC' o 'Printer'")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    diagnosticar_puertos()