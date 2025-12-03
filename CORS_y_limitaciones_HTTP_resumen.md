# CORS y Limitaciones HTTP - serial_server.py

## Configuración de CORS
Actualizada para permitir:
- Orígenes: *
- Métodos: GET, POST, PUT, DELETE, OPTIONS
- Headers: Content-Type, Authorization

## Limitaciones HTTP
- Sin rate limiting
- Sin autenticación
- Sin límites de tamaño de payload
- Modo debug activo (afecta rendimiento)

## Recomendaciones
1. Agregar autenticación/token
2. Implementar rate limiting
3. Desactivar modo debug en producción
4. Validar tamaño de payloads

## Conclusión
Los problemas de conexión probablemente no sean por CORS, sino por:
- Configuración de dispositivos
- Problemas de red
- Errores en la app cliente