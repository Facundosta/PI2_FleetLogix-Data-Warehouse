================================================================================
FLEETLOGIX - GUÍA DE IMPLEMENTACIÓN DE BASE DE DATOS
Autor: Facundo Acosta | Email: facundoacostast@gmail.com
Versión: 2.1 | Fecha: Octubre 2024
================================================================================

DESCRIPCIÓN GENERAL
--------------------------------------------------------------------------------
Este proyecto implementa una base de datos PostgreSQL para el sistema FleetLogix,
optimizada para procesos ETL y Data Warehouse en Snowflake. Incluye generación
de datos sintéticos, estructura dimensional y verificaciones de calidad.

PRERREQUISITOS
--------------------------------------------------------------------------------
- PostgreSQL 12 o superior
- DBeaver (recomendado) o cliente PostgreSQL
- Python 3.8 o superior
- Acceso de administrador a la base de datos

SECUENCIA DE EJECUCIÓN OBLIGATORIA
================================================================================

PASO 1: CONFIGURACIÓN INICIAL DE BASE DE DATOS
--------------------------------------------------------------------------------
Archivo: FA_fleetlogix_core.sql

1.1 Ejecutar en DBeaver:
    - Abrir DBeaver y conectarse a PostgreSQL
    - Presionar Control + 9 y seleccionar PostgreSQL como fuente de datos
    - Ejecutar solo la sección "PASO 1: CREAR LA BASE DE DATOS"

1.2 Cambiar a la base de datos creada:
    - Click derecho sobre 'fleetlogix_db' en el explorador de DBeaver
    - Seleccionar "Usar/Establecer por defecto" o "Set as Default"
    - Confirmar que la conexión activa apunta a 'fleetlogix_db'

1.3 Ejecutar el resto del script:
    - Ejecutar las secciones restantes del archivo FA_fleetlogix_core.sql
    - Verificar que no hay errores en la creación de tablas e índices

PASO 2: CONFIGURACIÓN DE CREDENCIALES
--------------------------------------------------------------------------------
Archivo: .env.example → Renombrar a .env

2.1 Renombrar archivo:
    - Cambiar nombre de '.env.example' a '.env'

2.2 Configurar credenciales:
    - Editar el archivo .env con las credenciales de su entorno PostgreSQL
    - Asegurar que todos los valores corresponden a su configuración local

Ejemplo de configuración completa:
----------------------------------
# DATABASE CONFIGURATION
DB_NAME=fleetlogix_db
DB_USER=postgres
DB_PASSWORD=su_password_seguro_aqui
DB_HOST=localhost
DB_PORT=5432

# LOGGING CONFIGURATION
LOG_LEVEL=INFO
LOG_FILE=logs/data_generation.log

PASO 3: GENERACIÓN DE DATOS SINTÉTICOS
--------------------------------------------------------------------------------
Archivo: FA_01_data_generation_V2.1.py

3.1 Instalar dependencias Python:
    - Ejecutar: pip install -r requirements.txt
    - Dependencias críticas: psycopg2, python-dotenv, faker, pandas, numpy

3.2 Ejecutar script de generación:
    - Comando: python FA_01_data_generation_V2.1.py
    - El script detectará automáticamente la configuración del archivo .env

3.3 Verificar ejecución:
    - Monitorizar logs en consola y archivo de log especificado
    - Confirmar que no hay errores durante la generación
    - Tiempo estimado: 5-15 minutos dependiendo del hardware

VOLÚMENES DE DATOS GENERADOS
--------------------------------------------------------------------------------
- Vehicles: 200 registros con SCD Type 2
- Drivers: 400 registros con SCD Type 2  
- Routes: 50 rutas con distancias consistentes
- Customers: 1,000 clientes categorizados
- Trips: 100,000 viajes con distribución temporal realista
- Deliveries: 400,000 entregas con métricas extendidas
- Maintenance: 5,000 registros de mantenimiento

VERIFICACIÓN POST-IMPLEMENTACIÓN
--------------------------------------------------------------------------------

1. Verificación de estructura:
   - Ejecutar consultas de verificación en DBeaver
   - Revisar vistas: data_quality_integrity y data_quality_completeness
   - Confirmar que no hay errores en las funciones de validación

2. Verificación de datos:
   - Ejecutar: SELECT COUNT(*) FROM vehicles; (debe retornar 200)
   - Ejecutar: SELECT COUNT(*) FROM deliveries; (debe retornar ~400,000)
   - Revisar logs de ingesta en tabla data_ingestion_logs

SOLUCIÓN DE PROBLEMAS COMUNES
================================================================================

ERROR: "connection to server failed"
CAUSA: Configuración incorrecta en .env
SOLUCIÓN: Verificar DB_HOST, DB_PORT, y credenciales

ERROR: "database 'fleetlogix_db' does not exist"  
CAUSA: No se ejecutó el PASO 1 correctamente
SOLUCIÓN: Volver a ejecutar creación de BD en DBeaver

ERROR: "permission denied for .env file"
CAUSA: Problemas de permisos de archivo
SOLUCIÓN: Asegurar que el archivo .env tiene permisos de lectura

ERROR: "module not found" en Python
CAUSA: Dependencias no instaladas
SOLUCIÓN: Ejecutar pip install con el requirements.txt

ERROR: "duplicate key value violates unique constraint"
CAUSA: Ejecución múltiple del script
SOLUCIÓN: Limpiar tablas o usar base de datos fresca

ESTRUCTURA DE ARCHIVOS CRÍTICOS
--------------------------------------------------------------------------------
FA_fleetlogix_core.sql          # Esquema de base de datos
FA_01_data_generation_V2.1.py   # Generador de datos sintéticos
.env.example                    # Plantilla de configuración (renombrar a .env)
requirements.txt                # Dependencias Python

NOTAS TÉCNICAS
--------------------------------------------------------------------------------
- El esquema incluye soporte para SCD Type 2 en vehículos y conductores
- Las rutas mantienen distancias consistentes basadas en ubicaciones reales
- Los datos están optimizados para transformación a modelo estrella en Snowflake
- Se incluyen métricas calculadas para análisis de profitability y efficiency

CONTACTO Y SOPORTE
================================================================================
Desarrollador: Facundo Acosta
Email: facundoacostast@gmail.com
LinkedIn: https://www.linkedin.com/in/facundo-acosta-marketing/

Para reportar problemas técnicos o requerir asistencia, contactar directamente
con el desarrollador incluyendo los logs generados y descripción detallada del error.

================================================================================