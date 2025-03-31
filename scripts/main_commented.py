#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script principal para ejecutar el sistema completo de descarga y almacenamiento
de datos históricos bursátiles de Alpha Vantage.
Versión compatible con macOS Apple Silicon usando pyodbc.

Este módulo coordina todos los componentes del sistema, proporcionando una interfaz
de línea de comandos para ejecutar diferentes funcionalidades del sistema.

Autor: Manus
Fecha: 31/03/2025
"""

# Importaciones de bibliotecas estándar
import os                      # Para operaciones del sistema de archivos
import sys                     # Para funcionalidades del sistema
import time                    # Para funciones relacionadas con el tiempo
import argparse                # Para procesar argumentos de línea de comandos
from datetime import datetime  # Para manejo de fechas y horas

# Importar módulos propios
# Añadimos el directorio padre al path para poder importar módulos del proyecto
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# Importamos los módulos necesarios para cada componente del sistema
from scripts.create_database_pyodbc import main as create_db  # Para crear la estructura de la base de datos
from scripts.alpha_vantage_api import AlphaVantageAPI  # Para interactuar con la API de Alpha Vantage
from scripts.metadata_manager_pyodbc import MetadataManager  # Para gestionar metadatos en la base de datos
from scripts.stock_downloader_pyodbc import StockDataDownloader  # Para descargar datos históricos
from scripts.stock_storage_pyodbc import StockDataStorage  # Para almacenar datos en la base de datos

def setup_environment():
    """
    Configura el entorno de ejecución.
    
    Este método verifica la existencia del archivo de configuración .env
    y crea los directorios necesarios para el funcionamiento del sistema.
    
    Returns:
        bool: True si la configuración fue exitosa, False en caso contrario.
    
    Mejores prácticas POO:
    - Responsabilidad única: Método dedicado exclusivamente a la configuración del entorno
    - Validación de precondiciones: Verifica que existan los archivos necesarios
    - Creación de recursos: Asegura que existan los directorios necesarios
    """
    # Informar al usuario sobre la operación
    print("=== Configurando entorno ===")
    
    # Verificar si existe el archivo .env
    # Este archivo contiene las credenciales y configuraciones necesarias
    env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', '.env')
    if not os.path.exists(env_file):
        # Advertir al usuario si no se encuentra el archivo
        print("ADVERTENCIA: No se encontró el archivo .env")
        print("Por favor, crea el archivo .env basado en .env.example con tus credenciales")
        return False
    
    # Crear directorios necesarios para el funcionamiento del sistema
    # Directorio para almacenar los datos descargados
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    # Directorio para almacenar logs
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    
    # Crear los directorios si no existen
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)
    # Crear subdirectorio para archivos procesados
    os.makedirs(os.path.join(data_dir, 'processed'), exist_ok=True)
    
    # Informar al usuario sobre el éxito de la operación
    print("Entorno configurado correctamente")
    return True

def create_database():
    """
    Crea la estructura de la base de datos.
    
    Este método llama al script de creación de la base de datos para
    configurar la estructura necesaria para el sistema.
    
    Mejores prácticas POO:
    - Delegación: Delega la creación de la base de datos a un módulo especializado
    - Método simple: Realiza una única tarea bien definida
    """
    # Informar al usuario sobre la operación
    print("\n=== Creando estructura de la base de datos ===")
    # Llamar al script de creación de la base de datos
    create_db()

def test_api_connection():
    """
    Prueba la conexión con la API de Alpha Vantage.
    
    Este método verifica que la conexión con la API de Alpha Vantage
    funcione correctamente, obteniendo datos de prueba para un símbolo.
    
    Returns:
        bool: True si la conexión fue exitosa, False en caso contrario.
    
    Mejores prácticas POO:
    - Validación temprana: Verifica que la API funcione antes de continuar
    - Feedback claro: Proporciona información detallada sobre el resultado
    """
    # Informar al usuario sobre la operación
    print("\n=== Probando conexión con Alpha Vantage ===")
    # Crear una instancia de la API
    api = AlphaVantageAPI()
    
    # Probar con un símbolo conocido
    symbol = 'AAPL'
    print(f"Obteniendo datos de prueba para {symbol}...")
    
    # Intentar obtener datos para el símbolo
    data = api.get_daily_adjusted(symbol, 'compact')
    
    # Verificar si se obtuvieron datos válidos
    if data and 'Time Series (Daily)' in data:
        # Extraer las fechas para mostrar información
        dates = list(data['Time Series (Daily)'].keys())
        print(f"Conexión exitosa. Se obtuvieron datos para {len(dates)} días.")
        print(f"Primer día: {dates[0]}")
        return True
    else:
        # Informar sobre el error
        print("Error al conectar con Alpha Vantage o al obtener datos.")
        if data and 'Error Message' in data:
            print(f"Error: {data['Error Message']}")
        return False

def test_metadata_manager():
    """
    Prueba el gestor de metadatos.
    
    Este método verifica que el gestor de metadatos funcione correctamente,
    obteniendo información sobre símbolos y configuración.
    
    Returns:
        bool: True si la prueba fue exitosa, False en caso contrario.
    
    Mejores prácticas POO:
    - Prueba de componentes: Verifica que cada componente funcione individualmente
    - Feedback informativo: Muestra información útil sobre el estado del sistema
    """
    # Informar al usuario sobre la operación
    print("\n=== Probando gestor de metadatos ===")
    # Crear una instancia del gestor de metadatos
    metadata = MetadataManager()
    
    # Obtener símbolos para verificar la conexión con la base de datos
    symbols = metadata.get_all_symbols()
    print(f"Símbolos en la base de datos: {len(symbols)}")
    
    # Mostrar información sobre cada símbolo
    for symbol in symbols:
        print(f"  - {symbol['Symbol']} ({symbol['CompanyName']})")
    
    # Verificar configuración
    api_function = metadata.get_config_value('API_FUNCTION')
    output_size = metadata.get_config_value('OUTPUT_SIZE')
    
    # Mostrar información sobre la configuración
    print(f"Configuración: API_FUNCTION={api_function}, OUTPUT_SIZE={output_size}")
    
    # Considerar exitosa la prueba si se encontraron símbolos
    return len(symbols) > 0

def download_stock_data():
    """
    Descarga datos históricos bursátiles.
    
    Este método coordina la descarga de datos históricos para los símbolos
    NVIDIA y Apple utilizando el descargador de datos.
    
    Returns:
        bool: True si la descarga fue exitosa, False en caso contrario.
    
    Mejores prácticas POO:
    - Delegación: Delega la descarga a una clase especializada
    - Feedback claro: Proporciona información sobre el resultado de cada descarga
    """
    # Informar al usuario sobre la operación
    print("\n=== Descargando datos históricos ===")
    # Crear una instancia del descargador de datos
    downloader = StockDataDownloader()
    
    # Definir los símbolos a descargar
    symbols = ['NVDA', 'AAPL']
    print(f"Descargando datos para los símbolos: {', '.join(symbols)}")
    
    # Descargar datos para los símbolos especificados
    results = downloader.download_specific_symbols(symbols)
    
    # Verificar resultados
    success_count = 0
    for symbol, result in results.items():
        # Determinar si la descarga fue exitosa
        status = "Exitosa" if result['success'] else "Fallida"
        print(f"  - {symbol}: {status}")
        if result['success']:
            success_count += 1
    
    # Considerar exitosa la operación si todas las descargas fueron exitosas
    return success_count == len(symbols)

def store_stock_data():
    """
    Almacena los datos históricos en la base de datos.
    
    Este método coordina el procesamiento de los archivos JSON descargados
    y su almacenamiento en la base de datos.
    
    Returns:
        bool: True si el almacenamiento fue exitoso, False en caso contrario.
    
    Mejores prácticas POO:
    - Delegación: Delega el almacenamiento a una clase especializada
    - Feedback detallado: Proporciona información sobre cada archivo procesado
    """
    # Informar al usuario sobre la operación
    print("\n=== Almacenando datos en la base de datos ===")
    # Crear una instancia del almacenamiento de datos
    storage = StockDataStorage()
    
    # Procesar todos los archivos JSON
    results = storage.process_all_json_files()
    
    # Mostrar resultados
    total_inserted = 0
    for file, result in results.items():
        print(f"  - {file}: {result['inserted_count']} registros insertados")
        total_inserted += result['inserted_count']
    
    # Mostrar el total de registros insertados
    print(f"Total de registros insertados: {total_inserted}")
    
    # Considerar exitosa la operación si se insertaron registros
    return total_inserted > 0

def verify_stored_data():
    """
    Verifica los datos almacenados en la base de datos.
    
    Este método consulta la base de datos para verificar que los datos
    hayan sido almacenados correctamente para los símbolos NVIDIA y Apple.
    
    Returns:
        bool: True si la verificación fue exitosa, False en caso contrario.
    
    Mejores prácticas POO:
    - Validación de resultados: Verifica que los datos estén correctamente almacenados
    - Feedback visual: Muestra ejemplos de los datos almacenados
    """
    # Informar al usuario sobre la operación
    print("\n=== Verificando datos almacenados ===")
    # Crear una instancia del almacenamiento de datos
    storage = StockDataStorage()
    
    # Definir los símbolos a verificar
    symbols = ['NVDA', 'AAPL']
    success_count = 0
    
    # Verificar cada símbolo
    for symbol in symbols:
        print(f"Verificando datos para {symbol}:")
        # Obtener los últimos 5 registros para el símbolo
        df = storage.get_stock_data_from_db(symbol, limit=5)
        
        # Verificar si se encontraron datos
        if not df.empty:
            print(f"Se encontraron {len(df)} registros para {symbol}")
            # Mostrar los datos
            print(df.to_string(index=False))
            success_count += 1
        else:
            print(f"No se encontraron datos para {symbol}")
    
    # Considerar exitosa la verificación si se encontraron datos para todos los símbolos
    return success_count == len(symbols)

def run_full_test():
    """
    Ejecuta una prueba completa del sistema.
    
    Este método coordina la ejecución de todas las funcionalidades del sistema
    en secuencia, verificando que cada paso se complete correctamente.
    
    Returns:
        bool: True si la prueba completa fue exitosa, False en caso contrario.
    
    Mejores prácticas POO:
    - Método de alto nivel: Coordina la ejecución de múltiples componentes
    - Validación secuencial: Verifica cada paso antes de continuar
    - Feedback detallado: Proporciona información sobre cada etapa del proceso
    """
    # Informar al usuario sobre la operación
    print("=== INICIANDO PRUEBA COMPLETA DEL SISTEMA ===")
    print(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Configurar entorno
    if not setup_environment():
        print("Error al configurar el entorno. Abortando prueba.")
        return False
    
    # Crear base de datos
    create_database()
    
    # Probar conexión con API
    if not test_api_connection():
        print("Error al conectar con Alpha Vantage. Abortando prueba.")
        return False
    
    # Probar gestor de metadatos
    if not test_metadata_manager():
        print("Error al probar el gestor de metadatos. Abortando prueba.")
        return False
    
    # Descargar datos
    if not download_stock_data():
        print("Error al descargar datos históricos. Abortando prueba.")
        return False
    
    # Almacenar datos
    if not store_stock_data():
        print("Error al almacenar datos en la base de datos. Abortando prueba.")
        return False
    
    # Verificar datos almacenados
    if not verify_stored_data():
        print("Error al verificar los datos almacenados. Abortando prueba.")
        return False
    
    # Informar sobre el éxito de la prueba completa
    print("\n=== PRUEBA COMPLETA EXITOSA ===")
    print("Todos los componentes del sistema funcionan correctamente.")
    return True

def main():
    """
    Función principal.
    
    Esta función procesa los argumentos de línea de comandos y ejecuta
    las funcionalidades correspondientes del sistema.
    
    Mejores prácticas POO:
    - Interfaz de línea de comandos: Proporciona una forma flexible de interactuar con el sistema
    - Opciones claras: Cada argumento tiene un propósito específico y bien documentado
    - Manejo de casos por defecto: Muestra ayuda si no se especifican argumentos
    """
    # Crear un parser de argumentos
    parser = argparse.ArgumentParser(description='Sistema de descarga y almacenamiento de datos bursátiles de Alpha Vantage')
    # Definir los argumentos disponibles
    parser.add_argument('--test', action='store_true', help='Ejecutar prueba completa del sistema')
    parser.add_argument('--download', action='store_true', help='Descargar datos históricos')
    parser.add_argument('--store', action='store_true', help='Almacenar datos en la base de datos')
    parser.add_argument('--verify', action='store_true', help='Verificar datos almacenados')
    parser.add_argument('--setup-db', action='store_true', help='Configurar la base de datos')
    
    # Procesar los argumentos
    args = parser.parse_args()
    
    # Si no se especifica ninguna opción, mostrar ayuda
    if not any(vars(args).values()):
        parser.print_help()
        return
    
    # Ejecutar las acciones solicitadas
    if args.test:
        # Ejecutar prueba completa
        run_full_test()
    else:
        # Ejecutar acciones individuales según los argumentos
        if args.setup_db:
            # Configurar entorno y crear base de datos
            setup_environment()
            create_database()
        
        if args.download:
            # Descargar datos históricos
            download_stock_data()
        
        if args.store:
            # Almacenar datos en la base de datos
            store_stock_data()
        
        if args.verify:
            # Verificar datos almacenados
            verify_stored_data()

# Punto de entrada para ejecución directa del script
if __name__ == "__main__":
    main()
