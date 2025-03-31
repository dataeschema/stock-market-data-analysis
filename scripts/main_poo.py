#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script principal para ejecutar el sistema completo de descarga y almacenamiento
de datos históricos bursátiles de Alpha Vantage.
Versión compatible con macOS Apple Silicon usando las mejores prácticas de POO.

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
import logging                 # Para registro de eventos
from datetime import datetime  # Para manejo de fechas y horas

# Importar módulos propios
# Añadimos el directorio padre al path para poder importar módulos del proyecto
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# Importamos los módulos necesarios para cada componente del sistema
from scripts.db_connector import SQLServerConnector  # Para conectar con SQL Server
from scripts.api_client import AlphaVantageClient  # Para interactuar con la API de Alpha Vantage
from scripts.metadata_manager_poo import MetadataManager  # Para gestionar metadatos en la base de datos
from scripts.stock_downloader_poo import StockDataDownloader  # Para descargar datos históricos
from scripts.stock_storage_poo import StockDataStorage  # Para almacenar datos en la base de datos
from scripts.base import ConfigManager  # Para gestionar la configuración

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', 'alpha_vantage.log')),
        logging.StreamHandler()
    ]
)

# Crear logger para este módulo
logger = logging.getLogger(__name__)

def setup_environment():
    """
    Configura el entorno de ejecución.
    
    Este método verifica la existencia del archivo de configuración .env
    y crea los directorios necesarios para el funcionamiento del sistema.
    
    Returns:
        bool: True si la configuración fue exitosa, False en caso contrario.
    """
    # Informar al usuario sobre la operación
    logger.info("=== Configurando entorno ===")
    
    # Verificar si existe el archivo .env
    # Este archivo contiene las credenciales y configuraciones necesarias
    env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', '.env')
    if not os.path.exists(env_file):
        # Advertir al usuario si no se encuentra el archivo
        logger.warning("No se encontró el archivo .env")
        logger.info("Por favor, crea el archivo .env basado en .env.example con tus credenciales")
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
    logger.info("Entorno configurado correctamente")
    return True

def test_database_connection():
    """
    Prueba la conexión con la base de datos SQL Server.
    
    Returns:
        bool: True si la conexión fue exitosa, False en caso contrario.
    """
    logger.info("\n=== Probando conexión con SQL Server ===")
    
    # Crear una instancia del conector de base de datos
    db = SQLServerConnector()
    
    # Verificar si la conexión fue exitosa
    if db.conn:
        logger.info("Conexión con SQL Server establecida correctamente")
        
        try:
            # Ejecutar una consulta de prueba para obtener la versión de SQL Server
            query = "SELECT @@VERSION AS Version"
            result = db.fetch_one_as_dict(query)
            
            if result:
                logger.info(f"Versión de SQL Server: {result['Version'][:50]}...")
            else:
                logger.warning("No se pudo obtener la versión de SQL Server")
                
            # Cerrar la conexión
            db.disconnect()
            return True
            
        except Exception as e:
            logger.error(f"Error al ejecutar consulta: {e}")
            return False
    else:
        logger.error("No se pudo establecer conexión con SQL Server")
        return False

def test_api_connection():
    """
    Prueba la conexión con la API de Alpha Vantage.
    
    Returns:
        bool: True si la conexión fue exitosa, False en caso contrario.
    """
    logger.info("\n=== Probando conexión con Alpha Vantage ===")
    
    # Crear una instancia del cliente de API
    api = AlphaVantageClient()
    
    # Verificar que la API key esté configurada
    if not api.api_key:
        logger.error("No se ha configurado la API key de Alpha Vantage")
        logger.info("Por favor, configura ALPHA_VANTAGE_API_KEY en el archivo .env")
        return False
    
    # Probar con un símbolo conocido
    symbol = 'AAPL'
    logger.info(f"Obteniendo datos de prueba para {symbol}...")
    
    try:
        # Obtener datos para el símbolo
        data = api.get_daily_adjusted(symbol, 'compact')
        
        # Verificar si se obtuvieron datos válidos
        if data and 'Time Series (Daily)' in data:
            # Extraer las fechas para mostrar información
            dates = list(data['Time Series (Daily)'].keys())
            logger.info(f"Conexión exitosa. Se obtuvieron datos para {len(dates)} días.")
            logger.info(f"Primer día: {dates[0]}")
            return True
        else:
            # Informar sobre el error
            logger.error("Error al obtener datos de Alpha Vantage")
            if data and 'Error Message' in data:
                logger.error(f"Error: {data['Error Message']}")
            elif data and 'Note' in data:
                logger.error(f"Nota: {data['Note']}")
            return False
            
    except Exception as e:
        logger.error(f"Error al conectar con Alpha Vantage: {e}")
        return False

def test_metadata_manager():
    """
    Prueba el gestor de metadatos.
    
    Returns:
        bool: True si la prueba fue exitosa, False en caso contrario.
    """
    logger.info("\n=== Probando gestor de metadatos ===")
    
    # Crear una instancia del gestor de metadatos
    metadata = MetadataManager()
    
    try:
        # Obtener símbolos para verificar la conexión con la base de datos
        symbols = metadata.get_all_symbols()
        logger.info(f"Símbolos en la base de datos: {len(symbols)}")
        
        # Mostrar información sobre cada símbolo
        for symbol in symbols:
            logger.info(f"  - {symbol['Symbol']} ({symbol['CompanyName']})")
        
        # Verificar configuración
        api_function = metadata.get_config_value('API_FUNCTION')
        output_size = metadata.get_config_value('OUTPUT_SIZE')
        
        # Mostrar información sobre la configuración
        logger.info(f"Configuración: API_FUNCTION={api_function}, OUTPUT_SIZE={output_size}")
        
        # Considerar exitosa la prueba si se encontraron símbolos
        return len(symbols) > 0
        
    except Exception as e:
        logger.error(f"Error al probar el gestor de metadatos: {e}")
        return False

def download_stock_data():
    """
    Descarga datos históricos bursátiles.
    
    Returns:
        bool: True si la descarga fue exitosa, False en caso contrario.
    """
    logger.info("\n=== Descargando datos históricos ===")
    
    # Crear una instancia del descargador de datos
    downloader = StockDataDownloader()
    
    try:
        # Definir los símbolos a descargar
        symbols = ['NVDA', 'AAPL']
        logger.info(f"Descargando datos para los símbolos: {', '.join(symbols)}")
        
        # Descargar datos para los símbolos especificados
        results = downloader.download_specific_symbols(symbols)
        
        # Verificar resultados
        success_count = 0
        for symbol, result in results.items():
            # Determinar si la descarga fue exitosa
            status = "Exitosa" if result['success'] else "Fallida"
            logger.info(f"  - {symbol}: {status}")
            if result['success']:
                logger.info(f"    Archivo: {result['file_path']}")
                success_count += 1
            else:
                logger.error(f"    Error: {result['error']}")
        
        # Considerar exitosa la operación si todas las descargas fueron exitosas
        return success_count == len(symbols)
        
    except Exception as e:
        logger.error(f"Error al descargar datos históricos: {e}")
        return False

def store_stock_data():
    """
    Almacena los datos históricos en la base de datos.
    
    Returns:
        bool: True si el almacenamiento fue exitoso, False en caso contrario.
    """
    logger.info("\n=== Almacenando datos en la base de datos ===")
    
    # Crear una instancia del almacenamiento de datos
    storage = StockDataStorage()
    
    try:
        # Procesar todos los archivos JSON
        results = storage.process_all_json_files()
        
        # Mostrar resultados
        total_inserted = 0
        for file, result in results.items():
            logger.info(f"  - {file}: {result['inserted_count']} registros insertados")
            total_inserted += result['inserted_count']
        
        # Mostrar el total de registros insertados
        logger.info(f"Total de registros insertados: {total_inserted}")
        
        # Considerar exitosa la operación si se insertaron registros
        return total_inserted > 0
        
    except Exception as e:
        logger.error(f"Error al almacenar datos en la base de datos: {e}")
        return False

def verify_stored_data():
    """
    Verifica los datos almacenados en la base de datos.
    
    Returns:
        bool: True si la verificación fue exitosa, False en caso contrario.
    """
    logger.info("\n=== Verificando datos almacenados ===")
    
    # Crear una instancia del almacenamiento de datos
    storage = StockDataStorage()
    
    try:
        # Definir los símbolos a verificar
        symbols = ['NVDA', 'AAPL']
        success_count = 0
        
        # Verificar cada símbolo
        for symbol in symbols:
            logger.info(f"Verificando datos para {symbol}:")
            # Obtener los últimos 5 registros para el símbolo
            df = storage.get_stock_data_from_db(symbol, limit=5)
            
            # Verificar si se encontraron datos
            if not df.empty:
                logger.info(f"Se encontraron {len(df)} registros para {symbol}")
                # Mostrar los primeros registros
                logger.info(f"Primeros registros:\n{df.head().to_string()}")
                success_count += 1
            else:
                logger.warning(f"No se encontraron datos para {symbol}")
        
        # Considerar exitosa la verificación si se encontraron datos para todos los símbolos
        return success_count == len(symbols)
        
    except Exception as e:
        logger.error(f"Error al verificar los datos almacenados: {e}")
        return False

def run_full_test():
    """
    Ejecuta una prueba completa del sistema.
    
    Returns:
        bool: True si la prueba completa fue exitosa, False en caso contrario.
    """
    logger.info("=== INICIANDO PRUEBA COMPLETA DEL SISTEMA ===")
    logger.info(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Configurar entorno
    if not setup_environment():
        logger.error("Error al configurar el entorno. Abortando prueba.")
        return False
    
    # Probar conexión con la base de datos
    if not test_database_connection():
        logger.error("Error al conectar con SQL Server. Abortando prueba.")
        return False
    
    # Probar conexión con API
    if not test_api_connection():
        logger.error("Error al conectar con Alpha Vantage. Abortando prueba.")
        return False
    
    # Probar gestor de metadatos
    if not test_metadata_manager():
        logger.error("Error al probar el gestor de metadatos. Abortando prueba.")
        return False
    
    # Descargar datos
    if not download_stock_data():
        logger.error("Error al descargar datos históricos. Abortando prueba.")
        return False
    
    # Almacenar datos
    if not store_stock_data():
        logger.error("Error al almacenar datos en la base de datos. Abortando prueba.")
        return False
    
    # Verificar datos almacenados
    if not verify_stored_data():
        logger.error("Error al verificar los datos almacenados. Abortando prueba.")
        return False
    
    # Informar sobre el éxito de la prueba completa
    logger.info("\n=== PRUEBA COMPLETA EXITOSA ===")
    logger.info("Todos los componentes del sistema funcionan correctamente.")
    return True

def main():
    """
    Función principal.
    
    Esta función procesa los argumentos de línea de comandos y ejecuta
    las funcionalidades correspondientes del sistema.
    """
    # Crear un parser de argumentos
    parser = argparse.ArgumentParser(description='Sistema de descarga y almacenamiento de datos bursátiles de Alpha Vantage')
    # Definir los argumentos disponibles
    parser.add_argument('--test', action='store_true', help='Ejecutar prueba completa del sistema')
    parser.add_argument('--download', action='store_true', help='Descargar datos históricos')
    parser.add_argument('--store', action='store_true', help='Almacenar datos en la base de datos')
    parser.add_argument('--verify', action='store_true', help='Verificar datos almacenados')
    parser.add_argument('--setup', action='store_true', help='Configurar el entorno')
    parser.add_argument('--test-db', action='store_true', help='Probar conexión con la base de datos')
    parser.add_argument('--test-api', action='store_true', help='Probar conexión con Alpha Vantage')
    
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
        if args.setup:
            # Configurar entorno
            setup_environment()
        
        if args.test_db:
            # Probar conexión con la base de datos
            test_database_connection()
        
        if args.test_api:
            # Probar conexión con Alpha Vantage
            test_api_connection()
        
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
