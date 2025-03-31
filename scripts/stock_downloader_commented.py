#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para descargar datos históricos bursátiles de Alpha Vantage
para los símbolos NVIDIA (NVDA) y Apple (AAPL).
Versión compatible con macOS Apple Silicon usando pyodbc.

Este módulo implementa una clase para descargar datos históricos bursátiles
desde Alpha Vantage, gestionando reintentos, límites de tasa y registro de metadatos.

Autor: Manus
Fecha: 31/03/2025
"""

# Importaciones de bibliotecas estándar
import os                      # Para operaciones del sistema de archivos
import sys                     # Para funcionalidades del sistema
import time                    # Para funciones relacionadas con el tiempo
from datetime import datetime, timedelta  # Para manejo de fechas y horas
from dotenv import load_dotenv  # Para cargar variables de entorno desde archivo .env

# Importar módulos propios
# Añadimos el directorio padre al path para poder importar módulos del proyecto
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from scripts.alpha_vantage_api import AlphaVantageAPI  # Para interactuar con la API de Alpha Vantage
from scripts.metadata_manager_pyodbc import MetadataManager  # Para gestionar metadatos en la base de datos

# Cargar variables de entorno desde el archivo .env
# Esto permite mantener información sensible fuera del código
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', '.env'))

class StockDataDownloader:
    """
    Clase para descargar datos históricos bursátiles de Alpha Vantage.
    
    Esta clase coordina la descarga de datos históricos bursátiles desde Alpha Vantage,
    gestionando la interacción con la API y el registro de metadatos en la base de datos.
    
    Atributos:
        api (AlphaVantageAPI): Instancia para interactuar con la API de Alpha Vantage.
        metadata (MetadataManager): Instancia para gestionar metadatos en la base de datos.
        data_dir (str): Directorio para almacenar los datos descargados.
        output_size (str): Tamaño de salida para las solicitudes a la API ('compact' o 'full').
        api_function (str): Función de Alpha Vantage a utilizar.
    
    Mejores prácticas POO:
    - Composición: Utiliza instancias de otras clases para delegar responsabilidades
    - Separación de responsabilidades: Cada clase tiene un propósito específico
    - Configuración dinámica: Lee parámetros de configuración desde la base de datos
    """
    
    def __init__(self):
        """
        Inicializa el descargador de datos.
        
        Crea instancias de las clases necesarias, configura el directorio de datos
        y obtiene parámetros de configuración desde la base de datos.
        
        Mejores prácticas POO:
        - Inicialización completa: Configura todos los atributos necesarios en el constructor
        - Creación de recursos: Asegura que el directorio de datos exista
        """
        # Crear instancias de las clases necesarias
        self.api = AlphaVantageAPI()  # Para interactuar con la API
        self.metadata = MetadataManager()  # Para gestionar metadatos
        
        # Configurar el directorio de datos
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        # Crear directorio de datos si no existe
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Obtener configuración desde la base de datos
        # Esto permite cambiar la configuración sin modificar el código
        self.output_size = self.metadata.get_config_value('OUTPUT_SIZE') or 'full'
        self.api_function = self.metadata.get_config_value('API_FUNCTION') or 'TIME_SERIES_DAILY_ADJUSTED'
    
    def download_symbol_data(self, symbol_ticker):
        """
        Descarga datos históricos para un símbolo específico.
        
        Este método coordina todo el proceso de descarga: obtiene información del símbolo,
        determina el rango de fechas a descargar, crea un registro de descarga,
        descarga los datos (con reintentos si es necesario) y actualiza el estado.
        
        Args:
            symbol_ticker (str): Ticker del símbolo (ej. 'AAPL').
            
        Returns:
            tuple: (download_id, data, success)
                download_id: ID del registro de descarga.
                data: Datos descargados.
                success: True si la descarga fue exitosa, False en caso contrario.
        
        Mejores prácticas POO:
        - Método complejo con responsabilidad única: Gestiona todo el proceso de descarga
        - Manejo de errores robusto: Implementa reintentos y reporta errores claramente
        - Retorno de información completa: Devuelve toda la información relevante
        """
        # Informar al usuario sobre la operación
        print(f"Iniciando descarga de datos para {symbol_ticker}...")
        
        # Obtener información del símbolo desde la base de datos
        symbol_info = self.metadata.get_symbol_by_ticker(symbol_ticker)
        if not symbol_info:
            # Si el símbolo no existe en la base de datos, reportar error
            print(f"Error: El símbolo {symbol_ticker} no existe en la base de datos.")
            return None, None, False
        
        # Obtener el ID del símbolo
        symbol_id = symbol_info['SymbolID']
        
        # Obtener la última descarga para determinar desde cuándo descargar
        last_download = self.metadata.get_last_download(symbol_id)
        
        # Determinar fechas de inicio y fin para la descarga
        end_date = datetime.now().date()  # Fecha actual como fecha de fin
        
        if last_download and last_download['EndDate']:
            # Si hay una descarga previa, comenzar desde el día siguiente
            start_date = last_download['EndDate'] + timedelta(days=1)
            
            # Si la fecha de inicio es posterior a la fecha actual, no hay nada que descargar
            if start_date > end_date:
                print(f"No hay nuevos datos para descargar para {symbol_ticker}.")
                return None, None, True
        else:
            # Si no hay descarga previa, usar None para obtener todo el historial
            start_date = None
        
        # Crear registro de descarga en la base de datos
        download_id = self.metadata.add_download_record(
            symbol_id, 
            start_date=start_date, 
            end_date=end_date, 
            status='Pending'  # Estado inicial: pendiente
        )
        
        if not download_id:
            # Si no se pudo crear el registro, reportar error
            print(f"Error: No se pudo crear el registro de descarga para {symbol_ticker}.")
            return None, None, False
        
        # Configuración para reintentos
        max_retries = 3  # Número máximo de intentos
        retry_delay = 60  # Segundos de espera inicial entre reintentos
        
        # Intentar descargar datos con reintentos si es necesario
        for attempt in range(1, max_retries + 1):
            try:
                # Informar sobre el intento actual
                print(f"Intento {attempt}/{max_retries} para descargar datos de {symbol_ticker}...")
                
                # Descargar datos según la función configurada
                if self.api_function == 'TIME_SERIES_DAILY':
                    # Usar función para datos diarios no ajustados
                    data = self.api.get_daily_time_series(symbol_ticker, self.output_size)
                else:  # Por defecto, usar TIME_SERIES_DAILY_ADJUSTED
                    # Usar función para datos diarios ajustados
                    data = self.api.get_daily_adjusted(symbol_ticker, self.output_size)
                
                # Verificar si se recibieron datos
                if not data:
                    raise Exception("No se recibieron datos de Alpha Vantage.")
                
                # Verificar si hay errores en la respuesta
                if 'Error Message' in data:
                    raise Exception(f"Error de Alpha Vantage: {data['Error Message']}")
                
                # Verificar si se ha alcanzado el límite de la API
                if 'Note' in data and 'call frequency' in data['Note']:
                    print(f"Advertencia: {data['Note']}")
                    if attempt < max_retries:
                        # Si no es el último intento, esperar y reintentar
                        print(f"Esperando {retry_delay} segundos antes de reintentar...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Aumentar el tiempo de espera exponencialmente
                        continue
                
                # Si llegamos aquí, la descarga fue exitosa
                
                # Guardar datos en archivo JSON
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                json_filename = f"{symbol_ticker}_{timestamp}.json"
                json_filepath = os.path.join(self.data_dir, json_filename)
                
                # Usar el método de la API para guardar los datos
                self.api.save_to_json(data, symbol_ticker, self.data_dir)
                
                # Actualizar estado de descarga a completado
                self.metadata.update_download_status(download_id, 'Completed', end_date)
                
                # Informar sobre el éxito de la operación
                print(f"Descarga completada para {symbol_ticker}. Datos guardados en {json_filepath}")
                
                # Devolver información sobre la descarga exitosa
                return download_id, data, True
                
            except Exception as e:
                # Manejar cualquier error durante la descarga
                print(f"Error en el intento {attempt}: {e}")
                
                if attempt < max_retries:
                    # Si no es el último intento, esperar y reintentar
                    print(f"Esperando {retry_delay} segundos antes de reintentar...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Aumentar el tiempo de espera exponencialmente
                else:
                    # Si se agotaron los reintentos, reportar fallo
                    print(f"Se agotaron los reintentos para {symbol_ticker}.")
                    # Actualizar estado de descarga a fallido
                    self.metadata.update_download_status(download_id, 'Failed')
                    return download_id, None, False
    
    def download_all_pending_symbols(self):
        """
        Descarga datos para todos los símbolos pendientes.
        
        Este método identifica qué símbolos necesitan actualización según los metadatos
        y descarga datos para todos ellos.
        
        Returns:
            dict: Resultados de las descargas por símbolo.
        
        Mejores prácticas POO:
        - Método de alto nivel: Coordina operaciones complejas
        - Delegación: Utiliza otros métodos para realizar el trabajo
        - Estructura de retorno consistente: Devuelve resultados en formato estándar
        """
        # Obtener símbolos que necesitan ser descargados
        symbols_to_download = self.metadata.get_symbols_to_download()
        
        # Verificar si hay símbolos para descargar
        if not symbols_to_download:
            print("No hay símbolos pendientes para descargar.")
            return {}
        
        # Diccionario para almacenar resultados
        results = {}
        
        # Procesar cada símbolo
        for item in symbols_to_download:
            symbol = item['symbol']
            symbol_ticker = symbol['Symbol']
            
            # Informar sobre el símbolo actual
            print(f"Procesando símbolo: {symbol_ticker} ({symbol['CompanyName']})")
            
            # Descargar datos para el símbolo
            download_id, data, success = self.download_symbol_data(symbol_ticker)
            
            # Registrar resultados
            results[symbol_ticker] = {
                'download_id': download_id,
                'success': success,
                'timestamp': datetime.now().isoformat()
            }
            
            # Esperar entre descargas para respetar límites de la API
            if len(symbols_to_download) > 1:
                print("Esperando 15 segundos antes de la siguiente descarga...")
                time.sleep(15)
        
        # Devolver resultados de todas las descargas
        return results
    
    def download_specific_symbols(self, symbols):
        """
        Descarga datos para símbolos específicos.
        
        Este método permite descargar datos para una lista específica de símbolos,
        independientemente de si necesitan actualización según los metadatos.
        
        Args:
            symbols (list): Lista de tickers (ej. ['AAPL', 'NVDA']).
            
        Returns:
            dict: Resultados de las descargas por símbolo.
        
        Mejores prácticas POO:
        - Flexibilidad: Permite especificar exactamente qué símbolos descargar
        - Validación de entrada: Verifica que la lista de símbolos no esté vacía
        - Estructura similar a download_all_pending_symbols para consistencia
        """
        # Verificar que la lista de símbolos no esté vacía
        if not symbols:
            print("No se especificaron símbolos para descargar.")
            return {}
        
        # Diccionario para almacenar resultados
        results = {}
        
        # Procesar cada símbolo
        for symbol_ticker in symbols:
            # Informar sobre el símbolo actual
            print(f"Procesando símbolo: {symbol_ticker}")
            
            # Descargar datos para el símbolo
            download_id, data, success = self.download_symbol_data(symbol_ticker)
            
            # Registrar resultados
            results[symbol_ticker] = {
                'download_id': download_id,
                'success': success,
                'timestamp': datetime.now().isoformat()
            }
            
            # Esperar entre descargas para respetar límites de la API
            # Solo esperar si no es el último símbolo
            if len(symbols) > 1 and symbol_ticker != symbols[-1]:
                print("Esperando 15 segundos antes de la siguiente descarga...")
                time.sleep(15)
        
        # Devolver resultados de todas las descargas
        return results

# Función principal
def main():
    """
    Función principal para descargar datos históricos.
    
    Esta función demuestra el uso básico de la clase StockDataDownloader
    descargando datos para NVIDIA y Apple.
    
    Mejores prácticas:
    - Incluir una función main() para permitir pruebas independientes del módulo
    - Usar if __name__ == "__main__" para permitir importación sin ejecución
    - Código de ejemplo claro y conciso
    """
    # Crear una instancia del descargador de datos
    downloader = StockDataDownloader()
    
    # Definir los símbolos a descargar
    symbols = ['NVDA', 'AAPL']
    print(f"Descargando datos para los símbolos: {', '.join(symbols)}")
    
    # Descargar datos para los símbolos especificados
    results = downloader.download_specific_symbols(symbols)
    
    # Mostrar resultados
    print("\nResultados de las descargas:")
    for symbol, result in results.items():
        status = "Exitosa" if result['success'] else "Fallida"
        print(f"  - {symbol}: {status}")

# Punto de entrada para ejecución directa del script
if __name__ == "__main__":
    main()
