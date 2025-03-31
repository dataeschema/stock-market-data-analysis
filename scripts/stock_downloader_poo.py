#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Implementación del descargador de datos bursátiles utilizando el cliente de API.
Versión compatible con macOS Apple Silicon.

Este módulo implementa una clase para descargar datos históricos bursátiles
desde Alpha Vantage y guardarlos en archivos JSON.

Autor: Manus
Fecha: 31/03/2025
"""

# Importaciones de bibliotecas estándar
import os                      # Para operaciones del sistema de archivos
import sys                     # Para funcionalidades del sistema
import logging                 # Para registro de eventos
from datetime import datetime, timedelta  # Para manejo de fechas y horas
from typing import Dict, List, Any, Optional, Tuple, Union  # Para tipado estático

# Importar módulos propios
# Añadimos el directorio padre al path para poder importar módulos del proyecto
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from scripts.base import Subject, Observer  # Importar clases base
from scripts.api_client import AlphaVantageClient  # Importar cliente de API
from scripts.metadata_manager_poo import MetadataManager  # Importar gestor de metadatos

# Crear logger para este módulo
logger = logging.getLogger(__name__)

class DownloadProgressObserver(Observer):
    """
    Observador para el progreso de descarga.
    
    Esta clase implementa la interfaz Observer para recibir notificaciones
    sobre el progreso de las descargas.
    
    Mejores prácticas POO:
    - Patrón Observer: Implementa la interfaz Observer
    - Principio de responsabilidad única: Solo se encarga de mostrar el progreso
    """
    
    def update(self, subject: 'Subject', *args, **kwargs) -> None:
        """
        Actualiza el observador cuando el sujeto cambia.
        
        Args:
            subject (Subject): Sujeto que ha cambiado.
            *args: Argumentos adicionales.
            **kwargs: Argumentos de palabra clave adicionales.
        """
        # Extraer información del progreso
        symbol = kwargs.get('symbol', 'Desconocido')
        status = kwargs.get('status', 'Desconocido')
        progress = kwargs.get('progress', 0)
        total = kwargs.get('total', 0)
        
        # Mostrar información de progreso
        if total > 0:
            percentage = (progress / total) * 100
            logger.info(f"Progreso de descarga para {symbol}: {percentage:.1f}% ({progress}/{total}) - {status}")
        else:
            logger.info(f"Progreso de descarga para {symbol}: {status}")

class StockDataDownloader(Subject):
    """
    Clase para descargar datos históricos bursátiles desde Alpha Vantage.
    
    Esta clase proporciona métodos para descargar datos históricos bursátiles
    desde Alpha Vantage y guardarlos en archivos JSON, utilizando el patrón Observer
    para notificar sobre el progreso de las descargas.
    
    Atributos:
        api_client (AlphaVantageClient): Cliente de API para Alpha Vantage.
        metadata (MetadataManager): Gestor de metadatos.
        data_dir (str): Directorio donde se guardarán los archivos JSON.
    
    Mejores prácticas POO:
    - Patrón Observer: Implementa el patrón de diseño Observer para notificar progreso
    - Composición: Utiliza instancias de otras clases para delegar responsabilidades
    - Principio de responsabilidad única: Solo se encarga de la descarga de datos
    """
    
    def __init__(self):
        """
        Inicializa el descargador de datos.
        
        Crea instancias del cliente de API y del gestor de metadatos,
        y configura el directorio de datos.
        """
        # Inicializar la clase base Subject
        super().__init__()
        
        # Crear instancias de las clases necesarias
        self.api_client = AlphaVantageClient()
        self.metadata = MetadataManager()
        
        # Configurar el directorio de datos
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Añadir un observador para el progreso
        self.attach(DownloadProgressObserver())
        
        logger.info("Descargador de datos inicializado")
    
    def download_symbol_data(self, symbol: str, output_size: str = 'full') -> Dict[str, Any]:
        """
        Descarga datos históricos para un símbolo específico.
        
        Args:
            symbol (str): Ticker del símbolo (ej. 'AAPL').
            output_size (str): 'compact' para los últimos 100 puntos de datos, 
                              'full' para todo el historial.
            
        Returns:
            dict: Resultados de la descarga, incluyendo éxito, ruta del archivo y errores.
        """
        # Inicializar resultado
        result = {
            'symbol': symbol,
            'success': False,
            'file_path': None,
            'error': None,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # Notificar inicio de descarga
            self.notify(symbol=symbol, status='Iniciando', progress=0, total=3)
            
            # Obtener información del símbolo
            symbol_info = self.metadata.get_symbol_by_ticker(symbol)
            if not symbol_info:
                # Si el símbolo no existe en la base de datos, añadirlo
                logger.info(f"El símbolo {symbol} no existe en la base de datos, añadiéndolo...")
                symbol_id = self.metadata.add_symbol(symbol, f"Empresa {symbol}", True)
                if not symbol_id:
                    raise Exception(f"No se pudo añadir el símbolo {symbol} a la base de datos")
                symbol_info = self.metadata.get_symbol_by_id(symbol_id)
            
            # Obtener el ID del símbolo
            symbol_id = symbol_info['SymbolID']
            
            # Notificar progreso
            self.notify(symbol=symbol, status='Obteniendo datos', progress=1, total=3)
            
            # Obtener la función de API a utilizar desde la configuración
            api_function = self.metadata.get_config_value('API_FUNCTION')
            if not api_function:
                api_function = 'TIME_SERIES_DAILY_ADJUSTED'  # Valor por defecto
            
            # Añadir registro de descarga
            download_id = self.metadata.add_download_record(symbol_id, status='In Progress')
            if not download_id:
                raise Exception(f"No se pudo añadir registro de descarga para {symbol}")
            
            # Descargar datos según la función de API configurada
            if api_function == 'TIME_SERIES_DAILY':
                data = self.api_client.get_daily_time_series(symbol, output_size)
            else:  # Por defecto, usar TIME_SERIES_DAILY_ADJUSTED
                data = self.api_client.get_daily_adjusted(symbol, output_size)
            
            # Verificar si se obtuvieron datos válidos
            if not data or 'Time Series (Daily)' not in data:
                error_message = "No se obtuvieron datos válidos"
                if 'Error Message' in data:
                    error_message = data['Error Message']
                elif 'Note' in data:
                    error_message = data['Note']
                
                # Actualizar registro de descarga como fallido
                self.metadata.update_download_status(download_id, 'Failed')
                
                # Actualizar resultado con error
                result['error'] = error_message
                logger.error(f"Error al descargar datos para {symbol}: {error_message}")
                return result
            
            # Notificar progreso
            self.notify(symbol=symbol, status='Guardando datos', progress=2, total=3)
            
            # Guardar datos en archivo JSON
            file_path = self.api_client.save_to_json(data, symbol, self.data_dir)
            if not file_path:
                raise Exception(f"No se pudieron guardar los datos para {symbol}")
            
            # Actualizar registro de descarga como completado
            self.metadata.update_download_status(download_id, 'Completed')
            
            # Notificar finalización
            self.notify(symbol=symbol, status='Completado', progress=3, total=3)
            
            # Actualizar resultado con éxito
            result['success'] = True
            result['file_path'] = file_path
            logger.info(f"Datos descargados exitosamente para {symbol} y guardados en {file_path}")
            
        except Exception as e:
            # Manejar cualquier error durante la descarga
            error_message = str(e)
            result['error'] = error_message
            logger.error(f"Error al descargar datos para {symbol}: {error_message}")
            
            # Intentar actualizar el registro de descarga como fallido si existe
            try:
                if 'download_id' in locals() and download_id:
                    self.metadata.update_download_status(download_id, 'Failed')
            except Exception as update_error:
                logger.error(f"Error al actualizar estado de descarga: {update_error}")
        
        return result
    
    def download_all_symbols(self) -> Dict[str, Dict[str, Any]]:
        """
        Descarga datos históricos para todos los símbolos activos.
        
        Returns:
            dict: Resultados de la descarga por símbolo.
        """
        # Obtener símbolos que necesitan ser descargados
        symbols_to_download = self.metadata.get_symbols_to_download()
        
        # Verificar si hay símbolos para descargar
        if not symbols_to_download:
            logger.info("No hay símbolos para descargar")
            return {}
        
        # Inicializar resultados
        results = {}
        
        # Obtener el tamaño de salida desde la configuración
        output_size = self.metadata.get_config_value('OUTPUT_SIZE')
        if not output_size:
            output_size = 'full'  # Valor por defecto
        
        # Descargar datos para cada símbolo
        for i, item in enumerate(symbols_to_download):
            symbol_info = item['symbol']
            symbol = symbol_info['Symbol']
            
            # Mostrar progreso general
            logger.info(f"Descargando datos para {symbol} ({i+1}/{len(symbols_to_download)})")
            
            # Descargar datos para el símbolo
            result = self.download_symbol_data(symbol, output_size)
            
            # Guardar resultado
            results[symbol] = result
        
        return results
    
    def download_specific_symbols(self, symbols: List[str], output_size: str = 'full') -> Dict[str, Dict[str, Any]]:
        """
        Descarga datos históricos para símbolos específicos.
        
        Args:
            symbols (list): Lista de tickers de símbolos (ej. ['AAPL', 'NVDA']).
            output_size (str): 'compact' para los últimos 100 puntos de datos, 
                              'full' para todo el historial.
            
        Returns:
            dict: Resultados de la descarga por símbolo.
        """
        # Verificar si hay símbolos para descargar
        if not symbols:
            logger.info("No se proporcionaron símbolos para descargar")
            return {}
        
        # Inicializar resultados
        results = {}
        
        # Descargar datos para cada símbolo
        for i, symbol in enumerate(symbols):
            # Mostrar progreso general
            logger.info(f"Descargando datos para {symbol} ({i+1}/{len(symbols)})")
            
            # Descargar datos para el símbolo
            result = self.download_symbol_data(symbol, output_size)
            
            # Guardar resultado
            results[symbol] = result
        
        return results

# Función principal para probar el descargador de datos
def main():
    """
    Función principal para probar el descargador de datos.
    
    Esta función demuestra el uso básico del descargador de datos.
    """
    # Configurar logging para mostrar mensajes en consola
    logging.basicConfig(level=logging.INFO)
    
    # Crear una instancia del descargador de datos
    downloader = StockDataDownloader()
    
    # Descargar datos para símbolos específicos
    symbols = ['AAPL', 'NVDA']
    print(f"Descargando datos para: {', '.join(symbols)}")
    
    results = downloader.download_specific_symbols(symbols, 'compact')
    
    # Mostrar resultados
    print("\nResultados de la descarga:")
    for symbol, result in results.items():
        status = "Exitosa" if result['success'] else "Fallida"
        print(f"  - {symbol}: {status}")
        if result['success']:
            print(f"    Archivo: {result['file_path']}")
        else:
            print(f"    Error: {result['error']}")

# Punto de entrada para ejecución directa del script
if __name__ == "__main__":
    main()
