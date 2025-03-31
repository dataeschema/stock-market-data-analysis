#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Implementación del almacenador de datos bursátiles utilizando el conector de base de datos.
Versión compatible con macOS Apple Silicon.

Este módulo implementa una clase para procesar y almacenar datos históricos bursátiles
en una base de datos SQL Server, con optimizaciones para inserciones masivas.

Autor: Manus
Fecha: 31/03/2025
"""

# Importaciones de bibliotecas estándar
import os                      # Para operaciones del sistema de archivos
import sys                     # Para funcionalidades del sistema
import json                    # Para manejo de datos JSON
import logging                 # Para registro de eventos
import pandas as pd            # Para manipulación y análisis de datos
from datetime import datetime  # Para manejo de fechas y horas
from typing import Dict, List, Any, Optional, Tuple, Union  # Para tipado estático

# Importar módulos propios
# Añadimos el directorio padre al path para poder importar módulos del proyecto
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from scripts.base import Subject, Observer, DataProcessor  # Importar clases base
from scripts.db_connector import SQLServerConnector  # Importar conector de base de datos
from scripts.metadata_manager_poo import MetadataManager  # Importar gestor de metadatos

# Crear logger para este módulo
logger = logging.getLogger(__name__)

class StorageProgressObserver(Observer):
    """
    Observador para el progreso de almacenamiento.
    
    Esta clase implementa la interfaz Observer para recibir notificaciones
    sobre el progreso del almacenamiento de datos.
    
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
            logger.info(f"Progreso de almacenamiento para {symbol}: {percentage:.1f}% ({progress}/{total}) - {status}")
        else:
            logger.info(f"Progreso de almacenamiento para {symbol}: {status}")

class JSONProcessor(DataProcessor):
    """
    Procesador de datos JSON.
    
    Esta clase implementa la interfaz DataProcessor para procesar archivos JSON
    con datos históricos bursátiles.
    
    Mejores prácticas POO:
    - Implementación de interfaz: Implementa todos los métodos definidos en DataProcessor
    - Principio de responsabilidad única: Solo se encarga del procesamiento de datos JSON
    """
    
    def process(self, data: Any) -> Any:
        """
        Procesa los datos JSON.
        
        Args:
            data (Any): Datos a procesar (ruta al archivo JSON o datos JSON).
            
        Returns:
            Any: Datos procesados (símbolo y lista de datos históricos).
        """
        # Verificar si data es una ruta a un archivo
        if isinstance(data, str) and os.path.isfile(data):
            # Cargar datos desde el archivo
            try:
                with open(data, 'r') as f:
                    json_data = json.load(f)
                
                # Extraer el símbolo del nombre del archivo
                filename = os.path.basename(data)
                symbol = filename.split('_')[0]  # Primera parte antes del primer guion bajo
                
                # Procesar los datos JSON
                return self._process_json_data(json_data, symbol)
                
            except Exception as e:
                logger.error(f"Error al procesar el archivo JSON {data}: {e}")
                return None, None
        
        # Si data ya es un diccionario JSON
        elif isinstance(data, dict):
            # Extraer el símbolo de los metadatos si está disponible
            symbol = None
            if 'Meta Data' in data and '2. Symbol' in data['Meta Data']:
                symbol = data['Meta Data']['2. Symbol']
            
            # Procesar los datos JSON
            return self._process_json_data(data, symbol)
        
        else:
            logger.error(f"Tipo de datos no soportado: {type(data)}")
            return None, None
    
    def _process_json_data(self, data: Dict[str, Any], symbol: Optional[str] = None) -> Tuple[Optional[str], Optional[List[Dict[str, Any]]]]:
        """
        Procesa los datos JSON y extrae los datos históricos.
        
        Args:
            data (dict): Datos JSON.
            symbol (str, optional): Símbolo de la acción.
            
        Returns:
            tuple: (symbol, data_list)
                symbol: Símbolo de la acción.
                data_list: Lista de diccionarios con los datos históricos.
        """
        try:
            # Verificar si es TIME_SERIES_DAILY o TIME_SERIES_DAILY_ADJUSTED
            if 'Time Series (Daily)' in data:
                # Extraer la serie temporal
                time_series = data['Time Series (Daily)']
                
                # Determinar si es ajustado o no basado en las claves
                # Los datos ajustados tienen una clave '5. adjusted close'
                is_adjusted = False
                for date, values in time_series.items():
                    if '5. adjusted close' in values:
                        is_adjusted = True
                    break
                
                # Lista para almacenar los datos procesados
                data_list = []
                
                # Procesar cada fecha y sus valores
                for date, values in time_series.items():
                    # Crear un diccionario con los datos básicos
                    item = {
                        'date': date,                          # Fecha como string (YYYY-MM-DD)
                        'open': float(values['1. open']),      # Precio de apertura como float
                        'high': float(values['2. high']),      # Precio más alto como float
                        'low': float(values['3. low']),        # Precio más bajo como float
                        'close': float(values['4. close']),    # Precio de cierre como float
                        'volume': int(values['5. volume' if not is_adjusted else '6. volume'])  # Volumen como entero
                    }
                    
                    # Añadir precio de cierre ajustado si está disponible
                    if is_adjusted:
                        item['adjusted_close'] = float(values['5. adjusted close'])
                    else:
                        # Si no hay ajustado, usar el cierre normal
                        item['adjusted_close'] = item['close']
                    
                    # Añadir el item a la lista
                    data_list.append(item)
                
                # Devolver el símbolo y la lista de datos
                return symbol, data_list
            else:
                # Si el formato no es reconocido, reportar error
                logger.error(f"Formato de datos no reconocido")
                return None, None
                
        except Exception as e:
            # Manejar cualquier error durante el procesamiento
            logger.error(f"Error al procesar los datos JSON: {e}")
            return None, None
    
    def validate(self, data: Any) -> bool:
        """
        Valida los datos.
        
        Args:
            data (Any): Datos a validar.
            
        Returns:
            bool: True si los datos son válidos, False en caso contrario.
        """
        # Verificar si data es una ruta a un archivo
        if isinstance(data, str) and os.path.isfile(data):
            try:
                # Intentar cargar el archivo JSON
                with open(data, 'r') as f:
                    json_data = json.load(f)
                
                # Verificar si contiene la serie temporal
                return 'Time Series (Daily)' in json_data
                
            except Exception as e:
                logger.error(f"Error al validar el archivo JSON {data}: {e}")
                return False
        
        # Si data ya es un diccionario JSON
        elif isinstance(data, dict):
            # Verificar si contiene la serie temporal
            return 'Time Series (Daily)' in data
        
        else:
            logger.error(f"Tipo de datos no soportado para validación: {type(data)}")
            return False

class StockDataStorage(Subject):
    """
    Clase para almacenar datos históricos bursátiles en SQL Server.
    
    Esta clase proporciona métodos para procesar archivos JSON con datos históricos
    bursátiles y almacenarlos en la base de datos SQL Server, con optimizaciones
    para inserciones masivas y manejo de errores.
    
    Atributos:
        db (SQLServerConnector): Conector de base de datos SQL Server.
        metadata (MetadataManager): Gestor de metadatos.
        processor (JSONProcessor): Procesador de datos JSON.
        data_dir (str): Directorio donde se encuentran los archivos JSON a procesar.
    
    Mejores prácticas POO:
    - Patrón Observer: Implementa el patrón de diseño Observer para notificar progreso
    - Composición: Utiliza instancias de otras clases para delegar responsabilidades
    - Principio de responsabilidad única: Solo se encarga del almacenamiento de datos
    """
    
    def __init__(self):
        """
        Inicializa el almacenamiento de datos.
        
        Crea instancias del conector de base de datos, gestor de metadatos y procesador de datos,
        y configura el directorio de datos.
        """
        # Inicializar la clase base Subject
        super().__init__()
        
        # Crear instancias de las clases necesarias
        self.db = SQLServerConnector()
        self.metadata = MetadataManager()
        self.processor = JSONProcessor()
        
        # Configurar el directorio de datos
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        
        # Añadir un observador para el progreso
        self.attach(StorageProgressObserver())
        
        logger.info("Almacenamiento de datos inicializado")
    
    def store_stock_data(self, symbol: str, data_list: List[Dict[str, Any]], download_id: int) -> int:
        """
        Almacena los datos históricos en la base de datos.
        
        Este método inserta los datos históricos en la tabla AVdata.StockPrices,
        optimizando el proceso mediante inserciones por lotes.
        
        Args:
            symbol (str): Símbolo de la acción.
            data_list (list): Lista de diccionarios con los datos históricos.
            download_id (int): ID del registro de descarga.
            
        Returns:
            int: Número de registros insertados.
        """
        # Verificar que haya datos para almacenar
        if not data_list:
            logger.warning(f"No hay datos para almacenar para el símbolo {symbol}")
            return 0
        
        # Obtener el ID del símbolo
        symbol_info = self.metadata.get_symbol_by_ticker(symbol)
        if not symbol_info:
            # Si el símbolo no existe en la base de datos, reportar error
            logger.error(f"Error: El símbolo {symbol} no existe en la base de datos.")
            return 0
        
        # Obtener el ID del símbolo
        symbol_id = symbol_info['SymbolID']
        
        # Notificar inicio de almacenamiento
        self.notify(symbol=symbol, status='Iniciando', progress=0, total=len(data_list))
        
        # Preparar los datos para inserción masiva
        # Creamos una lista de tuplas con los valores a insertar
        values_list = []
        for item in data_list:
            values_list.append((
                symbol_id,                # ID del símbolo
                item['date'],             # Fecha
                item['open'],             # Precio de apertura
                item['high'],             # Precio más alto
                item['low'],              # Precio más bajo
                item['close'],            # Precio de cierre
                item['adjusted_close'],   # Precio de cierre ajustado
                item['volume'],           # Volumen
                download_id               # ID de la descarga
            ))
        
        # Configuración para inserciones por lotes
        batch_size = 1000  # Tamaño de cada lote
        total_inserted = 0  # Contador de registros insertados
        
        try:
            # Procesar los datos en lotes para optimizar rendimiento
            for i in range(0, len(values_list), batch_size):
                # Obtener un lote de valores
                batch = values_list[i:i+batch_size]
                
                # Consulta SQL parametrizada para la inserción
                query = """
                INSERT INTO AVdata.StockPrices 
                (SymbolID, Date, Open, High, Low, Close, AdjustedClose, Volume, DownloadID)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                # Ejecutar la consulta para cada fila en el lote
                self.db.execute_many(query, batch)
                self.db.commit()  # Confirmar la transacción
                
                # Actualizar contador y mostrar progreso
                total_inserted += len(batch)
                
                # Notificar progreso
                self.notify(symbol=symbol, status='Insertando', progress=total_inserted, total=len(values_list))
            
            # Notificar finalización
            self.notify(symbol=symbol, status='Completado', progress=total_inserted, total=len(values_list))
            
            # Devolver el número total de registros insertados
            return total_inserted
            
        except Exception as e:
            # En caso de error, revertir la transacción
            self.db.rollback()
            logger.error(f"Error al insertar datos para {symbol}: {e}")
            
            # Notificar error
            self.notify(symbol=symbol, status=f"Error: {e}", progress=total_inserted, total=len(values_list))
            
            return total_inserted
    
    def process_and_store_file(self, json_file: str, download_id: int) -> int:
        """
        Procesa un archivo JSON y almacena los datos en la base de datos.
        
        Este método combina el procesamiento del archivo JSON y el almacenamiento
        de los datos en la base de datos en una sola operación.
        
        Args:
            json_file (str): Ruta al archivo JSON.
            download_id (int): ID del registro de descarga.
            
        Returns:
            int: Número de registros insertados.
        """
        # Validar el archivo JSON
        if not self.processor.validate(json_file):
            logger.error(f"El archivo {json_file} no es válido")
            return 0
        
        # Procesar el archivo JSON para obtener los datos
        symbol, data_list = self.processor.process(json_file)
        
        # Verificar que se hayan obtenido datos válidos
        if not symbol or not data_list:
            logger.error(f"No se pudieron procesar los datos del archivo {json_file}")
            return 0
        
        # Almacenar los datos en la base de datos
        return self.store_stock_data(symbol, data_list, download_id)
    
    def process_all_json_files(self) -> Dict[str, Dict[str, Any]]:
        """
        Procesa todos los archivos JSON en el directorio de datos.
        
        Este método busca todos los archivos JSON en el directorio de datos,
        los procesa y almacena en la base de datos, y luego los mueve a un
        subdirectorio 'processed'.
        
        Returns:
            dict: Resultados del procesamiento por archivo.
        """
        # Verificar que el directorio de datos exista
        if not os.path.exists(self.data_dir):
            logger.error(f"El directorio de datos {self.data_dir} no existe.")
            return {}
        
        # Obtener la lista de archivos JSON en el directorio
        json_files = [f for f in os.listdir(self.data_dir) if f.endswith('.json')]
        
        # Verificar que haya archivos para procesar
        if not json_files:
            logger.info(f"No hay archivos JSON en el directorio {self.data_dir}")
            return {}
        
        # Diccionario para almacenar resultados
        results = {}
        
        # Procesar cada archivo JSON
        for i, json_file in enumerate(json_files):
            # Ruta completa al archivo
            file_path = os.path.join(self.data_dir, json_file)
            logger.info(f"Procesando archivo {i+1}/{len(json_files)}: {json_file}")
            
            # Extraer el símbolo del nombre del archivo
            symbol = json_file.split('_')[0]
            
            # Obtener el último registro de descarga para el símbolo
            symbol_info = self.metadata.get_symbol_by_ticker(symbol)
            if not symbol_info:
                # Si el símbolo no existe en la base de datos, reportar error
                logger.error(f"Error: El símbolo {symbol} no existe en la base de datos.")
                continue
            
            # Obtener el último registro de descarga
            last_download = self.metadata.get_last_download(symbol_info['SymbolID'])
            if not last_download:
                # Si no hay registros de descarga, reportar error
                logger.error(f"Error: No hay registros de descarga para el símbolo {symbol}.")
                continue
            
            # Obtener el ID de la descarga
            download_id = last_download['DownloadID']
            
            # Procesar y almacenar los datos
            inserted_count = self.process_and_store_file(file_path, download_id)
            
            # Registrar resultados
            results[json_file] = {
                'symbol': symbol,
                'download_id': download_id,
                'inserted_count': inserted_count,
                'timestamp': datetime.now().isoformat()
            }
            
            # Mover el archivo procesado a un subdirectorio 'processed'
            processed_dir = os.path.join(self.data_dir, 'processed')
            os.makedirs(processed_dir, exist_ok=True)
            
            try:
                # Mover el archivo
                os.rename(file_path, os.path.join(processed_dir, json_file))
                logger.info(f"Archivo {json_file} movido a {processed_dir}")
            except Exception as e:
                # Manejar cualquier error al mover el archivo
                logger.error(f"Error al mover el archivo {json_file}: {e}")
        
        # Devolver resultados de todos los archivos procesados
        return results
    
    def get_stock_data_from_db(self, symbol: str, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 100) -> pd.DataFrame:
        """
        Obtiene datos históricos de la base de datos.
        
        Este método permite consultar datos históricos almacenados en la base de datos
        para un símbolo específico, con opciones para filtrar por fechas y limitar resultados.
        
        Args:
            symbol (str): Símbolo de la acción.
            start_date (str, optional): Fecha de inicio (formato: 'YYYY-MM-DD').
            end_date (str, optional): Fecha de fin (formato: 'YYYY-MM-DD').
            limit (int, optional): Límite de registros a devolver.
            
        Returns:
            pandas.DataFrame: DataFrame con los datos históricos.
        """
        try:
            # Obtener el ID del símbolo
            symbol_info = self.metadata.get_symbol_by_ticker(symbol)
            if not symbol_info:
                # Si el símbolo no existe en la base de datos, reportar error
                logger.error(f"Error: El símbolo {symbol} no existe en la base de datos.")
                return pd.DataFrame()  # Devolver DataFrame vacío
            
            # Obtener el ID del símbolo
            symbol_id = symbol_info['SymbolID']
            
            # Construir la consulta SQL base
            # Seleccionamos campos relevantes y unimos con la tabla de símbolos
            query = """
            SELECT 
                s.Symbol,
                p.Date,
                p.Open,
                p.High,
                p.Low,
                p.Close,
                p.AdjustedClose,
                p.Volume
            FROM 
                AVdata.StockPrices p
                JOIN Metadata.Symbols s ON p.SymbolID = s.SymbolID
            WHERE 
                p.SymbolID = ?
            """
            
            # Lista de parámetros para la consulta
            params = [symbol_id]
            
            # Añadir filtro de fecha de inicio si se proporciona
            if start_date:
                query += " AND p.Date >= ?"
                params.append(start_date)
            
            # Añadir filtro de fecha de fin si se proporciona
            if end_date:
                query += " AND p.Date <= ?"
                params.append(end_date)
            
            # Ordenar por fecha descendente (más reciente primero)
            query += " ORDER BY p.Date DESC"
            
            # Añadir límite si se proporciona
            if limit:
                # Sintaxis específica de SQL Server para limitar resultados
                query += f" OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
            
            # Ejecutar la consulta y obtener los resultados como diccionarios
            results = self.db.fetch_as_dict(query, tuple(params))
            
            # Convertir a DataFrame de pandas
            df = pd.DataFrame(results)
            
            # Devolver el DataFrame
            return df
            
        except Exception as e:
            # Manejar cualquier error durante la consulta
            logger.error(f"Error al obtener datos de la base de datos para {symbol}: {e}")
            return pd.DataFrame()  # Devolver DataFrame vacío

# Función principal para probar el almacenamiento de datos
def main():
    """
    Función principal para probar el almacenamiento de datos.
    
    Esta función demuestra el uso básico del almacenamiento de datos.
    """
    # Configurar logging para mostrar mensajes en consola
    logging.basicConfig(level=logging.INFO)
    
    # Crear una instancia del almacenamiento de datos
    storage = StockDataStorage()
    
    # Procesar todos los archivos JSON
    print("Procesando archivos JSON y almacenando datos en SQL Server...")
    results = storage.process_all_json_files()
    
    # Mostrar resultados del procesamiento
    print("\nResultados del procesamiento:")
    total_inserted = 0
    for file, result in results.items():
        print(f"  - {file}: {result['inserted_count']} registros insertados")
        total_inserted += result['inserted_count']
    
    print(f"\nTotal de registros insertados: {total_inserted}")
    
    # Verificar datos almacenados para algunos símbolos
    symbols = ['NVDA', 'AAPL']
    for symbol in symbols:
        print(f"\nVerificando datos almacenados para {symbol}:")
        # Obtener los últimos 5 registros para el símbolo
        df = storage.get_stock_data_from_db(symbol, limit=5)
        if not df.empty:
            print(f"Últimos {len(df)} registros para {symbol}:")
            # Mostrar los datos sin índice para mayor claridad
            print(df.to_string(index=False))
        else:
            print(f"No se encontraron datos para {symbol}")

# Punto de entrada para ejecución directa del script
if __name__ == "__main__":
    main()
