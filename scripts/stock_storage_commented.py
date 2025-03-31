#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para almacenar datos históricos bursátiles en la base de datos SQL Server.
Procesa los archivos JSON descargados y los guarda en la tabla AVdata.StockPrices.
Versión compatible con macOS Apple Silicon usando pyodbc.

Este módulo implementa una clase para procesar y almacenar datos históricos bursátiles
en una base de datos SQL Server, con optimizaciones para inserciones masivas.

Autor: Manus
Fecha: 31/03/2025
"""

# Importaciones de bibliotecas estándar
import os                      # Para operaciones del sistema de archivos
import sys                     # Para funcionalidades del sistema
import json                    # Para manejo de datos JSON
import pyodbc                  # Para conexión con SQL Server (compatible con macOS Apple Silicon)
import pandas as pd            # Para manipulación y análisis de datos
from datetime import datetime  # Para manejo de fechas y horas
from dotenv import load_dotenv  # Para cargar variables de entorno desde archivo .env

# Importar módulos propios
# Añadimos el directorio padre al path para poder importar módulos del proyecto
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from scripts.metadata_manager_pyodbc import MetadataManager  # Para gestionar metadatos en la base de datos

# Cargar variables de entorno desde el archivo .env
# Esto permite mantener información sensible fuera del código
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', '.env'))

# Configuración de la conexión a SQL Server
# Obtenemos los parámetros desde las variables de entorno para mayor seguridad
SERVER = os.getenv('SQL_SERVER', 'localhost')     # Servidor SQL (por defecto: localhost)
PORT = os.getenv('SQL_PORT', '1433')              # Puerto SQL (por defecto: 1433)
USER = os.getenv('SQL_USER', 'sa')                # Usuario SQL (por defecto: sa)
PASSWORD = os.getenv('SQL_PASSWORD', '')          # Contraseña SQL
DATABASE = os.getenv('SQL_DATABASE', 'AlphaVantageDB')  # Base de datos

class StockDataStorage:
    """
    Clase para almacenar datos históricos bursátiles en SQL Server.
    
    Esta clase proporciona métodos para procesar archivos JSON con datos históricos
    bursátiles y almacenarlos en la base de datos SQL Server, con optimizaciones
    para inserciones masivas y manejo de errores.
    
    Atributos:
        conn (pyodbc.Connection): Conexión a la base de datos SQL Server.
        cursor (pyodbc.Cursor): Cursor para ejecutar consultas SQL.
        metadata (MetadataManager): Instancia para gestionar metadatos en la base de datos.
        data_dir (str): Directorio donde se encuentran los archivos JSON a procesar.
    
    Mejores prácticas POO:
    - Composición: Utiliza instancias de otras clases para delegar responsabilidades
    - Separación de responsabilidades: Cada clase tiene un propósito específico
    - Manejo de recursos: Implementa __del__ para asegurar la liberación de recursos
    """
    
    def __init__(self):
        """
        Inicializa el almacenamiento de datos.
        
        Establece una conexión con SQL Server, crea una instancia del gestor de metadatos
        y configura el directorio de datos.
        
        Raises:
            SystemExit: Si no se puede establecer la conexión con la base de datos.
        """
        try:
            # Construir la cadena de conexión para pyodbc
            # Utilizamos el formato adecuado para SQL Server con ODBC
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER},{PORT};DATABASE={DATABASE};UID={USER};PWD={PASSWORD}'
            
            # Alternativa para FreeTDS si el driver ODBC no funciona en macOS
            # conn_str = f'DRIVER={{FreeTDS}};SERVER={SERVER};PORT={PORT};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};TDS_VERSION=8.0'
            
            # Establecer la conexión
            self.conn = pyodbc.connect(conn_str)
            # Crear un cursor para ejecutar consultas
            self.cursor = self.conn.cursor()
            # Crear una instancia del gestor de metadatos
            self.metadata = MetadataManager()
            # Configurar el directorio de datos
            self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        except Exception as e:
            # Informar del error y proporcionar información útil para solucionarlo
            print(f"Error al conectar a la base de datos: {e}")
            print("Nota: Asegúrate de tener instalado el driver ODBC para SQL Server o FreeTDS.")
            print("Puedes instalarlo con: brew install unixodbc freetds")
            sys.exit(1)  # Salir del programa con código de error
    
    def __del__(self):
        """
        Cierra la conexión a la base de datos al destruir el objeto.
        
        Mejores prácticas POO:
        - Gestión de recursos: Asegura que los recursos se liberan adecuadamente
        - Destructor: Implementa la limpieza necesaria cuando el objeto es eliminado
        """
        # Verificar si el objeto tiene el atributo 'conn' y si existe una conexión activa
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()  # Cerrar la conexión para liberar recursos
    
    def process_json_file(self, json_file):
        """
        Procesa un archivo JSON y extrae los datos históricos.
        
        Este método lee un archivo JSON con datos históricos bursátiles,
        identifica el formato (ajustado o no) y extrae los datos relevantes.
        
        Args:
            json_file (str): Ruta al archivo JSON.
            
        Returns:
            tuple: (symbol, data_list)
                symbol: Símbolo de la acción.
                data_list: Lista de diccionarios con los datos históricos.
        
        Mejores prácticas POO:
        - Método con responsabilidad única: Procesar un archivo JSON
        - Manejo de errores robusto: Captura y reporta errores específicos
        - Detección automática de formato: Identifica si los datos son ajustados o no
        """
        try:
            # Abrir y cargar el archivo JSON
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            # Extraer el símbolo del nombre del archivo
            # Formato esperado: SYMBOL_TIMESTAMP.json (ej. AAPL_20250331_120000.json)
            filename = os.path.basename(json_file)
            symbol = filename.split('_')[0]  # Primera parte antes del primer guion bajo
            
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
                print(f"Formato de datos no reconocido en el archivo {json_file}")
                return None, None
                
        except Exception as e:
            # Manejar cualquier error durante el procesamiento
            print(f"Error al procesar el archivo JSON {json_file}: {e}")
            return None, None
    
    def store_stock_data(self, symbol, data_list, download_id):
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
        
        Mejores prácticas POO:
        - Optimización de rendimiento: Usa inserciones por lotes para mejorar eficiencia
        - Validación de datos: Verifica que haya datos para almacenar
        - Manejo de transacciones: Usa commit/rollback para asegurar integridad
        """
        # Verificar que haya datos para almacenar
        if not data_list:
            print(f"No hay datos para almacenar para el símbolo {symbol}")
            return 0
        
        # Obtener el ID del símbolo
        symbol_info = self.metadata.get_symbol_by_ticker(symbol)
        if not symbol_info:
            # Si el símbolo no existe en la base de datos, reportar error
            print(f"Error: El símbolo {symbol} no existe en la base de datos.")
            return 0
        
        # Obtener el ID del símbolo
        symbol_id = symbol_info['SymbolID']
        
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
                self.cursor.executemany(query, batch)
                self.conn.commit()  # Confirmar la transacción
                
                # Actualizar contador y mostrar progreso
                total_inserted += len(batch)
                print(f"Insertados {len(batch)} registros para {symbol} (Total: {total_inserted}/{len(values_list)})")
            
            # Devolver el número total de registros insertados
            return total_inserted
            
        except Exception as e:
            # En caso de error, revertir la transacción
            self.conn.rollback()
            print(f"Error al insertar datos para {symbol}: {e}")
            return 0
    
    def process_and_store_file(self, json_file, download_id):
        """
        Procesa un archivo JSON y almacena los datos en la base de datos.
        
        Este método combina el procesamiento del archivo JSON y el almacenamiento
        de los datos en la base de datos en una sola operación.
        
        Args:
            json_file (str): Ruta al archivo JSON.
            download_id (int): ID del registro de descarga.
            
        Returns:
            int: Número de registros insertados.
        
        Mejores prácticas POO:
        - Método de conveniencia: Combina operaciones relacionadas
        - Delegación: Utiliza otros métodos para realizar el trabajo
        """
        # Procesar el archivo JSON para obtener los datos
        symbol, data_list = self.process_json_file(json_file)
        
        # Verificar que se hayan obtenido datos válidos
        if not symbol or not data_list:
            return 0
        
        # Almacenar los datos en la base de datos
        return self.store_stock_data(symbol, data_list, download_id)
    
    def process_all_json_files(self):
        """
        Procesa todos los archivos JSON en el directorio de datos.
        
        Este método busca todos los archivos JSON en el directorio de datos,
        los procesa y almacena en la base de datos, y luego los mueve a un
        subdirectorio 'processed'.
        
        Returns:
            dict: Resultados del procesamiento por archivo.
        
        Mejores prácticas POO:
        - Método de alto nivel: Coordina operaciones complejas
        - Gestión de archivos: Mueve los archivos procesados para evitar reprocesamiento
        - Estructura de retorno consistente: Devuelve resultados en formato estándar
        """
        # Verificar que el directorio de datos exista
        if not os.path.exists(self.data_dir):
            print(f"El directorio de datos {self.data_dir} no existe.")
            return {}
        
        # Obtener la lista de archivos JSON en el directorio
        json_files = [f for f in os.listdir(self.data_dir) if f.endswith('.json')]
        
        # Verificar que haya archivos para procesar
        if not json_files:
            print(f"No hay archivos JSON en el directorio {self.data_dir}")
            return {}
        
        # Diccionario para almacenar resultados
        results = {}
        
        # Procesar cada archivo JSON
        for json_file in json_files:
            # Ruta completa al archivo
            file_path = os.path.join(self.data_dir, json_file)
            print(f"Procesando archivo: {json_file}")
            
            # Extraer el símbolo del nombre del archivo
            symbol = json_file.split('_')[0]
            
            # Obtener el último registro de descarga para el símbolo
            symbol_info = self.metadata.get_symbol_by_ticker(symbol)
            if not symbol_info:
                # Si el símbolo no existe en la base de datos, reportar error
                print(f"Error: El símbolo {symbol} no existe en la base de datos.")
                continue
            
            # Obtener el último registro de descarga
            last_download = self.metadata.get_last_download(symbol_info['SymbolID'])
            if not last_download:
                # Si no hay registros de descarga, reportar error
                print(f"Error: No hay registros de descarga para el símbolo {symbol}.")
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
                print(f"Archivo {json_file} movido a {processed_dir}")
            except Exception as e:
                # Manejar cualquier error al mover el archivo
                print(f"Error al mover el archivo {json_file}: {e}")
        
        # Devolver resultados de todos los archivos procesados
        return results
    
    def get_stock_data_from_db(self, symbol, start_date=None, end_date=None, limit=100):
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
        
        Mejores prácticas POO:
        - Flexibilidad: Permite múltiples criterios de filtrado
        - Uso de pandas: Devuelve los datos en un formato conveniente para análisis
        - Construcción dinámica de consultas: Adapta la consulta según los parámetros
        """
        try:
            # Obtener el ID del símbolo
            symbol_info = self.metadata.get_symbol_by_ticker(symbol)
            if not symbol_info:
                # Si el símbolo no existe en la base de datos, reportar error
                print(f"Error: El símbolo {symbol} no existe en la base de datos.")
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
            
            # Ejecutar la consulta
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            
            # Obtener nombres de columnas y datos
            columns = [column[0] for column in cursor.description]
            data = cursor.fetchall()
            
            # Convertir a DataFrame de pandas
            df = pd.DataFrame.from_records(data, columns=columns)
            
            # Devolver el DataFrame
            return df
            
        except Exception as e:
            # Manejar cualquier error durante la consulta
            print(f"Error al obtener datos de la base de datos para {symbol}: {e}")
            return pd.DataFrame()  # Devolver DataFrame vacío

# Función principal
def main():
    """
    Función principal para almacenar datos históricos.
    
    Esta función demuestra el uso básico de la clase StockDataStorage
    procesando archivos JSON y consultando datos almacenados.
    
    Mejores prácticas:
    - Incluir una función main() para permitir pruebas independientes del módulo
    - Usar if __name__ == "__main__" para permitir importación sin ejecución
    - Código de ejemplo claro y conciso
    """
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
