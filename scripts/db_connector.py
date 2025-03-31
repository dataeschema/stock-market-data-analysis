#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Implementación del conector de base de datos SQL Server usando pyodbc.
Versión compatible con macOS Apple Silicon.

Este módulo implementa la interfaz DatabaseConnector para SQL Server,
proporcionando métodos para interactuar con la base de datos.

Autor: Manus
Fecha: 31/03/2025
"""

# Importaciones de bibliotecas estándar
import os                      # Para operaciones del sistema de archivos
import sys                     # Para funcionalidades del sistema
import logging                 # Para registro de eventos
import pyodbc                  # Para conexión con SQL Server (compatible con macOS Apple Silicon)
from typing import Dict, List, Any, Optional, Tuple, Union  # Para tipado estático

# Importar módulos propios
# Añadimos el directorio padre al path para poder importar módulos del proyecto
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from scripts.base import DatabaseConnector, ConfigManager  # Importar clases base

# Crear logger para este módulo
logger = logging.getLogger(__name__)

class SQLServerConnector(DatabaseConnector):
    """
    Implementación de DatabaseConnector para SQL Server usando pyodbc.
    
    Esta clase proporciona métodos para interactuar con una base de datos SQL Server,
    implementando la interfaz definida en DatabaseConnector.
    
    Atributos:
        conn (pyodbc.Connection): Conexión a la base de datos SQL Server.
        cursor (pyodbc.Cursor): Cursor para ejecutar consultas SQL.
        config (ConfigManager): Gestor de configuración.
    
    Mejores prácticas POO:
    - Implementación de interfaz: Implementa todos los métodos definidos en DatabaseConnector
    - Principio de responsabilidad única: Solo se encarga de la conexión con SQL Server
    - Manejo de recursos: Implementa métodos para gestionar la conexión
    """
    
    def __init__(self, database: Optional[str] = None):
        """
        Inicializa el conector de SQL Server.
        
        Args:
            database (str, optional): Nombre de la base de datos a la que conectarse.
                Si no se proporciona, se usa el valor de configuración.
        """
        # Obtener configuración
        self.config = ConfigManager()
        self.server = self.config.get('SQL_SERVER', 'localhost')
        self.port = self.config.get('SQL_PORT', '1433')
        self.user = self.config.get('SQL_USER', 'sa')
        self.password = self.config.get('SQL_PASSWORD', '')
        self.database = database or self.config.get('SQL_DATABASE', 'AlphaVantageDB')
        
        # Inicializar atributos de conexión
        self.conn = None
        self.cursor = None
        
        # Intentar conectar automáticamente
        self.connect()
    
    def __del__(self):
        """
        Destructor que asegura que la conexión se cierre al eliminar el objeto.
        
        Mejores prácticas POO:
        - Gestión de recursos: Asegura que los recursos se liberan adecuadamente
        - Destructor: Implementa la limpieza necesaria cuando el objeto es eliminado
        """
        self.disconnect()
    
    def connect(self) -> bool:
        """
        Establece una conexión con la base de datos SQL Server.
        
        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario.
        """
        try:
            # Construir la cadena de conexión para pyodbc
            # Utilizamos el formato adecuado para SQL Server con ODBC
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server},{self.port};DATABASE={self.database};UID={self.user};PWD={self.password}'
            
            # Alternativa para FreeTDS si el driver ODBC no funciona en macOS
            # conn_str = f'DRIVER={{FreeTDS}};SERVER={self.server};PORT={self.port};DATABASE={self.database};UID={self.user};PWD={self.password};TDS_VERSION=8.0'
            
            # Establecer la conexión
            self.conn = pyodbc.connect(conn_str)
            # Crear un cursor para ejecutar consultas
            self.cursor = self.conn.cursor()
            
            # Registrar conexión exitosa
            logger.info(f"Conexión establecida con SQL Server: {self.server},{self.port}/{self.database}")
            return True
            
        except Exception as e:
            # Registrar error de conexión
            logger.error(f"Error al conectar a SQL Server: {e}")
            # Proporcionar información útil para solucionar el problema
            logger.info("Nota: Asegúrate de tener instalado el driver ODBC para SQL Server o FreeTDS.")
            logger.info("Puedes instalarlo con: brew install unixodbc freetds")
            return False
    
    def disconnect(self) -> bool:
        """
        Cierra la conexión con la base de datos SQL Server.
        
        Returns:
            bool: True si la desconexión fue exitosa, False en caso contrario.
        """
        try:
            # Verificar si hay una conexión activa
            if self.conn:
                # Cerrar cursor si existe
                if self.cursor:
                    self.cursor.close()
                # Cerrar conexión
                self.conn.close()
                # Limpiar referencias
                self.conn = None
                self.cursor = None
                
                # Registrar desconexión exitosa
                logger.info(f"Conexión cerrada con SQL Server: {self.server},{self.port}/{self.database}")
                return True
            return True  # No había conexión activa
            
        except Exception as e:
            # Registrar error de desconexión
            logger.error(f"Error al desconectar de SQL Server: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Any:
        """
        Ejecuta una consulta SQL.
        
        Args:
            query (str): Consulta SQL a ejecutar.
            params (tuple, optional): Parámetros para la consulta.
            
        Returns:
            Any: Cursor con los resultados de la consulta.
            
        Raises:
            Exception: Si hay un error al ejecutar la consulta.
        """
        try:
            # Verificar si hay una conexión activa
            if not self.conn or not self.cursor:
                # Intentar reconectar
                if not self.connect():
                    raise Exception("No hay conexión con la base de datos")
            
            # Ejecutar la consulta con o sin parámetros
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            # Registrar ejecución exitosa
            logger.debug(f"Consulta ejecutada: {query}")
            return self.cursor
            
        except Exception as e:
            # Registrar error de ejecución
            logger.error(f"Error al ejecutar consulta: {e}")
            logger.error(f"Consulta: {query}")
            if params:
                logger.error(f"Parámetros: {params}")
            raise
    
    def execute_many(self, query: str, params_list: List[Tuple]) -> int:
        """
        Ejecuta una consulta SQL múltiples veces con diferentes parámetros.
        
        Args:
            query (str): Consulta SQL a ejecutar.
            params_list (list): Lista de tuplas con parámetros para cada ejecución.
            
        Returns:
            int: Número de filas afectadas.
            
        Raises:
            Exception: Si hay un error al ejecutar la consulta.
        """
        try:
            # Verificar si hay una conexión activa
            if not self.conn or not self.cursor:
                # Intentar reconectar
                if not self.connect():
                    raise Exception("No hay conexión con la base de datos")
            
            # Ejecutar la consulta múltiples veces
            self.cursor.executemany(query, params_list)
            
            # Obtener el número de filas afectadas
            row_count = self.cursor.rowcount
            
            # Registrar ejecución exitosa
            logger.debug(f"Consulta masiva ejecutada: {query}")
            logger.debug(f"Filas afectadas: {row_count}")
            return row_count
            
        except Exception as e:
            # Registrar error de ejecución
            logger.error(f"Error al ejecutar consulta masiva: {e}")
            logger.error(f"Consulta: {query}")
            raise
    
    def commit(self) -> bool:
        """
        Confirma los cambios en la base de datos.
        
        Returns:
            bool: True si la confirmación fue exitosa, False en caso contrario.
        """
        try:
            # Verificar si hay una conexión activa
            if not self.conn:
                logger.warning("No hay conexión activa para confirmar cambios")
                return False
            
            # Confirmar cambios
            self.conn.commit()
            
            # Registrar confirmación exitosa
            logger.debug("Cambios confirmados en la base de datos")
            return True
            
        except Exception as e:
            # Registrar error de confirmación
            logger.error(f"Error al confirmar cambios: {e}")
            return False
    
    def rollback(self) -> bool:
        """
        Revierte los cambios en la base de datos.
        
        Returns:
            bool: True si la reversión fue exitosa, False en caso contrario.
        """
        try:
            # Verificar si hay una conexión activa
            if not self.conn:
                logger.warning("No hay conexión activa para revertir cambios")
                return False
            
            # Revertir cambios
            self.conn.rollback()
            
            # Registrar reversión exitosa
            logger.debug("Cambios revertidos en la base de datos")
            return True
            
        except Exception as e:
            # Registrar error de reversión
            logger.error(f"Error al revertir cambios: {e}")
            return False
    
    def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """
        Ejecuta una consulta y devuelve todos los resultados.
        
        Args:
            query (str): Consulta SQL a ejecutar.
            params (tuple, optional): Parámetros para la consulta.
            
        Returns:
            list: Lista de tuplas con los resultados.
        """
        try:
            # Ejecutar la consulta
            cursor = self.execute_query(query, params)
            # Obtener todos los resultados
            results = cursor.fetchall()
            return results
            
        except Exception as e:
            # Registrar error
            logger.error(f"Error al obtener resultados: {e}")
            return []
    
    def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        """
        Ejecuta una consulta y devuelve el primer resultado.
        
        Args:
            query (str): Consulta SQL a ejecutar.
            params (tuple, optional): Parámetros para la consulta.
            
        Returns:
            tuple: Primera fila de resultados o None si no hay resultados.
        """
        try:
            # Ejecutar la consulta
            cursor = self.execute_query(query, params)
            # Obtener el primer resultado
            result = cursor.fetchone()
            return result
            
        except Exception as e:
            # Registrar error
            logger.error(f"Error al obtener resultado: {e}")
            return None
    
    def fetch_as_dict(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        Ejecuta una consulta y devuelve los resultados como lista de diccionarios.
        
        Args:
            query (str): Consulta SQL a ejecutar.
            params (tuple, optional): Parámetros para la consulta.
            
        Returns:
            list: Lista de diccionarios con los resultados.
        """
        try:
            # Ejecutar la consulta
            cursor = self.execute_query(query, params)
            
            # Obtener nombres de columnas
            columns = [column[0] for column in cursor.description]
            
            # Obtener todos los resultados y convertirlos a diccionarios
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            return results
            
        except Exception as e:
            # Registrar error
            logger.error(f"Error al obtener resultados como diccionarios: {e}")
            return []
    
    def fetch_one_as_dict(self, query: str, params: Optional[Tuple] = None) -> Optional[Dict[str, Any]]:
        """
        Ejecuta una consulta y devuelve el primer resultado como diccionario.
        
        Args:
            query (str): Consulta SQL a ejecutar.
            params (tuple, optional): Parámetros para la consulta.
            
        Returns:
            dict: Primer resultado como diccionario o None si no hay resultados.
        """
        try:
            # Ejecutar la consulta
            cursor = self.execute_query(query, params)
            
            # Obtener el primer resultado
            row = cursor.fetchone()
            if not row:
                return None
            
            # Convertir a diccionario
            columns = [column[0] for column in cursor.description]
            return dict(zip(columns, row))
            
        except Exception as e:
            # Registrar error
            logger.error(f"Error al obtener resultado como diccionario: {e}")
            return None

# Función principal para probar el conector
def main():
    """
    Función principal para probar el conector de SQL Server.
    
    Esta función demuestra el uso básico del conector de SQL Server.
    """
    # Crear una instancia del conector
    connector = SQLServerConnector()
    
    # Verificar conexión
    if connector.conn:
        print("Conexión establecida con SQL Server")
        
        try:
            # Ejecutar una consulta de prueba
            query = "SELECT @@VERSION AS Version"
            result = connector.fetch_one_as_dict(query)
            
            if result:
                print(f"Versión de SQL Server: {result['Version']}")
            else:
                print("No se pudo obtener la versión de SQL Server")
                
        except Exception as e:
            print(f"Error al ejecutar consulta: {e}")
            
        finally:
            # Cerrar conexión
            connector.disconnect()
            print("Conexión cerrada")
    else:
        print("No se pudo establecer conexión con SQL Server")

# Punto de entrada para ejecución directa del script
if __name__ == "__main__":
    main()
