#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Implementación del gestor de metadatos utilizando el conector de base de datos.
Versión compatible con macOS Apple Silicon.

Este módulo implementa una clase para gestionar los metadatos relacionados con
los símbolos bursátiles y sus descargas en la base de datos SQL Server.

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
from scripts.base import Singleton  # Importar clases base
from scripts.db_connector import SQLServerConnector  # Importar conector de base de datos

# Crear logger para este módulo
logger = logging.getLogger(__name__)

class MetadataManager(Singleton):
    """
    Clase para gestionar los metadatos en la base de datos SQL Server.
    
    Esta clase proporciona métodos para interactuar con las tablas del esquema Metadata,
    permitiendo gestionar símbolos, registros de descarga y configuraciones.
    
    Atributos:
        db (SQLServerConnector): Conector de base de datos SQL Server.
    
    Mejores prácticas POO:
    - Patrón Singleton: Asegura que solo exista una instancia de la clase
    - Encapsulamiento: Oculta los detalles de implementación de la base de datos
    - Principio de responsabilidad única: Solo se encarga de la gestión de metadatos
    """
    
    def __init__(self):
        """
        Inicializa el gestor de metadatos.
        
        Crea una instancia del conector de base de datos SQL Server.
        """
        # Evitar reinicialización si ya existe una instancia (debido a Singleton)
        if hasattr(self, 'db'):
            return
            
        # Crear una instancia del conector de base de datos
        self.db = SQLServerConnector()
        logger.info("Gestor de metadatos inicializado")
    
    def get_all_symbols(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Obtiene todos los símbolos de la base de datos.
        
        Args:
            active_only (bool): Si es True, solo devuelve símbolos activos.
            
        Returns:
            list: Lista de diccionarios con la información de los símbolos.
        """
        try:
            # Construir la consulta SQL según el parámetro active_only
            query = "SELECT * FROM Metadata.Symbols"
            if active_only:
                query += " WHERE IsActive = 1"
            
            # Ejecutar la consulta y obtener los resultados como diccionarios
            symbols = self.db.fetch_as_dict(query)
            logger.debug(f"Obtenidos {len(symbols)} símbolos")
            return symbols
            
        except Exception as e:
            # Manejar cualquier error durante la consulta
            logger.error(f"Error al obtener los símbolos: {e}")
            return []
    
    def get_symbol_by_id(self, symbol_id: int) -> Optional[Dict[str, Any]]:
        """
        Obtiene un símbolo por su ID.
        
        Args:
            symbol_id (int): ID del símbolo.
            
        Returns:
            dict: Información del símbolo o None si no existe.
        """
        try:
            # Ejecutar consulta parametrizada para evitar inyección SQL
            query = "SELECT * FROM Metadata.Symbols WHERE SymbolID = ?"
            symbol = self.db.fetch_one_as_dict(query, (symbol_id,))
            
            if symbol:
                logger.debug(f"Obtenido símbolo con ID {symbol_id}: {symbol['Symbol']}")
            else:
                logger.debug(f"No se encontró símbolo con ID {symbol_id}")
                
            return symbol
            
        except Exception as e:
            # Manejar cualquier error durante la consulta
            logger.error(f"Error al obtener el símbolo con ID {symbol_id}: {e}")
            return None
    
    def get_symbol_by_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene un símbolo por su ticker.
        
        Args:
            symbol (str): Ticker del símbolo (ej. 'AAPL').
            
        Returns:
            dict: Información del símbolo o None si no existe.
        """
        try:
            # Ejecutar consulta parametrizada para evitar inyección SQL
            query = "SELECT * FROM Metadata.Symbols WHERE Symbol = ?"
            symbol_info = self.db.fetch_one_as_dict(query, (symbol,))
            
            if symbol_info:
                logger.debug(f"Obtenido símbolo {symbol}: {symbol_info['CompanyName']}")
            else:
                logger.debug(f"No se encontró símbolo {symbol}")
                
            return symbol_info
            
        except Exception as e:
            # Manejar cualquier error durante la consulta
            logger.error(f"Error al obtener el símbolo {symbol}: {e}")
            return None
    
    def add_symbol(self, symbol: str, company_name: str, is_active: bool = True) -> Optional[int]:
        """
        Añade un nuevo símbolo a la base de datos.
        
        Args:
            symbol (str): Ticker del símbolo (ej. 'AAPL').
            company_name (str): Nombre de la empresa.
            is_active (bool): Si el símbolo está activo.
            
        Returns:
            int: ID del símbolo añadido o None si hay un error.
        """
        try:
            # Verificar si el símbolo ya existe
            existing = self.get_symbol_by_ticker(symbol)
            if existing:
                logger.info(f"El símbolo {symbol} ya existe en la base de datos")
                return existing['SymbolID']
            
            # Insertar nuevo símbolo usando consulta parametrizada
            query = """
            INSERT INTO Metadata.Symbols (Symbol, CompanyName, IsActive)
            VALUES (?, ?, ?)
            """
            self.db.execute_query(query, (symbol, company_name, 1 if is_active else 0))
            self.db.commit()
            
            # Obtener el ID del símbolo insertado
            query = "SELECT SymbolID FROM Metadata.Symbols WHERE Symbol = ?"
            result = self.db.fetch_one(query, (symbol,))
            
            if result:
                symbol_id = result[0]
                logger.info(f"Símbolo {symbol} añadido con ID {symbol_id}")
                return symbol_id
            else:
                logger.error(f"No se pudo obtener el ID del símbolo {symbol} después de insertarlo")
                return None
                
        except Exception as e:
            # Manejar cualquier error durante la inserción
            logger.error(f"Error al añadir el símbolo {symbol}: {e}")
            return None
    
    def update_symbol(self, symbol_id: int, company_name: Optional[str] = None, is_active: Optional[bool] = None) -> bool:
        """
        Actualiza la información de un símbolo.
        
        Args:
            symbol_id (int): ID del símbolo.
            company_name (str, optional): Nuevo nombre de la empresa.
            is_active (bool, optional): Nuevo estado activo.
            
        Returns:
            bool: True si se actualizó correctamente, False en caso contrario.
        """
        try:
            # Construir la consulta de actualización dinámicamente
            update_parts = []  # Partes de la consulta SET
            params = []        # Parámetros para la consulta parametrizada
            
            # Añadir partes de la consulta según los parámetros proporcionados
            if company_name is not None:
                update_parts.append("CompanyName = ?")
                params.append(company_name)
            
            if is_active is not None:
                update_parts.append("IsActive = ?")
                params.append(1 if is_active else 0)
            
            # Verificar si hay campos para actualizar
            if not update_parts:
                logger.warning("No hay campos para actualizar")
                return False
            
            # Añadir el ID del símbolo a los parámetros
            params.append(symbol_id)
            
            # Ejecutar la consulta de actualización
            query = f"UPDATE Metadata.Symbols SET {', '.join(update_parts)} WHERE SymbolID = ?"
            self.db.execute_query(query, tuple(params))
            self.db.commit()
            
            logger.info(f"Símbolo con ID {symbol_id} actualizado correctamente")
            return True
            
        except Exception as e:
            # Manejar cualquier error durante la actualización
            logger.error(f"Error al actualizar el símbolo con ID {symbol_id}: {e}")
            return False
    
    def get_last_download(self, symbol_id: int) -> Optional[Dict[str, Any]]:
        """
        Obtiene la información de la última descarga para un símbolo.
        
        Args:
            symbol_id (int): ID del símbolo.
            
        Returns:
            dict: Información de la última descarga o None si no existe.
        """
        try:
            # Ejecutar consulta para obtener la última descarga ordenada por fecha
            query = """
            SELECT TOP 1 * FROM Metadata.Downloads 
            WHERE SymbolID = ? 
            ORDER BY LastDownloadDate DESC
            """
            download = self.db.fetch_one_as_dict(query, (symbol_id,))
            
            if download:
                logger.debug(f"Obtenida última descarga para símbolo con ID {symbol_id}")
            else:
                logger.debug(f"No se encontraron descargas para símbolo con ID {symbol_id}")
                
            return download
            
        except Exception as e:
            # Manejar cualquier error durante la consulta
            logger.error(f"Error al obtener la última descarga para el símbolo con ID {symbol_id}: {e}")
            return None
    
    def add_download_record(self, symbol_id: int, start_date: Optional[datetime] = None, 
                           end_date: Optional[datetime] = None, status: str = 'Pending') -> Optional[int]:
        """
        Añade un nuevo registro de descarga.
        
        Args:
            symbol_id (int): ID del símbolo.
            start_date (datetime, optional): Fecha de inicio de la descarga.
            end_date (datetime, optional): Fecha de fin de la descarga.
            status (str, optional): Estado de la descarga ('Pending', 'Completed', 'Failed').
            
        Returns:
            int: ID del registro de descarga añadido o None si hay un error.
        """
        try:
            # Insertar nuevo registro de descarga usando consulta parametrizada
            query = """
            INSERT INTO Metadata.Downloads 
            (SymbolID, LastDownloadDate, StartDate, EndDate, Status) 
            VALUES (?, GETDATE(), ?, ?, ?)
            """
            self.db.execute_query(query, (symbol_id, start_date, end_date, status))
            self.db.commit()
            
            # Obtener el ID del registro insertado
            query = """
            SELECT TOP 1 DownloadID FROM Metadata.Downloads 
            WHERE SymbolID = ? 
            ORDER BY LastDownloadDate DESC
            """
            result = self.db.fetch_one(query, (symbol_id,))
            
            if result:
                download_id = result[0]
                logger.info(f"Registro de descarga añadido con ID {download_id} para símbolo con ID {symbol_id}")
                return download_id
            else:
                logger.error(f"No se pudo obtener el ID del registro de descarga después de insertarlo")
                return None
                
        except Exception as e:
            # Manejar cualquier error durante la inserción
            logger.error(f"Error al añadir registro de descarga para el símbolo con ID {symbol_id}: {e}")
            return None
    
    def update_download_status(self, download_id: int, status: str, end_date: Optional[datetime] = None) -> bool:
        """
        Actualiza el estado de un registro de descarga.
        
        Args:
            download_id (int): ID del registro de descarga.
            status (str): Nuevo estado ('Pending', 'Completed', 'Failed').
            end_date (datetime, optional): Nueva fecha de fin.
            
        Returns:
            bool: True si se actualizó correctamente, False en caso contrario.
        """
        try:
            # Construir la consulta de actualización dinámicamente
            update_parts = ["Status = ?"]  # Siempre actualizamos el estado
            params = [status]              # Parámetros para la consulta parametrizada
            
            # Añadir la fecha de fin si se proporciona
            if end_date is not None:
                update_parts.append("EndDate = ?")
                params.append(end_date)
            
            # Añadir el ID de descarga a los parámetros
            params.append(download_id)
            
            # Ejecutar la consulta de actualización
            query = f"UPDATE Metadata.Downloads SET {', '.join(update_parts)} WHERE DownloadID = ?"
            self.db.execute_query(query, tuple(params))
            self.db.commit()
            
            logger.info(f"Estado de descarga con ID {download_id} actualizado a '{status}'")
            return True
            
        except Exception as e:
            # Manejar cualquier error durante la actualización
            logger.error(f"Error al actualizar el estado de la descarga con ID {download_id}: {e}")
            return False
    
    def get_config_value(self, key: str) -> Optional[str]:
        """
        Obtiene un valor de configuración.
        
        Args:
            key (str): Clave de configuración.
            
        Returns:
            str: Valor de configuración o None si no existe.
        """
        try:
            # Ejecutar consulta parametrizada para obtener el valor de configuración
            query = "SELECT ConfigValue FROM Metadata.Configuration WHERE ConfigKey = ?"
            result = self.db.fetch_one(query, (key,))
            
            if result:
                value = result[0]
                logger.debug(f"Obtenido valor de configuración para {key}: {value}")
                return value
            else:
                logger.debug(f"No se encontró valor de configuración para {key}")
                return None
                
        except Exception as e:
            # Manejar cualquier error durante la consulta
            logger.error(f"Error al obtener el valor de configuración {key}: {e}")
            return None
    
    def set_config_value(self, key: str, value: str, description: Optional[str] = None) -> bool:
        """
        Establece un valor de configuración.
        
        Args:
            key (str): Clave de configuración.
            value (str): Valor de configuración.
            description (str, optional): Descripción de la configuración.
            
        Returns:
            bool: True si se estableció correctamente, False en caso contrario.
        """
        try:
            # Verificar si la clave ya existe
            existing_value = self.get_config_value(key)
            
            if existing_value is not None:
                # Actualizar valor existente
                query = """
                UPDATE Metadata.Configuration 
                SET ConfigValue = ?, ModifiedDate = GETDATE() 
                WHERE ConfigKey = ?
                """
                self.db.execute_query(query, (value, key))
                logger.info(f"Valor de configuración actualizado para {key}: {value}")
            else:
                # Insertar nuevo valor
                query = """
                INSERT INTO Metadata.Configuration 
                (ConfigKey, ConfigValue, Description) 
                VALUES (?, ?, ?)
                """
                self.db.execute_query(query, (key, value, description))
                logger.info(f"Nuevo valor de configuración añadido para {key}: {value}")
            
            self.db.commit()
            return True
            
        except Exception as e:
            # Manejar cualquier error durante la operación
            logger.error(f"Error al establecer el valor de configuración {key}: {e}")
            return False
    
    def get_symbols_to_download(self) -> List[Dict[str, Any]]:
        """
        Obtiene los símbolos que necesitan ser descargados.
        
        Determina qué símbolos necesitan actualización basándose en la fecha de última descarga.
        
        Returns:
            list: Lista de diccionarios con la información de los símbolos a descargar.
        """
        try:
            # Obtener todos los símbolos activos
            symbols = self.get_all_symbols(active_only=True)
            symbols_to_download = []
            
            for symbol in symbols:
                # Obtener la última descarga para el símbolo
                last_download = self.get_last_download(symbol['SymbolID'])
                
                # Si no hay descarga previa o la última descarga fue hace más de 7 días
                if not last_download or (
                    last_download['LastDownloadDate'] and 
                    (datetime.now() - last_download['LastDownloadDate']).days >= 7
                ):
                    # Añadir el símbolo a la lista de símbolos para descargar
                    symbols_to_download.append({
                        'symbol': symbol,
                        'last_download': last_download
                    })
            
            logger.info(f"Se encontraron {len(symbols_to_download)} símbolos para descargar")
            return symbols_to_download
            
        except Exception as e:
            # Manejar cualquier error durante la operación
            logger.error(f"Error al obtener los símbolos para descargar: {e}")
            return []

# Función principal para probar el gestor de metadatos
def main():
    """
    Función principal para probar el gestor de metadatos.
    
    Esta función demuestra el uso básico del gestor de metadatos.
    """
    # Configurar logging para mostrar mensajes en consola
    logging.basicConfig(level=logging.INFO)
    
    # Crear una instancia del gestor de metadatos
    manager = MetadataManager()
    
    # Obtener todos los símbolos
    symbols = manager.get_all_symbols()
    print(f"Símbolos en la base de datos: {len(symbols)}")
    for symbol in symbols:
        print(f"  - {symbol['Symbol']} ({symbol['CompanyName']})")
    
    # Obtener símbolos para descargar
    symbols_to_download = manager.get_symbols_to_download()
    print(f"Símbolos para descargar: {len(symbols_to_download)}")
    for item in symbols_to_download:
        symbol = item['symbol']
        last_download = item['last_download']
        
        # Mostrar la fecha de última descarga o "Nunca" si no hay descargas previas
        last_date = last_download['LastDownloadDate'] if last_download else "Nunca"
        print(f"  - {symbol['Symbol']}: Última descarga: {last_date}")

# Punto de entrada para ejecución directa del script
if __name__ == "__main__":
    main()
