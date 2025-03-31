#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para gestionar los metadatos en la base de datos SQL Server.
Maneja la información sobre símbolos y fechas de última descarga.
Versión compatible con macOS Apple Silicon usando pyodbc.

Este módulo implementa una clase para gestionar los metadatos relacionados con
los símbolos bursátiles y sus descargas en la base de datos SQL Server.

Autor: Manus
Fecha: 31/03/2025
"""

# Importaciones de bibliotecas estándar
import os                      # Para operaciones del sistema de archivos
import sys                     # Para funcionalidades del sistema
import pyodbc                  # Para conexión con SQL Server (compatible con macOS Apple Silicon)
from datetime import datetime, timedelta  # Para manejo de fechas y horas
from dotenv import load_dotenv  # Para cargar variables de entorno desde archivo .env

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

class MetadataManager:
    """
    Clase para gestionar los metadatos en la base de datos SQL Server.
    
    Esta clase proporciona métodos para interactuar con las tablas del esquema Metadata,
    permitiendo gestionar símbolos, registros de descarga y configuraciones.
    
    Atributos:
        conn (pyodbc.Connection): Conexión a la base de datos SQL Server.
        cursor (pyodbc.Cursor): Cursor para ejecutar consultas SQL.
    
    Mejores prácticas POO:
    - Encapsulamiento: Los detalles de conexión a la base de datos están encapsulados
    - Responsabilidad única: Esta clase se encarga exclusivamente de la gestión de metadatos
    - Manejo de recursos: Implementa __del__ para asegurar la liberación de recursos
    """
    
    def __init__(self):
        """
        Inicializa la conexión a la base de datos.
        
        Establece una conexión con SQL Server utilizando los parámetros de configuración
        definidos en las variables de entorno.
        
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
    
    def get_all_symbols(self, active_only=True):
        """
        Obtiene todos los símbolos de la base de datos.
        
        Args:
            active_only (bool): Si es True, solo devuelve símbolos activos.
            
        Returns:
            list: Lista de diccionarios con la información de los símbolos.
            
        Mejores prácticas POO:
        - Parámetros con valores por defecto: Facilita el uso del método
        - Retorno de estructuras de datos consistentes: Siempre devuelve una lista
        """
        try:
            # Construir y ejecutar la consulta SQL según el parámetro active_only
            if active_only:
                self.cursor.execute("SELECT * FROM Metadata.Symbols WHERE IsActive = 1")
            else:
                self.cursor.execute("SELECT * FROM Metadata.Symbols")
            
            # Obtener los nombres de las columnas para crear diccionarios
            columns = [column[0] for column in self.cursor.description]
            # Convertir cada fila en un diccionario y devolver una lista de diccionarios
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            # Manejar cualquier error durante la consulta
            print(f"Error al obtener los símbolos: {e}")
            return []  # Devolver lista vacía en caso de error
    
    def get_symbol_by_id(self, symbol_id):
        """
        Obtiene un símbolo por su ID.
        
        Args:
            symbol_id (int): ID del símbolo.
            
        Returns:
            dict: Información del símbolo o None si no existe.
            
        Mejores prácticas POO:
        - Métodos específicos: Proporciona una forma clara de buscar por ID
        - Consistencia en el manejo de errores: Devuelve None si no se encuentra
        """
        try:
            # Ejecutar consulta parametrizada para evitar inyección SQL
            self.cursor.execute("SELECT * FROM Metadata.Symbols WHERE SymbolID = ?", (symbol_id,))
            # Obtener el primer resultado (o None si no hay resultados)
            row = self.cursor.fetchone()
            if row:
                # Convertir la fila en un diccionario usando los nombres de columnas
                columns = [column[0] for column in self.cursor.description]
                return dict(zip(columns, row))
            return None  # No se encontró ningún símbolo con ese ID
        except Exception as e:
            # Manejar cualquier error durante la consulta
            print(f"Error al obtener el símbolo con ID {symbol_id}: {e}")
            return None
    
    def get_symbol_by_ticker(self, symbol):
        """
        Obtiene un símbolo por su ticker.
        
        Args:
            symbol (str): Ticker del símbolo (ej. 'AAPL').
            
        Returns:
            dict: Información del símbolo o None si no existe.
            
        Mejores prácticas POO:
        - Sobrecarga conceptual: Proporciona múltiples formas de buscar símbolos
        - Nombres descriptivos: El nombre del método indica claramente su propósito
        """
        try:
            # Ejecutar consulta parametrizada para evitar inyección SQL
            self.cursor.execute("SELECT * FROM Metadata.Symbols WHERE Symbol = ?", (symbol,))
            # Obtener el primer resultado (o None si no hay resultados)
            row = self.cursor.fetchone()
            if row:
                # Convertir la fila en un diccionario usando los nombres de columnas
                columns = [column[0] for column in self.cursor.description]
                return dict(zip(columns, row))
            return None  # No se encontró ningún símbolo con ese ticker
        except Exception as e:
            # Manejar cualquier error durante la consulta
            print(f"Error al obtener el símbolo {symbol}: {e}")
            return None
    
    def add_symbol(self, symbol, company_name, is_active=True):
        """
        Añade un nuevo símbolo a la base de datos.
        
        Args:
            symbol (str): Ticker del símbolo (ej. 'AAPL').
            company_name (str): Nombre de la empresa.
            is_active (bool): Si el símbolo está activo.
            
        Returns:
            int: ID del símbolo añadido o None si hay un error.
            
        Mejores prácticas POO:
        - Validación interna: Verifica si el símbolo ya existe antes de intentar añadirlo
        - Transacciones implícitas: Usa commit para asegurar la persistencia de los datos
        """
        try:
            # Verificar si el símbolo ya existe
            existing = self.get_symbol_by_ticker(symbol)
            if existing:
                print(f"El símbolo {symbol} ya existe en la base de datos.")
                return existing['SymbolID']  # Devolver el ID existente
            
            # Insertar nuevo símbolo usando consulta parametrizada
            self.cursor.execute(
                "INSERT INTO Metadata.Symbols (Symbol, CompanyName, IsActive) VALUES (?, ?, ?)",
                (symbol, company_name, 1 if is_active else 0)
            )
            self.conn.commit()  # Confirmar la transacción
            
            # Obtener el ID del símbolo insertado
            self.cursor.execute("SELECT SymbolID FROM Metadata.Symbols WHERE Symbol = ?", (symbol,))
            row = self.cursor.fetchone()
            return row[0] if row else None  # Devolver el ID o None si no se pudo obtener
        except Exception as e:
            # Manejar cualquier error durante la inserción
            print(f"Error al añadir el símbolo {symbol}: {e}")
            return None
    
    def update_symbol(self, symbol_id, company_name=None, is_active=None):
        """
        Actualiza la información de un símbolo.
        
        Args:
            symbol_id (int): ID del símbolo.
            company_name (str, optional): Nuevo nombre de la empresa.
            is_active (bool, optional): Nuevo estado activo.
            
        Returns:
            bool: True si se actualizó correctamente, False en caso contrario.
            
        Mejores prácticas POO:
        - Parámetros opcionales: Permite actualizar solo los campos necesarios
        - Construcción dinámica de consultas: Adapta la consulta según los parámetros proporcionados
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
                print("No hay campos para actualizar.")
                return False
            
            # Añadir el ID del símbolo a los parámetros
            params.append(symbol_id)
            
            # Ejecutar la consulta de actualización
            self.cursor.execute(
                f"UPDATE Metadata.Symbols SET {', '.join(update_parts)} WHERE SymbolID = ?",
                tuple(params)
            )
            self.conn.commit()  # Confirmar la transacción
            
            return True  # Actualización exitosa
        except Exception as e:
            # Manejar cualquier error durante la actualización
            print(f"Error al actualizar el símbolo con ID {symbol_id}: {e}")
            return False
    
    def get_last_download(self, symbol_id):
        """
        Obtiene la información de la última descarga para un símbolo.
        
        Args:
            symbol_id (int): ID del símbolo.
            
        Returns:
            dict: Información de la última descarga o None si no existe.
            
        Mejores prácticas POO:
        - Consultas optimizadas: Usa TOP 1 y ORDER BY para obtener el registro más reciente
        - Consistencia en el formato de retorno: Devuelve un diccionario como otros métodos
        """
        try:
            # Ejecutar consulta para obtener la última descarga ordenada por fecha
            self.cursor.execute(
                """
                SELECT TOP 1 * FROM Metadata.Downloads 
                WHERE SymbolID = ? 
                ORDER BY LastDownloadDate DESC
                """, 
                (symbol_id,)
            )
            # Obtener el primer resultado (o None si no hay resultados)
            row = self.cursor.fetchone()
            if row:
                # Convertir la fila en un diccionario usando los nombres de columnas
                columns = [column[0] for column in self.cursor.description]
                return dict(zip(columns, row))
            return None  # No se encontró ninguna descarga para ese símbolo
        except Exception as e:
            # Manejar cualquier error durante la consulta
            print(f"Error al obtener la última descarga para el símbolo con ID {symbol_id}: {e}")
            return None
    
    def add_download_record(self, symbol_id, start_date=None, end_date=None, status='Pending'):
        """
        Añade un nuevo registro de descarga.
        
        Args:
            symbol_id (int): ID del símbolo.
            start_date (datetime, optional): Fecha de inicio de la descarga.
            end_date (datetime, optional): Fecha de fin de la descarga.
            status (str, optional): Estado de la descarga ('Pending', 'Completed', 'Failed').
            
        Returns:
            int: ID del registro de descarga añadido o None si hay un error.
            
        Mejores prácticas POO:
        - Valores por defecto significativos: 'Pending' como estado inicial por defecto
        - Parámetros opcionales: Permite flexibilidad en la creación de registros
        """
        try:
            # Insertar nuevo registro de descarga usando consulta parametrizada
            self.cursor.execute(
                """
                INSERT INTO Metadata.Downloads 
                (SymbolID, LastDownloadDate, StartDate, EndDate, Status) 
                VALUES (?, GETDATE(), ?, ?, ?)
                """,
                (symbol_id, start_date, end_date, status)
            )
            self.conn.commit()  # Confirmar la transacción
            
            # Obtener el ID del registro insertado
            self.cursor.execute(
                """
                SELECT TOP 1 DownloadID FROM Metadata.Downloads 
                WHERE SymbolID = ? 
                ORDER BY LastDownloadDate DESC
                """, 
                (symbol_id,)
            )
            row = self.cursor.fetchone()
            return row[0] if row else None  # Devolver el ID o None si no se pudo obtener
        except Exception as e:
            # Manejar cualquier error durante la inserción
            print(f"Error al añadir registro de descarga para el símbolo con ID {symbol_id}: {e}")
            return None
    
    def update_download_status(self, download_id, status, end_date=None):
        """
        Actualiza el estado de un registro de descarga.
        
        Args:
            download_id (int): ID del registro de descarga.
            status (str): Nuevo estado ('Pending', 'Completed', 'Failed').
            end_date (datetime, optional): Nueva fecha de fin.
            
        Returns:
            bool: True si se actualizó correctamente, False en caso contrario.
            
        Mejores prácticas POO:
        - Métodos específicos: Proporciona una forma clara de actualizar el estado
        - Construcción dinámica de consultas: Similar a update_symbol para consistencia
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
            self.cursor.execute(
                f"UPDATE Metadata.Downloads SET {', '.join(update_parts)} WHERE DownloadID = ?",
                tuple(params)
            )
            self.conn.commit()  # Confirmar la transacción
            
            return True  # Actualización exitosa
        except Exception as e:
            # Manejar cualquier error durante la actualización
            print(f"Error al actualizar el estado de la descarga con ID {download_id}: {e}")
            return False
    
    def get_config_value(self, key):
        """
        Obtiene un valor de configuración.
        
        Args:
            key (str): Clave de configuración.
            
        Returns:
            str: Valor de configuración o None si no existe.
            
        Mejores prácticas POO:
        - Métodos simples y específicos: Realiza una única tarea bien definida
        - Manejo de errores consistente: Devuelve None si no se encuentra
        """
        try:
            # Ejecutar consulta parametrizada para obtener el valor de configuración
            self.cursor.execute("SELECT ConfigValue FROM Metadata.Configuration WHERE ConfigKey = ?", (key,))
            # Obtener el primer resultado (o None si no hay resultados)
            row = self.cursor.fetchone()
            return row[0] if row else None  # Devolver el valor o None si no existe
        except Exception as e:
            # Manejar cualquier error durante la consulta
            print(f"Error al obtener el valor de configuración {key}: {e}")
            return None
    
    def set_config_value(self, key, value, description=None):
        """
        Establece un valor de configuración.
        
        Args:
            key (str): Clave de configuración.
            value (str): Valor de configuración.
            description (str, optional): Descripción de la configuración.
            
        Returns:
            bool: True si se estableció correctamente, False en caso contrario.
            
        Mejores prácticas POO:
        - Validación interna: Verifica si la clave ya existe antes de decidir insertar o actualizar
        - Métodos complementarios: Trabaja en conjunto con get_config_value
        """
        try:
            # Verificar si la clave ya existe
            existing_value = self.get_config_value(key)
            
            if existing_value is not None:
                # Actualizar valor existente
                self.cursor.execute(
                    "UPDATE Metadata.Configuration SET ConfigValue = ?, ModifiedDate = GETDATE() WHERE ConfigKey = ?",
                    (value, key)
                )
            else:
                # Insertar nuevo valor
                self.cursor.execute(
                    """
                    INSERT INTO Metadata.Configuration 
                    (ConfigKey, ConfigValue, Description) 
                    VALUES (?, ?, ?)
                    """,
                    (key, value, description)
                )
            
            self.conn.commit()  # Confirmar la transacción
            return True  # Operación exitosa
        except Exception as e:
            # Manejar cualquier error durante la operación
            print(f"Error al establecer el valor de configuración {key}: {e}")
            return False
    
    def get_symbols_to_download(self):
        """
        Obtiene los símbolos que necesitan ser descargados.
        
        Determina qué símbolos necesitan actualización basándose en la fecha de última descarga.
        
        Returns:
            list: Lista de diccionarios con la información de los símbolos a descargar.
            
        Mejores prácticas POO:
        - Lógica de negocio encapsulada: Implementa reglas para determinar qué símbolos actualizar
        - Reutilización de métodos: Usa get_all_symbols y get_last_download
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
            
            return symbols_to_download  # Devolver la lista de símbolos para descargar
        except Exception as e:
            # Manejar cualquier error durante la operación
            print(f"Error al obtener los símbolos para descargar: {e}")
            return []

# Función principal para probar el gestor de metadatos
def main():
    """
    Función principal para probar el gestor de metadatos.
    
    Esta función demuestra el uso básico de la clase MetadataManager
    obteniendo información sobre símbolos y descargas.
    
    Mejores prácticas:
    - Incluir una función main() para permitir pruebas independientes del módulo
    - Usar if __name__ == "__main__" para permitir importación sin ejecución
    """
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
