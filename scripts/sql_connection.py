"""
Módulo para conectar con SQL Server mediante SQLAlchemy y pyodbc.
"""
import os
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
def conectar_sql_server():
    """
    Crea una conexión a SQL Server utilizando SQLAlchemy y pyodbc.
    
    Retorna:
    --------
    engine : sqlalchemy.engine.Engine
        Objeto engine de SQLAlchemy para interactuar con la base de datos.
    """
    mySQLPasssord = os.getenv('SQL_PASSWORD').replace("@", "%40")
    engineURL = f"mssql+pyodbc://{os.getenv('SQL_USER')}:{mySQLPasssord}@{os.getenv('SQL_SERVER')}:{os.getenv('SQL_PORT')}/{os.getenv('SQL_DATABASE')}?driver=/opt/homebrew/lib/libmsodbcsql.18.dylib&TrustServerCertificate=yes"

    engine = create_engine(engineURL)  
    
    try:
        engine.connect()
        print("Conexión exitosa")
          
        return engine
    
    except SQLAlchemyError as e:
        print(f"Error de SQLAlchemy: {str(e)}")
        raise
    except Exception as e:
        print(f"Error al conectar con SQL Server: {str(e)}")
        raise

def ejecutar_consulta(engine, consulta, parametros=None):
    """
    Ejecuta una consulta SQL y devuelve los resultados.
    
    Parámetros:
    -----------
    engine : sqlalchemy.engine.Engine
        Objeto engine de SQLAlchemy creado con la función conectar_sql_server.
    consulta : str
        Consulta SQL a ejecutar.
    parametros : dict, opcional
        Diccionario con los parámetros para la consulta.
    
    Retorna:
    --------
    list
        Lista de diccionarios con los resultados de la consulta.
    
    Ejemplo:
    --------
    >>> engine = conectar_sql_server('SERVIDOR', 'MiBaseDeDatos', 'usuario', 'contraseña')
    >>> resultados = ejecutar_consulta(engine, "SELECT * FROM Clientes WHERE Ciudad = :ciudad", {"ciudad": "Madrid"})
    """
    try:
        with engine.connect() as conn:
            if parametros:
                result = pd.read_sql(text(consulta), parametros)
            else:
                result = pd.read_sql(consulta, engine)
            
            # Convertir resultados a lista de diccionarios
            return result
    
    except SQLAlchemyError as e:
        print(f"Error al ejecutar la consulta: {str(e)}")
        raise
