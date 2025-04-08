"""
Script principal que combina funcionalidades de conexión a SQL Server y descarga de datos de Yahoo Finance.

Este script proporciona dos funcionalidades principales:
1. Conexión a bases de datos SQL Server mediante SQLAlchemy y pyodbc
2. Descarga de datos históricos de símbolos bursátiles desde Yahoo Finance

"""

import os
import pandas as pd
import pyodbc
import time
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import text

# Importar funciones de los módulos creados
from sql_connection import conectar_sql_server, ejecutar_consulta
from yahoo_finance import descargar_datos_yahoo, guardar_datos

def guardar_datos_en_sql(engine, df, tabla, schema=None, if_exists='replace'):
    """
    Guarda un DataFrame en una tabla de SQL Server.
    
    Parámetros:
    -----------
    engine : sqlalchemy.engine.Engine
        Objeto engine de SQLAlchemy para la conexión.
    df : pandas.DataFrame
        DataFrame con los datos a guardar.
    tabla : str
        Nombre de la tabla donde guardar los datos.
    schema : str, opcional
        Nombre del esquema de la base de datos.
    if_exists : str, opcional
        Acción a tomar si la tabla ya existe:
        - 'fail': Lanza un error (por defecto)
        - 'replace': Elimina la tabla existente y crea una nueva
        - 'append': Añade los datos a la tabla existente
    
    Retorna:
    --------
    bool
        True si la operación fue exitosa, False en caso contrario.
    """
    try:
        df.to_sql(
            name=tabla,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False
        )
        print(f"Datos guardados exitosamente en la tabla '{tabla}'")
        return True
    except Exception as e:
        print(f"Error al guardar datos en SQL Server: {str(e)}")
        return False

def ejemplo_uso():
    
    conexion = conectar_sql_server()
    
    consulta = '''
    SELECT [SymbolID]
        ,[Symbol]
        ,[CompanyName]
        ,[IsActive]
        ,[CreatedDate]
    FROM [AlphaVantageDB].[Metadata].[Symbols]
    WHERE [Symbol] in ('AAPL','MSFT','NVDA','GOOGL','AMZN')
    order by [SymbolID] ASC
    '''

    symbols_df = ejecutar_consulta(conexion, consulta)

    # Verificar si symbols_df es un DataFrame
    if not isinstance(symbols_df, pd.DataFrame):
        print("Error: La consulta no devolvió un DataFrame.")
        exit()

    # Extraer los valores de las columnas SymbolID y Symbol como una lista de diccionarios
    symbols_lista = symbols_df[['SymbolID', 'Symbol']].to_dict(orient='records')

    if not symbols_lista:
        print("No se encontraron símbolos para procesar.")
        exit()


    # SQL para insertar históricos (ajusta al nombre exacto de tu tabla destino)
    insert_sql = '''
        INSERT INTO [AVData].[StockPrices] 
        ([SymbolId], [Date], [Open], [High], [Low], [Close], [Volume], [CreatedDate])
        VALUES (:symbol_id, :date, :open, :high, :low, :close, :volume, GETDATE())
    '''

    for symbol_data in symbols_lista:
        try:
            symbol_id = symbol_data['SymbolID']
            symbol = symbol_data['Symbol']
            print(f"Procesando SymbolID: {symbol_id}, Symbol: {symbol}")
            # Descargar datos históricos de Yahoo Finance
            df_historico = descargar_datos_yahoo(symbol, periodo_inicio='2010-01-01', periodo_fin='2024-12-31')

            with conexion.connect() as conn:
            # Insertar cada fila en SQL Server
                for index, row in df_historico.iterrows():
                    params = {
                        "symbol_id": symbol_id,
                        "date": row['Date'],
                        "open": row['Open'],
                        "high": row['High'],    
                        "low": row['Low'],
                        "close": row['Close'],
                        "volume": row['Volume']
                    }
                    conn.execute(text(insert_sql),params)
                
                # Guardar el DataFrame en SQL Server
                conn.commit()
                print(f"Histórico de {symbol} insertado correctamente.")

                time.sleep(12)  # 12 segundos entre cada símbolo garantiza cumplir límites

        except Exception as e:
            print(f"Error con {symbol}: {e}")
            continue

    conexion.dispose()
    # Cerrar la conexión

if __name__ == "__main__":
    
    #Cargamos configuración 
    env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', '.env')
    if not os.path.exists(env_file):
        # Advertir al usuario si no se encuentra el archivo
        print(f"Advertencia: No se encontró el archivo .env en {env_file}.")

    # Cargar las variables del archivo .env
    load_dotenv(env_file)

    # Conexión a SQL Server
    ejemplo_uso()
