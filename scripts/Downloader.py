import requests
import pandas as pd
import pyodbc
import time
from dotenv import load_dotenv
import os

# CONFIGURACIÓN

env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', '.env')
if not os.path.exists(env_file):
    # Advertir al usuario si no se encuentra el archivo
    print(f"Advertencia: No se encontró el archivo .env en {env_file}.")

# Cargar las variables del archivo .env
load_dotenv(env_file)


# Conexión a SQL Server
cadena_conexion = f'DRIVER=/opt/homebrew/lib/libmsodbcsql.18.dylib;' + \
    f"SERVER={os.getenv('SQL_SERVER')},{os.getenv('SQL_PORT')};" + \
    f"DATABASE={os.getenv('SQL_DATABASE')};" + \
    f"UID={os.getenv('SQL_USER')};" + \
    f"PWD={os.getenv('SQL_PASSWORD')};" + \
    f"TrustServerCertificate=yes;"


try:
    conexion = pyodbc.connect(cadena_conexion)
    print("Conexión exitosa")
    conexion.close()
except Exception as e:
    print(f"Error al conectar: {e}")

# Función para descargar históricos desde Alpha Vantage
def obtener_historico_accion(symbol, api_key):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=full'
    respuesta = requests.get(url)
    datos_json = respuesta.json()

    if 'Time Series (Daily)' not in datos_json:
        raise ValueError(f"No hay datos disponibles para {symbol}: {datos_json.get('Note', 'Error desconocido')}")

    df = pd.DataFrame(datos_json['Time Series (Daily)']).T
    df.reset_index(inplace=True)
    df.columns = ['fecha', 'apertura', 'alto', 'bajo', 'cierre', 'volumen']

    df['fecha'] = pd.to_datetime(df['fecha'])
    df[['apertura', 'alto', 'bajo', 'cierre']] = df[['apertura', 'alto', 'bajo', 'cierre']].astype(float)
    df['volumen'] = df['volumen'].astype(int)
    df['Symbol'] = symbol  # Añadir columna de símbolo para identificar datos

    return df


conexion = pyodbc.connect(cadena_conexion)
consulta = '''
SELECT TOP (5) [SymbolID]
      ,[Symbol]
      ,[CompanyName]
      ,[IsActive]
      ,[CreatedDate]
  FROM [AlphaVantageDB].[Metadata].[Symbols]
  order by [SymbolID] ASC
'''

symbols_df = pd.read_sql(consulta, conexion)
# Extraer los valores de las columnas SymbolID y Symbol como una lista de diccionarios
symbols_lista = symbols_df[['SymbolID', 'Symbol']].to_dict(orient='records')

if not symbols_lista:
    print("No se encontraron símbolos para procesar.")
    exit()
# Crear cursor para ejecutar comandos SQL   

cursor = conexion.cursor()

# SQL para insertar históricos (ajusta al nombre exacto de tu tabla destino)
insert_sql = '''
    INSERT INTO [AVData].[StockPrices] 
    ([SymbolId], [Date], [Open], [High], [Low], [Close], [Volume], [CreatedDate])
    VALUES (?, ?, ?, ?, ?, ?, ?, getdate())
'''

for symbol_data in symbols_lista:
    try:
        symbol_id = symbol_data['SymbolID']
        symbol = symbol_data['Symbol']
        print(f"Procesando SymbolID: {symbol_id}, Symbol: {symbol}")
        df_historico = obtener_historico_accion(symbol, os.getenv('ALPHA_VANTAGE_API_KEY'))

        # Insertar cada fila en SQL Server
        for index, row in df_historico.iterrows():
            cursor.execute(
                insert_sql,
                symbol_id, row['fecha'], row['apertura'], row['alto'], row['bajo'],
                row['cierre'], row['volumen']
            )
        
        conexion.commit()
        print(f"Histórico de {symbol} insertado correctamente.")

        # Alpha Vantage limita peticiones a 5 por minuto
        time.sleep(12)  # 12 segundos entre cada símbolo garantiza cumplir límites
        
    except Exception as e:
        print(f"Error con {symbol}: {e}")
        continue

cursor.close()
conexion.close()