"""
Módulo para descargar datos históricos de Yahoo Finance.
"""
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def descargar_datos_yahoo(symbol, periodo_inicio=None, periodo_fin=None, intervalo="1d"):
    """
    Descarga datos históricos de un símbolo bursátil desde Yahoo Finance.
    
    Parámetros:
    -----------
    symbol : str
        Símbolo bursátil a descargar (ej. 'AAPL', 'MSFT', 'EURUSD=X').
    periodo_inicio : str, opcional
        Fecha de inicio en formato 'YYYY-MM-DD'. Si no se especifica, 
        se utilizará un año atrás desde la fecha actual.
    periodo_fin : str, opcional
        Fecha de fin en formato 'YYYY-MM-DD'. Si no se especifica,
        se utilizará la fecha actual.
    intervalo : str, opcional
        Intervalo de tiempo para los datos. Opciones válidas:
        - '1d': diario (por defecto)
        - '1wk': semanal
        - '1mo': mensual
        - '1h': horario (solo disponible para los últimos 730 días)
        - '5m', '15m', '30m', '60m': minutos (disponibilidad limitada)
    
    Retorna:
    --------
    pandas.DataFrame
        DataFrame con los datos históricos del símbolo, incluyendo:
        - Date: fecha
        - Open: precio de apertura
        - High: precio máximo
        - Low: precio mínimo
        - Close: precio de cierre
        - Adj Close: precio de cierre ajustado
        - Volume: volumen de negociación
    
    Ejemplo:
    --------
    >>> # Descargar datos del último año para Apple
    >>> df_apple = descargar_datos_yahoo('AAPL')
    >>> # Descargar datos de Microsoft para un período específico
    >>> df_msft = descargar_datos_yahoo('MSFT', '2022-01-01', '2022-12-31', '1wk')
    """
    try:
        # Si no se especifica fecha de inicio, usar un año atrás
        if periodo_inicio is None:
            periodo_inicio = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        # Si no se especifica fecha de fin, usar la fecha actual
        if periodo_fin is None:
            periodo_fin = datetime.now().strftime('%Y-%m-%d')
        
        # Validar el símbolo
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        # Verificar si el símbolo existe
        if 'regularMarketPrice' not in info and 'previousClose' not in info:
            print(f"Advertencia: El símbolo '{symbol}' podría no ser válido o no tener datos disponibles.")
        
        # Descargar los datos históricos
        df = ticker.history(start=periodo_inicio, end=periodo_fin, interval=intervalo)
        
        # Verificar si se obtuvieron datos
        if df.empty:
            print(f"No se encontraron datos para el símbolo '{symbol}' en el período especificado.")
            return df
        
        # Resetear el índice para que la fecha sea una columna
        df = df.reset_index()
        
        # Si el índice es DatetimeIndex, convertir a columna Date
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            df = df.rename(columns={'index': 'Date'})
        
        print(f"Datos descargados exitosamente para '{symbol}' desde {periodo_inicio} hasta {periodo_fin}")
        return df
    
    except Exception as e:
        print(f"Error al descargar datos de Yahoo Finance: {str(e)}")
        raise

def guardar_datos(df, formato='csv', ruta_archivo=None, symbol=None):
    """
    Guarda los datos descargados en un archivo.
    
    Parámetros:
    -----------
    df : pandas.DataFrame
        DataFrame con los datos a guardar.
    formato : str, opcional
        Formato del archivo ('csv' o 'excel'). Por defecto es 'csv'.
    ruta_archivo : str, opcional
        Ruta completa donde guardar el archivo. Si no se especifica,
        se generará automáticamente basado en el símbolo y la fecha actual.
    symbol : str, opcional
        Símbolo bursátil para generar el nombre del archivo si no se especifica ruta_archivo.
    
    Retorna:
    --------
    str
        Ruta del archivo guardado.
    """
    if df.empty:
        print("No hay datos para guardar.")
        return None
    
    # Generar nombre de archivo si no se especifica
    if ruta_archivo is None:
        fecha_actual = datetime.now().strftime('%Y%m%d')
        if symbol is None:
            symbol = 'datos'
        
        if formato.lower() == 'csv':
            ruta_archivo = f"{symbol}_{fecha_actual}.csv"
        elif formato.lower() == 'excel':
            ruta_archivo = f"{symbol}_{fecha_actual}.xlsx"
        else:
            raise ValueError("Formato no soportado. Use 'csv' o 'excel'.")
    
    try:
        # Guardar según el formato
        if formato.lower() == 'csv':
            df.to_csv(ruta_archivo, index=False)
        elif formato.lower() == 'excel':
            df.to_excel(ruta_archivo, index=False)
        
        print(f"Datos guardados exitosamente en '{ruta_archivo}'")
        return ruta_archivo
    
    except Exception as e:
        print(f"Error al guardar los datos: {str(e)}")
        raise
