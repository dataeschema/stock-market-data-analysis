#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para conectar con la API de Alpha Vantage y obtener datos históricos de acciones.
Versión compatible con macOS Apple Silicon.

Este módulo implementa una clase para interactuar con la API de Alpha Vantage,
respetando los límites de tasa y proporcionando métodos para obtener y procesar
datos históricos de acciones.

Autor: Manus
Fecha: 31/03/2025
"""

# Importaciones de bibliotecas estándar
import os                  # Para operaciones del sistema de archivos
import sys                 # Para funcionalidades del sistema
import json                # Para manejo de datos JSON
import time                # Para funciones relacionadas con el tiempo
import requests            # Para realizar solicitudes HTTP
from datetime import datetime  # Para manejo de fechas y horas
from dotenv import load_dotenv  # Para cargar variables de entorno desde archivo .env

# Cargar variables de entorno desde el archivo .env
# Esto permite mantener información sensible fuera del código
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', '.env'))

# Configuración de Alpha Vantage
# Obtenemos la API key desde las variables de entorno para mayor seguridad
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
BASE_URL = 'https://www.alphavantage.co/query'  # URL base para todas las solicitudes a la API

# Verificar que la API key esté configurada
# Es importante validar esto antes de intentar hacer cualquier solicitud
if not API_KEY:
    print("Error: No se ha configurado la API key de Alpha Vantage.")
    print("Por favor, crea un archivo .env en el directorio config con tu API key.")
    sys.exit(1)  # Salir del programa con código de error

class AlphaVantageAPI:
    """
    Clase para interactuar con la API de Alpha Vantage.
    
    Esta clase proporciona métodos para obtener datos históricos de acciones
    desde Alpha Vantage, respetando los límites de tasa de la API y
    procesando los datos recibidos.
    
    Atributos:
        api_key (str): Clave de API para autenticación con Alpha Vantage.
        base_url (str): URL base para las solicitudes a la API.
        last_call_timestamp (float): Marca de tiempo de la última llamada a la API.
        min_call_interval (float): Intervalo mínimo entre llamadas a la API en segundos.
    """
    
    def __init__(self, api_key=None):
        """
        Inicializa la clase con la API key proporcionada o la del archivo .env.
        
        Args:
            api_key (str, optional): Clave de API para Alpha Vantage. Si no se proporciona,
                                    se usa la del archivo .env.
        """
        # Inicializar atributos de la clase
        self.api_key = api_key or API_KEY  # Usar la API key proporcionada o la del .env
        self.base_url = BASE_URL  # URL base para las solicitudes
        self.last_call_timestamp = 0  # Inicializar el timestamp de la última llamada
        self.min_call_interval = 12  # Alpha Vantage limita a 5 llamadas por minuto (12 segundos entre llamadas)
    
    def _respect_rate_limit(self):
        """
        Respeta el límite de tasa de la API esperando si es necesario.
        
        Este método calcula el tiempo transcurrido desde la última llamada a la API
        y espera si es necesario para cumplir con el intervalo mínimo entre llamadas.
        
        Mejores prácticas:
        - Respetar los límites de tasa de las APIs es crucial para evitar bloqueos
        - Implementar esperas dinámicas mejora la robustez del código
        """
        # Obtener el tiempo actual
        current_time = time.time()
        # Calcular el tiempo transcurrido desde la última llamada
        time_since_last_call = current_time - self.last_call_timestamp
        
        # Si no ha pasado suficiente tiempo desde la última llamada, esperar
        if time_since_last_call < self.min_call_interval:
            sleep_time = self.min_call_interval - time_since_last_call
            print(f"Esperando {sleep_time:.2f} segundos para respetar el límite de tasa de la API...")
            time.sleep(sleep_time)  # Esperar el tiempo necesario
        
        # Actualizar el timestamp de la última llamada
        self.last_call_timestamp = time.time()
    
    def get_daily_time_series(self, symbol, output_size='full'):
        """
        Obtiene los datos históricos diarios para un símbolo específico.
        
        Args:
            symbol (str): El símbolo de la acción (ej. 'AAPL', 'NVDA').
            output_size (str): 'compact' para los últimos 100 puntos de datos, 
                              'full' para todo el historial.
            
        Returns:
            dict: Datos históricos diarios o None si hay un error.
            
        Mejores prácticas:
        - Validar parámetros de entrada
        - Manejar errores de forma explícita
        - Respetar límites de tasa de la API
        """
        # Respetar el límite de tasa antes de hacer la solicitud
        self._respect_rate_limit()
        
        # Preparar los parámetros para la solicitud
        params = {
            'function': 'TIME_SERIES_DAILY',  # Función de la API para datos diarios
            'symbol': symbol,                 # Símbolo de la acción
            'outputsize': output_size,        # Tamaño de salida (compact o full)
            'apikey': self.api_key            # Clave de API
        }
        
        try:
            # Informar al usuario sobre la operación
            print(f"Obteniendo datos históricos diarios para {symbol}...")
            
            # Realizar la solicitud HTTP GET
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()  # Lanza una excepción para códigos de error HTTP
            
            # Convertir la respuesta a formato JSON
            data = response.json()
            
            # Verificar si hay un mensaje de error en la respuesta
            if 'Error Message' in data:
                print(f"Error de Alpha Vantage: {data['Error Message']}")
                return None
                
            # Verificar si se ha alcanzado el límite de la API
            if 'Note' in data and 'call frequency' in data['Note']:
                print(f"Advertencia: {data['Note']}")
                # Esperar un minuto adicional si se ha alcanzado el límite
                time.sleep(60)
                # Reintentar la solicitud
                return self.get_daily_time_series(symbol, output_size)
                
            # Devolver los datos si todo está bien
            return data
            
        except requests.exceptions.RequestException as e:
            # Manejar errores de solicitud HTTP
            print(f"Error al realizar la solicitud a Alpha Vantage: {e}")
            return None
        except json.JSONDecodeError:
            # Manejar errores de decodificación JSON
            print("Error al decodificar la respuesta JSON de Alpha Vantage.")
            return None
    
    def get_daily_adjusted(self, symbol, output_size='full'):
        """
        Obtiene los datos históricos diarios ajustados para un símbolo específico.
        
        Los datos ajustados incluyen ajustes por dividendos y splits de acciones.
        
        Args:
            symbol (str): El símbolo de la acción (ej. 'AAPL', 'NVDA').
            output_size (str): 'compact' para los últimos 100 puntos de datos, 
                              'full' para todo el historial.
            
        Returns:
            dict: Datos históricos diarios ajustados o None si hay un error.
            
        Mejores prácticas:
        - Estructura similar a get_daily_time_series para consistencia
        - Manejo de errores consistente en toda la clase
        """
        # Respetar el límite de tasa antes de hacer la solicitud
        self._respect_rate_limit()
        
        # Preparar los parámetros para la solicitud
        params = {
            'function': 'TIME_SERIES_DAILY_ADJUSTED',  # Función de la API para datos diarios ajustados
            'symbol': symbol,                         # Símbolo de la acción
            'outputsize': output_size,                # Tamaño de salida (compact o full)
            'apikey': self.api_key                    # Clave de API
        }
        
        try:
            # Informar al usuario sobre la operación
            print(f"Obteniendo datos históricos diarios ajustados para {symbol}...")
            
            # Realizar la solicitud HTTP GET
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()  # Lanza una excepción para códigos de error HTTP
            
            # Convertir la respuesta a formato JSON
            data = response.json()
            
            # Verificar si hay un mensaje de error en la respuesta
            if 'Error Message' in data:
                print(f"Error de Alpha Vantage: {data['Error Message']}")
                return None
                
            # Verificar si se ha alcanzado el límite de la API
            if 'Note' in data and 'call frequency' in data['Note']:
                print(f"Advertencia: {data['Note']}")
                # Esperar un minuto adicional si se ha alcanzado el límite
                time.sleep(60)
                # Reintentar la solicitud
                return self.get_daily_adjusted(symbol, output_size)
                
            # Devolver los datos si todo está bien
            return data
            
        except requests.exceptions.RequestException as e:
            # Manejar errores de solicitud HTTP
            print(f"Error al realizar la solicitud a Alpha Vantage: {e}")
            return None
        except json.JSONDecodeError:
            # Manejar errores de decodificación JSON
            print("Error al decodificar la respuesta JSON de Alpha Vantage.")
            return None
    
    def parse_daily_time_series(self, data):
        """
        Analiza los datos de la serie temporal diaria y los convierte a un formato más utilizable.
        
        Args:
            data (dict): Datos de la API de Alpha Vantage.
            
        Returns:
            list: Lista de diccionarios con los datos históricos formateados.
            
        Mejores prácticas:
        - Validar la estructura de los datos de entrada
        - Convertir tipos de datos apropiadamente (str a float/int)
        - Ordenar los resultados para facilitar su uso
        """
        # Verificar que los datos sean válidos y contengan la serie temporal
        if not data or 'Time Series (Daily)' not in data:
            print("No hay datos de serie temporal para analizar.")
            return []
        
        # Extraer la serie temporal
        time_series = data['Time Series (Daily)']
        formatted_data = []
        
        # Procesar cada fecha y sus valores
        for date, values in time_series.items():
            # Crear un diccionario con los datos formateados
            formatted_data.append({
                'date': date,                          # Fecha como string (YYYY-MM-DD)
                'open': float(values['1. open']),      # Precio de apertura como float
                'high': float(values['2. high']),      # Precio más alto como float
                'low': float(values['3. low']),        # Precio más bajo como float
                'close': float(values['4. close']),    # Precio de cierre como float
                'volume': int(values['5. volume'])     # Volumen como entero
            })
        
        # Ordenar por fecha (más reciente primero)
        formatted_data.sort(key=lambda x: x['date'], reverse=True)
        return formatted_data
    
    def parse_daily_adjusted(self, data):
        """
        Analiza los datos de la serie temporal diaria ajustada y los convierte a un formato más utilizable.
        
        Args:
            data (dict): Datos de la API de Alpha Vantage.
            
        Returns:
            list: Lista de diccionarios con los datos históricos formateados.
            
        Mejores prácticas:
        - Estructura similar a parse_daily_time_series para consistencia
        - Incluir todos los campos relevantes de los datos ajustados
        """
        # Verificar que los datos sean válidos y contengan la serie temporal
        if not data or 'Time Series (Daily)' not in data:
            print("No hay datos de serie temporal ajustada para analizar.")
            return []
        
        # Extraer la serie temporal
        time_series = data['Time Series (Daily)']
        formatted_data = []
        
        # Procesar cada fecha y sus valores
        for date, values in time_series.items():
            # Crear un diccionario con los datos formateados
            formatted_data.append({
                'date': date,                                  # Fecha como string (YYYY-MM-DD)
                'open': float(values['1. open']),              # Precio de apertura como float
                'high': float(values['2. high']),              # Precio más alto como float
                'low': float(values['3. low']),                # Precio más bajo como float
                'close': float(values['4. close']),            # Precio de cierre como float
                'adjusted_close': float(values['5. adjusted close']),  # Precio de cierre ajustado
                'volume': int(values['6. volume']),            # Volumen como entero
                'dividend_amount': float(values['7. dividend amount']),  # Monto de dividendo
                'split_coefficient': float(values['8. split coefficient'])  # Coeficiente de split
            })
        
        # Ordenar por fecha (más reciente primero)
        formatted_data.sort(key=lambda x: x['date'], reverse=True)
        return formatted_data
    
    def save_to_json(self, data, symbol, output_dir='../data'):
        """
        Guarda los datos en un archivo JSON.
        
        Args:
            data (dict): Datos a guardar.
            symbol (str): Símbolo de la acción.
            output_dir (str): Directorio de salida.
            
        Returns:
            str: Ruta del archivo guardado o None si hay un error.
            
        Mejores prácticas:
        - Crear directorios si no existen
        - Usar nombres de archivo descriptivos con timestamps
        - Manejar errores de escritura de archivos
        """
        # Verificar que haya datos para guardar
        if not data:
            print("No hay datos para guardar.")
            return None
        
        # Crear el directorio si no existe
        # Esto es una buena práctica para evitar errores al guardar archivos
        os.makedirs(output_dir, exist_ok=True)
        
        # Generar nombre de archivo con timestamp para evitar sobreescrituras
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{symbol}_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        
        try:
            # Escribir los datos en formato JSON con indentación para legibilidad
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Datos guardados en {filepath}")
            return filepath
        except Exception as e:
            # Manejar cualquier error durante la escritura del archivo
            print(f"Error al guardar los datos en JSON: {e}")
            return None

# Función principal para probar la API
def main():
    """
    Función principal para probar la conexión con Alpha Vantage.
    
    Esta función demuestra el uso básico de la clase AlphaVantageAPI
    obteniendo datos para un símbolo y guardándolos en un archivo JSON.
    
    Mejores prácticas:
    - Incluir una función main() para permitir pruebas independientes del módulo
    - Usar if __name__ == "__main__" para permitir importación sin ejecución
    """
    # Crear directorio de datos si no existe
    os.makedirs(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data'), exist_ok=True)
    
    # Inicializar la API
    api = AlphaVantageAPI()
    
    # Probar con un símbolo
    symbol = 'AAPL'
    data = api.get_daily_adjusted(symbol)
    
    if data:
        # Analizar y guardar los datos
        parsed_data = api.parse_daily_adjusted(data)
        print(f"Se obtuvieron {len(parsed_data)} registros históricos para {symbol}.")
        
        # Guardar en JSON
        output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        api.save_to_json(data, symbol, output_dir)
    else:
        print(f"No se pudieron obtener datos para {symbol}.")

# Punto de entrada para ejecución directa del script
if __name__ == "__main__":
    main()
