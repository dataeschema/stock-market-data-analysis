#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Implementación del cliente de API para Alpha Vantage.
Versión compatible con macOS Apple Silicon.

Este módulo implementa la interfaz ApiClient para Alpha Vantage,
proporcionando métodos para interactuar con la API y manejar límites de tasa.

Autor: Manus
Fecha: 31/03/2025
"""

# Importaciones de bibliotecas estándar
import os                      # Para operaciones del sistema de archivos
import sys                     # Para funcionalidades del sistema
import json                    # Para manejo de datos JSON
import time                    # Para funciones relacionadas con el tiempo
import logging                 # Para registro de eventos
import requests                # Para realizar solicitudes HTTP
from datetime import datetime  # Para manejo de fechas y horas
from typing import Dict, List, Any, Optional, Tuple, Union  # Para tipado estático

# Importar módulos propios
# Añadimos el directorio padre al path para poder importar módulos del proyecto
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from scripts.base import ApiClient, ConfigManager, Singleton  # Importar clases base

# Crear logger para este módulo
logger = logging.getLogger(__name__)

class AlphaVantageClient(ApiClient, Singleton):
    """
    Implementación de ApiClient para Alpha Vantage.
    
    Esta clase proporciona métodos para interactuar con la API de Alpha Vantage,
    implementando la interfaz definida en ApiClient y utilizando el patrón Singleton.
    
    Atributos:
        api_key (str): Clave de API para Alpha Vantage.
        base_url (str): URL base para las solicitudes a la API.
        last_call_timestamp (float): Marca de tiempo de la última llamada a la API.
        min_call_interval (float): Intervalo mínimo entre llamadas a la API en segundos.
        config (ConfigManager): Gestor de configuración.
    
    Mejores prácticas POO:
    - Implementación de interfaz: Implementa todos los métodos definidos en ApiClient
    - Patrón Singleton: Asegura que solo exista una instancia de la clase
    - Principio de responsabilidad única: Solo se encarga de la comunicación con Alpha Vantage
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Inicializa el cliente de Alpha Vantage.
        
        Args:
            api_key (str, optional): Clave de API para Alpha Vantage.
                Si no se proporciona, se usa el valor de configuración.
        """
        # Evitar reinicialización si ya existe una instancia (debido a Singleton)
        if hasattr(self, 'api_key'):
            return
            
        # Obtener configuración
        self.config = ConfigManager()
        self.api_key = api_key or self.config.get('ALPHA_VANTAGE_API_KEY', '')
        self.base_url = self.config.get('ALPHA_VANTAGE_BASE_URL', 'https://www.alphavantage.co/query')
        
        # Inicializar atributos para manejo de límites de tasa
        self.last_call_timestamp = 0
        self.min_call_interval = 12  # Alpha Vantage limita a 5 llamadas por minuto (12 segundos entre llamadas)
        
        # Verificar que la API key esté configurada
        if not self.api_key:
            logger.error("No se ha configurado la API key de Alpha Vantage")
            logger.info("Por favor, configura ALPHA_VANTAGE_API_KEY en el archivo .env")
    
    def handle_rate_limit(self) -> None:
        """
        Maneja los límites de tasa de la API esperando si es necesario.
        
        Este método calcula el tiempo transcurrido desde la última llamada a la API
        y espera si es necesario para cumplir con el intervalo mínimo entre llamadas.
        
        Mejores prácticas POO:
        - Encapsulamiento: Oculta la lógica de manejo de límites de tasa
        - Método con responsabilidad única: Solo se encarga de manejar límites de tasa
        """
        # Obtener el tiempo actual
        current_time = time.time()
        # Calcular el tiempo transcurrido desde la última llamada
        time_since_last_call = current_time - self.last_call_timestamp
        
        # Si no ha pasado suficiente tiempo desde la última llamada, esperar
        if time_since_last_call < self.min_call_interval:
            sleep_time = self.min_call_interval - time_since_last_call
            logger.info(f"Esperando {sleep_time:.2f} segundos para respetar el límite de tasa de la API...")
            time.sleep(sleep_time)  # Esperar el tiempo necesario
        
        # Actualizar el timestamp de la última llamada
        self.last_call_timestamp = time.time()
    
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Realiza una solicitud GET a la API de Alpha Vantage.
        
        Args:
            endpoint (str): Endpoint de la API (no utilizado en Alpha Vantage).
            params (dict, optional): Parámetros para la solicitud.
            
        Returns:
            dict: Respuesta de la API.
            
        Raises:
            Exception: Si hay un error al realizar la solicitud.
        """
        # Respetar el límite de tasa antes de hacer la solicitud
        self.handle_rate_limit()
        
        # Preparar los parámetros para la solicitud
        request_params = params.copy() if params else {}
        # Añadir la API key a los parámetros
        request_params['apikey'] = self.api_key
        
        try:
            # Realizar la solicitud HTTP GET
            logger.debug(f"Realizando solicitud GET a {self.base_url} con parámetros: {request_params}")
            response = requests.get(self.base_url, params=request_params)
            response.raise_for_status()  # Lanza una excepción para códigos de error HTTP
            
            # Convertir la respuesta a formato JSON
            data = response.json()
            
            # Verificar si hay un mensaje de error en la respuesta
            if 'Error Message' in data:
                error_message = data['Error Message']
                logger.error(f"Error de Alpha Vantage: {error_message}")
                raise Exception(f"Error de Alpha Vantage: {error_message}")
                
            # Verificar si se ha alcanzado el límite de la API
            if 'Note' in data and 'call frequency' in data['Note']:
                logger.warning(f"Advertencia de Alpha Vantage: {data['Note']}")
                # No lanzamos excepción aquí, solo registramos la advertencia
            
            return data
            
        except requests.exceptions.RequestException as e:
            # Manejar errores de solicitud HTTP
            logger.error(f"Error al realizar la solicitud a Alpha Vantage: {e}")
            raise
        except json.JSONDecodeError:
            # Manejar errores de decodificación JSON
            logger.error("Error al decodificar la respuesta JSON de Alpha Vantage")
            raise
    
    def post(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Realiza una solicitud POST a la API de Alpha Vantage.
        
        Alpha Vantage no utiliza solicitudes POST, por lo que este método
        lanza una excepción.
        
        Args:
            endpoint (str): Endpoint de la API.
            data (dict, optional): Datos para la solicitud.
            
        Returns:
            dict: Respuesta de la API.
            
        Raises:
            NotImplementedError: Alpha Vantage no soporta solicitudes POST.
        """
        # Alpha Vantage no utiliza solicitudes POST
        logger.error("Alpha Vantage no soporta solicitudes POST")
        raise NotImplementedError("Alpha Vantage no soporta solicitudes POST")
    
    def get_daily_time_series(self, symbol: str, output_size: str = 'full') -> Dict[str, Any]:
        """
        Obtiene los datos históricos diarios para un símbolo específico.
        
        Args:
            symbol (str): El símbolo de la acción (ej. 'AAPL', 'NVDA').
            output_size (str): 'compact' para los últimos 100 puntos de datos, 
                              'full' para todo el historial.
            
        Returns:
            dict: Datos históricos diarios.
            
        Raises:
            Exception: Si hay un error al obtener los datos.
        """
        # Preparar los parámetros para la solicitud
        params = {
            'function': 'TIME_SERIES_DAILY',  # Función de la API para datos diarios
            'symbol': symbol,                 # Símbolo de la acción
            'outputsize': output_size,        # Tamaño de salida (compact o full)
        }
        
        # Realizar la solicitud GET
        logger.info(f"Obteniendo datos históricos diarios para {symbol}...")
        return self.get('', params)
    
    def get_daily_adjusted(self, symbol: str, output_size: str = 'full') -> Dict[str, Any]:
        """
        Obtiene los datos históricos diarios ajustados para un símbolo específico.
        
        Los datos ajustados incluyen ajustes por dividendos y splits de acciones.
        
        Args:
            symbol (str): El símbolo de la acción (ej. 'AAPL', 'NVDA').
            output_size (str): 'compact' para los últimos 100 puntos de datos, 
                              'full' para todo el historial.
            
        Returns:
            dict: Datos históricos diarios ajustados.
            
        Raises:
            Exception: Si hay un error al obtener los datos.
        """
        # Preparar los parámetros para la solicitud
        params = {
            'function': 'TIME_SERIES_DAILY_ADJUSTED',  # Función de la API para datos diarios ajustados
            'symbol': symbol,                         # Símbolo de la acción
            'outputsize': output_size,                # Tamaño de salida (compact o full)
        }
        
        # Realizar la solicitud GET
        logger.info(f"Obteniendo datos históricos diarios ajustados para {symbol}...")
        return self.get('', params)
    
    def save_to_json(self, data: Dict[str, Any], symbol: str, output_dir: str = '../data') -> Optional[str]:
        """
        Guarda los datos en un archivo JSON.
        
        Args:
            data (dict): Datos a guardar.
            symbol (str): Símbolo de la acción.
            output_dir (str): Directorio de salida.
            
        Returns:
            str: Ruta del archivo guardado o None si hay un error.
        """
        # Verificar que haya datos para guardar
        if not data:
            logger.error("No hay datos para guardar")
            return None
        
        # Crear el directorio si no existe
        os.makedirs(output_dir, exist_ok=True)
        
        # Generar nombre de archivo con timestamp para evitar sobreescrituras
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{symbol}_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        
        try:
            # Escribir los datos en formato JSON con indentación para legibilidad
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=4)
            logger.info(f"Datos guardados en {filepath}")
            return filepath
        except Exception as e:
            # Manejar cualquier error durante la escritura del archivo
            logger.error(f"Error al guardar los datos en JSON: {e}")
            return None
    
    def parse_daily_time_series(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Analiza los datos de la serie temporal diaria y los convierte a un formato más utilizable.
        
        Args:
            data (dict): Datos de la API de Alpha Vantage.
            
        Returns:
            list: Lista de diccionarios con los datos históricos formateados.
        """
        # Verificar que los datos sean válidos y contengan la serie temporal
        if not data or 'Time Series (Daily)' not in data:
            logger.error("No hay datos de serie temporal para analizar")
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
    
    def parse_daily_adjusted(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Analiza los datos de la serie temporal diaria ajustada y los convierte a un formato más utilizable.
        
        Args:
            data (dict): Datos de la API de Alpha Vantage.
            
        Returns:
            list: Lista de diccionarios con los datos históricos formateados.
        """
        # Verificar que los datos sean válidos y contengan la serie temporal
        if not data or 'Time Series (Daily)' not in data:
            logger.error("No hay datos de serie temporal ajustada para analizar")
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

# Función principal para probar el cliente de API
def main():
    """
    Función principal para probar el cliente de Alpha Vantage.
    
    Esta función demuestra el uso básico del cliente de Alpha Vantage.
    """
    # Crear una instancia del cliente
    client = AlphaVantageClient()
    
    # Verificar que la API key esté configurada
    if not client.api_key:
        print("Error: No se ha configurado la API key de Alpha Vantage")
        print("Por favor, configura ALPHA_VANTAGE_API_KEY en el archivo .env")
        return
    
    # Probar con un símbolo
    symbol = 'AAPL'
    try:
        # Obtener datos diarios ajustados
        print(f"Obteniendo datos para {symbol}...")
        data = client.get_daily_adjusted(symbol, 'compact')
        
        # Verificar si se obtuvieron datos
        if data and 'Time Series (Daily)' in data:
            # Analizar los datos
            parsed_data = client.parse_daily_adjusted(data)
            print(f"Se obtuvieron {len(parsed_data)} registros históricos para {symbol}")
            
            # Mostrar el primer registro
            if parsed_data:
                print("\nPrimer registro:")
                for key, value in parsed_data[0].items():
                    print(f"  {key}: {value}")
            
            # Guardar en JSON
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
            filepath = client.save_to_json(data, symbol, output_dir)
            if filepath:
                print(f"\nDatos guardados en: {filepath}")
        else:
            print(f"No se pudieron obtener datos para {symbol}")
            
    except Exception as e:
        print(f"Error: {e}")

# Punto de entrada para ejecución directa del script
if __name__ == "__main__":
    main()
