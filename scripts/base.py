#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Módulo base para implementar patrones de diseño y mejores prácticas de POO.
Proporciona clases abstractas y utilidades para el proyecto Alpha Vantage.

Este módulo define interfaces y clases base que serán utilizadas por los demás
componentes del sistema, asegurando consistencia y aplicación de principios SOLID.

Autor: Manus
Fecha: 31/03/2025
"""

# Importaciones de bibliotecas estándar
import os                      # Para operaciones del sistema de archivos
import sys                     # Para funcionalidades del sistema
import abc                     # Para definir clases abstractas
import logging                 # Para registro de eventos
from datetime import datetime  # Para manejo de fechas y horas
from typing import Dict, List, Any, Optional, Tuple, Union  # Para tipado estático
from dotenv import load_dotenv
# Cargar variables de entorno desde el archivo .env

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', 'alpha_vantage.log')),
        logging.StreamHandler()
    ]
)

# Crear logger para este módulo
logger = logging.getLogger(__name__)

class Singleton:
    """
    Implementación del patrón de diseño Singleton.
    
    Esta clase permite implementar el patrón Singleton en cualquier clase
    que la herede, asegurando que solo exista una instancia de la clase.
    
    Mejores prácticas POO:
    - Patrón de diseño: Implementa el patrón Singleton
    - Metaclase: Utiliza una metaclase para controlar la creación de instancias
    """
    
    _instances = {}
    
    def __new__(cls, *args, **kwargs):
        """
        Crea una nueva instancia solo si no existe una previamente.
        
        Args:
            *args: Argumentos posicionales para el constructor.
            **kwargs: Argumentos de palabra clave para el constructor.
            
        Returns:
            object: La única instancia de la clase.
        """
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__new__(cls)
        return cls._instances[cls]

class DatabaseConnector(abc.ABC):
    """
    Interfaz abstracta para conectores de base de datos.
    
    Esta clase define la interfaz que deben implementar todos los conectores
    de base de datos utilizados en el proyecto.
    
    Mejores prácticas POO:
    - Principio de sustitución de Liskov: Cualquier subclase puede sustituir a esta clase
    - Principio de inversión de dependencia: Depende de abstracciones, no de implementaciones
    - Interfaz clara: Define métodos que deben implementar las subclases
    """
    
    @abc.abstractmethod
    def connect(self) -> bool:
        """
        Establece una conexión con la base de datos.
        
        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario.
        """
        pass
    
    @abc.abstractmethod
    def disconnect(self) -> bool:
        """
        Cierra la conexión con la base de datos.
        
        Returns:
            bool: True si la desconexión fue exitosa, False en caso contrario.
        """
        pass
    
    @abc.abstractmethod
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Any:
        """
        Ejecuta una consulta SQL.
        
        Args:
            query (str): Consulta SQL a ejecutar.
            params (tuple, optional): Parámetros para la consulta.
            
        Returns:
            Any: Resultado de la consulta.
        """
        pass
    
    @abc.abstractmethod
    def execute_many(self, query: str, params_list: List[Tuple]) -> int:
        """
        Ejecuta una consulta SQL múltiples veces con diferentes parámetros.
        
        Args:
            query (str): Consulta SQL a ejecutar.
            params_list (list): Lista de tuplas con parámetros para cada ejecución.
            
        Returns:
            int: Número de filas afectadas.
        """
        pass
    
    @abc.abstractmethod
    def commit(self) -> bool:
        """
        Confirma los cambios en la base de datos.
        
        Returns:
            bool: True si la confirmación fue exitosa, False en caso contrario.
        """
        pass
    
    @abc.abstractmethod
    def rollback(self) -> bool:
        """
        Revierte los cambios en la base de datos.
        
        Returns:
            bool: True si la reversión fue exitosa, False en caso contrario.
        """
        pass

class ApiClient(abc.ABC):
    """
    Interfaz abstracta para clientes de API.
    
    Esta clase define la interfaz que deben implementar todos los clientes
    de API utilizados en el proyecto.
    
    Mejores prácticas POO:
    - Principio de responsabilidad única: Solo se encarga de la comunicación con APIs
    - Principio abierto/cerrado: Abierto para extensión, cerrado para modificación
    - Interfaz clara: Define métodos que deben implementar las subclases
    """
    
    @abc.abstractmethod
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Realiza una solicitud GET a la API.
        
        Args:
            endpoint (str): Endpoint de la API.
            params (dict, optional): Parámetros para la solicitud.
            
        Returns:
            dict: Respuesta de la API.
        """
        pass
    
    @abc.abstractmethod
    def post(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Realiza una solicitud POST a la API.
        
        Args:
            endpoint (str): Endpoint de la API.
            data (dict, optional): Datos para la solicitud.
            
        Returns:
            dict: Respuesta de la API.
        """
        pass
    
    @abc.abstractmethod
    def handle_rate_limit(self) -> None:
        """
        Maneja los límites de tasa de la API.
        """
        pass

class DataProcessor(abc.ABC):
    """
    Interfaz abstracta para procesadores de datos.
    
    Esta clase define la interfaz que deben implementar todos los procesadores
    de datos utilizados en el proyecto.
    
    Mejores prácticas POO:
    - Principio de responsabilidad única: Solo se encarga del procesamiento de datos
    - Principio de segregación de interfaces: Define solo los métodos necesarios
    - Interfaz clara: Define métodos que deben implementar las subclases
    """
    
    @abc.abstractmethod
    def process(self, data: Any) -> Any:
        """
        Procesa los datos.
        
        Args:
            data (Any): Datos a procesar.
            
        Returns:
            Any: Datos procesados.
        """
        pass
    
    @abc.abstractmethod
    def validate(self, data: Any) -> bool:
        """
        Valida los datos.
        
        Args:
            data (Any): Datos a validar.
            
        Returns:
            bool: True si los datos son válidos, False en caso contrario.
        """
        pass

class ConfigManager(Singleton):
    """
    Gestor de configuración del sistema.
    
    Esta clase implementa el patrón Singleton para gestionar la configuración
    del sistema, proporcionando acceso centralizado a los parámetros de configuración.
    
    Atributos:
        config (dict): Diccionario con la configuración del sistema.
    
    Mejores prácticas POO:
    - Patrón Singleton: Asegura que solo exista una instancia
    - Encapsulamiento: Oculta los detalles de implementación
    - Centralización: Proporciona un punto único de acceso a la configuración
    """
    
    def __init__(self):
        """
        Inicializa el gestor de configuración.
        
        Carga la configuración desde el archivo .env y la almacena en memoria.
        """
        # Evitar reinicialización si ya existe una instancia
        if hasattr(self, 'config'):
            return
            
        # Inicializar diccionario de configuración
        self.config = {}
        
        # Cargar configuración desde variables de entorno
        self._load_from_env()
    
    def _load_from_env(self):
        """
        Carga la configuración desde variables de entorno.
        
        Mejores prácticas POO:
        - Método privado: Implementación interna no expuesta
        - Separación de responsabilidades: Método dedicado a una tarea específica
        """
        # Cargar variables de entorno desde el archivo .env
            # Ruta al archivo .env
        env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', '.env')

        # Verificar si el archivo .env existe
        if os.path.exists(env_file):
            load_dotenv(env_file)  # Carga las variables de entorno
            logger.info(f"Archivo .env cargado desde: {env_file}")
        else:
            logger.warning(f"No se encontró el archivo .env en la ruta: {env_file}")
        
        # Cargar configuración de base de datos
        self.config['SQL_SERVER'] = os.getenv('SQL_SERVER', 'localhost')
        self.config['SQL_PORT'] = os.getenv('SQL_PORT', '1433')
        self.config['SQL_USER'] = os.getenv('SQL_USER', 'sa')
        self.config['SQL_PASSWORD'] = os.getenv('SQL_PASSWORD', '')
        self.config['SQL_DATABASE'] = os.getenv('SQL_DATABASE', 'AlphaVantageDB')
        
        # Cargar configuración de Alpha Vantage
        self.config['ALPHA_VANTAGE_API_KEY'] = os.getenv('ALPHA_VANTAGE_API_KEY', '')
        self.config['ALPHA_VANTAGE_BASE_URL'] = os.getenv('ALPHA_VANTAGE_BASE_URL', 'https://www.alphavantage.co/query')
        
        # Cargar configuración general
        self.config['LOG_LEVEL'] = os.getenv('LOG_LEVEL', 'INFO')
        
        # Registrar la carga de configuración
        logger.info("Configuración cargada desde variables de entorno")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Obtiene un valor de configuración.
        
        Args:
            key (str): Clave de configuración.
            default (Any, optional): Valor por defecto si la clave no existe.
            
        Returns:
            Any: Valor de configuración o valor por defecto.
        """
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Establece un valor de configuración.
        
        Args:
            key (str): Clave de configuración.
            value (Any): Valor de configuración.
        """
        self.config[key] = value
        logger.debug(f"Configuración actualizada: {key}={value}")
    
    def get_all(self) -> Dict[str, Any]:
        """
        Obtiene toda la configuración.
        
        Returns:
            dict: Diccionario con toda la configuración.
        """
        return self.config.copy()

class Observer(abc.ABC):
    """
    Interfaz para el patrón Observer.
    
    Esta clase define la interfaz que deben implementar todos los observadores
    en el patrón Observer.
    
    Mejores prácticas POO:
    - Patrón Observer: Implementa el patrón de diseño Observer
    - Interfaz clara: Define métodos que deben implementar las subclases
    """
    
    @abc.abstractmethod
    def update(self, subject: 'Subject', *args, **kwargs) -> None:
        """
        Actualiza el observador cuando el sujeto cambia.
        
        Args:
            subject (Subject): Sujeto que ha cambiado.
            *args: Argumentos adicionales.
            **kwargs: Argumentos de palabra clave adicionales.
        """
        pass

class Subject:
    """
    Clase base para el patrón Observer.
    
    Esta clase implementa la funcionalidad básica del sujeto en el patrón Observer,
    permitiendo registrar, eliminar y notificar a observadores.
    
    Atributos:
        _observers (list): Lista de observadores registrados.
    
    Mejores prácticas POO:
    - Patrón Observer: Implementa el patrón de diseño Observer
    - Encapsulamiento: Oculta la lista de observadores
    - Métodos claros: Define métodos para gestionar observadores
    """
    
    def __init__(self):
        """
        Inicializa el sujeto.
        
        Crea una lista vacía de observadores.
        """
        self._observers = []
    
    def attach(self, observer: Observer) -> None:
        """
        Registra un observador.
        
        Args:
            observer (Observer): Observador a registrar.
        """
        if observer not in self._observers:
            self._observers.append(observer)
    
    def detach(self, observer: Observer) -> None:
        """
        Elimina un observador.
        
        Args:
            observer (Observer): Observador a eliminar.
        """
        if observer in self._observers:
            self._observers.remove(observer)
    
    def notify(self, *args, **kwargs) -> None:
        """
        Notifica a todos los observadores.
        
        Args:
            *args: Argumentos adicionales.
            **kwargs: Argumentos de palabra clave adicionales.
        """
        for observer in self._observers:
            observer.update(self, *args, **kwargs)

# Ejemplo de uso de las clases definidas
def main():
    """
    Función principal para demostrar el uso de las clases definidas.
    
    Esta función muestra ejemplos de cómo utilizar las clases definidas en este módulo.
    """
    # Ejemplo de uso del gestor de configuración
    config = ConfigManager()
    print("Configuración cargada:")
    print(f"Servidor SQL: {config.get('SQL_SERVER')}")
    print(f"API Key de Alpha Vantage: {config.get('ALPHA_VANTAGE_API_KEY')}")
    
    # Ejemplo de uso del patrón Observer
    class ConcreteObserver(Observer):
        def update(self, subject, *args, **kwargs):
            print(f"Observador notificado: {args}, {kwargs}")
    
    subject = Subject()
    observer = ConcreteObserver()
    subject.attach(observer)
    subject.notify("Cambio de estado", valor=42)

# Punto de entrada para ejecución directa del script
if __name__ == "__main__":
    main()
