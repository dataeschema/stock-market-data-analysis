# Guía de Usuario - Proyecto Alpha Vantage

Esta guía detallada explica cómo utilizar el sistema de descarga y almacenamiento de datos bursátiles de Alpha Vantage, diseñado específicamente para macOS con chip Apple Silicon.

## Índice

1. [Instalación y Configuración](#1-instalación-y-configuración)
2. [Configuración de la Base de Datos](#2-configuración-de-la-base-de-datos)
3. [Uso de los Scripts Python](#3-uso-de-los-scripts-python)
4. [Estructura de la Base de Datos](#4-estructura-de-la-base-de-datos)
5. [Añadir Nuevos Símbolos](#5-añadir-nuevos-símbolos)
6. [Programación de Ejecuciones](#6-programación-de-ejecuciones)
7. [Solución de Problemas](#7-solución-de-problemas)
8. [Explicación de las Mejores Prácticas POO](#8-explicación-de-las-mejores-prácticas-poo)

## 1. Instalación y Configuración

### 1.1 Requisitos previos

- macOS con chip Apple Silicon (M1, M2, M3, M4)
- Python 3.8 o superior
- SQL Server (en contenedor Docker o remoto)
- Clave API de Alpha Vantage (obtener en [alphavantage.co](https://www.alphavantage.co/support/#api-key))

### 1.2 Instalación de dependencias

```bash
# Instalar Homebrew si no está instalado
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Instalar dependencias para pyodbc
brew install unixodbc freetds

# Instalar paquetes Python
pip install pyodbc pandas requests python-dotenv
```

### 1.3 Configuración del archivo .env

1. Copia el archivo de ejemplo:
```bash
cp config/.env.example config/.env
```

2. Edita el archivo con tus credenciales:
```bash
nano config/.env
```

3. Completa los siguientes campos:
```
# Configuración de SQL Server
SQL_SERVER=localhost
SQL_PORT=1433
SQL_USER=sa
SQL_PASSWORD=TuContraseñaSegura
SQL_DATABASE=AlphaVantageDB

# Configuración de Alpha Vantage
ALPHA_VANTAGE_API_KEY=TuClaveAPIAquí
ALPHA_VANTAGE_BASE_URL=https://www.alphavantage.co/query

# Configuración general
LOG_LEVEL=INFO
```

## 2. Configuración de la Base de Datos

### 2.1 Usando Visual Studio Code

1. Instala la extensión "SQL Server (mssql)" en Visual Studio Code.
2. Conéctate a tu instancia de SQL Server:
   - Presiona F1 y escribe "SQL Server: Connect"
   - Introduce los detalles de conexión (servidor, usuario, contraseña)

3. Ejecuta los scripts SQL en el siguiente orden:
   - `sql/01_create_database.sql` - Crea la base de datos
   - `sql/02_create_tables.sql` - Crea los esquemas y tablas
   - `sql/03_insert_initial_data.sql` - Inserta datos iniciales

### 2.2 Verificación de la estructura

Después de ejecutar los scripts, deberías tener:
- Base de datos: `AlphaVantageDB`
- Esquemas: `Metadata` y `AVdata`
- Tablas en `Metadata`: `Symbols`, `Downloads`, `Configuration`
- Tablas en `AVdata`: `StockPrices`

## 3. Uso de los Scripts Python

### 3.1 Versión con POO (recomendada para uso regular)

```bash
# Configurar el entorno
python scripts/main_poo.py --setup

# Probar conexión con la base de datos
python scripts/main_poo.py --test-db

# Probar conexión con Alpha Vantage
python scripts/main_poo.py --test-api

# Descargar datos históricos
python scripts/main_poo.py --download

# Almacenar datos en la base de datos
python scripts/main_poo.py --store

# Verificar datos almacenados
python scripts/main_poo.py --verify

# Ejecutar prueba completa del sistema
python scripts/main_poo.py --test
```

### 3.2 Versión con comentarios exhaustivos (para aprendizaje)

```bash
# Explorar los scripts con comentarios exhaustivos
python scripts/main_commented.py --test
```

### 3.3 Ejecución individual de componentes

Si deseas entender mejor cada componente, puedes ejecutarlos individualmente:

```bash
# Probar el cliente de API
python -c "from scripts.api_client import main; main()"

# Probar el gestor de metadatos
python -c "from scripts.metadata_manager_poo import main; main()"

# Probar el descargador de datos
python -c "from scripts.stock_downloader_poo import main; main()"

# Probar el almacenamiento de datos
python -c "from scripts.stock_storage_poo import main; main()"
```

## 4. Estructura de la Base de Datos

### 4.1 Esquema Metadata

- **Symbols**: Almacena información sobre los símbolos bursátiles
  - `SymbolID`: ID único del símbolo
  - `Symbol`: Ticker del símbolo (ej. 'AAPL')
  - `CompanyName`: Nombre de la empresa
  - `IsActive`: Indica si el símbolo está activo

- **Downloads**: Registra las descargas realizadas
  - `DownloadID`: ID único de la descarga
  - `SymbolID`: ID del símbolo descargado
  - `LastDownloadDate`: Fecha de la última descarga
  - `StartDate`: Fecha de inicio de los datos
  - `EndDate`: Fecha de fin de los datos
  - `Status`: Estado de la descarga ('Pending', 'Completed', 'Failed')

- **Configuration**: Almacena configuraciones del sistema
  - `ConfigID`: ID único de la configuración
  - `ConfigKey`: Clave de configuración
  - `ConfigValue`: Valor de configuración
  - `Description`: Descripción de la configuración
  - `CreatedDate`: Fecha de creación
  - `ModifiedDate`: Fecha de modificación

### 4.2 Esquema AVdata

- **StockPrices**: Almacena los datos históricos bursátiles
  - `PriceID`: ID único del precio
  - `SymbolID`: ID del símbolo
  - `Date`: Fecha del registro
  - `Open`: Precio de apertura
  - `High`: Precio más alto
  - `Low`: Precio más bajo
  - `Close`: Precio de cierre
  - `AdjustedClose`: Precio de cierre ajustado
  - `Volume`: Volumen de negociación
  - `DownloadID`: ID de la descarga

## 5. Añadir Nuevos Símbolos

### 5.1 Mediante SQL

```sql
-- Insertar un nuevo símbolo
INSERT INTO Metadata.Symbols (Symbol, CompanyName, IsActive)
VALUES ('MSFT', 'Microsoft Corporation', 1);
```

### 5.2 Mediante Python

```python
from scripts.metadata_manager_poo import MetadataManager

# Crear instancia del gestor de metadatos
metadata = MetadataManager()

# Añadir un nuevo símbolo
symbol_id = metadata.add_symbol('MSFT', 'Microsoft Corporation', True)
print(f"Símbolo añadido con ID: {symbol_id}")
```

## 6. Programación de Ejecuciones

Para ejecutar el sistema automáticamente 1-2 veces por semana, puedes usar cron en macOS:

```bash
# Abrir el editor de cron
crontab -e

# Añadir una línea para ejecutar el script los lunes y jueves a las 9:00 AM
0 9 * * 1,4 cd /ruta/a/tu/proyecto && python scripts/main_poo.py --download && python scripts/main_poo.py --store
```

## 7. Solución de Problemas

### 7.1 Problemas con pyodbc en macOS

Si encuentras errores relacionados con pyodbc:

1. Verifica que FreeTDS esté correctamente instalado:
```bash
brew info freetds
```

2. Configura las variables de entorno:
```bash
export ODBCINI=/usr/local/etc/odbc.ini
export ODBCSYSINI=/usr/local/etc
```

3. Crea o edita el archivo `/usr/local/etc/odbcinst.ini`:
```ini
[FreeTDS]
Description = FreeTDS Driver
Driver = /usr/local/lib/libtdsodbc.so
Setup = /usr/local/lib/libtdsodbc.so
```

4. Crea o edita el archivo `/usr/local/etc/odbc.ini`:
```ini
[MSSQL]
Driver = FreeTDS
Server = tu_servidor
Port = 1433
Database = AlphaVantageDB
TDS_Version = 8.0
```

5. Prueba la conexión:
```bash
isql -v MSSQL tu_usuario tu_contraseña
```

### 7.2 Problemas con Alpha Vantage API

- **Límite de tasa excedido**: La API gratuita tiene un límite de 5 llamadas por minuto y 500 por día. El sistema implementa esperas automáticas entre llamadas.
- **Clave API inválida**: Verifica que tu clave API esté correctamente configurada en el archivo `.env`.

### 7.3 Problemas con SQL Server

- **Error de conexión**: Verifica que SQL Server esté en ejecución y accesible desde tu máquina.
- **Error de autenticación**: Comprueba las credenciales en el archivo `.env`.
- **Error de permisos**: Asegúrate de que el usuario tenga permisos para crear bases de datos y tablas.

## 8. Explicación de las Mejores Prácticas POO

### 8.1 Patrón Singleton

Implementado en `ConfigManager` para asegurar que solo exista una instancia de la configuración en toda la aplicación, evitando duplicación y asegurando consistencia.

```python
# Ejemplo de uso:
config = ConfigManager()  # Primera instancia
config2 = ConfigManager()  # Devuelve la misma instancia que config
assert config is config2  # True
```

### 8.2 Patrón Observer

Implementado en `StockDataDownloader` y `StockDataStorage` para notificar sobre el progreso de las operaciones sin acoplar el código.

```python
# Ejemplo de uso:
downloader = StockDataDownloader()
downloader.attach(MiObservador())  # Añadir un observador personalizado
downloader.download_symbol_data('AAPL')  # El observador recibirá notificaciones
```

### 8.3 Interfaces Abstractas

Definidas en `base.py` para establecer contratos claros entre componentes:

- `DatabaseConnector`: Define la interfaz para conectores de base de datos
- `ApiClient`: Define la interfaz para clientes de API
- `DataProcessor`: Define la interfaz para procesadores de datos
- `Observer`: Define la interfaz para observadores

### 8.4 Principio de Responsabilidad Única

Cada clase tiene una única responsabilidad:
- `AlphaVantageClient`: Solo se encarga de comunicarse con la API
- `SQLServerConnector`: Solo se encarga de la conexión con SQL Server
- `MetadataManager`: Solo se encarga de gestionar metadatos
- `StockDataDownloader`: Solo se encarga de descargar datos
- `StockDataStorage`: Solo se encarga de almacenar datos

### 8.5 Principio Abierto/Cerrado

Las clases están diseñadas para ser extendidas sin modificar su código:
- Puedes crear nuevos observadores sin modificar las clases Subject
- Puedes implementar nuevos conectores de base de datos sin modificar el código existente

### 8.6 Composición sobre Herencia

El sistema utiliza composición para reutilizar funcionalidad:
- `StockDataDownloader` usa `AlphaVantageClient` y `MetadataManager`
- `StockDataStorage` usa `SQLServerConnector` y `MetadataManager`

### 8.7 Tipado Estático

El código utiliza anotaciones de tipo para mejorar la legibilidad y detectar errores:

```python
def get_symbol_by_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
    # Implementación...
```

Estas prácticas hacen que el código sea más mantenible, extensible y fácil de entender, siguiendo los principios de diseño de software modernos.
