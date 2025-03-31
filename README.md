# Proyecto Alpha Vantage - Versión 2.0

Este proyecto proporciona un sistema completo para descargar y almacenar datos históricos bursátiles de Alpha Vantage en una base de datos SQL Server, siguiendo las mejores prácticas de Programación Orientada a Objetos (POO) y con código exhaustivamente comentado para fines didácticos.

## Características principales

- **Compatibilidad con macOS Apple Silicon (M4)**: Todos los scripts están optimizados para funcionar en macOS con chips Apple Silicon.
- **Scripts DDL para SQL Server**: Archivos SQL para crear la estructura de la base de datos directamente en Visual Studio Code.
- **Código Python exhaustivamente comentado**: Cada línea de código está documentada para facilitar el aprendizaje.
- **Implementación de mejores prácticas de POO**: Uso de patrones de diseño, principios SOLID y otras prácticas avanzadas.
- **Descarga automática de datos históricos**: Para los símbolos NVIDIA (NVDA) y Apple (AAPL).
- **Almacenamiento optimizado en SQL Server**: Con esquemas separados para metadatos y datos históricos.

## Estructura del proyecto

```
alpha_vantage_project/
├── config/                # Configuración del sistema
│   └── .env.example       # Plantilla para archivo de configuración
├── docs/                  # Documentación adicional
│   └── guia_usuario.md    # Guía de usuario detallada
├── scripts/               # Scripts Python
│   ├── base.py            # Clases base e interfaces (POO)
│   ├── db_connector.py    # Conector de base de datos (POO)
│   ├── api_client.py      # Cliente de API Alpha Vantage (POO)
│   ├── metadata_manager_poo.py  # Gestor de metadatos (POO)
│   ├── stock_downloader_poo.py  # Descargador de datos (POO)
│   ├── stock_storage_poo.py     # Almacenamiento de datos (POO)
│   ├── main_poo.py              # Script principal (POO)
│   └── *_commented.py     # Versiones con comentarios exhaustivos
└── sql/                   # Scripts SQL para Visual Studio Code
    ├── 01_create_database.sql  # Creación de la base de datos
    ├── 02_create_tables.sql    # Creación de tablas y esquemas
    └── 03_insert_initial_data.sql  # Datos iniciales
```

## Requisitos previos

- macOS con chip Apple Silicon (M1, M2, M3, M4)
- Python 3.8 o superior
- SQL Server (en contenedor Docker o remoto)
- Clave API de Alpha Vantage

## Configuración para macOS Apple Silicon

1. Instalar las dependencias necesarias:

```bash
# Instalar Homebrew si no está instalado
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Instalar dependencias para pyodbc
brew install unixodbc freetds

# Instalar paquetes Python
pip install pyodbc pandas requests python-dotenv
```

2. Configurar el archivo `.env`:

```bash
# Copiar el archivo de ejemplo
cp config/.env.example config/.env

# Editar el archivo con tus credenciales
nano config/.env
```

## Uso de los scripts SQL en Visual Studio Code

1. Abrir Visual Studio Code y instalar la extensión "SQL Server (mssql)".
2. Conectar a tu instancia de SQL Server.
3. Abrir y ejecutar los scripts SQL en el siguiente orden:
   - `01_create_database.sql`
   - `02_create_tables.sql`
   - `03_insert_initial_data.sql`

## Uso de los scripts Python

### Versión con POO (recomendada)

```bash
# Ejecutar prueba completa del sistema
python scripts/main_poo.py --test

# O ejecutar componentes individuales
python scripts/main_poo.py --setup      # Configurar entorno
python scripts/main_poo.py --download   # Descargar datos
python scripts/main_poo.py --store      # Almacenar datos
python scripts/main_poo.py --verify     # Verificar datos almacenados
```

### Versión con comentarios exhaustivos (para aprendizaje)

```bash
# Explorar los scripts con comentarios exhaustivos
python scripts/main_commented.py --test
```

## Mejores prácticas de POO implementadas

- **Patrón Singleton**: Implementado en `ConfigManager` para gestión centralizada de configuración.
- **Patrón Observer**: Implementado en `StockDataDownloader` y `StockDataStorage` para notificaciones de progreso.
- **Interfaces abstractas**: Definidas en `base.py` para asegurar contratos claros entre componentes.
- **Principio de responsabilidad única**: Cada clase tiene una única responsabilidad bien definida.
- **Principio abierto/cerrado**: Las clases están abiertas para extensión pero cerradas para modificación.
- **Principio de sustitución de Liskov**: Las subclases pueden sustituir a sus clases base sin alterar el comportamiento.
- **Principio de segregación de interfaces**: Interfaces específicas para cada cliente.
- **Principio de inversión de dependencia**: Dependencia de abstracciones, no de implementaciones concretas.
- **Composición sobre herencia**: Uso de composición para reutilizar funcionalidad.
- **Tipado estático**: Uso de anotaciones de tipo para mejorar la legibilidad y detectar errores.

## Notas adicionales

- Los scripts están diseñados para ejecutarse 1-2 veces por semana.
- La clave API de Alpha Vantage debe almacenarse de forma segura en el archivo `.env`.
- Para añadir más símbolos, modifica la tabla `Metadata.Symbols` en la base de datos.

## Solución de problemas

Si encuentras problemas con pyodbc en macOS Apple Silicon:

1. Verifica que FreeTDS esté correctamente instalado:
```bash
brew info freetds
```

2. Asegúrate de que la variable de entorno ODBCINI esté configurada:
```bash
export ODBCINI=/usr/local/etc/odbc.ini
```

3. Prueba la conexión ODBC:
```bash
isql -v -k "DRIVER=FreeTDS;SERVER=tu_servidor;PORT=1433;DATABASE=tu_base_de_datos;UID=tu_usuario;PWD=tu_contraseña;TDS_VERSION=8.0"
```

Para más información, consulta el archivo `docs/guia_usuario.md`.
