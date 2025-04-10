import os
import sys
from flask import Flask, render_template, request, redirect, url_for, flash
from sqlalchemy import text
from dotenv import load_dotenv
from datetime import datetime

# Añadir el directorio padre (raíz del proyecto) al sys.path para encontrar la carpeta 'scripts'
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    from scripts.sql_connection import conectar_sql_server, ejecutar_consulta
    from scripts.yahoo_finance import descargar_datos_yahoo
except ImportError as e:
    print(f"Error importando módulos de 'scripts': {e}")
    print("Asegúrate de que la estructura de carpetas es correcta y que los archivos .py existen.")
    # Podrías querer manejar esto de forma más robusta o salir
    sys.exit(1)

# Cargar variables de entorno desde config/.env
env_path = os.path.join(project_root, 'config', '.env')
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path)
    print(f"Archivo .env cargado desde: {env_path}")
else:
    print(f"Advertencia: No se encontró el archivo .env en {env_path}. La aplicación podría no funcionar.")
    # Considera añadir valores por defecto o lanzar un error si son cruciales

app = Flask(__name__)
# Es crucial configurar una SECRET_KEY para usar flash messages
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'una-clave-secreta-por-defecto-muy-segura') # Usa una variable de entorno o genera una aleatoria

@app.route('/')
def index():
    """Muestra el formulario principal con la lista de símbolos."""
    symbols = []
    engine = None
    try:
        engine = conectar_sql_server()
        if engine:
            consulta_symbols = text("SELECT SymbolID, Symbol FROM Metadata.Symbols WHERE IsActive = 1 ORDER BY Symbol")
            # Usamos read_sql directamente ya que ejecutar_consulta devuelve un DataFrame
            # y aquí necesitamos una lista de diccionarios o similar para la plantilla.
            with engine.connect() as connection:
                result = connection.execute(consulta_symbols)
                symbols = result.fetchall() # Obtiene [(SymbolID1, Symbol1), (SymbolID2, Symbol2), ...]
                # Convertimos a lista de diccionarios si es necesario para la plantilla,
                # aunque fetchall() devuelve una lista de RowProxy que se pueden acceder por índice o nombre.
                # symbols = [{'SymbolID': row.SymbolID, 'Symbol': row.Symbol} for row in result] # Alternativa
        else:
            flash("Error al conectar con la base de datos.", "error")

    except Exception as e:
        flash(f"Error al obtener símbolos de la base de datos: {str(e)}", "error")
        print(f"Error en la ruta '/': {str(e)}") # Log para depuración
    finally:
        if engine:
            engine.dispose()

    return render_template('index.html', symbols=symbols)

@app.route('/descargar', methods=['POST'])
def descargar():
    """Maneja la solicitud de descarga y guardado de datos."""
    symbol_id = request.form.get('symbol_id')
    fecha_inicio_str = request.form.get('fecha_inicio')
    fecha_fin_str = request.form.get('fecha_fin')

    # --- Validación básica ---
    if not all([symbol_id, fecha_inicio_str, fecha_fin_str]):
        flash("Por favor, selecciona un símbolo y ambas fechas.", "warning")
        return redirect(url_for('index'))

    try:
        # Validar formato de fechas (opcional pero recomendado)
        fecha_inicio = datetime.strptime(fecha_inicio_str, '%Y-%m-%d').strftime('%Y-%m-%d')
        fecha_fin = datetime.strptime(fecha_fin_str, '%Y-%m-%d').strftime('%Y-%m-%d')
        if fecha_inicio > fecha_fin:
             flash("La fecha de inicio no puede ser posterior a la fecha de fin.", "warning")
             return redirect(url_for('index'))
    except ValueError:
        flash("Formato de fecha inválido. Usa YYYY-MM-DD.", "warning")
        return redirect(url_for('index'))

    engine = None
    symbol = None
    try:
        engine = conectar_sql_server()
        if not engine:
            flash("Error de conexión a la base de datos.", "error")
            return redirect(url_for('index'))

        # --- Obtener el Symbol ---
        with engine.connect() as connection:
            consulta_symbol = text("SELECT Symbol FROM Metadata.Symbols WHERE SymbolID = :symbol_id")
            result = connection.execute(consulta_symbol, {"symbol_id": symbol_id})
            symbol_row = result.fetchone()
            if not symbol_row:
                flash(f"No se encontró el símbolo con ID {symbol_id}.", "error")
                return redirect(url_for('index'))
            symbol = symbol_row[0] # o symbol_row.Symbol

        # --- Descargar datos ---
        print(f"Intentando descargar datos para {symbol} ({symbol_id}) desde {fecha_inicio} hasta {fecha_fin}")
        df_historico = descargar_datos_yahoo(symbol, fecha_inicio, fecha_fin)

        if df_historico is None or df_historico.empty:
            flash(f"No se encontraron datos en Yahoo Finance para '{symbol}' en el período especificado o hubo un error en la descarga.", "warning")
            return redirect(url_for('index'))

        # --- Borrar datos existentes e Insertar nuevos ---
        with engine.connect() as connection:
            with connection.begin(): # Iniciar transacción
                try:
                    # Borrar datos existentes para el SymbolID
                    delete_sql = text("DELETE FROM AVData.StockPrices WHERE SymbolID = :symbol_id")
                    connection.execute(delete_sql, {"symbol_id": symbol_id})
                    print(f"Datos existentes borrados para SymbolID: {symbol_id}")

                    # Insertar nuevos datos
                    insert_sql = text("""
                        INSERT INTO AVData.StockPrices
                        (SymbolID, Date, [Open], High, Low, [Close], Volume, CreatedDate)
                        VALUES (:symbol_id, :date, :open, :high, :low, :close, :volume, GETDATE())
                    """)
                    # Convertir DataFrame a lista de diccionarios para la inserción
                    datos_para_insertar = df_historico.to_dict(orient='records')
                    for row in datos_para_insertar:
                        params = {
                            "symbol_id": int(symbol_id), # Asegurar que es int
                            "date": row['Date'].strftime('%Y-%m-%d') if isinstance(row.get('Date'), datetime) else row.get('Date'), # Formatear fecha si es datetime
                            "open": row.get('Open'),
                            "high": row.get('High'),
                            "low": row.get('Low'),
                            "close": row.get('Close'),
                            "volume": row.get('Volume')
                        }
                        # Asegurarse de que las claves coinciden con las columnas de la tabla y los parámetros SQL
                        # Corregir nombres si es necesario (ej. 'Adj Close' no está en la tabla destino)
                        connection.execute(insert_sql, params)

                    # connection.begin() maneja el commit automáticamente si no hay errores
                    flash(f"Datos para '{symbol}' descargados e insertados correctamente ({len(df_historico)} filas).", "success")
                    print(f"Inserción completada para SymbolID: {symbol_id}")

                except Exception as insert_error:
                    # connection.begin() maneja el rollback automáticamente en caso de error
                    flash(f"Error al guardar datos en la base de datos para '{symbol}': {str(insert_error)}", "error")
                    print(f"Error durante la transacción (borrado/inserción): {str(insert_error)}")


    except Exception as e:
        flash(f"Error inesperado durante el proceso: {str(e)}", "error")
        print(f"Error en la ruta '/descargar': {str(e)}") # Log para depuración
    finally:
        if engine:
            engine.dispose()

    return redirect(url_for('index'))

if __name__ == '__main__':
    # Obtener puerto de variable de entorno o usar 5000 por defecto
    port = int(os.environ.get('PORT', 5005))
    # Ejecutar en 0.0.0.0 para ser accesible en la red local si es necesario
    # debug=True es útil para desarrollo, pero debe desactivarse en producción
    app.run(host='0.0.0.0', port=port, debug=True)