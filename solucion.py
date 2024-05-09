import os
import sqlite3
import pandas as pd
import streamlit as st
import schedule
import time
from datetime import datetime, timedelta

# Configuración de directorios
directorio_origen = "Origen"
directorio_respaldo = "Respaldo"

# Configuración de la base de datos
db_file = "ventas.db"

# Función para crear la tabla en la base de datos
def crear_tabla():
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS Ventas_Consolidadas (
                    IdTransaccion INTEGER,
                    IdLocal INTEGER,
                    Sucursal TEXT,
                    Fecha TEXT,
                    IdCategoria INTEGER,
                    IdProducto INTEGER,
                    Producto TEXT,
                    Cantidad INTEGER,
                    PrecioUnitario REAL,
                    TotalVenta REAL
                )""")
    conn.commit()
    conn.close()

def consolidar_ventas():
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    # Obtener la lista de archivos en el directorio de origen
    archivos = os.listdir(directorio_origen)
    
    # Procesar cada archivo
    for archivo in archivos:
        if archivo.endswith(".csv"):
            ruta_archivo = os.path.join(directorio_origen, archivo)
            
            # Leer el archivo CSV
            df = pd.read_csv(ruta_archivo)
            
            # Asignar el IdLocal y el nombre de la sucursal correspondiente al archivo
            id_local, sucursal = obtener_id_local_y_sucursal(archivo)
            df["IdLocal"] = id_local
            df["Sucursal"] = sucursal
            
            # Insertar los datos en la base de datos
            df.to_sql("Ventas_Consolidadas", conn, if_exists="append", index=False)
            
            st.success(f"Archivo {archivo} procesado correctamente.")
    
    conn.close()
    st.success("Consolidación de ventas completada.")
    st.experimental_rerun()
    
# Función para obtener el IdLocal y el nombre de la sucursal a partir del nombre del archivo
def obtener_id_local_y_sucursal(nombre_archivo):
    if "Quito" in nombre_archivo:
        return 1, "Quito"
    elif "Ambato" in nombre_archivo:
        return 2, "Ambato"
    elif "Latacunga" in nombre_archivo:
        return 3, "Latacunga"
    elif "Cuenca" in nombre_archivo:
        return 4, "Cuenca"
    else:
        return 0, "Desconocido"

# Función para mover archivos manualmente
def mover_archivos_manualmente():
    archivos_origen = os.listdir(directorio_origen)
    
    for archivo in archivos_origen:
        if archivo.endswith(".csv"):
            ruta_origen = os.path.join(directorio_origen, archivo)
            ruta_respaldo = os.path.join(directorio_respaldo, archivo)
            os.rename(ruta_origen, ruta_respaldo)
    
    st.success("Archivos movidos manualmente.")
    st.experimental_rerun()

# Función para mover archivos automáticamente a las 12 de la noche
def mover_archivos_automaticamente():
    mover_archivos_manualmente()

# Configurar el proceso de mover archivos automáticamente a las 12 de la noche
schedule.every().day.at("00:00").do(mover_archivos_automaticamente)

# Función para obtener el tiempo restante para el próximo respaldo automático
def obtener_tiempo_restante():
    proximo_respaldo = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    tiempo_restante = proximo_respaldo - datetime.now()
    return tiempo_restante

# Interfaz de usuario con Streamlit
st.title("Consolidación de Ventas")

# Mostrar información de la base de datos
if os.path.exists(db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM Ventas_Consolidadas")
    total_registros = c.fetchone()[0]
    conn.close()
    st.info(f"Base de datos: {db_file}")
    st.info(f"Total de registros en la tabla Ventas_Consolidadas: {total_registros}")
else:
    st.warning("La base de datos no existe. Se creará al consolidar las ventas.")

# Mostrar tabla Ventas_Consolidadas
if os.path.exists(db_file):
    conn = sqlite3.connect(db_file)
    df = pd.read_sql_query("SELECT * FROM Ventas_Consolidadas", conn)
    conn.close()
    st.subheader("Tabla Ventas_Consolidadas")
    st.dataframe(df)
else:
    st.warning("La tabla Ventas_Consolidadas no existe. Se creará al consolidar las ventas.")

# Botón para consolidar las ventas manualmente
consolidar_button = st.button("Consolidar Ventas")
if consolidar_button:
    consolidar_ventas()

# Botón para mover archivos manualmente
mover_button = st.button("Mover Archivos Manualmente")
if mover_button:
    mover_archivos_manualmente()

# Mostrar archivos en los directorios de origen y respaldo
st.subheader("Archivos en el directorio de origen")
archivos_origen = os.listdir(directorio_origen)
st.write(archivos_origen)

st.subheader("Archivos en el directorio de respaldo")
archivos_respaldo = os.listdir(directorio_respaldo)
st.write(archivos_respaldo)

# Mostrar tiempo restante para el próximo respaldo automático
tiempo_restante = obtener_tiempo_restante()
st.info(f"Tiempo restante para el próximo respaldo automático: {tiempo_restante}")

# Crear la tabla en la base de datos si no existe
crear_tabla()

# Ejecutar el proceso de mover archivos automáticamente a las 12 de la noche en segundo plano
while True:
    schedule.run_pending()
    time.sleep(1)