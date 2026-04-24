import psycopg2
import csv
import os
from dotenv import load_dotenv

load_dotenv()
DB_pass= os.getenv("DB_PASS")

print("Conectando con la bóveda Silver para exportar datos...")
conexion=None

try: 
    conexion=psycopg2.connect(
        host="localhost",
        database="tmdb_pipeline",
        user="postgres",
        password=DB_pass,
        port="5432" ,
        options="-c lc_messages=C"
    )
    cursor=conexion.cursor()
    sql_export= "SELECT * FROM silver_tmdb_movies ORDER BY popularity DESC;"
    cursor.execute(sql_export)

    peliculas = cursor.fetchall()

    nombres_columnas = [desc[0] for desc in cursor.description]

    nombre_archivo = "mis_500_peliculas_tmdb.csv"

    with open(nombre_archivo, mode='w', newline='', encoding='UTF-8-sig') as archivo_csv:
        escritor = csv.writer(archivo_csv, delimiter=';')
        escritor.writerow(nombres_columnas)
        escritor.writerows(peliculas)

    print(f"Éxito al crear el archivo '{nombre_archivo}' con {len(peliculas)}películas.")
except Exception as e:
    print(f"Ocurrió un error: {e}")
finally:
    if 'conexion' in locals() and conexion is not None:
        conexion.close()
        print("Conexión cerrada.")