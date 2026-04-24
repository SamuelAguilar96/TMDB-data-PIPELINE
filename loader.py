import psycopg2
import json
import os
from dotenv import load_dotenv

load_dotenv()
DB_pass= os.getenv("DB_PASS")

print("Abriendo la bóveda Silver para análisis")

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

    sql_insert = """
        SELECT title, vote_average, popularity, release_date, original_language
        FROM silver_tmdb_movies
        WHERE vote_average > 0 AND vote_count > 1000
        ORDER BY vote_average DESC
        LIMIT 10;
    """
    cursor.execute(sql_insert)

    top_peliculas = cursor.fetchall()
    print("\n"+ "="*50)
    print("EL TOP 10: Las mejores valoradas del momento")
    print("="*50)

    posicion = 1
    for peli in top_peliculas:
        titulo = peli[0]
        nota=peli[1]
        popularidad=peli[2]
        fecha=peli[3]
        idioma=peli[4]
    

        print(f"#{posicion} | {nota}/10 | {titulo} (Estreno:{fecha})")
        print(f"Nivel de popularidad: {popularidad}")
        print(f"Idioma original:{idioma}")
        print("="*50)

        posicion += 1

except Exception as e:
    print(f"Alarma analítica, ocurrió un error: {e}")

finally:
    if 'conexion' in locals() and conexion is not None:
        conexion.close()
print("Análisis finalizado. Conexión cerrada de forma segura.")