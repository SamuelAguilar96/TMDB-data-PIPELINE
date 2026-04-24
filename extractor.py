import requests
import psycopg2
import json
import os
from dotenv import load_dotenv

load_dotenv()
DB_pass= os.getenv("DB_PASS")
TOKEN_API = os.getenv("TOKEN_API")

url_pop = 'https://api.themoviedb.org/3/movie/popular?language=es-ES&page=1'

headers = {
    "accept":"application/json",
    "Authorization": f"Bearer {TOKEN_API}"
}
todas_las_peliculas=[]
for pagina in range(1, 26):
    url = f"https://api.themoviedb.org/3/movie/popular?language=en-US&page={pagina}"

    print("Conectando con TMDb...")
    respuesta = requests.get(url, headers=headers)

    if respuesta.status_code == 200:
        print("¡Conexión exitosa! Código 200.")
        datos_pagina = respuesta.json()

        todas_las_peliculas.extend(datos_pagina['results'])
        print(f"Página {pagina} descargada. Total acumulado: {len(todas_las_peliculas)} películas")
    else:
        print(f"Error en la pagina{pagina}. Código: {respuesta.status_code}")

json_gigante= {
    "results":todas_las_peliculas
}
texto_json_final = json.dumps(json_gigante)

conexion= None
try:
        print("Intentando conectar con PostgreSQL")
        conexion= psycopg2.connect(
            host="localhost",
            database="tmdb_pipeline",
            user="postgres",
            password=DB_pass,
            port="5432",
            options="-c lc_messages=C"
        )
        cursor = conexion.cursor()
        sql_insert = """
            INSERT INTO bronze_tmdb_staging (endpoint_source, raw_data)
        
        
            VALUES (%s,%s);
            """
        fuente_api = "https://api.themoviedb.org/3/movie/popular (Pages 1-25)"""
        cursor.execute(sql_insert, (fuente_api, texto_json_final))
        conexion.commit()
        print("500 películas guardadas en la Capa Bronze correctamente.")

except Exception as e:
        print(f"Alarma, ocurrió un error: {e}")

finally:
        print("Este bloque se ejecuta SIEMPRE, falle o no.")
        if 'conexion' in locals() and conexion is not None:
            conexion.close()
            print("Conexión a la base de datos cerrada.")

        primera_pelicula = json_gigante['results'][0]['title']
        print(f"La primera pelicula más popular ahora mismo es: {primera_pelicula}")