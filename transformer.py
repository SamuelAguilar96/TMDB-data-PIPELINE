import psycopg2
import json
import os
from dotenv import load_dotenv

load_dotenv()
DB_pass= os.getenv("DB_PASS")

try:
    print("Conectando con PostgreSQL...")
    conexion= psycopg2.connect(
        host="localhost",
        database="tmdb_pipeline",
        user="postgres",
        password=DB_pass,
        port="5432",
        options="-c lc_messages=C"
    )
    print("Conexión lista, esperando instrucciones SQL.")
    cursor = conexion.cursor()
    sql_insert = """
        SELECT staging_id, raw_data
        FROM bronze_tmdb_staging
        ORDER BY staging_id DESC
        LIMIT 1; 
        """
    cursor.execute(sql_insert)
    registro = cursor.fetchone()

    bronze_id = registro[0]
    datos_json = registro[1]
    print(f"¡Éxito! Hemos extraído el JSON del registro Bronze número {bronze_id}.")

    if isinstance(datos_json, str):
        datos_json = json.loads(datos_json)
    lista_peliculas = datos_json['results']
    print(f"Desempaquetando {len(lista_peliculas)} películas...")

    sql_insert_silver = """
        INSERT INTO silver_tmdb_movies (movie_id, title, release_date, popularity, vote_average, vote_count, original_language, bronze_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (movie_id) DO NOTHING;
        """
    for peli in lista_peliculas:
        id_peli = peli['id']
        titulo = peli['title']
        fecha = peli.get('release_date', None)
        if fecha == '':
            fecha=None
        popularidad = peli['popularity']
        votos=peli['vote_average']
        cantidad_votos = peli.get('vote_count', 0)
        idioma = peli.get('original_language', 'N/A')
        cursor.execute(sql_insert_silver, (id_peli, titulo, fecha, popularidad, votos, cantidad_votos, idioma, bronze_id))
    conexion.commit()
    print("¡Películas insertadas en Silver con éxito!")


except Exception as e:
    print(f"Alarma, ocurrió un error: {e}")
finally:
    print("Este bloque se ejecuta SIEMPRE, falle o no.")
    if 'conexion' in locals() and conexion is not None:
        conexion.close()
        print("Conexión a la base de datos cerrada.")

print("--- Primera película ---")
print(json.dumps(lista_peliculas[0], indent=4))
print("------------------------")
