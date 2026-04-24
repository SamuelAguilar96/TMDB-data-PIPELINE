# TMDb Data Pipeline (ETL)

Pipeline ETL modular en Python y PostgreSQL para procesar datos de The Movie Database (TMDb) implementando la **Arquitectura Medallón**.

## 🏗️ Arquitectura

* **Capa Bronze (`extractor.py`):** Extracción dinámica vía API REST y almacenamiento de datos crudos (JSON).
* **Capa Silver (`transformer.py`):** Limpieza, extracción de variables y carga relacional con control de duplicados (`ON CONFLICT DO NOTHING`).
* **Capa Gold (`loader.py` & `exporter.py`):** Filtrado analítico SQL (ej. películas >1000 votos) y exportación automatizada a CSV.

## 🛠️ Stack Tecnológico

* **Lenguaje:** Python (requests, psycopg2, python-dotenv)
* **Base de Datos:** PostgreSQL
* **Fuente:** TMDb API

## 🚀 Uso Rápido

**1. Instalar dependencias:**
```bash
pip install -r requirements.txt
