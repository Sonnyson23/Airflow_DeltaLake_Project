# -*- coding: utf-8 -*-
"""
Airflow DAG to create TMDB relational schema in PostgreSQL.
"""
from __future__ import annotations

import pendulum

from datetime import timedelta

from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# --- Sabitler ---
POSTGRES_CONN_ID = 'postgresql_conn' # Airflow UI'daki Postgres bağlantı ID'si

# --- SQL Komutları ---
# Yukarıdaki Adım 1'de tanımlanan SQL komutlarını buraya kopyalayın
# Her komut ayrı bir string olmalı
SQL_CREATE_MOVIES = """
CREATE TABLE IF NOT EXISTS public.movies (
    movie_id INT PRIMARY KEY, title VARCHAR(255), budget DOUBLE PRECISION, homepage TEXT,
    original_language VARCHAR(10), original_title VARCHAR(255), overview TEXT, popularity FLOAT,
    release_date DATE, revenue DOUBLE PRECISION, runtime INT, status VARCHAR(50),
    tagline TEXT, vote_average FLOAT, vote_count INT
);
"""
SQL_CREATE_CAST = """
CREATE TABLE IF NOT EXISTS public.cast (
    movie_id INT, cast_id INT, character TEXT, credit_id VARCHAR(255), gender INT, id INT, name VARCHAR(255),
    PRIMARY KEY (movie_id, cast_id), FOREIGN KEY (movie_id) REFERENCES public.movies(movie_id) ON DELETE CASCADE
);
"""
SQL_CREATE_CREW = """
CREATE TABLE IF NOT EXISTS public.crew (
    movie_id INT, credit_id VARCHAR(255) PRIMARY KEY, department VARCHAR(100), gender INT, id INT,
    job VARCHAR(255), name VARCHAR(255), FOREIGN KEY (movie_id) REFERENCES public.movies(movie_id) ON DELETE CASCADE
);
"""
SQL_CREATE_GENRES = """
CREATE TABLE IF NOT EXISTS public.genres (
    movie_id INT, id INT, name VARCHAR(100), PRIMARY KEY (movie_id, id),
    FOREIGN KEY (movie_id) REFERENCES public.movies(movie_id) ON DELETE CASCADE
);
"""
SQL_CREATE_KEYWORDS = """
CREATE TABLE IF NOT EXISTS public.keywords (
    movie_id INT, id INT, name VARCHAR(255), PRIMARY KEY (movie_id, id),
    FOREIGN KEY (movie_id) REFERENCES public.movies(movie_id) ON DELETE CASCADE
);
"""
SQL_CREATE_PROD_COMPANIES = """
CREATE TABLE IF NOT EXISTS public.production_companies (
    movie_id INT, id INT, name VARCHAR(255), PRIMARY KEY (movie_id, id),
    FOREIGN KEY (movie_id) REFERENCES public.movies(movie_id) ON DELETE CASCADE
);
"""
SQL_CREATE_PROD_COUNTRIES = """
CREATE TABLE IF NOT EXISTS public.production_countries (
    movie_id INT, iso_3166_1 VARCHAR(10), name VARCHAR(255), PRIMARY KEY (movie_id, iso_3166_1),
    FOREIGN KEY (movie_id) REFERENCES public.movies(movie_id) ON DELETE CASCADE
);
"""
SQL_CREATE_SPOKEN_LANGUAGES = """
CREATE TABLE IF NOT EXISTS public.spoken_languages (
    movie_id INT, iso_639_1 VARCHAR(10), name VARCHAR(100), PRIMARY KEY (movie_id, iso_639_1),
    FOREIGN KEY (movie_id) REFERENCES public.movies(movie_id) ON DELETE CASCADE
);
"""

# DAG Varsayılan Argümanları
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG Tanımı
with DAG(
    dag_id='create_tmdb_postgres_schema',
    default_args=default_args,
    start_date=pendulum.datetime(2024, 4, 1, tz="UTC"),
    schedule=None, # Manuel tetikleme için
    catchup=False,
    tags=['tmdb', 'postgres', 'schema', 'dag_2'],
    doc_md="""
    ### Create TMDB Relational Schema in PostgreSQL

    This DAG creates the necessary tables with primary and foreign keys
    in the PostgreSQL database for the TMDB project using the PostgresOperator.
    It assumes the 'postgresql_conn' Airflow connection is configured.
    """,
) as dag:

    # Her tablo için ayrı PostgresOperator görevi oluşturabiliriz veya tek bir görevde listeleyebiliriz.
    # Tek görev daha basit görünüyor.
    create_tables_task = PostgresOperator(
        task_id='create_tmdb_tables',
        postgres_conn_id=POSTGRES_CONN_ID, # Airflow Connection ID'sini kullanıyoruz
        sql=[ # SQL komutlarını bir liste olarak veriyoruz
            SQL_CREATE_MOVIES,
            SQL_CREATE_CAST,
            SQL_CREATE_CREW,
            SQL_CREATE_GENRES,
            SQL_CREATE_KEYWORDS,
            SQL_CREATE_PROD_COMPANIES,
            SQL_CREATE_PROD_COUNTRIES,
            SQL_CREATE_SPOKEN_LANGUAGES,
        ],
    )

# Bu DAG'ın başka bir bağımlılığı yok, tek bir görevi var.