# -*- coding: utf-8 -*-
"""
Airflow DAG to create TMDB relational schema in PostgreSQL.
"""
from __future__ import annotations
import pendulum
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook


# --- Sabitler ---
POSTGRES_CONN_ID = 'postgresql_conn' # Mevcut sabit
SPARK_SSH_CONN_ID = 'spark_ssh_conn'        # <-- EKLENDİ
MINIO_ENDPOINT_URL = Variable.get("minio_endpoint_url", default_var="http://minio:9000") # <-- EKLENDİ
MINIO_ACCESS_KEY = Variable.get("minio_access_key", default_var="dataops") # <-- EKLENDİ
MINIO_SECRET_KEY = Variable.get("minio_secret_key", default_var="Ankara06") # <-- EKLENDİ
# Doğrulanmış DOĞRU Spark betik yolunu buraya yazın!
SPARK_SCRIPT_SOURCE_PATH = "/opt/airflow/code_base/Airflow_DeltaLake_Project/python_apps/tmdb_load_postgres.py" # <-- EKLENDİ (Yolu kontrol edin!)
SPARK_SCRIPT_DEST_PATH = "/tmp/tmdb_load_postgres.py" # <-- EKLENDİ
SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4" # <-- EKLENDİ
SILVER_BUCKET = "tmdb-silver" # <-- EKLENDİ

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
    task_create_tables_task = PostgresOperator(
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

# --- Görev 2: Statik Spark Betiğini Kopyala --- # <-- YENİ GÖREV
    task_copy_script_task = BashOperator(
        task_id='copy_script_to_spark',
        bash_command=f"docker cp {SPARK_SCRIPT_SOURCE_PATH} spark_client:{SPARK_SCRIPT_DEST_PATH}",
    )

    # --- Görev 3: Spark İşini SSH ile Çalıştır --- # <-- YENİ GÖREV
    # Bağlantı detaylarını al (şifre dahil)
    try:
        pg_conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        jdbc_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port or 5432}/{pg_conn.schema or ''}"
        pg_user = pg_conn.login
        pg_password = pg_conn.get_password() # Şifreyi alıyoruz ama argüman olarak geçeceğiz
    except Exception as e:
        raise ValueError(f"Could not get PostgreSQL connection details for {POSTGRES_CONN_ID}: {e}")

    # spark-submit komutu (şifre argümanı eklendi)
    spark_submit_command = f"""
    echo '--- SSHOperator: Starting Spark Job Execution ---' && \\
    echo 'Installing dependencies (boto3)...' && \\
    pip install boto3 && \\
    echo 'Running spark-submit...' && \\
    cd /tmp && \\
    spark-submit --master local[*] \\
        --packages {SPARK_PACKAGES} \\
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \\
        --conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT_URL} \\
        --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} \\
        --conf spark.hadoop.fs.s3a.secret.key='{MINIO_SECRET_KEY}' \\
        --conf spark.hadoop.fs.s3a.path.style.access=true \\
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
        --conf spark.sql.warehouse.dir=s3a://{SILVER_BUCKET}/delta-warehouse \\
        {SPARK_SCRIPT_DEST_PATH} \\
        --jdbc-url "{jdbc_url}" \\
        --pg-user "{pg_user}" \\
        --pg-password "{pg_password}" # <-- ŞİFRE ARGÜMANI BURADA

    EXIT_CODE=$?
    echo "--- SSHOperator: Spark Job finished with exit code $EXIT_CODE ---"
    exit $EXIT_CODE
    """

    task_run_spark_job_task = SSHOperator(
        task_id='run_spark_load_to_postgres',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=spark_submit_command,
        # environment parametresi yok (şifre argüman olarak geçti)
        conn_timeout=60,
        cmd_timeout=3600, # Spark işinin süresine göre ayarlayın
        get_pty=False,
    )

    # --- Görev Bağımlılıkları --- # <-- YENİ EKLENEN BAĞIMLILIKLAR
    # Önce tabloları oluştur, sonra betiği kopyala, sonra Spark işini çalıştır
    task_create_tables_task >> task_copy_script_task >> task_run_spark_job_task