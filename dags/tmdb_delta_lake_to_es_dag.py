# -*- coding: utf-8 -*-
"""
Airflow DAG to load TMDB data from Delta Lake (MinIO) to Elasticsearch using Spark.
Optionally creates PostgreSQL schema first.
"""
from __future__ import annotations

import pendulum
from datetime import timedelta
import os

from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook # Artık Postgres bağlantısı için gerekmeyebilir

# --- Sabitler ---
POSTGRES_CONN_ID = 'postgresql_conn' # Sadece tablo oluşturma için gerekli
SPARK_SSH_CONN_ID = 'spark_ssh_conn'
MINIO_ENDPOINT_URL = Variable.get("minio_endpoint_url", default_var="http://minio:9000")
MINIO_ACCESS_KEY = Variable.get("minio_access_key", default_var="dataops")
MINIO_SECRET_KEY = Variable.get("minio_secret_key", default_var="Ankara06")

# Spark Betik Yolu (DOĞRU YOLU GİRİN! Yeni isim kullandıysanız güncelleyin)
SPARK_SCRIPT_SOURCE_PATH = "/opt/airflow/code_base/Airflow_DeltaLake_Project/python_apps/tmdb_delta_to_es.py" # Veya eski isim tmdb_load_postgres.py
SPARK_SCRIPT_DEST_PATH = "/tmp/tmdb_delta_to_es.py" # Hedef ismi de değiştirebiliriz

# Spark Paketleri (Elasticsearch eklendi!)
# ES 8.16.1 ve Spark 3.5 ile uyumluluk için 8.11.x veya üstü bir sürüm denenmeli.
# En güncel uyumlu versiyonu Elastic belgelerinden kontrol etmek iyi olur.
ES_SPARK_CONNECTOR_VERSION = "8.11.4" # Örnek versiyon, test edilmeli
SPARK_PACKAGES = f"io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:{ES_SPARK_CONNECTOR_VERSION}"
SILVER_BUCKET = "tmdb-silver"

# --- SQL Komutları (Postgres şeması hala oluşturulacaksa) ---
SQL_CREATE_MOVIES = """...""" # Önceki CREATE TABLE komutları burada
# ... diğer SQL_CREATE_... tanımları ...
SQL_CREATE_SPOKEN_LANGUAGES = """..."""

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
    dag_id='load_tmdb_delta_to_elasticsearch', # Yeni DAG ID
    default_args=default_args,
    start_date=pendulum.datetime(2024, 4, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['tmdb', 'elasticsearch', 'kibana', 'spark', 'delta'], # Yeni etiketler
    doc_md="""
    ### Load TMDB Delta Lake Data to Elasticsearch

    1. (Optional) Creates TMDB relational schema in PostgreSQL.
    2. Copies PySpark script to Spark client.
    3. Executes PySpark script via SSH to load data from MinIO Delta tables into Elasticsearch indices.
    """,
) as dag:

    # --- Görev 1 (Opsiyonel): Tabloları Oluştur ---
    # Eğer Postgres şemasını oluşturmaya devam etmek istiyorsanız bu görevi tutun.
    create_postgres_tables_task = PostgresOperator(
        task_id='create_postgres_tables', # Task ID güncellendi
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=[
            SQL_CREATE_MOVIES, SQL_CREATE_CAST, SQL_CREATE_CREW, SQL_CREATE_GENRES,
            SQL_CREATE_KEYWORDS, SQL_CREATE_PROD_COMPANIES, SQL_CREATE_PROD_COUNTRIES,
            SQL_CREATE_SPOKEN_LANGUAGES,
        ],
    )

    # --- Görev 2: Spark Betiğini Kopyala ---
    # Betik adını (SPARK_SCRIPT_SOURCE_PATH, SPARK_SCRIPT_DEST_PATH) güncellediyseniz burada da güncelleyin.
    task_copy_script_task = BashOperator(
        task_id='copy_delta_to_es_script', # Task ID güncellendi
        bash_command=f"docker cp {SPARK_SCRIPT_SOURCE_PATH} spark_client:{SPARK_SCRIPT_DEST_PATH}",
    )

    # --- Görev 3: Spark İşini SSH ile Çalıştır (ES Hedefli) ---
    # Postgres bağlantı alma kısmı kaldırıldı.
    # Ortam değişkeni hazırlama kısmı kaldırıldı.

    # spark-submit komutu (ES paketleri ve ayarları eklendi, PG argümanları kaldırıldı)
    spark_submit_command = f"""
    echo '--- SSHOperator: Starting Spark Job Execution (Delta to ES) ---' && \\
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
        --conf es.nodes=es \\
        --conf es.port=9200 \\
        --conf es.nodes.wan.only=false \\
        {SPARK_SCRIPT_DEST_PATH} # Çalıştırılacak betik (yeni isim?)

    EXIT_CODE=$?
    echo "--- SSHOperator: Spark Job finished with exit code $EXIT_CODE ---"
    exit $EXIT_CODE
    """

    task_run_spark_delta_to_es_job = SSHOperator(
        task_id='run_spark_delta_to_es_job', # Task ID güncellendi
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=spark_submit_command,
        # environment=ssh_environment, # Kaldırıldı
        conn_timeout=60,
        cmd_timeout=3600,
        get_pty=False,
    )

    # --- Görev Bağımlılıkları ---
    # Eğer Postgres tablolarını oluşturmaya devam ediyorsanız:
    task_copy_script_task >> task_run_spark_delta_to_es_job
    # Eğer Postgres tablosu oluşturma görevini kaldırırsanız:
    # copy_script_task >> run_spark_job_task