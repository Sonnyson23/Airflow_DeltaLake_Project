# -*- coding: utf-8 -*-
"""
Airflow DAG to load TMDB data from Delta Lake (MinIO) to Elasticsearch using Spark.
"""
from __future__ import annotations

import pendulum
from datetime import timedelta
# import os # Artık PG_PASSWORD için gerekli değil

from airflow.models.dag import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator # Kaldırıldı
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable
# from airflow.hooks.base import BaseHook # Kaldırıldı

# -----------------------------------------------------------------------------
# Sabitler & Konfigürasyon
# -----------------------------------------------------------------------------

# --- Airflow Bağlantı ID'leri ---
SPARK_SSH_CONN_ID = 'spark_ssh_conn'

# --- Airflow Değişkenleri ---
MINIO_ENDPOINT_URL = Variable.get("minio_endpoint_url", default_var="http://minio:9000")
MINIO_ACCESS_KEY = Variable.get("minio_access_key", default_var="dataops")
MINIO_SECRET_KEY = Variable.get("minio_secret_key", default_var="Ankara06")

# --- Spark Betik Konfigürasyonu ---
# !!! ÖNEMLİ: Bu yolun, Elasticsearch'e yazmak üzere DÜZENLENMİŞ
# Spark betiğinizin Airflow ortamındaki DOĞRU konumu olduğundan emin olun !!!
SPARK_SCRIPT_SOURCE_PATH = "/opt/airflow/code_base/Airflow_DeltaLake_Project/python_apps/tmdb_delta_to_es.py" # Örnek isim, sizinki farklı olabilir
SPARK_SCRIPT_DEST_PATH = "/tmp/tmdb_delta_to_es.py" # spark_client içindeki hedef yol

# --- Spark Konfigürasyonu ---
# Elasticsearch Spark Connector versiyonunu kontrol edin (Spark 3.5 ve ES 8.16.1 ile uyumlu olmalı)
ES_SPARK_CONNECTOR_VERSION = "8.11.4" # Örnek versiyon
# Spark paketleri (PostgreSQL JDBC kaldırıldı, ES eklendi)
SPARK_PACKAGES = f"io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:{ES_SPARK_CONNECTOR_VERSION}"
SILVER_BUCKET = "tmdb-silver"

# -----------------------------------------------------------------------------
# DAG Varsayılan Argümanları
# -----------------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# -----------------------------------------------------------------------------
# DAG Tanımı
# -----------------------------------------------------------------------------
with DAG(
    dag_id='load_tmdb_delta_to_elasticsearch_v2', # ID güncellendi
    default_args=default_args,
    start_date=pendulum.datetime(2024, 4, 1, tz="UTC"),
    schedule=None, # Manuel tetikleme
    catchup=False,
    tags=['tmdb', 'elasticsearch', 'kibana', 'spark', 'delta', 'load-only'], # Etiketler güncellendi
    doc_md="""
    ### Load TMDB Delta Lake Data to Elasticsearch

    Loads data directly from MinIO Delta Lake tables to Elasticsearch using a Spark job run via SSH.

    1. Copies PySpark script (configured for ES) to Spark client.
    2. Executes PySpark script via SSH.
    """, # Açıklama güncellendi
) as dag:

    # --- Görev 1: Spark Betiğini Kopyala ---
    # SPARK_SCRIPT_SOURCE_PATH'ın Elasticsearch'e yazan doğru betiği gösterdiğinden emin olun.
    copy_script_task = BashOperator(
        task_id='copy_delta_to_es_script',
        bash_command=f"docker cp {SPARK_SCRIPT_SOURCE_PATH} spark_client:{SPARK_SCRIPT_DEST_PATH}",
    )

    # --- Görev 2: Spark İşini SSH ile Çalıştır (ES Hedefli) ---

# spark-submit komutu (ES için yapılandırıldı)
    spark_submit_command = f"""
    echo '--- SSHOperator: Starting Spark Job Execution (Delta to ES) ---' && \\
    echo 'Installing dependencies (boto3)...' && \\
    pip install boto3 && \\
    echo 'Setting execute permission on script...' && \\
    chmod +x {SPARK_SCRIPT_DEST_PATH} && \\  # <-- *** EKLENMESİ GEREKEN SATIR ***
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
         --conf es.nodes=http://es \\ # <-- DEĞİŞİKLİK BURADA: http:// eklendi
        --conf es.port=9200 \\
        --conf es.nodes.discovery=false \\
        {SPARK_SCRIPT_DEST_PATH} # Kopyalanan betiği çalıştır

    EXIT_CODE=$?
    echo "--- SSHOperator: Spark Job finished with exit code $EXIT_CODE ---"
    exit $EXIT_CODE
    """

    run_spark_job_task = SSHOperator(
        task_id='run_spark_delta_to_es_job',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=spark_submit_command,
        # environment parametresi artık gerekli değil
        conn_timeout=60,
        cmd_timeout=3600, # Spark işinin süresine göre ayarlayın
        get_pty=False,
    )

    # --- Görev Bağımlılıkları ---
    # Önce betiği kopyala, sonra Spark işini çalıştır
    copy_script_task >> run_spark_job_task