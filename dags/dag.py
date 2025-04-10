# -*- coding: utf-8 -*-
"""
Airflow DAG to load TMDB data from Delta Lake (MinIO) to PostgreSQL using a static Spark script.
"""
from __future__ import annotations

import pendulum # Tarih/zaman işlemleri için pendulum kullanalım

from datetime import timedelta
import os # Spark betiği ortam değişkeni okuyacaksa gerekli

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

# -----------------------------------------------------------------------------
# Sabitler & Konfigürasyon
# -----------------------------------------------------------------------------

# --- Airflow Bağlantı ID'leri ---
SPARK_SSH_CONN_ID = 'spark_ssh_conn'        # Spark client'a SSH erişimi için Airflow Bağlantı ID'si
POSTGRES_CONN_ID = 'postgresql_conn'      # PostgreSQL detayları için Airflow Bağlantı ID'si

# --- Airflow Değişkenleri (Gizli bilgiler için Bağlantıları kullanmak daha iyi olabilir) ---
# Bu değişkenlerin Airflow UI -> Admin -> Variables altında tanımlı olduğundan emin olun
MINIO_ENDPOINT_URL = Variable.get("minio_endpoint_url", default_var="http://minio:9000")
MINIO_ACCESS_KEY = Variable.get("minio_access_key", default_var="dataops")
MINIO_SECRET_KEY = Variable.get("minio_secret_key", default_var="Ankara06") # default_var'ı kendi varsayılanınızla değiştirin

# --- Spark Betik Konfigürasyonu ---
# !!! ÇOK ÖNEMLİ: Bu yolu, statik Spark betiğinizin Airflow ortamındaki
# (DAG'larınızın senkronize edildiği/mount edildiği yerdeki) DOĞRU konumuyla güncelleyin !!!
SPARK_SCRIPT_SOURCE_PATH = "/opt/airflow/code_base/Airflow_DeltaLake_Project/spark_scripts/tmdb_load_postgres.py"
SPARK_SCRIPT_DEST_PATH = "/tmp/tmdb_load_postgres.py" # spark_client container'ı içindeki hedef yol

# --- Spark Konfigürasyonu ---
SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4"
SILVER_BUCKET = "tmdb-silver" # Eğer farklıysa güncelleyin

# -----------------------------------------------------------------------------
# DAG Varsayılan Argümanları
# -----------------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # Hata ayıklama için deneme sayısını 1'e düşürelim
    'retry_delay': timedelta(minutes=2), # Deneme aralığı
}

# -----------------------------------------------------------------------------
# DAG Tanımı
# -----------------------------------------------------------------------------
with DAG(
    dag_id='tmdb_delta_to_postgres_static_script_v1', # Bu versiyon için yeni ID
    default_args=default_args,
    start_date=pendulum.datetime(2024, 4, 1, tz="UTC"), # Sabit başlangıç tarihi
    schedule=None, # Manuel tetikleme veya istediğiniz aralık ('@daily' vb.)
    catchup=False,
    tags=['tmdb', 'spark', 'postgres', 'delta', 'jdbc', 'static-script'],
    doc_md="""
    ### TMDB Delta Lake'ten PostgreSQL'e ETL DAG'ı (Statik Betik Versiyonu)

    Bu DAG aşağıdaki adımları gerçekleştirir:
    1. Statik bir PySpark betiğini (`tmdb_load_postgres.py`) Spark client container'ına kopyalar.
    2. PySpark betiğini Spark client container'ında SSH üzerinden çalıştırır.
    3. Betik, MinIO/S3 üzerindeki birden fazla TMDB Delta tablosunu okur.
    4. Betik, veriyi JDBC kullanarak PostgreSQL veritabanındaki ilgili tablolara yazar.
    5. PostgreSQL bağlantı detayları (şifre hariç) komut satırı argümanı olarak iletilir.
    6. PostgreSQL şifresi güvenli bir şekilde ortam değişkeni (`PG_PASSWORD`) olarak iletilir.
    7. Hedef PostgreSQL tablolarının önceden var olduğu varsayılır.
    """,
) as dag:

    # --- Görev 1: Statik Spark Betiğini Kopyala ---
    # SPARK_SCRIPT_SOURCE_PATH'ın doğru olduğundan emin olun!
    copy_script_task = BashOperator(
        task_id='copy_script_to_spark',
        bash_command=f"docker cp {SPARK_SCRIPT_SOURCE_PATH} spark_client:{SPARK_SCRIPT_DEST_PATH}",
        # 'spark_client' container adı farklıysa veya IP/Hostname gerekiyorsa komutu düzenleyin.
    )

    # --- Görev 2: Spark İşini SSH ile Çalıştır ---

    # PostgreSQL bağlantı detaylarını DAG context'i içinde al (daha verimli)
    try:
        pg_conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        # Eksik port veya şema durumunu ele alalım
        jdbc_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port or 5432}/{pg_conn.schema or ''}"
        pg_user = pg_conn.login
        pg_password = pg_conn.get_password() # get_password() metoduyla al
    except Exception as e:
        # Bağlantı alınamazsa DAG'ı burada durdurmak mantıklı olabilir
        raise ValueError(f"Could not get PostgreSQL connection details for {POSTGRES_CONN_ID}: {e}")

    # Güvenli şifre aktarımı için ortam değişkeni hazırla
    ssh_environment = {'PG_PASSWORD': pg_password}

    # spark-submit komutunu oluştur
    # Not: Statik betiğin şifreyi PG_PASSWORD ortam değişkeninden, diğerlerini CLI argümanlarından okuduğu varsayılır.
    # Not: --pg-table argümanı kaldırıldı, statik betiğin tablo eşlemesini kendi içinde yaptığı varsayılır.
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
        --pg-user "{pg_user}"

    # Spark-submit'in çıkış kodunu al ve SSHOperator'ın bunu kullanmasını sağla
    EXIT_CODE=$?
    echo "--- SSHOperator: Spark Job finished with exit code $EXIT_CODE ---"
    exit $EXIT_CODE
    """

    run_spark_job_task = SSHOperator(
        task_id='run_spark_load_to_postgres',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=spark_submit_command,
        environment=ssh_environment, # Ortam değişkenini SSH oturumuna aktar
        conn_timeout=60,             # Bağlantı zaman aşımı (saniye)
        cmd_timeout=3600,            # Komut zaman aşımı (örn: Spark işi için 1 saat)
        get_pty=False,               # Uzun süren komutlar için genellikle False önerilir
    )

    # --- Görev Bağımlılıkları ---
    copy_script_task >> run_spark_job_task