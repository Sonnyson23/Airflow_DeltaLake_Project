from datetime import datetime, timedelta
import os
import requests
import json
import time
from typing import Dict, List, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# DAG definition and default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# MinIO connection information
MINIO_ENDPOINT = Variable.get("endpoint_url", default_var="http://minio:9000")
AWS_ACCESS_KEY = Variable.get("aws_access_key_id", default_var="dataops")
AWS_SECRET_KEY = Variable.get("aws_secret_access_key", default_var="root12345")

# Spark SSH connection
SPARK_SSH_CONN_ID = 'spark_ssh_conn'

# MinIO bucket names
SILVER_BUCKET = "tmdb-silver"

# PostgreSQL connection ID
POSTGRES_CONN_ID = 'postgresql_conn'


# PySpark code for reading Delta tables and loading to PostgreSQL
def generate_spark_load_postgres_script(**context):
    """Generates PySpark code to read Delta tables from MinIO and load them to PostgreSQL."""

    postgres_db = Variable.get("postgres_db")
    postgres_host = Variable.get("postgres_host")
    postgres_user = Variable.get("postgres_user")
    postgres_password = Variable.get("postgres_password")

    spark_script = f"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import boto3
from botocore.client import Config
from delta.tables import *
from sqlalchemy import create_engine, text
import os

# PostgreSQL connection details (Airflow Connection'dan alınacak)
postgres_conn_id = "postgresql_conn"  # Airflow Connection ID'niz

# MinIO connection details (Spark Conf'dan alınacak)
minio_endpoint = "{MINIO_ENDPOINT}"
aws_access_key = "{AWS_ACCESS_KEY}"
aws_secret_key = "{AWS_SECRET_KEY}"

# S3 configuration for Delta Lake
spark = SparkSession.builder \
    .appName("TMDB Delta to PostgreSQL") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.warehouse.dir", f"s3a://tmdb-silver/delta-warehouse") \
    .getOrCreate()

# Read Delta Lake tables
delta_path = "s3a://tmdb-silver/"
cast_df = spark.read.format("delta").load(delta_path + "cast")
crew_df = spark.read.format("delta").load(delta_path + "crew")
movies_df = spark.read.format("delta").load(delta_path + "movies")
genres_df = spark.read.format("delta").load(delta_path + "genres")
keywords_df = spark.read.format("delta").load(delta_path + "keywords")
production_companies_df = spark.read.format("delta").load(delta_path + "production_companies")
production_countries_df = spark.read.format("delta").load(delta_path + "production_countries")
spoken_languages_df = spark.read.format("delta").load(delta_path + "spoken_languages")

# PostgreSQL connection details (Airflow Connection kullanılarak alınmalı)
hook = PostgresHook(postgres_conn_id=postgres_conn_id)
engine = hook.get_sqlalchemy_engine()


def execute_sql(sql_statement):
    with engine.begin() as conn:
        conn.execute(text(sql_statement))


# Create tables with primary and foreign keys
execute_sql(\"""
CREATE TABLE IF NOT EXISTS movies (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255),
    budget DOUBLE PRECISION,
    homepage VARCHAR(255),
    original_language VARCHAR(10),
    original_title VARCHAR(255),
    overview TEXT,
    popularity FLOAT,
    release_date DATE,
    revenue DOUBLE PRECISION,
    runtime INT,
    status VARCHAR(20),
    tagline VARCHAR(255),
    vote_average FLOAT,
    vote_count INT
);
\"\"\")

execute_sql(\"""
CREATE TABLE IF NOT EXISTS cast (
    movie_id INT,
    cast_id INT,
    character VARCHAR(255),
    credit_id VARCHAR(255),
    gender INT,
    id INT,
    name VARCHAR(255),
    PRIMARY KEY (movie_id, cast_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
\"\"\")

execute_sql(\"""
CREATE TABLE IF NOT EXISTS crew (
    movie_id INT,
    credit_id VARCHAR(255) PRIMARY KEY,
    department VARCHAR(100),
    gender INT,
    id INT,
    job VARCHAR(100),
    name VARCHAR(255),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
\"\"\")

execute_sql(\"""
CREATE TABLE IF NOT EXISTS genres (
    movie_id INT,
    id INT,
    name VARCHAR(100),
    PRIMARY KEY (movie_id, id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
\"\"\")

execute_sql(\"""
CREATE TABLE IF NOT EXISTS keywords (
    movie_id INT,
    id INT,
    name VARCHAR(100),
    PRIMARY KEY (movie_id, id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
\"\"\")

execute_sql(\"""
CREATE TABLE IF NOT EXISTS production_companies (
    movie_id INT,
    id INT,
    name VARCHAR(255),
    PRIMARY KEY (movie_id, id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
\"\"\")

execute_sql(\"""
CREATE TABLE IF NOT EXISTS production_countries (
    movie_id INT,
    iso_3166_1 VARCHAR(10),
    name VARCHAR(255),
    PRIMARY KEY (movie_id, iso_3166_1),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
\"\"\")

execute_sql(\"""
CREATE TABLE IF NOT EXISTS spoken_languages (
    movie_id INT,
    iso_639_1 VARCHAR(10),
    name VARCHAR(100),
    PRIMARY KEY (movie_id, iso_639_1),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
\"\"\")


# Write DataFrames to PostgreSQL (after tables are created)
def write_df_to_postgres(df, table_name):
    df.write.format("jdbc") \
        .option("url", engine.url) \
        .option("dbtable", table_name) \
        .option("user", engine.url.username) \
        .option("password", engine.url.password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()


write_df_to_postgres(cast_df, "cast")
write_df_to_postgres(crew_df, "crew")
write_df_to_postgres(movies_df, "movies")
write_df_to_postgres(genres_df, "genres")
write_df_to_postgres(keywords_df, "keywords")
write_df_to_postgres(production_companies_df, "production_companies")
write_df_to_postgres(production_countries_df, "production_countries")
write_df_to_postgres(spoken_languages_df, "spoken_languages")

print("Delta Lake tables loaded to PostgreSQL with schema definitions.")

spark.stop()
    """

    # Write PySpark code to file
    script_path = "/tmp/tmdb_load_postgres.py"
    with open(script_path, "w") as f:
        f.write(spark_script)

    # Share file path via XCom
    context['ti'].xcom_push(key='spark_script_path', value=script_path)

    return script_path


# DAG definition
dag = DAG(
    'tmdb_delta_lake_to_postgres_dag',
    default_args=default_args,
    description='Pipeline to load TMDB Delta Lake data from MinIO to PostgreSQL with schema definitions',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 7),
    catchup=False,
    tags=['tmdb', 'delta_lake', 'postgres'],
)

# Script'i oluştur
task_generate_spark_script = PythonOperator(
    task_id='generate_spark_load_postgres_script',
    python_callable=generate_spark_load_postgres_script,
    provide_context=True,
    dag=dag,
)

# Script'i Spark container'ına kopyala
task_copy_script_to_spark = BashOperator(
    task_id='copy_script_to_spark',
    bash_command='docker cp {{ ti.xcom_pull(task_ids="generate_spark_load_postgres_script") }} spark_client:/tmp/tmdb_load_postgres.py',
    dag=dag,
)

task_run_spark_transformation = SSHOperator(
    task_id='run_spark_load_to_postgres',
    ssh_conn_id='spark_ssh_conn',
    command=f"""
        cd /tmp && \
        spark-submit --master local[*] \
        --packages io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} \
        --conf spark.hadoop.fs.s3a.access.key={AWS_ACCESS_KEY} \
        --conf spark.hadoop.fs.s3a.secret.key={AWS_SECRET_KEY} \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        /tmp/tmdb_load_postgres.py
    """,
    cmd_timeout=3600,
    conn_timeout=60,
    dag=dag,
)

# Task bağımlılıklarını güncelle
task_generate_spark_script >> task_copy_script_to_spark >> task_run_spark_transformation