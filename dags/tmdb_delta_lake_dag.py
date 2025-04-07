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

# Dataset URLs
TMDB_CREDITS_URL = "https://github.com/erkansirin78/datasets/raw/master/tmdb_5000_movies_and_credits.zip"

# MinIO bucket names
BRONZE_BUCKET = "tmdb-bronze"
SILVER_BUCKET = "tmdb-silver"

# Function to download datasets
def download_datasets(**context):
    """Downloads TMDB datasets and saves them to a temporary directory"""
    import zipfile
    import requests
    import os
    
    # Create temporary directory
    temp_dir = "/tmp/tmdb_data"
    os.makedirs(temp_dir, exist_ok=True)
    
    # Download zip file
    zip_path = f"{temp_dir}/tmdb_data.zip"
    response = requests.get(TMDB_CREDITS_URL)
    with open(zip_path, 'wb') as f:
        f.write(response.content)
    
    # Extract zip file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
    
    # Share file paths via XCom
    context['ti'].xcom_push(key='credits_path', value=f"{temp_dir}/tmdb_5000_credits.csv")
    context['ti'].xcom_push(key='movies_path', value=f"{temp_dir}/tmdb_5000_movies.csv")
    
    return {"credits_path": f"{temp_dir}/tmdb_5000_credits.csv", 
            "movies_path": f"{temp_dir}/tmdb_5000_movies.csv"}

# Function to upload data to MinIO
def upload_to_minio(**context):
    """Uploads downloaded datasets to MinIO"""
    import boto3
    from botocore.client import Config
    
    # Get file paths from XCom
    ti = context['ti']
    credits_path = ti.xcom_pull(task_ids='download_datasets', key='credits_path')
    movies_path = ti.xcom_pull(task_ids='download_datasets', key='movies_path')
    
    # Create MinIO connection
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    # Create buckets (if they don't exist)
    for bucket in [BRONZE_BUCKET, SILVER_BUCKET]:
        try:
            s3_client.head_bucket(Bucket=bucket)
        except:
            s3_client.create_bucket(Bucket=bucket)
    
    # Upload files
    s3_client.upload_file("/tmp/tmdb_data/tmdb_5000_movies_and_credits/tmdb_5000_credits.csv", BRONZE_BUCKET, "credits/credits_part_001.csv")
    s3_client.upload_file("/tmp/tmdb_data/tmdb_5000_movies_and_credits/tmdb_5000_movies.csv", BRONZE_BUCKET, "movies/movies_part_001.csv")

    
    return "Files successfully uploaded to MinIO"

# PySpark code for data transformation
def generate_spark_transformation_script(**context):
    """Generates PySpark code for data transformation"""
    
    spark_script = """
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import boto3
from botocore.client import Config
from delta.tables import *

# Create Spark session
spark = (SparkSession.builder
         .appName("TMDB Data Transformation")
         .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

# MinIO connection information
minio_endpoint = "http://minio:9000"
aws_access_key = "dataops"
aws_secret_key = "root12345"

# S3 configuration
spark.conf.set("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
spark.conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Read data from MinIO
credits_df = spark.read.csv("s3a://tmdb-bronze/credits/", header=True, multiLine=True, escape='"')
movies_df = spark.read.csv("s3a://tmdb-bronze/movies/", header=True, multiLine=True, escape='"')

# Data cleaning and transformation operations
# 1. Create Cast table
from pyspark.sql.functions import explode, col, lit
import json

# Parse Cast JSON data
def parse_cast(cast_json):
    try:
        cast_data = json.loads(cast_json.replace("'", '"'))
        return cast_data
    except:
        return []

parse_cast_udf = F.udf(parse_cast, ArrayType(MapType(StringType(), StringType())))

# Extract cast data
credits_with_cast = credits_df.withColumn("cast_data", parse_cast_udf(col("cast")))
cast_df = credits_with_cast.select(
    col("movie_id"),
    col("title"),
    explode(col("cast_data")).alias("cast_member")
).select(
    col("movie_id"),
    col("title"),
    col("cast_member.cast_id").alias("cast_id"),
    col("cast_member.character").alias("character"),
    col("cast_member.credit_id").alias("credit_id"),
    col("cast_member.gender").alias("gender"),
    col("cast_member.id").alias("id"),
    col("cast_member.name").alias("name")
)

# Fill null values
cast_df = cast_df.fillna({"credit_id": "0000000000"})

# 2. Create Crew table
def parse_crew(crew_json):
    try:
        crew_data = json.loads(crew_json.replace("'", '"'))
        return crew_data
    except:
        return []

parse_crew_udf = F.udf(parse_crew, ArrayType(MapType(StringType(), StringType())))

# Extract crew data
credits_with_crew = credits_df.withColumn("crew_data", parse_crew_udf(col("crew")))
crew_df = credits_with_crew.select(
    col("movie_id"),
    col("title"),
    explode(col("crew_data")).alias("crew_member")
).select(
    col("movie_id"),
    col("title"),
    col("crew_member.credit_id").alias("credit_id"),
    col("crew_member.department").alias("department"),
    col("crew_member.gender").alias("gender"),
    col("crew_member.id").alias("id"),
    col("crew_member.job").alias("job"),
    col("crew_member.name").alias("name")
)

# Fill null values
crew_df = crew_df.fillna({"credit_id": "0000000000"})

# 3. Create Movies table
# Functions to parse JSON columns
def extract_value(json_str, key):
    try:
        data = json.loads(json_str.replace("'", '"'))
        return data.get(key, None)
    except:
        return None

# Create Movies table
movies_clean_df = movies_df.select(
    col("id").alias("movie_id"),
    col("title"),
    col("budget").cast("double"),
    col("homepage"),
    col("original_language"),
    col("original_title"),
    col("overview"),
    col("popularity").cast("float"),
    F.to_date(col("release_date"), "yyyy-MM-dd").alias("release_date"),
    col("revenue").cast("double"),
    col("runtime").cast("integer"),
    col("status"),
    col("tagline"),
    col("vote_average").cast("float"),
    col("vote_count").cast("integer")
)

# 4. Create Genres table
def parse_genres(genres_json):
    try:
        genres_data = json.loads(genres_json.replace("'", '"'))
        return genres_data
    except:
        return []

parse_genres_udf = F.udf(parse_genres, ArrayType(MapType(StringType(), StringType())))

# Extract genres data
movies_with_genres = movies_df.withColumn("genres_data", parse_genres_udf(col("genres")))
genres_df = movies_with_genres.select(
    col("id").alias("movie_id"),
    explode(col("genres_data")).alias("genre")
).select(
    col("movie_id"),
    col("genre.id").cast("integer").alias("id"),
    col("genre.name")
)

# Fill null values
genres_df = genres_df.fillna({"id": -9999})

# 5. Create Keywords table
def parse_keywords(keywords_json):
    try:
        keywords_data = json.loads(keywords_json.replace("'", '"'))
        return keywords_data
    except:
        return []

parse_keywords_udf = F.udf(parse_keywords, ArrayType(MapType(StringType(), StringType())))

# Extract keywords data
movies_with_keywords = movies_df.withColumn("keywords_data", parse_keywords_udf(col("keywords")))
keywords_df = movies_with_keywords.select(
    col("id").alias("movie_id"),
    explode(col("keywords_data")).alias("keyword")
).select(
    col("movie_id"),
    col("keyword.id").cast("integer").alias("id"),
    col("keyword.name")
)

# Fill null values
keywords_df = keywords_df.fillna({"id": -9999})

# 6. Create Production Companies table
def parse_production_companies(pc_json):
    try:
        pc_data = json.loads(pc_json.replace("'", '"'))
        return pc_data
    except:
        return []

parse_pc_udf = F.udf(parse_production_companies, ArrayType(MapType(StringType(), StringType())))

# Extract production companies data
movies_with_pc = movies_df.withColumn("pc_data", parse_pc_udf(col("production_companies")))
production_companies_df = movies_with_pc.select(
    col("id").alias("movie_id"),
    explode(col("pc_data")).alias("company")
).select(
    col("movie_id"),
    col("company.id").cast("integer").alias("id"),
    col("company.name")
)

# Fill null values
production_companies_df = production_companies_df.fillna({"id": -9999})

# 7. Create Production Countries table
def parse_production_countries(pc_json):
    try:
        pc_data = json.loads(pc_json.replace("'", '"'))
        return pc_data
    except:
        return []

parse_countries_udf = F.udf(parse_production_countries, ArrayType(MapType(StringType(), StringType())))

# Extract production countries data
movies_with_countries = movies_df.withColumn("countries_data", parse_countries_udf(col("production_countries")))
production_countries_df = movies_with_countries.select(
    col("id").alias("movie_id"),
    explode(col("countries_data")).alias("country")
).select(
    col("movie_id"),
    col("country.iso_3166_1").alias("iso_3166_1"),
    col("country.name")
)

# Fill null values
production_countries_df = production_countries_df.fillna({"iso_3166_1": "XX"})

# 8. Create Spoken Languages table
def parse_spoken_languages(sl_json):
    try:
        sl_data = json.loads(sl_json.replace("'", '"'))
        return sl_data
    except:
        return []

parse_languages_udf = F.udf(parse_spoken_languages, ArrayType(MapType(StringType(), StringType())))

# Extract spoken languages data
movies_with_languages = movies_df.withColumn("languages_data", parse_languages_udf(col("spoken_languages")))
spoken_languages_df = movies_with_languages.select(
    col("id").alias("movie_id"),
    explode(col("languages_data")).alias("language")
).select(
    col("movie_id"),
    col("language.iso_639_1").alias("iso_639_1"),
    col("language.name")
)

# Fill null values
spoken_languages_df = spoken_languages_df.fillna({"iso_639_1": "XX"})

# Save in Delta Lake format
# Cast table
cast_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/cast")

# Crew table
crew_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/crew")

# Movies table
movies_clean_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/movies")

# Genres table
genres_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/genres")

# Keywords table
keywords_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/keywords")

# Production Companies table
production_companies_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/production_companies")

# Production Countries table
production_countries_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/production_countries")

# Spoken Languages table
spoken_languages_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/spoken_languages")

print("Data transformation completed and saved in Delta Lake format.")
    """
    
    # Write PySpark code to file
    script_path = "/tmp/tmdb_transformation.py"
    with open(script_path, "w") as f:
        f.write(spark_script)
    
    # Share file path via XCom
    context['ti'].xcom_push(key='spark_script_path', value=script_path)
    
    return script_path

# DAG definition
dag = DAG(
    'tmdb_delta_lake_pipeline',
    default_args=default_args,
    description='Pipeline to process TMDB data and store in Delta Lake format',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 7),
    catchup=False,
    tags=['tmdb', 'delta_lake', 'spark'],
)

# Define tasks
task_download_datasets = PythonOperator(
    task_id='download_datasets',
    python_callable=download_datasets,
    provide_context=True,
    dag=dag,
)

task_upload_to_minio = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_to_minio,
    provide_context=True,
    dag=dag,
)

# Script'i oluştur
task_generate_spark_script = PythonOperator(
    task_id='generate_spark_script',
    python_callable=generate_spark_transformation_script,
    provide_context=True,
    dag=dag,
)

# Script'i Spark container'ına kopyala
task_copy_script_to_spark = BashOperator(
    task_id='copy_script_to_spark',
    bash_command='docker cp {{ ti.xcom_pull(task_ids="generate_spark_script") }} spark_client:/tmp/tmdb_transformation.py',
    dag=dag,
)

# Spark transformation'ı çalıştır
task_run_spark_transformation = BashOperator(
    task_id='run_spark_transformation',
    bash_command="""
    docker exec spark_client spark-submit --master local[*] \
    --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=dataops \
    --conf spark.hadoop.fs.s3a.secret.key=root12345 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    /tmp/tmdb_transformation.py
    """,
    get_pty=True,  # Add this for better error reporting
    dag=dag,
) 

# Task bağımlılıklarını güncelle
task_download_datasets >> task_upload_to_minio >> task_generate_spark_script >> task_copy_script_to_spark >> task_run_spark_transformation

