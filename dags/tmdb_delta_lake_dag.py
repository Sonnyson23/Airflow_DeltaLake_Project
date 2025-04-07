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

# DAG tanımı ve varsayılan argümanlar
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# MinIO bağlantı bilgileri
MINIO_ENDPOINT = Variable.get("endpoint_url", default_var="http://minio:9000")
AWS_ACCESS_KEY = Variable.get("aws_access_key_id", default_var="dataops")
AWS_SECRET_KEY = Variable.get("aws_secret_access_key", default_var="********")

# Spark SSH bağlantısı
SPARK_SSH_CONN_ID = 'spark_ssh'

# Veri setlerinin URL'leri
TMDB_CREDITS_URL = "https://github.com/erkansirin78/datasets/raw/master/tmdb_5000_movies_and_credits.zip"

# MinIO bucket isimleri
BRONZE_BUCKET = "tmdb-bronze"
SILVER_BUCKET = "tmdb-silver"

# Veri setlerini indirme fonksiyonu
def download_datasets(**context):
    """TMDB veri setlerini indirir ve geçici bir dizine kaydeder"""
    import zipfile
    import requests
    import os
    
    # Geçici dizin oluştur
    temp_dir = "/tmp/tmdb_data"
    os.makedirs(temp_dir, exist_ok=True)
    
    # Zip dosyasını indir
    zip_path = f"{temp_dir}/tmdb_data.zip"
    response = requests.get(TMDB_CREDITS_URL)
    with open(zip_path, 'wb') as f:
        f.write(response.content)
    
    # Zip dosyasını aç
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
    
    # Dosya yollarını XCom ile paylaş
    context['ti'].xcom_push(key='credits_path', value=f"{temp_dir}/tmdb_5000_credits.csv")
    context['ti'].xcom_push(key='movies_path', value=f"{temp_dir}/tmdb_5000_movies.csv")
    
    return {"credits_path": f"{temp_dir}/tmdb_5000_credits.csv", 
            "movies_path": f"{temp_dir}/tmdb_5000_movies.csv"}

# MinIO'ya veri yükleme fonksiyonu
def upload_to_minio(**context):
    """İndirilen veri setlerini MinIO'ya yükler"""
    import boto3
    from botocore.client import Config
    
    # XCom'dan dosya yollarını al
    ti = context['ti']
    credits_path = ti.xcom_pull(task_ids='download_datasets', key='credits_path')
    movies_path = ti.xcom_pull(task_ids='download_datasets', key='movies_path')
    
    # MinIO bağlantısı oluştur
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    # Bucket'ları oluştur (eğer yoksa)
    for bucket in [BRONZE_BUCKET, SILVER_BUCKET]:
        try:
            s3_client.head_bucket(Bucket=bucket)
        except:
            s3_client.create_bucket(Bucket=bucket)
    
    # Dosyaları yükle
    s3_client.upload_file("/tmp/tmdb_data/tmdb_5000_movies_and_credits/tmdb_5000_credits.csv", BRONZE_BUCKET, "credits/credits_part_001.csv")
    s3_client.upload_file("/tmp/tmdb_data/tmdb_5000_movies_and_credits/tmdb_5000_movies.csv", BRONZE_BUCKET, "movies/movies_part_001.csv")

    
    return "Dosyalar MinIO'ya başarıyla yüklendi"

# Spark ile veri dönüşümü için PySpark kodu
def generate_spark_transformation_script(**context):
    """Spark ile veri dönüşümü için PySpark kodu oluşturur"""
    
    spark_script = """
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import boto3
from botocore.client import Config
from delta.tables import *

# Spark oturumu oluştur
spark = (SparkSession.builder
         .appName("TMDB Data Transformation")
         .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

# MinIO bağlantı bilgileri
minio_endpoint = "http://minio:9000"
aws_access_key = "dataops"
aws_secret_key = "root12345"

# S3 yapılandırması
spark.conf.set("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
spark.conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# MinIO'dan veri okuma
credits_df = spark.read.csv("s3a://tmdb-bronze/credits/", header=True, multiLine=True, escape='"')
movies_df = spark.read.csv("s3a://tmdb-bronze/movies/", header=True, multiLine=True, escape='"')

# Veri temizleme ve dönüştürme işlemleri
# 1. Cast tablosu oluşturma
from pyspark.sql.functions import explode, col, lit
import json

# Cast JSON verilerini parse etme
def parse_cast(cast_json):
    try:
        cast_data = json.loads(cast_json.replace("'", '"'))
        return cast_data
    except:
        return []

parse_cast_udf = F.udf(parse_cast, ArrayType(MapType(StringType(), StringType())))

# Cast verilerini ayıklama
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

# Null değerleri doldurma
cast_df = cast_df.fillna({"credit_id": "0000000000"})

# 2. Crew tablosu oluşturma
def parse_crew(crew_json):
    try:
        crew_data = json.loads(crew_json.replace("'", '"'))
        return crew_data
    except:
        return []

parse_crew_udf = F.udf(parse_crew, ArrayType(MapType(StringType(), StringType())))

# Crew verilerini ayıklama
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

# Null değerleri doldurma
crew_df = crew_df.fillna({"credit_id": "0000000000"})

# 3. Movies tablosu oluşturma
# JSON sütunlarını parse etme fonksiyonları
def extract_value(json_str, key):
    try:
        data = json.loads(json_str.replace("'", '"'))
        return data.get(key, None)
    except:
        return None

# Movies tablosunu oluşturma
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

# 4. Genres tablosu oluşturma
def parse_genres(genres_json):
    try:
        genres_data = json.loads(genres_json.replace("'", '"'))
        return genres_data
    except:
        return []

parse_genres_udf = F.udf(parse_genres, ArrayType(MapType(StringType(), StringType())))

# Genres verilerini ayıklama
movies_with_genres = movies_df.withColumn("genres_data", parse_genres_udf(col("genres")))
genres_df = movies_with_genres.select(
    col("id").alias("movie_id"),
    explode(col("genres_data")).alias("genre")
).select(
    col("movie_id"),
    col("genre.id").cast("integer").alias("id"),
    col("genre.name")
)

# Null değerleri doldurma
genres_df = genres_df.fillna({"id": -9999})

# 5. Keywords tablosu oluşturma
def parse_keywords(keywords_json):
    try:
        keywords_data = json.loads(keywords_json.replace("'", '"'))
        return keywords_data
    except:
        return []

parse_keywords_udf = F.udf(parse_keywords, ArrayType(MapType(StringType(), StringType())))

# Keywords verilerini ayıklama
movies_with_keywords = movies_df.withColumn("keywords_data", parse_keywords_udf(col("keywords")))
keywords_df = movies_with_keywords.select(
    col("id").alias("movie_id"),
    explode(col("keywords_data")).alias("keyword")
).select(
    col("movie_id"),
    col("keyword.id").cast("integer").alias("id"),
    col("keyword.name")
)

# Null değerleri doldurma
keywords_df = keywords_df.fillna({"id": -9999})

# 6. Production Companies tablosu oluşturma
def parse_production_companies(pc_json):
    try:
        pc_data = json.loads(pc_json.replace("'", '"'))
        return pc_data
    except:
        return []

parse_pc_udf = F.udf(parse_production_companies, ArrayType(MapType(StringType(), StringType())))

# Production Companies verilerini ayıklama
movies_with_pc = movies_df.withColumn("pc_data", parse_pc_udf(col("production_companies")))
production_companies_df = movies_with_pc.select(
    col("id").alias("movie_id"),
    explode(col("pc_data")).alias("company")
).select(
    col("movie_id"),
    col("company.id").cast("integer").alias("id"),
    col("company.name")
)

# Null değerleri doldurma
production_companies_df = production_companies_df.fillna({"id": -9999})

# 7. Production Countries tablosu oluşturma
def parse_production_countries(pc_json):
    try:
        pc_data = json.loads(pc_json.replace("'", '"'))
        return pc_data
    except:
        return []

parse_countries_udf = F.udf(parse_production_countries, ArrayType(MapType(StringType(), StringType())))

# Production Countries verilerini ayıklama
movies_with_countries = movies_df.withColumn("countries_data", parse_countries_udf(col("production_countries")))
production_countries_df = movies_with_countries.select(
    col("id").alias("movie_id"),
    explode(col("countries_data")).alias("country")
).select(
    col("movie_id"),
    col("country.iso_3166_1").alias("iso_3166_1"),
    col("country.name")
)

# Null değerleri doldurma
production_countries_df = production_countries_df.fillna({"iso_3166_1": "XX"})

# 8. Spoken Languages tablosu oluşturma
def parse_spoken_languages(sl_json):
    try:
        sl_data = json.loads(sl_json.replace("'", '"'))
        return sl_data
    except:
        return []

parse_languages_udf = F.udf(parse_spoken_languages, ArrayType(MapType(StringType(), StringType())))

# Spoken Languages verilerini ayıklama
movies_with_languages = movies_df.withColumn("languages_data", parse_languages_udf(col("spoken_languages")))
spoken_languages_df = movies_with_languages.select(
    col("id").alias("movie_id"),
    explode(col("languages_data")).alias("language")
).select(
    col("movie_id"),
    col("language.iso_639_1").alias("iso_639_1"),
    col("language.name")
)

# Null değerleri doldurma
spoken_languages_df = spoken_languages_df.fillna({"iso_639_1": "XX"})

# Delta Lake formatında kaydetme
# Cast tablosu
cast_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/cast")

# Crew tablosu
crew_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/crew")

# Movies tablosu
movies_clean_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/movies")

# Genres tablosu
genres_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/genres")

# Keywords tablosu
keywords_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/keywords")

# Production Companies tablosu
production_companies_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/production_companies")

# Production Countries tablosu
production_countries_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/production_countries")

# Spoken Languages tablosu
spoken_languages_df.write.format("delta").mode("overwrite").save("s3a://tmdb-silver/spoken_languages")

print("Veri dönüşümü tamamlandı ve Delta Lake formatında kaydedildi.")
    """
    
    # PySpark kodunu dosyaya yaz
    script_path = "/tmp/tmdb_transformation.py"
    with open(script_path, "w") as f:
        f.write(spark_script)
    
    # Dosya yolunu XCom ile paylaş
    context['ti'].xcom_push(key='spark_script_path', value=script_path)
    
    return script_path

# DAG tanımı
dag = DAG(
    'tmdb_delta_lake_pipeline',
    default_args=default_args,
    description='TMDB verilerini işleyip Delta Lake formatında depolayan pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 7),
    catchup=False,
    tags=['tmdb', 'delta_lake', 'spark'],
)

# Görevleri tanımla
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

task_generate_spark_script = PythonOperator(
    task_id='generate_spark_script',
    python_callable=generate_spark_transformation_script,
    provide_context=True,
    dag=dag,
)

# Spark dönüşümünü çalıştır
task_run_spark_transformation = BashOperator(
    task_id='run_spark_transformation',
    bash_command="""
        GIT_REPO_URL=<repo_url>
        SSH_PASSWORD=$(airflow variables get ssh_train_password)
        git clone $GIT_REPO_URL /tmp/tmdb_transformation_repo
        scp /tmp/tmdb_transformation_repo/tmdb_transformation.py ssh_train@spark_client:/tmp/tmdb_transformation.py
        sshpass -p "$SSH_PASSWORD" ssh -o StrictHostKeyChecking=no ssh_train@spark_client "cd /tmp && 
        spark-submit --master local[*] 
        --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000
        --conf spark.hadoop.fs.s3a.access.key=dataops
        --conf spark.hadoop.fs.s3a.secret.key=root12345
        --conf spark.hadoop.fs.s3a.path.style.access=true
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
        /tmp/tmdb_transformation.py"
    """,
    dag=dag,
)

# Görev bağımlılıklarını ayarla
task_download_datasets >> task_upload_to_minio >> task_generate_spark_script >> task_run_spark_transformation
