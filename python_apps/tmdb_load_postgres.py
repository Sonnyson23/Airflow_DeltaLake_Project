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
minio_endpoint = spark.conf.get("spark.hadoop.fs.s3a.endpoint")
aws_access_key = spark.conf.get("spark.hadoop.fs.s3a.access.key")
aws_secret_key = spark.conf.get("spark.hadoop.fs.s3a.secret.key")

# S3 configuration for Delta Lake
spark = SparkSession.builder \
    .appName("TMDB Delta to PostgreSQL") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", spark.conf.get("spark.hadoop.fs.s3a.endpoint")) \
    .config("spark.hadoop.fs.s3a.access.key", spark.conf.get("spark.hadoop.fs.s3a.access.key")) \
    .config("spark.hadoop.fs.s3a.secret.key", spark.conf.get("spark.hadoop.fs.s3a.secret.key")) \
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
execute_sql("""
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
""")

execute_sql("""
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
""")

execute_sql("""
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
""")

execute_sql("""
CREATE TABLE IF NOT EXISTS genres (
    movie_id INT,
    id INT,
    name VARCHAR(100),
    PRIMARY KEY (movie_id, id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
""")

execute_sql("""
CREATE TABLE IF NOT EXISTS keywords (
    movie_id INT,
    id INT,
    name VARCHAR(100),
    PRIMARY KEY (movie_id, id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
""")

execute_sql("""
CREATE TABLE IF NOT EXISTS production_companies (
    movie_id INT,
    id INT,
    name VARCHAR(255),
    PRIMARY KEY (movie_id, id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
""")

execute_sql("""
CREATE TABLE IF NOT EXISTS production_countries (
    movie_id INT,
    iso_3166_1 VARCHAR(10),
    name VARCHAR(255),
    PRIMARY KEY (movie_id, iso_3166_1),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
""")

execute_sql("""
CREATE TABLE IF NOT EXISTS spoken_languages (
    movie_id INT,
    iso_639_1 VARCHAR(10),
    name VARCHAR(100),
    PRIMARY KEY (movie_id, iso_639_1),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
""")


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