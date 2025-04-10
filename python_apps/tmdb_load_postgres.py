# tmdb_load_postgres.py dosyasının GÜNCEL HALİ

import argparse
import logging
import os # Ortam değişkeni artık kullanılmayacak ama import kalabilir
from pyspark.sql import SparkSession
# from pyspark.sql import functions as F # Gerekirse aktif edin
# from pyspark.sql.types import * # Gerekirse aktif edin

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    Ana fonksiyon: Argümanları okur, Spark Session başlatır, Delta tablolarını okur
    ve JDBC kullanarak PostgreSQL'e yazar.
    """
    # --- Argümanları Oku ---
    parser = argparse.ArgumentParser(description="Load TMDB Delta tables to PostgreSQL using Spark JDBC.")
    parser.add_argument("--jdbc-url", required=True, help="PostgreSQL JDBC URL")
    parser.add_argument("--pg-user", required=True, help="PostgreSQL username")
    parser.add_argument("--pg-password", required=True, help="PostgreSQL password") # <-- ŞİFRE ARGÜMANI EKLENDİ
    # --pg-table argümanı kaldırıldı, tablo eşlemesi içeride yapılıyor.
    try:
        args = parser.parse_args()
        logger.info("Command line arguments parsed successfully.")
        logger.info(f"JDBC URL: {args.jdbc_url}")
        logger.info(f"PostgreSQL User: {args.pg_user}")
        # Şifreyi loglamayın! logger.info(f"PostgreSQL Password: {'*' * len(args.pg_password)}")
    except Exception as e:
        logger.error(f"Error parsing command line arguments: {e}")
        raise
    # --------------------------

    # --- Ortam Değişkeninden Şifre Alma Kısmı Kaldırıldı ---
    # pg_password = os.environ.get('PG_PASSWORD')
    # if not pg_password:
    #     logger.error("Environment variable PG_PASSWORD is not set.")
    #     raise ValueError("PostgreSQL password not found in environment variable PG_PASSWORD")
    # logger.info("PostgreSQL password retrieved from environment variable PG_PASSWORD.")
    # --------------------------------------------

    # --- Spark Session Başlatma ---
    try:
        spark = SparkSession.builder \
            .appName("TMDB Delta to PostgreSQL JDBC Loader") \
            .getOrCreate()
        logger.info("Spark Session created successfully.")
    except Exception as e:
        logger.error(f"Failed to create Spark Session: {e}")
        raise
    # ------------------------------------

    # --- Kaynak ve Hedef Tabloları Tanımla ---
    delta_base_path = "s3a://tmdb-silver/"
    tables_to_process = {
        "movies": "public.movies",
        "cast": "public.cast",
        "crew": "public.crew",
        "genres": "public.genres",
        "keywords": "public.keywords",
        "production_companies": "public.production_companies",
        "production_countries": "public.production_countries",
        "spoken_languages": "public.spoken_languages"
    }
    # ------------------------------------------

    # --- Delta Tablolarını Oku ve PostgreSQL'e Yaz ---
    jdbc_write_options = {
        "url": args.jdbc_url,
        "user": args.pg_user,
        "password": args.pg_password, # <-- Ortam değişkeni yerine argüman kullanılıyor
        "driver": "org.postgresql.Driver"
    }

    logger.info("Starting process to read Delta tables and write to PostgreSQL...")
    processed_tables = 0
    failed_tables = []

    for delta_suffix, pg_table_name in tables_to_process.items():
        delta_path = f"{delta_base_path}{delta_suffix}"
        logger.info(f"--- Processing table: {delta_suffix} ---")
        logger.info(f"Reading from Delta path: {delta_path}")
        try:
            # Delta tablosunu oku
            delta_df = spark.read.format("delta").load(delta_path)
            count = delta_df.count()
            logger.info(f"Successfully read {count} rows from {delta_path}.")

            # --- >>> VERİYİ İNCELEME KISMINI BURAYA EKLEYİN (Sadece 'movies' için) <<< ---
            if delta_suffix == "movies":
                logger.info("--- Inspecting movies_df Schema ---")
                delta_df.printSchema()
                logger.info("--- Inspecting movies_df Data (Sample) ---")
                # İlk 10 satırı gösterelim, sütunları kesmeden (truncate=False)
                delta_df.show(10, False)
                logger.info("--- Finished Inspecting movies_df ---")
            # --- >>> İNCELEME KISMI SONU <<< ---

            # Gerekirse burada DataFrame üzerinde dönüşümler (transformations) yapılabilir.
            # Örnek: delta_df = delta_df.withColumn("load_timestamp", F.current_timestamp())



            logger.info(f"Writing {count} rows to PostgreSQL table: {pg_table_name}")
            (delta_df.write
             .format("jdbc")
             .options(**jdbc_write_options)
             .option("dbtable", pg_table_name)
             .mode("overwrite") # veya "append"
             .save())
            logger.info(f"Successfully wrote data to {pg_table_name}.")
            processed_tables += 1
        except Exception as e:
            logger.error(f"FAILED to process table {delta_suffix}. Error: {e}", exc_info=True)
            failed_tables.append(delta_suffix)
            # raise # İlk hatada durmak için yorumu kaldırın
    # ... (Summary ve Spark Stop kısmı aynı kalabilir) ...

    logger.info("--- Processing Summary ---")
    logger.info(f"Successfully processed {processed_tables} out of {len(tables_to_process)} tables.")
    if failed_tables:
        logger.warning(f"Failed to process the following tables: {', '.join(failed_tables)}")
    else:
        logger.info("All tables processed successfully.")

    try:
        spark.stop()
        logger.info("Spark Session stopped.")
    except Exception as e:
        logger.warning(f"An error occurred while stopping Spark Session: {e}", exc_info=True)

    if failed_tables:
        exit(1)

if __name__ == "__main__":
    main()