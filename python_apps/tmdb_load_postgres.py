import argparse
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F # Gerekirse veri dönüşümü için kullanılabilir
from pyspark.sql.types import * # Gerekirse şema tanımlama için kullanılabilir

# Hata ayıklama ve bilgilendirme için loglama ayarları
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    Ana fonksiyon: Argümanları okur, Spark Session başlatır, Delta tablolarını okur
    ve JDBC kullanarak PostgreSQL'e yazar.
    """
    # --- Argümanları Oku ---
    parser = argparse.ArgumentParser(description="Load TMDB Delta tables to PostgreSQL using Spark JDBC.")
    parser.add_argument("--jdbc-url", required=True, help="PostgreSQL JDBC URL (e.g., jdbc:postgresql://host:port/database)")
    parser.add_argument("--pg-user", required=True, help="PostgreSQL username")
    
    try:
        args = parser.parse_args()
        logger.info("Command line arguments parsed successfully.")
        logger.info(f"JDBC URL: {args.jdbc_url}")
        logger.info(f"PostgreSQL User: {args.pg_user}")
    except Exception as e:
        logger.error(f"Error parsing command line arguments: {e}")
        raise

    pg_password = os.environ.get('PG_PASSWORD')
    if not pg_password:
        logger.error("Environment variable PG_PASSWORD is not set.")
        # Uygulamanın şifre olmadan devam etmesini engellemek için hata veriyoruz.
        raise ValueError("PostgreSQL password not found in environment variable PG_PASSWORD")
    logger.info("PostgreSQL password retrieved from environment variable PG_PASSWORD.")
    # --------------------------------------------

    # --- Spark Session Başlatma ---
    # Konfigürasyonlar (paketler, S3, Delta) spark-submit komutu ile dışarıdan verilecek.
    try:
        spark = SparkSession.builder \
            .appName("TMDB Delta to PostgreSQL JDBC Loader") \
            .getOrCreate()
        logger.info("Spark Session created successfully.")
        # Gerekirse S3 endpoint'ini loglayarak kontrol edebilirsiniz:
        # s3_endpoint = spark.sparkContext.getConf().get("spark.hadoop.fs.s3a.endpoint", "Not Set")
        # logger.info(f"Spark configured with S3A endpoint: {s3_endpoint}")
    except Exception as e:
        logger.error(f"Failed to create Spark Session: {e}")
        raise
    # ------------------------------------

    # --- Kaynak ve Hedef Tabloları Tanımla ---
    delta_base_path = "s3a://tmdb-silver/" # Airflow'daki SILVER_BUCKET değişkenine karşılık gelmeli
    # Yüklenecek Delta tabloları ve karşılık gelen PostgreSQL tablo adları (schema dahil)
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
    # JDBC yazma seçenekleri (şifre hariç hepsi argümanlardan)
    jdbc_write_options = {
        "url": args.jdbc_url,
        "user": args.pg_user,
        "password": pg_password, # Ortam değişkeninden alınan şifre
        "driver": "org.postgresql.Driver" # PostgreSQL JDBC driver sınıfı
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

            # Gerekirse burada DataFrame üzerinde dönüşümler (transformations) yapılabilir.
            # Örnek: delta_df = delta_df.withColumn("load_timestamp", F.current_timestamp())

            # Veriyi PostgreSQL'e yaz
            logger.info(f"Writing {count} rows to PostgreSQL table: {pg_table_name}")
            (delta_df.write
             .format("jdbc")
             .options(**jdbc_write_options) # Ortak seçenekleri uygula
             .option("dbtable", pg_table_name) # Hedef tabloyu belirt
             .mode("overwrite") # Mod: overwrite (üzerine yaz), append (ekle)
             .save())
            logger.info(f"Successfully wrote data to {pg_table_name}.")
            processed_tables += 1

        except Exception as e:
            # Hata durumunda loglama yap ve başarısız tabloyu kaydet
            logger.error(f"FAILED to process table {delta_suffix}. Error: {e}", exc_info=True)
            failed_tables.append(delta_suffix)
            # Hata anında durmak yerine diğer tablolara devam etmek için 'raise' yorum satırı yapıldı.
            # Eğer ilk hatada durmasını istiyorsanız aşağıdaki satırı aktif edin:
            # raise

    logger.info("--- Processing Summary ---")
    logger.info(f"Successfully processed {processed_tables} out of {len(tables_to_process)} tables.")
    if failed_tables:
        logger.warning(f"Failed to process the following tables: {', '.join(failed_tables)}")
    else:
        logger.info("All tables processed successfully.")
    # -----------------------------------------------

    # --- Spark Session'ı Durdur ---
    try:
        spark.stop()
        logger.info("Spark Session stopped.")
    except Exception as e:
        # Spark durdurulurken hata olursa sadece uyar
        logger.warning(f"An error occurred while stopping Spark Session: {e}", exc_info=True)
    # --------------------------

    # Eğer herhangi bir tablo işlenirken hata olduysa betiğin hata koduyla çıkmasını sağla
    if failed_tables:
        exit(1)

# Betik doğrudan çalıştırıldığında main fonksiyonunu çağır
if __name__ == "__main__":
    main()