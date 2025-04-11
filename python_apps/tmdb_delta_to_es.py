# Yeni dosya adı önerisi: tmdb_delta_to_es.py

import argparse # Artık argümana gerek yok ama kalabilir veya kaldırılabilir
import logging
import os
from pyspark.sql import SparkSession
# from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    Ana fonksiyon: Spark Session başlatır, MinIO'daki Delta tablolarını okur
    ve Elasticsearch'e yazar.
    """
    # ES bağlantı testi
    import subprocess
    try:
        result = subprocess.run(
            ["curl", "-X", "GET", "http://es:9200"],
            capture_output=True, text=True, check=True
        )   
        logger.info(f"ES Connection Test Result: {result.stdout}")
    except Exception as e:
        logger.error(f"ES Connection Test Failed: {e}")

    # --- Spark Session Başlatma ---
    # Konfigürasyonlar (paketler, S3, Delta, ES) spark-submit komutu ile dışarıdan verilecek.
    try:
        spark = SparkSession.builder \
            .appName("TMDB Delta to Elasticsearch Loader") \
            .getOrCreate()
        logger.info("Spark Session created successfully.")
    except Exception as e:
        logger.error(f"Failed to create Spark Session: {e}")
        raise
    # ------------------------------------

    # --- Kaynak ve Hedef Tanımları ---
    delta_base_path = "s3a://tmdb-silver/"
    # Yüklenecek Delta tabloları ve karşılık gelen Elasticsearch index adları
    # Index adlarının küçük harf olması genellikle tercih edilir.
    tables_to_process = {
        "movies": "movies", # Delta tablo adı -> Elasticsearch index adı
        "cast": "cast",
        "crew": "crew",
        "genres": "genres",
        "keywords": "keywords",
        "production_companies": "production_companies",
        "production_countries": "production_countries",
        "spoken_languages": "spoken_languages"
    }
    # ------------------------------------------

    # --- Elasticsearch Yazma Seçenekleri ---
    # Bu ayarların çoğu spark-submit --conf ile verilecek ama bazıları burada tanımlanabilir
    # VEYA doğrudan .save() içinde belirtilebilir.
    es_write_options = {
        # "es.nodes": "es", # spark-submit --conf ile verilecek
        # "es.port": "9200", # spark-submit --conf ile verilecek
        # "es.nodes.wan.only": "false", # spark-submit --conf ile verilecek
        "es.mapping.id": "movie_id", # Hangi sütunun ES _id alanı olacağı (varsa)
                                     # Bu sadece 'movies' için uygun olabilir, diğerleri için kaldırılabilir.
        "es.index.auto.create": "true" # Index yoksa otomatik oluşturulsun
        "es.mapping.id": "movie_id",
        "es.index.auto.create": "true",
        "es.nodes.wan.only": "false",
        "es.nodes.discovery": "false"
    }
    # -----------------------------------

    logger.info("Starting process to read Delta tables and write to Elasticsearch...")
    processed_tables = 0
    failed_tables = []

    for delta_suffix, es_index_name in tables_to_process.items():
        delta_path = f"{delta_base_path}{delta_suffix}"
        es_resource = f"{es_index_name}/_doc" # ES 7+ için _doc tipi kullanılır
        logger.info(f"--- Processing table: {delta_suffix} ---")
        logger.info(f"Reading from Delta path: {delta_path}")

        try:
            delta_df = spark.read.format("delta").load(delta_path)
            count = delta_df.count()
            logger.info(f"Successfully read {count} rows from {delta_path}.")

            # Gerekirse ES için veri dönüşümü/temizliği burada yapılabilir
            # Özellikle veri tiplerinin ES mapping'leri ile uyumlu olması önemlidir.
            # Örneğin movie_id'nin string olması ES için sorun olmayabilir.
            # Tarih formatları genellikle ES tarafından otomatik algılanır.

            # Veriyi Elasticsearch'e yaz
            logger.info(f"Writing {count} rows to Elasticsearch index: {es_index_name}")
            current_es_options = es_write_options.copy()

            # Sadece movies index'i için _id olarak movie_id kullanalım (diğerlerinde movie_id unique olmayabilir)
            # Diğer indexler için ES'in otomatik ID oluşturmasına izin verelim.
            if delta_suffix != "movies":
                current_es_options.pop("es.mapping.id", None) # movie_id mapping'ini kaldır

            (delta_df.write
             .format("org.elasticsearch.spark.sql") # Elasticsearch Spark formatı
             .options(**current_es_options)         # Ortak ES ayarları
             .option("es.resource", es_resource)    # Hedef index/tip
             .mode("overwrite")                     # Index'in içeriğini değiştir (dikkatli kullanın!)
                                                    # Veya .mode("append")
             .save())
            logger.info(f"Successfully wrote data to Elasticsearch index: {es_index_name}")
            processed_tables += 1

        except Exception as e:
            logger.error(f"FAILED to process table {delta_suffix} for Elasticsearch. Error: {e}", exc_info=True)
            failed_tables.append(delta_suffix)
            # raise # İlk hatada durmak için

    # ... (Özet ve Spark Stop kısmı önceki gibi kalabilir) ...
    logger.info("--- Processing Summary ---")
    logger.info(f"Successfully processed {processed_tables} out of {len(tables_to_process)} tables for Elasticsearch.")
    if failed_tables:
        logger.warning(f"Failed to process the following tables for Elasticsearch: {', '.join(failed_tables)}")
    else:
        logger.info("All tables processed successfully for Elasticsearch.")

    try:
        spark.stop()
        logger.info("Spark Session stopped.")
    except Exception as e:
        logger.warning(f"An error occurred while stopping Spark Session: {e}", exc_info=True)

    if failed_tables:
        exit(1)

if __name__ == "__main__":
    main()