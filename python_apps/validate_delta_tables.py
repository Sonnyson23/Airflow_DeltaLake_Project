from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json

# Spark oturumu oluştur
spark = (SparkSession.builder
         .appName("TMDB Data Transformation Test")
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

# Delta Lake tabloları doğrulama
def validate_delta_tables():
    """Delta Lake tablolarını doğrular ve örnek veriler gösterir"""
    tables = [
        "cast", "crew", "movies", "genres", "keywords", 
        "production_companies", "production_countries", "spoken_languages"
    ]
    
    results = {}
    
    for table in tables:
        try:
            # Delta Lake tablosunu oku
            df = spark.read.format("delta").load(f"s3a://tmdb-silver/{table}")
            
            # Tablo şemasını al
            schema = df.schema
            
            # Satır sayısını al
            count = df.count()
            
            # Örnek veri al (ilk 5 satır)
            sample = df.limit(5).toPandas().to_dict('records')
            
            results[table] = {
                "status": "success",
                "count": count,
                "schema": str(schema),
                "sample": sample
            }
            
            print(f"Tablo {table} doğrulandı: {count} satır")
            
        except Exception as e:
            results[table] = {
                "status": "error",
                "error": str(e)
            }
            print(f"Tablo {table} doğrulanamadı: {str(e)}")
    
    # Sonuçları JSON olarak kaydet
    with open("/tmp/delta_validation_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    return results

# Örnek sorgu çalıştırma
def run_sample_queries():
    """Örnek sorgular çalıştırır ve sonuçları gösterir"""
    queries = {
        "tom_cruise_highest_grossing": """
            SELECT m.title, m.revenue, c.name
            FROM delta.`s3a://tmdb-silver/movies` m
            JOIN delta.`s3a://tmdb-silver/cast` c ON m.movie_id = c.movie_id
            WHERE c.name LIKE '%Tom Cruise%'
            ORDER BY m.revenue DESC
            LIMIT 5
        """,
        
        "genre_revenue_relation": """
            SELECT g.name as genre, AVG(m.revenue) as avg_revenue, COUNT(*) as movie_count
            FROM delta.`s3a://tmdb-silver/movies` m
            JOIN delta.`s3a://tmdb-silver/genres` g ON m.movie_id = g.movie_id
            WHERE m.revenue > 0
            GROUP BY g.name
            ORDER BY avg_revenue DESC
            LIMIT 10
        """,
        
        "release_date_revenue": """
            SELECT YEAR(m.release_date) as year, AVG(m.revenue) as avg_revenue, COUNT(*) as movie_count
            FROM delta.`s3a://tmdb-silver/movies` m
            WHERE m.revenue > 0 AND m.release_date IS NOT NULL
            GROUP BY YEAR(m.release_date)
            ORDER BY year DESC
            LIMIT 20
        """,
        
        "director_crew_consistency": """
            SELECT c.name as director, COUNT(DISTINCT c.movie_id) as movie_count, 
                   COUNT(DISTINCT cr.name) as unique_crew_members,
                   COUNT(cr.name) as total_crew_positions
            FROM delta.`s3a://tmdb-silver/crew` c
            JOIN delta.`s3a://tmdb-silver/crew` cr ON c.movie_id = cr.movie_id
            WHERE c.job = 'Director'
            GROUP BY c.name
            HAVING COUNT(DISTINCT c.movie_id) > 1
            ORDER BY movie_count DESC
            LIMIT 10
        """
    }
    
    results = {}
    
    for name, query in queries.items():
        try:
            # Sorguyu çalıştır
            df = spark.sql(query)
            
            # Sonuçları al
            result = df.limit(20).toPandas().to_dict('records')
            
            results[name] = {
                "status": "success",
                "result": result
            }
            
            print(f"Sorgu {name} başarıyla çalıştırıldı")
            
        except Exception as e:
            results[name] = {
                "status": "error",
                "error": str(e)
            }
            print(f"Sorgu {name} çalıştırılamadı: {str(e)}")
    
    # Sonuçları JSON olarak kaydet
    with open("/tmp/query_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    return results

if __name__ == "__main__":
    print("Delta Lake tablolarını doğrulama...")
    validate_delta_tables()
    
    print("\nÖrnek sorguları çalıştırma...")
    run_sample_queries()
    
    print("\nDoğrulama tamamlandı!")
