# TMDB Veri İşleme Pipeline Dokümantasyonu

## Proje Özeti

Bu proje, TMDB (The Movie Database) film verilerini işleyerek analiz edilebilir hale getiren bir veri işleme pipeline'ı oluşturmayı amaçlamaktadır. Proje, aşağıdaki teknolojileri kullanmaktadır:

- **Apache Airflow**: İş akışı orkestrasyon aracı
- **Apache Spark**: Büyük veri işleme motoru
- **Delta Lake**: Veri depolama formatı
- **MinIO**: Nesne depolama servisi (S3 uyumlu)

Veri akışı şu şekildedir:
1. TMDB veri setleri indirilir ve MinIO'daki Bronze Layer'a yüklenir
2. Spark ile veriler işlenir ve dönüştürülür
3. İşlenen veriler Delta Lake formatında Silver Layer'a kaydedilir

## Mimari

Proje mimarisi üç ana katmandan oluşmaktadır:

1. **Bronze Layer**: Ham verilerin depolandığı katman (MinIO)
2. **Silver Layer**: İşlenmiş ve dönüştürülmüş verilerin depolandığı katman (Delta Lake)
3. **Orchestration Layer**: Tüm iş akışını yöneten katman (Airflow)

![Mimari Diyagramı](architecture.png)

## Kurulum

### Gereksinimler

- Docker ve Docker Compose
- Python 3.x
- Apache Airflow
- Apache Spark
- Delta Lake
- MinIO

### Ortam Kurulumu

1. Gerekli dizinleri oluşturun:
```bash
mkdir -p airflow/dags airflow/logs airflow/config airflow/plugins data
```

2. `.env` dosyasını oluşturun:
```
AIRFLOW_UID=50000
AIRFLOW_GID=0

# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CORE__FERNET_KEY=
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# MinIO
MINIO_ROOT_USER=root
MINIO_ROOT_PASSWORD=root12345
MINIO_ENDPOINT=http://minio:9000
```

3. Docker Compose ile servisleri başlatın:
```bash
docker-compose up -d
```

## Veri İşleme Pipeline'ı

### 1. Veri Çekme

TMDB veri setleri şu URL'den indirilir:
```
https://github.com/erkansirin78/datasets/raw/master/tmdb_5000_movies_and_credits.zip
```

İndirilen veri setleri şunlardır:
- `tmdb_5000_credits.csv`: Film kredileri (oyuncular ve ekip)
- `tmdb_5000_movies.csv`: Film bilgileri

Bu veriler, MinIO'daki Bronze Layer'a yüklenir:
- `tmdb-bronze/credits/`: Kredi verileri
- `tmdb-bronze/movies/`: Film verileri

### 2. Veri Dönüşümü

Spark ile aşağıdaki tablolar oluşturulur:

1. **Cast**: Oyuncu bilgileri
   - movie_id, title, cast_id, character, credit_id, gender, id, name

2. **Crew**: Ekip bilgileri
   - movie_id, title, credit_id, department, gender, id, job, name

3. **Movies**: Film bilgileri
   - movie_id, title, budget, homepage, original_language, original_title, overview, popularity, release_date, revenue, runtime, status, tagline, vote_average, vote_count

4. **Genres**: Film türleri
   - movie_id, id, name

5. **Keywords**: Film anahtar kelimeleri
   - movie_id, id, name

6. **Production Companies**: Yapım şirketleri
   - movie_id, id, name

7. **Production Countries**: Yapım ülkeleri
   - movie_id, iso_3166_1, name

8. **Spoken Languages**: Konuşulan diller
   - movie_id, iso_639_1, name

### 3. Delta Lake Entegrasyonu

İşlenen veriler Delta Lake formatında Silver Layer'a kaydedilir:
- `tmdb-silver/cast/`
- `tmdb-silver/crew/`
- `tmdb-silver/movies/`
- `tmdb-silver/genres/`
- `tmdb-silver/keywords/`
- `tmdb-silver/production_companies/`
- `tmdb-silver/production_countries/`
- `tmdb-silver/spoken_languages/`

Delta Lake, aşağıdaki avantajları sağlar:
- ACID işlemler
- Şema uygulama
- Zaman seyahati (Time travel)
- Upsert işlemleri

## Airflow DAG

Airflow DAG'ı, tüm veri işleme pipeline'ını orkestre eder. DAG, aşağıdaki görevleri içerir:

1. **download_datasets**: TMDB veri setlerini indirir
2. **upload_to_minio**: İndirilen veri setlerini MinIO'ya yükler
3. **generate_spark_script**: Spark dönüşüm scriptini oluşturur
4. **run_spark_transformation**: Spark dönüşümünü çalıştırır

DAG, günlük olarak çalışacak şekilde ayarlanmıştır.

## Test ve Doğrulama

Pipeline'ı test etmek için aşağıdaki script kullanılabilir:
```bash
./data/test_pipeline.sh
```

Bu script:
1. Veri indirme ve MinIO'ya yükleme işlemini gerçekleştirir
2. Spark transformasyonlarını çalıştırır
3. Delta Lake tablolarını doğrular

## Örnek Sorgular

Delta Lake tablolarını kullanarak aşağıdaki örnek sorguları çalıştırabilirsiniz:

1. **Tom Cruise'un en yüksek hasılatlı filmi**:
```sql
SELECT m.title, m.revenue, c.name
FROM delta.`s3a://tmdb-silver/movies` m
JOIN delta.`s3a://tmdb-silver/cast` c ON m.movie_id = c.movie_id
WHERE c.name LIKE '%Tom Cruise%'
ORDER BY m.revenue DESC
LIMIT 5
```

2. **Film türleri ve gişe hasılatı arasındaki ilişki**:
```sql
SELECT g.name as genre, AVG(m.revenue) as avg_revenue, COUNT(*) as movie_count
FROM delta.`s3a://tmdb-silver/movies` m
JOIN delta.`s3a://tmdb-silver/genres` g ON m.movie_id = g.movie_id
WHERE m.revenue > 0
GROUP BY g.name
ORDER BY avg_revenue DESC
LIMIT 10
```

3. **Yayın tarihleri ve hasılat arasındaki ilişki**:
```sql
SELECT YEAR(m.release_date) as year, AVG(m.revenue) as avg_revenue, COUNT(*) as movie_count
FROM delta.`s3a://tmdb-silver/movies` m
WHERE m.revenue > 0 AND m.release_date IS NOT NULL
GROUP BY YEAR(m.release_date)
ORDER BY year DESC
LIMIT 20
```

4. **Yönetmenlerin aynı ekiple çalışıp çalışmadığı**:
```sql
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
```

## Sonuç

Bu proje, TMDB film verilerini işleyerek analiz edilebilir hale getiren bir veri işleme pipeline'ı oluşturmuştur. Airflow, Spark ve Delta Lake kullanılarak oluşturulan bu pipeline, veri analistlerinin film verileri üzerinde gelişmiş analizler yapmasını sağlar.

Proje, veri mühendisliği en iyi uygulamalarını takip ederek, veri işleme sürecini otomatikleştirmiş ve veri kalitesini artırmıştır.
