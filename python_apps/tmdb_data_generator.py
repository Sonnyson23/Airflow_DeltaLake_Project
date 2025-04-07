import os
import requests
import zipfile
import boto3
from botocore.client import Config

# TMDB veri setinin URL'i
TMDB_DATASET_URL = "https://github.com/erkansirin78/datasets/raw/master/tmdb_5000_movies_and_credits.zip"

# MinIO bağlantı bilgileri
MINIO_ENDPOINT = "http://minio:9000"
AWS_ACCESS_KEY = "dataops"
AWS_SECRET_KEY = "root12345"

# MinIO bucket isimleri
BRONZE_BUCKET = "tmdb-bronze"

def download_dataset():
    """TMDB veri setini indirir ve geçici bir dizine kaydeder"""
    print("Veri seti indiriliyor...")
    
    # Geçici dizin oluştur
    temp_dir = "/tmp/tmdb_data"
    os.makedirs(temp_dir, exist_ok=True)
    
    # Zip dosyasını indir
    zip_path = f"{temp_dir}/tmdb_data.zip"
    response = requests.get(TMDB_DATASET_URL)
    with open(zip_path, 'wb') as f:
        f.write(response.content)
    
    print(f"Veri seti indirildi: {zip_path}")
    
    # Zip dosyasını aç
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
    
    print(f"Zip dosyası açıldı: {temp_dir}")
    
    return {
        "credits_path": f"{temp_dir}/tmdb_5000_credits.csv",
        "movies_path": f"{temp_dir}/tmdb_5000_movies.csv"
    }

def upload_to_minio(file_paths):
    """İndirilen veri setlerini MinIO'ya yükler"""
    print("MinIO'ya veri yükleniyor...")
    
    # MinIO bağlantısı oluştur
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    # Bucket'ı oluştur (eğer yoksa)
    try:
        s3_client.head_bucket(Bucket=BRONZE_BUCKET)
        print(f"Bucket zaten mevcut: {BRONZE_BUCKET}")
    except:
        s3_client.create_bucket(Bucket=BRONZE_BUCKET)
        print(f"Bucket oluşturuldu: {BRONZE_BUCKET}")
    
    # Dosyaları yükle
    credits_path = file_paths["credits_path"]
    movies_path = file_paths["movies_path"]
    
    # Dosyaları parçalara ayırarak yükle (data generator simülasyonu)
    with open(credits_path, 'r', encoding='utf-8') as f:
        header = f.readline()
        lines = f.readlines()
    
    # Credits dosyasını parçalara ayır
    chunk_size = len(lines) // 5  # 5 parça
    for i in range(5):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < 4 else len(lines)
        
        chunk_file = f"/tmp/tmdb_data/credits_part_{i+1:03d}.csv"
        with open(chunk_file, 'w', encoding='utf-8') as f:
            f.write(header)
            f.writelines(lines[start:end])
        
        # MinIO'ya yükle
        s3_client.upload_file(chunk_file, BRONZE_BUCKET, f"credits/credits_part_{i+1:03d}.csv")
        print(f"Yüklendi: credits_part_{i+1:03d}.csv")
    
    # Movies dosyasını parçalara ayır
    with open(movies_path, 'r', encoding='utf-8') as f:
        header = f.readline()
        lines = f.readlines()
    
    chunk_size = len(lines) // 5  # 5 parça
    for i in range(5):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < 4 else len(lines)
        
        chunk_file = f"/tmp/tmdb_data/movies_part_{i+1:03d}.csv"
        with open(chunk_file, 'w', encoding='utf-8') as f:
            f.write(header)
            f.writelines(lines[start:end])
        
        # MinIO'ya yükle
        s3_client.upload_file(chunk_file, BRONZE_BUCKET, f"movies/movies_part_{i+1:03d}.csv")
        print(f"Yüklendi: movies_part_{i+1:03d}.csv")
    
    print("Veri yükleme tamamlandı!")
    return True

if __name__ == "__main__":
    # Veri setini indir
    file_paths = download_dataset()
    
    # MinIO'ya yükle
    upload_to_minio(file_paths)
