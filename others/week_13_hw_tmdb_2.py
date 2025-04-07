from datetime import datetime, timedelta
import json
import os
import requests
import time
from typing import Dict, List, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# DAG definition and default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Function to fetch data from TMDB API
def fetch_tmdb_data(**context):
    """Fetches movies from TMDB API"""
    # Get API token securely
    api_key = Variable.get("tmdb_api_key", default_var=None)
    if not api_key:
        raise ValueError("TMDB API key not found. Please add 'tmdb_api_key' to Airflow Variables.")
    
    # Total number of movies to fetch
    total_movies = 500
    movies_per_page = 20  # Maximum movies per page supported by the API
    total_pages = (total_movies + movies_per_page - 1) // movies_per_page  # Round up
    
    all_movies = []
    
    # Fetch different pages
    for page in range(1, total_pages + 1):
        url = f"https://api.themoviedb.org/3/discover/movie"
        
        # Using api_key parameter instead of Bearer token in header
        params = {
            "api_key": api_key,
            "include_adult": "false",
            "include_video": "false",
            "language": "en-US",
            "page": page,
            "sort_by": "popularity.desc"
        }
        
        headers = {
            "accept": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Check for HTTP errors
            
            data = response.json()
            movies = data.get('results', [])
            all_movies.extend(movies)
            
            print(f"Page {page}/{total_pages} successfully fetched. Got {len(movies)} movies.")
            
            # Short wait to avoid hitting API rate limits
            if page < total_pages:
                time.sleep(0.5)
                
        except requests.exceptions.RequestException as e:
            print(f"API request error: {e}")
            raise
    
    print(f"Total {len(all_movies)} movies fetched.")
    
    # Pass results to next tasks via XCom
    context['ti'].xcom_push(key='all_movies', value=all_movies[:total_movies])  # Limit to requested number of movies
    
    return len(all_movies)

# Function to load data to PostgreSQL
def load_to_postgresql(**context):
    """Loads fetched movie data to PostgreSQL"""
    # Get movie data from XCom
    ti = context['ti']
    movies = ti.xcom_pull(task_ids='fetch_tmdb_data', key='all_movies')
    
    if not movies:
        raise ValueError("Could not retrieve movie data from XCom")
    
    # PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgresql_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS tmdb_movie_discover (
        adult BOOLEAN,
        backdrop_path TEXT,
        genre_ids INTEGER[],
        id BIGINT PRIMARY KEY,
        original_language TEXT,
        original_title TEXT,
        overview TEXT,
        popularity DOUBLE PRECISION,
        poster_path TEXT,
        release_date TEXT,
        title TEXT,
        video BOOLEAN,
        vote_average DOUBLE PRECISION,
        vote_count INTEGER
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    
    # Insert each movie into the database
    inserted_count = 0
    for movie in movies:
        try:
            insert_sql = """
            INSERT INTO tmdb_movie_discover (
                adult, backdrop_path, genre_ids, id, original_language, original_title,
                overview, popularity, poster_path, release_date, title, video,
                vote_average, vote_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """
            
            # Prepare data
            values = (
                movie.get('adult'),
                movie.get('backdrop_path'),
                movie.get('genre_ids'),
                movie.get('id'),
                movie.get('original_language'),
                movie.get('original_title'),
                movie.get('overview'),
                movie.get('popularity'),
                movie.get('poster_path'),
                movie.get('release_date'),
                movie.get('title'),
                movie.get('video'),
                movie.get('vote_average'),
                movie.get('vote_count')
            )
            
            cursor.execute(insert_sql, values)
            inserted_count += 1
            
            # Commit every 100 records
            if inserted_count % 100 == 0:
                conn.commit()
                print(f"{inserted_count} movies saved to database.")
        
        except Exception as e:
            print(f"Database error for movie ID {movie.get('id')}: {e}")
    
    # Commit final changes
    conn.commit()
    print(f"Total {inserted_count} movies successfully loaded into database.")
    
    # Check table contents
    cursor.execute("SELECT COUNT(*) FROM tmdb_movie_discover")
    total_rows = cursor.fetchone()[0]
    
    return total_rows

dag = DAG(
    'tmdb_movie_pipeline',
    default_args=default_args,
    description='DAG to fetch movie data from TMDB API and load to PostgreSQL',
    schedule_interval='@once',
    start_date=datetime(2025, 3, 22),
    catchup=False,
    tags=['tmdb', 'movie', 'api'],
)

# Define tasks
task_fetch_tmdb_data = PythonOperator(
    task_id='fetch_tmdb_data',
    python_callable=fetch_tmdb_data,
    provide_context=True,
    dag=dag,
)

task_load_to_postgresql = PythonOperator(
    task_id='load_to_postgresql',
    python_callable=load_to_postgresql,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task_fetch_tmdb_data >> task_load_to_postgresql