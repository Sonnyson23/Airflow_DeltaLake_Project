# VBO Data Engineering Bootcamp Final Project-4: Airflow/Delta Lake

- Use this dataset: https://github.com/erkansirin78/datasets/raw/master/tmdb_5000_movies_and_credits.zip

There are two different datasets in this zip file.
- tmdb_5000_credits.csv
- tmdb_5000_movies.csv

These datasets come into object storage at regular intervals in a batch manner. Data analysts need some advanced analytics on this data.
Examples:
- Which is the highest-grossing movie starring Tom Cruise?
- What is the relationship between movie genres and box office revenue?
- What is the relationship between release dates and revenue?
- Does a director always work with the same crew?
- As a crew, which movie sets and roles Ahmet has been involved in throughout his career?

This or similar analytics are not possible in the raw form of the data. By processing this raw data you are expected to make it easily queryable with SQL language. In this direction, the definitions and requirements related to the task you will perform are shared below.

# Architecture
![](images/architecture.png)

-----

# 1. Data Ingestion
- At this stage, the datasets should be generated into the `tmdb-bronze` bucket representing the bronze layer (with data-generator).
- You can use MinIO as object storage with docker.
- Generate data-generator datasets into `tmdb-bronze`. Example commands are below.

## Start generating credits data
```commandline
python dataframe_to_s3.py -buc tmdb-bronze \
-k credits/credits_part \
-aki root -sac root12345 \
-eu http://localhost:9000 \
-i /home/train/datasets/tmdb_5000_credits.csv \
-ofp True -z 500 -b 0.1
```

## Start generating movies data
```commandline
python dataframe_to_s3.py -buc tmdb-bronze \
-k   movies/movies_part \
-aki root -sac root12345 \
-eu http://localhost:9000 \
-i /home/train/datasets/tmdb_5000_movies.csv \
-ofp True -z 500 -b 0.1
```

# 2. Data transformation 
At this stage, the raw data in the bronze layer is converted to meet the following requirements and written into the silver layer `tmdb-silver/<table_name>` bucket in the form of delta tables specified below.

## 2.1. cast
```commandline
+--------+------+-------+-------------------+------------------------+------+-----+----------------+
|movie_id|title |cast_id|character          |credit_id               |gender|id   |name            |
+--------+------+-------+-------------------+------------------------+------+-----+----------------+
|19995   |Avatar|242    |Jake Sully         |5602a8a7c3a3685532001c9a|2     |65731|Sam Worthington |
|19995   |Avatar|3      |Neytiri            |52fe48009251416c750ac9cb|1     |8691 |Zoe Saldana     |
|19995   |Avatar|25     |Dr. Grace Augustine|52fe48009251416c750aca39|1     |10205|Sigourney Weaver|
|19995   |Avatar|4      |Col. Quaritch      |52fe48009251416c750ac9cf|2     |32747|Stephen Lang    |
+--------+------+-------+-------------------+------------------------+------+-----+----------------+
```
- One row represents one player. 
- credit_id nulls must be imputed with "0000000000".

## 2.2. crew
```commandline
+--------+------+------------------------+----------+------+----+------------------------+-----------------+
|movie_id|title |credit_id               |department|gender|id  |job                     |name             |
+--------+------+------------------------+----------+------+----+------------------------+-----------------+
|19995   |Avatar|52fe48009251416c750aca23|Editing   |0     |1721|Editor                  |Stephen E. Rivkin|
|19995   |Avatar|539c47ecc3a36810e3001f87|Art       |2     |496 |Production Design       |Rick Carter      |
|19995   |Avatar|54491c89c3a3680fb4001cf7|Sound     |0     |900 |Sound Designer          |Christopher Boyes|
|19995   |Avatar|54491cb70e0a267480001bd0|Sound     |0     |900 |Supervising Sound Editor|Christopher Boyes|
+--------+------+------------------------+----------+------+----+------------------------+-----------------+
```
- One row represents one crew.
- credit_id nulls must be imputed with "0000000000".


## 2.3. movies
```commandline 
+--------+--------------------+------+--------------------+-----------------+--------------------+--------------------+----------+------------+-------------+-------+--------+--------------------+------------+----------+
|movie_id|               title|budget|            homepage|original_language|      original_title|            overview|popularity|release_date|      revenue|runtime|  status|             tagline|vote_average|vote_count|
+--------+--------------------+------+--------------------+-----------------+--------------------+--------------------+----------+------------+-------------+-------+--------+--------------------+------------+----------+
|   19995|              Avatar|2.37E8|http://www.avatar...|               en|              Avatar|In the 22nd centu...| 150.43758|  2009-12-10|2.787965087E9|    162|Released|Enter the World o...|         7.2|     11800|
|     285|Pirates of the Ca...| 3.0E8|http://disney.go....|               en|Pirates of the Ca...|Captain Barbossa,...| 139.08261|  2007-05-19|       9.61E8|    169|Released|At the end of the...|         6.9|      4500|
|  206647|             Spectre|2.45E8|http://www.sonypi...|               en|             Spectre|A cryptic message...|107.376785|  2015-10-26| 8.80674609E8|    148|Released|A Plan No One Esc...|         6.3|      4466|
|   49026|The Dark Knight R...| 2.5E8|http://www.thedar...|               en|The Dark Knight R...|Following the dea...| 112.31295|  2012-07-16|1.084939099E9|    165|Released|     The Legend Ends|         7.6|      9106|
|   49529|         John Carter| 2.6E8|http://movies.dis...|               en|         John Carter|John Carter is a ...| 43.926994|  2012-03-07|   2.841391E8|    132|Released|Lost in our world...|         6.1|      2124|
+--------+--------------------+------+--------------------+-----------------+--------------------+--------------------+----------+------------+-------------+-------+--------+--------------------+------------+----------+
```
- Schema:
```commandline
root
 |-- movie_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- budget: double (nullable = true)
 |-- homepage: string (nullable = true)
 |-- original_language: string (nullable = true)
 |-- original_title: string (nullable = true)
 |-- overview: string (nullable = true)
 |-- popularity: float (nullable = true)
 |-- release_date: date (nullable = true)
 |-- revenue: double (nullable = true)
 |-- runtime: integer (nullable = true)
 |-- status: string (nullable = true)
 |-- tagline: string (nullable = true)
 |-- vote_average: float (nullable = true)
 |-- vote_count: integer (nullable = true)
```

## 2.4. genres
```commandline
+--------+---+---------------+
|movie_id| id|           name|
+--------+---+---------------+
|   19995| 28|         Action|
|   19995| 12|      Adventure|
|   19995| 14|        Fantasy|
|   19995|878|Science Fiction|
|     285| 12|      Adventure|
+--------+---+---------------+
```
- Schema
```commandline
root
 |-- movie_id: string (nullable = true)
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
```
- id nulls must be imputed with -9999.

## 2.5. keywords
```commandline
+--------+----+-------------+
|movie_id|  id|         name|
+--------+----+-------------+
|   19995|1463|culture clash|
|   19995|2964|       future|
|   19995|3386|    space war|
|   19995|3388| space colony|
|   19995|3679|      society|
+--------+----+-------------+
```
- Schema
```commandline
root
 |-- movie_id: string (nullable = true)
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
```
- id nulls must be imputed with -9999.

## 2.6. production_companies
```commandline
+--------+---+--------------------+
|movie_id| id|                name|
+--------+---+--------------------+
|   19995|289|Ingenious Film Pa...|
|   19995|306|Twentieth Century...|
|   19995|444|  Dune Entertainment|
|   19995|574|Lightstorm Entert...|
|     285|  2|Walt Disney Pictures|
+--------+---+--------------------+
```
- Schema
```commandline
root
 |-- movie_id: string (nullable = true)
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
```
- id nulls must be imputed with -9999.

## 2.7. production_countries
```commandline
+--------+----------+--------------------+
|movie_id|iso_3166_1|                name|
+--------+----------+--------------------+
|   19995|        US|United States of ...|
|   19995|        GB|      United Kingdom|
|     285|        US|United States of ...|
|  206647|        GB|      United Kingdom|
|  206647|        US|United States of ...|
+--------+----------+--------------------+
```
- Schema
```commandline
root
 |-- movie_id: string (nullable = true)
 |-- iso_3166_1: string (nullable = true)
 |-- name: string (nullable = true)
```
- iso_3166_1 nulls must be imputed with XX.

## 2.8. spoken_languages
```commandline
+--------+---------+--------+
|movie_id|iso_639_1|    name|
+--------+---------+--------+
|   19995|       en| English|
|   19995|       es| Español|
|     285|       en| English|
|  206647|       fr|Français|
|  206647|       en| English|
+--------+---------+--------+
```
- Schema
```commandline
root
 |-- movie_id: string (nullable = true)
 |-- iso_639_1: string (nullable = true)
 |-- name: string (nullable = true)
```
- iso_639_1 nulls must be imputed with XX.

# 3. Pipeline
- Create a pipeline with Airflow to meet the above requirements.
- Pipeline should run daily.

### Data source
https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata?select=tmdb_5000_movies.csv

