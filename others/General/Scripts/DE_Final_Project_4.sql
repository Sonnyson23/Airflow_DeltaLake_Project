SELECT 'movies' AS table_name, COUNT(*) AS row_count FROM public.movies
UNION ALL
SELECT 'cast' AS table_name, COUNT(*) AS row_count FROM public.cast
UNION ALL
SELECT 'crew' AS table_name, COUNT(*) AS row_count FROM public.crew
UNION ALL
SELECT 'genres' AS table_name, COUNT(*) AS row_count FROM public.genres
UNION ALL
SELECT 'keywords' AS table_name, COUNT(*) AS row_count FROM public.keywords
UNION ALL
SELECT 'production_companies' AS table_name, COUNT(*) AS row_count FROM public.production_companies
UNION ALL
SELECT 'production_countries' AS table_name, COUNT(*) AS row_count FROM public.production_countries
UNION ALL
SELECT 'spoken_languages' AS table_name, COUNT(*) AS row_count FROM public.spoken_languages
ORDER BY table_name;

SELECT
    m.title,
    m.revenue,
    c.name
FROM
    public.movies m
JOIN
    public.cast c ON m.movie_id = c.movie_id
WHERE
    c.name LIKE '%Tom Cruise%' -- Standart SQL büyük/küçük harfe duyarlıdır.
    -- Duyarsız arama için: c.name ILIKE '%Tom Cruise%'
ORDER BY
    m.revenue DESC NULLS LAST -- Hasılatı NULL olanları sona atar
LIMIT 5;


SELECT
    g.name AS genre,
    AVG(m.revenue) AS avg_revenue,
    COUNT(*) AS movie_count
FROM
    public.movies m
JOIN
    public.genres g ON m.movie_id = g.movie_id
WHERE
    m.revenue > 0
GROUP BY
    g.name
ORDER BY
    avg_revenue DESC NULLS LAST -- Ortalaması NULL olanları sona atar
LIMIT 10;



SELECT
    EXTRACT(YEAR FROM m.release_date) AS release_year, -- YEAR() yerine EXTRACT() kullanıldı
    AVG(m.revenue) AS avg_revenue,
    COUNT(*) AS movie_count
FROM
    public.movies m
WHERE
    m.revenue > 0 AND m.release_date IS NOT NULL
GROUP BY
    release_year -- PostgreSQL alias ile gruplamaya izin verir
ORDER BY
    release_year DESC
LIMIT 20;



SELECT
    c.name AS director,
    COUNT(DISTINCT c.movie_id) AS movie_count,
    COUNT(DISTINCT cr.name) AS unique_crew_members,
    COUNT(cr.name) AS total_crew_positions
FROM
    public.crew c -- Yönetmen satırları için 'c' aliası
JOIN
    public.crew cr ON c.movie_id = cr.movie_id -- Aynı filmdeki tüm ekip üyeleri için 'cr' aliası
WHERE
    c.job = 'Director'
GROUP BY
    c.name -- Yönetmenin adına göre grupla
HAVING
    COUNT(DISTINCT c.movie_id) > 1 -- Birden fazla film yönetenler
ORDER BY
    movie_count DESC
LIMIT 10;
