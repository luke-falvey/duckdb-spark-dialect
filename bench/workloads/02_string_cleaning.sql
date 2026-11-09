-- spark
SELECT lower(trim(country)) AS c, regexp_extract(url, '^https?://([^/]+)', 1) AS host, count(*) AS n,
       sum(length(regexp_replace(url, '[0-9]+', '#'))) AS len
FROM events WHERE url LIKE 'http%' GROUP BY lower(trim(country)), regexp_extract(url, '^https?://([^/]+)', 1) ORDER BY n DESC, c, host LIMIT 100;
-- native
SELECT lower(trim(country)) AS c, regexp_extract(url, '^https?://([^/]+)', 1) AS host, count(*) AS n,
       sum(length(regexp_replace(url, '[0-9]+', '#', 'g'))) AS len
FROM events WHERE url LIKE 'http%' GROUP BY lower(trim(country)), regexp_extract(url, '^https?://([^/]+)', 1) ORDER BY n DESC, c, host LIMIT 100;
