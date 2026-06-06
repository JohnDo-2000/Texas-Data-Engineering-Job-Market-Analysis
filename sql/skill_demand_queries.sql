SELECT skill, COUNT(*) as demand
FROM (
    SELECT 'Python'           as skill FROM "job_market_db"."jobs_csv" WHERE lower(description) LIKE '%python%'           OR lower(title) LIKE '%python%'
    UNION ALL
    SELECT 'SQL'              as skill FROM "job_market_db"."jobs_csv" WHERE lower(description) LIKE '%sql%'              OR lower(title) LIKE '%sql%'
    UNION ALL
    SELECT 'AWS'              as skill FROM "job_market_db"."jobs_csv" WHERE lower(description) LIKE '%aws%'              OR lower(title) LIKE '%aws%'
    UNION ALL
    SELECT 'Azure'            as skill FROM "job_market_db"."jobs_csv" WHERE lower(description) LIKE '%azure%'            OR lower(title) LIKE '%azure%'
    UNION ALL
    SELECT 'Spark'            as skill FROM "job_market_db"."jobs_csv" WHERE lower(description) LIKE '%spark%'            OR lower(title) LIKE '%spark%'
    UNION ALL
    SELECT 'Scala'            as skill FROM "job_market_db"."jobs_csv" WHERE lower(description) LIKE '% scala %' OR lower(description) LIKE '%apache scala%' OR lower(title) LIKE '% scala %' OR lower(title) LIKE '%apache scala%'
    UNION ALL
    SELECT 'Databricks'       as skill FROM "job_market_db"."jobs_csv" WHERE lower(description) LIKE '%databricks%'       OR lower(title) LIKE '%databricks%'
    UNION ALL
    SELECT 'Snowflake'        as skill FROM "job_market_db"."jobs_csv" WHERE lower(description) LIKE '%snowflake%'        OR lower(title) LIKE '%snowflake%'
    UNION ALL
    SELECT 'ML'               as skill FROM "job_market_db"."jobs_csv" WHERE lower(description) LIKE '%machine learning%' OR lower(title) LIKE '%machine learning%'
    UNION ALL
    SELECT 'AI'               as skill FROM "job_market_db"."jobs_csv" WHERE lower(description) LIKE '% ai %'             OR lower(title) LIKE '% ai %' OR lower(description) LIKE '%artificial intelligence%' OR lower(title) LIKE '%artificial intelligence%'
    UNION ALL
    SELECT 'ETL'              as skill FROM "job_market_db"."jobs_csv" WHERE lower(description) LIKE '%etl%'              OR lower(title) LIKE '%etl%'
    UNION ALL
    SELECT 'Git' as skill FROM "job_market_db"."jobs_csv" WHERE lower(description) LIKE '% git %' OR lower(description) LIKE '%github%' OR lower(description) LIKE '%gitlab%' OR lower(title) LIKE '% git %' OR lower(title) LIKE '%github%' OR lower(title) LIKE '%gitlab%'
) sub
GROUP BY skill
