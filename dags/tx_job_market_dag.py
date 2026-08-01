from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# ---- Config: your real AWS resource names ----
AWS_CONN_ID = "aws_default"
LAMBDA_FUNCTION_NAME = "job-market-extractor"
GLUE_CRAWLER_NAME = "job-market-crawler"
ATHENA_DATABASE = "job_market_db"
ATHENA_OUTPUT_LOCATION = "s3://job-market-tracker-2026-john-do-631823087532-us-east-2-an/athena-results/"

# Full skill-demand query from sql/skill_demand_queries.sql
SKILL_DEMAND_QUERY = """
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
ORDER BY demand DESC
"""

default_args = {
    "owner": "triet",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="tx_job_market_pipeline",
    description="Texas DE Job Market: Lambda extract -> Glue catalog -> Athena skill query",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 7, 31),
    catchup=False,
    tags=["portfolio", "job-market"],
) as dag:

    extract_jobs = LambdaInvokeFunctionOperator(
        task_id="extract_adzuna_jobs",
        function_name=LAMBDA_FUNCTION_NAME,
        aws_conn_id=AWS_CONN_ID,
    )

    run_crawler = GlueCrawlerOperator(
        task_id="run_glue_crawler",
        config={"Name": GLUE_CRAWLER_NAME},
        aws_conn_id=AWS_CONN_ID,
    )

    wait_for_crawler = GlueCrawlerSensor(
        task_id="wait_for_crawler",
        crawler_name=GLUE_CRAWLER_NAME,
        aws_conn_id=AWS_CONN_ID,
    )

    run_skill_query = AthenaOperator(
        task_id="skill_demand_query",
        query=SKILL_DEMAND_QUERY,
        database=ATHENA_DATABASE,
        output_location=ATHENA_OUTPUT_LOCATION,
        aws_conn_id=AWS_CONN_ID,
    )

    extract_jobs >> run_crawler >> wait_for_crawler >> run_skill_query
