# Texas Data Engineering Job Market Analysis
### Automated ELT Pipeline + Tableau Dashboard

An end-to-end automated data pipeline that extracts daily job postings from the Adzuna API, stores them in AWS S3, catalogs them with AWS Glue, queries them with AWS Athena, and visualizes insights in Tableau.

---

## Architecture

```
EventBridge (daily 8am UTC)
        ↓
AWS Lambda (Python 3.11)
        ↓
Adzuna Jobs API → 3,000+ job postings
        ↓
Amazon S3 (partitioned by run_date)
        ↓
AWS Glue Crawler (auto schema detection)
        ↓
AWS Athena (SQL queries)
        ↓
Tableau Dashboard (live visualizations)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Extraction | Python, Adzuna REST API |
| Orchestration | AWS EventBridge (daily cron) |
| Compute | AWS Lambda (Python 3.11) |
| Storage | Amazon S3 (data lake) |
| Cataloging | AWS Glue Crawler |
| Querying | AWS Athena (SQL) |
| Visualization | Tableau Desktop |
| Monitoring | AWS CloudWatch |

---

## Key Findings

From **3,032 job postings** across Texas (Dallas, Houston, Austin, San Antonio, Fort Worth):

- **AI is the #1 in-demand skill** — appearing in 38% of all postings, 2.5× more than the next skill
- **Average data engineering salary: $124,383**
- **Houston** has the most job postings (562) but mid-range salary
- **Tarrytown/Fort Worth** are the sweet spot — above average demand AND salary
- **Friday in May** is peak hiring activity (358 postings in a single cell)
- **Monday/Sunday** are the slowest posting days across all months

---

## Dashboard Visualizations

### 1. KPI Summary Cards
Total Jobs · Avg Salary · Top City · Top Skill

### 2. Top In-Demand Skills (Bar Chart)
AI (472) · ML (162) · SQL (103) · AWS (90) · Python (87) · Azure (79) · Databricks (77) · ETL (74) · Snowflake (56) · Spark (39)

### 3. Job Demand vs Avg Salary by City (Scatter Plot)
4-quadrant analysis identifying sweet spot markets using reference line averages

### 4. Job Posting Activity (Heat Map)
Day of week vs month using `DATEPART()` calculated fields — reveals peak hiring patterns

---

## Pipeline Details

### Data Extraction
- Queries 4 job categories × 5 Texas cities × 5 pages = up to 1,000 API calls per run
- Deduplicates by job ID to avoid double-counting
- Stores raw JSON + parsed CSV to S3

### S3 Structure
```
s3://job-market-tracker/
└── adzuna/
    ├── jobs_json/
    │   └── run_date=2026-06-06/
    │       └── results.json
    └── jobs_csv/
        └── run_date=2026-06-06/
            └── results.csv   ← 2.9 MB
```

### Automation
- **EventBridge rule**: `cron(0 8 * * ? *)` — runs daily at 8am UTC
- **Lambda timeout**: 5 minutes
- **Lambda memory**: 256 MB (peak usage: 232 MB)
- **Execution time**: ~130 seconds per run

### SQL Skill Analysis
Custom SQL queries in Athena using `UNION ALL` pattern to count skill mentions across job titles and descriptions:

```sql
SELECT skill, COUNT(*) as demand
FROM (
    SELECT 'AI' as skill FROM "job_market_db"."jobs_csv"
    WHERE lower(description) LIKE '% ai %'
       OR lower(description) LIKE '%artificial intelligence%'
    UNION ALL
    SELECT 'Python' as skill FROM "job_market_db"."jobs_csv"
    WHERE lower(description) LIKE '%python%'
    -- ... additional skills
) sub
GROUP BY skill
ORDER BY demand DESC
```

---

## Screenshots

### Lambda Test — Successful Execution

![Lambda](screenshots/Lambda.png)

![Lambda Success](screenshots/Lambda_test.png)

- Status: 200
- Jobs fetched: 3,044
- Duration: 129,953 ms

### S3 Data Lake — Partitioned Storage

![S3 Bucket](screenshots/S3.png)

- File: `results.csv` (2.9 MB)
- Partition: `run_date=2026-06-06`

### Glue Crawler — 20 Successful Runs

![Glue Crawler](screenshots/Glue.png)

- Running daily since May 30, 2026
- All runs completed successfully

### Athena Query — Live Data Verification

![Athena Query](screenshots/Athena.png)

- `SELECT COUNT(*) FROM jobs_csv` → 3,032 rows
- Query time: 491ms, Data scanned: 2.87 MB

### Tableau Dashboard

![Dashboard](screenshots/Tableau.png)

---

## Project Setup

### Prerequisites
- AWS account with Lambda, S3, Glue, Athena access
- Adzuna API credentials (free at developer.adzuna.com)
- Tableau Desktop

### Environment Variables (Lambda)
```
ADZUNA_APP_ID=your_app_id
ADZUNA_APP_KEY=your_app_key
S3_BUCKET=your_bucket_name
```

### Lambda Layer
```
arn:aws:lambda:us-east-2:336392948345:layer:AWSSDKPandas-Python311:31
```

### EventBridge Schedule
```
cron(0 8 * * ? *)
```

---

## Repository Structure

```
├── README.md
├── lambda_function.py          ← Main ETL script (Lambda entry point)
├── requirements.txt            ← Python dependencies
├── sql/
│   └── skill_demand_queries.sql ← Athena SQL for Tableau
└── screenshots/
    ├── lambda_test_success.png
    ├── s3_partitioned_data.png
    ├── glue_crawler_runs.png
    ├── athena_count_query.png
    └── tableau_dashboard.png
```
