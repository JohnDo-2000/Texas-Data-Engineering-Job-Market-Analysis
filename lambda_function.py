import boto3
import urllib.request
import urllib.parse
import json
import time
import pandas as pd
import io
import csv
import logging
import os
from datetime import datetime, timezone

# ── Logging (shows up in CloudWatch automatically) ──────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config from environment variables (no hardcoded secrets) ────────
ADZUNA_APP_ID   = os.environ["ADZUNA_APP_ID"]
ADZUNA_APP_KEY  = os.environ["ADZUNA_APP_KEY"]
ADZUNA_COUNTRY  = "us"
S3_BUCKET       = os.environ["S3_BUCKET"]
AWS_REGION      = os.environ.get("AWS_REGION", "us-east-2")

JOB_QUERIES = [
    "data analyst",
    "data engineer",
    "business analyst",
    "data scientist",
]

TX_LOCATIONS = [
    "dallas",
    "houston",
    "austin",
    "san antonio",
    "fort worth",
]

# ── Fetch from Adzuna API ────────────────────────────────────────────
def fetch_adzuna_jobs(query, location, pages=5):
    all_results = []
    for page in range(1, pages + 1):
        params = urllib.parse.urlencode({
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "results_per_page": 50,
            "what": query,
            "where": location,
            "content-type": "application/json",
        })
        url = f"https://api.adzuna.com/v1/api/jobs/{ADZUNA_COUNTRY}/search/{page}?{params}"
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                data = json.loads(response.read().decode())
            results = data.get("results", [])
            if not results:
                break
            all_results.extend(results)
            logger.info(f"  '{query}' in {location} — page {page}: {len(results)} jobs")
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"  API error on '{query}' in {location} page {page}: {e}")
            break
    return all_results


def fetch_all_jobs():
    all_jobs = []
    seen_ids = set()
    for query in JOB_QUERIES:
        for location in TX_LOCATIONS:
            logger.info(f"Fetching '{query}' in {location}...")
            jobs = fetch_adzuna_jobs(query=query, location=location, pages=5)
            for job in jobs:
                job_id = job.get("id")
                if job_id not in seen_ids:
                    seen_ids.add(job_id)
                    job["_search_query"] = query
                    job["_search_location"] = location
                    job["_fetched_date"] = datetime.now(timezone.utc).date().isoformat()
                    all_jobs.append(job)
    logger.info(f"Total unique jobs fetched: {len(all_jobs)}")
    return all_jobs


# ── Parse to DataFrame ───────────────────────────────────────────────
def parse_jobs(jobs):
    records = []
    for job in jobs:
        location = job.get("location", {})
        area = location.get("area", [])
        category = job.get("category", {})
        company = job.get("company", {})
        records.append({
            "id":                 job.get("id"),
            "adref":              job.get("adref"),
            "title":              job.get("title"),
            "description":        job.get("description"),
            "contract_time":      job.get("contract_time"),
            "contract_type":      job.get("contract_type"),
            "created":            job.get("created"),
            "company":            company.get("display_name"),
            "location_display":   location.get("display_name"),
            "city":               area[3] if len(area) > 3 else None,
            "county":             area[2] if len(area) > 2 else None,
            "state":              area[1] if len(area) > 1 else None,
            "country":            area[0] if len(area) > 0 else None,
            "latitude":           job.get("latitude"),
            "longitude":          job.get("longitude"),
            "salary_min":         job.get("salary_min"),
            "salary_max":         job.get("salary_max"),
            "salary_is_predicted":job.get("salary_is_predicted"),
            "category":           category.get("label"),
            "category_tag":       category.get("tag"),
            "redirect_url":       job.get("redirect_url"),
        })
    df = pd.DataFrame(records)
    df["salary_min"] = pd.to_numeric(df["salary_min"], errors="coerce")
    df["salary_max"] = pd.to_numeric(df["salary_max"], errors="coerce")
    df["salary_avg"] = (df["salary_min"] + df["salary_max"]) / 2
    df["salary_is_predicted"] = df["salary_is_predicted"].astype(str).map({"1": True, "0": False})
    return df


# ── Upload JSON to S3 ────────────────────────────────────────────────
def upload_json_to_s3(s3_client, data, s3_key):
    json_data = json.dumps(data, indent=2)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json_data,
        ContentType="application/json",
    )
    logger.info(f"JSON uploaded → s3://{S3_BUCKET}/{s3_key}")


# ── Upload CSV to S3 ─────────────────────────────────────────────────
def upload_csv_to_s3(s3_client, df, s3_key):
    df = df.copy()
    for col in ["title", "description"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('"', '', regex=False)
    for col in ["title", "description", "company", "location_display"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace("'", '', regex=False)
    for col in ["latitude", "longitude", "salary_min", "salary_max", "salary_avg"]:
        if col in df.columns:
            df[col] = df[col].where(df[col].notna(), other="")

    csv_buffer = io.StringIO()
    df.to_csv(
        csv_buffer,
        index=False,
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        lineterminator='\n',
    )
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    logger.info(f"CSV uploaded → s3://{S3_BUCKET}/{s3_key}")


# ── Lambda Entry Point ───────────────────────────────────────────────
def lambda_handler(event, context):
    try:
        now = datetime.now(timezone.utc)
        date_folder = f"run_date={now.strftime('%Y-%m-%d')}"

        logger.info("=== Adzuna ELT pipeline started ===")

        # 1 — Fetch
        jobs = fetch_all_jobs()
        if not jobs:
            logger.warning("No jobs fetched — exiting early")
            return {"statusCode": 200, "body": "No jobs fetched"}

        # 2 — Parse
        df = parse_jobs(jobs)
        logger.info(f"DataFrame shape: {df.shape}")

        # 3 — Upload to S3
        # Lambda uses IAM role automatically — no keys needed here
        s3 = boto3.client("s3", region_name=AWS_REGION)

        json_key = f"adzuna/jobs_json/{date_folder}/results.json"
        upload_json_to_s3(s3, {"timestamp": now.isoformat(), "count": len(jobs), "jobs": jobs}, json_key)

        csv_key = f"adzuna/jobs_csv/{date_folder}/results.csv"
        upload_csv_to_s3(s3, df, csv_key)

        logger.info("=== Pipeline completed successfully ===")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "jobs_fetched": len(jobs),
                "date": date_folder,
                "csv_key": csv_key,
            })
        }

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
