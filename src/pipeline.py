import os
import dbt
import polars as pl
import boto3
import smtplib
from pathlib import Path
from email.message import EmailMessage
from dagster import (
    asset, Definitions, AssetIn, AssetSelection, 
    define_asset_job, ScheduleDefinition, AssetExecutionContext,
    run_status_sensor, DagsterRunStatus,
    RunStatusSensorContext
)
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets
from google.cloud import bigquery
from datetime import datetime

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "fuzzy_dbt").resolve()
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()

MY_EMAIL = os.getenv("MY_EMAIL")  
APP_PASSWORD = os.getenv("APP_PASSWORD")  

TABLES = ["order_item_refunds", "order_items", "orders", "products", "website_pageviews", "website_sessions"]

def get_mysql_uri():
    db_pass = os.getenv("MYSQL_ROOT_PASSWORD")
    db_name = os.getenv("MYSQL_DATABASE")
    return f"mysql://root:{db_pass}@mysql_source:3306/{db_name}"

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

def create_assets(table_name):
    @asset(name=f"raw_{table_name}", group_name="extract_layer")
    def extract_from_mysql():
        uri = get_mysql_uri()
        df = pl.read_database_uri(f"SELECT * FROM {table_name}", uri)

        row_count = len(df)
        if row_count == 0:
            raise ValueError(f"{table_name} has 0 rows.")
        return df

    
    @asset(name=f"s3_{table_name}", ins={"df": AssetIn(f"raw_{table_name}")}, group_name="s3_datalake")
    def upload_to_s3(df: pl.DataFrame):
        local_path = f"/tmp/{table_name}.parquet"
        df.write_parquet(local_path)

        s3 = get_s3_client()
        bucket = os.getenv("S3_BUCKET_NAME")
        s3_key = f"raw/{table_name}/{datetime.now().strftime('%Y%m%d_%H%M')}.parquet"
        s3.upload_file(local_path, bucket, s3_key)
        os.remove(local_path)
        return s3_key
    
    @asset(name=f"bq_{table_name}", ins={"df": AssetIn(f"raw_{table_name}")}, group_name="bigquery_warehouse")
    def load_to_bq(df: pl.DataFrame):
        client = bigquery.Client()
        project_id = os.getenv("BQ_PROJECT_ID")
        dataset_id = os.getenv("BQ_DATASET")
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", source_format=bigquery.SourceFormat.PARQUET)
        client.load_table_from_dataframe(df.to_pandas(), table_id, job_config=job_config).result()
        return table_id
    return [extract_from_mysql, upload_to_s3, load_to_bq]

all_raw_assets = []
for t in TABLES:
    all_raw_assets.extend(create_assets(t))

@dbt_assets(manifest=dbt_project.manifest_path)
def fuzzy_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    dbt.cli(["deps"]).wait()
    yield from dbt.cli(["run"], context=context).stream()
    dbt.cli(["docs", "generate"]).wait()

def send_gmail_notification(subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = MY_EMAIL
    msg['To'] = MY_EMAIL
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(MY_EMAIL, APP_PASSWORD)
            smtp.send_message(msg)
    except Exception as e:
        print(f"Lỗi gửi mail: {e}")

@run_status_sensor(run_status=DagsterRunStatus.SUCCESS)
def success_email_sensor(context: RunStatusSensorContext):
    run_id = context.dagster_run.run_id
    send_gmail_notification(
        "Fuzzy Pipeline Success", 
        f"Chào Minh, Pipeline {run_id} đã hoàn thành."
    )

ecommerce_all_job = define_asset_job(name="ecommerce_full_pipeline", selection=AssetSelection.all())

defs = Definitions(
    assets=all_raw_assets + [fuzzy_dbt_assets], 
    resources={
        "dbt": DbtCliResource(
            project_dir=os.fspath(dbt_project.project_dir),
            profiles_dir=os.fspath(dbt_project.project_dir),
        ),
    },
    jobs=[ecommerce_all_job],
    schedules=[
        ScheduleDefinition(name="daily_ecommerce_schedule", job=ecommerce_all_job, cron_schedule="0 8 * * *")
    ],
    sensors=[success_email_sensor], 
)










