from pathlib import Path
import pandas as pd

from utils.online_judge_to_offical_website import connect_to_local_ccclub_db
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def insert_csv_to_ccclub_db(sql, **context):
    # connect to local ccclub db
    pg_hook = connect_to_local_ccclub_db()
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    # download result csv from s3
    s3_hook = S3Hook(aws_conn_id="s3_airflow_data")
    result_csv_path = s3_hook.download_file(
        key='result.csv',
        bucket_name="ccclub-airflow-data",
    )

    # copy result csv to ccclub db
    # TODO: avoid duplicate
    copy_result_csv = Path(sql).read_text()
    with open(result_csv_path, 'r') as f:
        cur.copy_expert(copy_result_csv, f)
        conn.commit()
