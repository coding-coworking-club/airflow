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
    filename = sql.split('/')[-1].split('.')[0].split('_')[-1]
    s3_hook = S3Hook(aws_conn_id="s3_airflow_data")
    result_csv_path = s3_hook.download_file(
        key=f'result/{filename}.csv',
        bucket_name="ccclub-airflow-data",
    )

    # insert result csv into ccclub db
    # TODO: avoid duplicate
    df = pd.read_csv(result_csv_path)
    query_cmd = Path(sql).read_text()
    for index, row in df.iterrows():
        record_to_insert = (row['course_id'], row['type'], row['judge_contest_id'], row['name'],
                            row['start_time'], row['end_time'], row['create_time'], row['update_time'])
        cur.execute(query_cmd, record_to_insert)
        conn.commit()
