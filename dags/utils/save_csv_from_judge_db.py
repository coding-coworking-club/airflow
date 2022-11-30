import os
from pathlib import Path
import pandas as pd

from utils.online_judge_to_offical_website import connect_to_judge_db
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def save_csv_from_judge_db(sql, **context):
    # connect to judge db
    pg_hook = connect_to_judge_db()
    pg_cursor = pg_hook.get_conn().cursor()

    # Execute query from sql file
    query = Path(sql).read_text()
    pg_cursor.execute(query)
    results = pg_cursor.fetchall()

    # save raw csv
    col_names = [i[0] for i in pg_cursor.description]
    raw_df = pd.DataFrame.from_records(results, columns=col_names)
    raw_df.to_csv('data/raw.csv', index=False)
    s3_hook = S3Hook(aws_conn_id="s3_airflow_data")
    s3_hook.load_file(
        filename='data/raw.csv',
        key='raw.csv',
        bucket_name="ccclub-airflow-data",
        replace=True
    )
    raw_csv_path = os.path.abspath('data/raw.csv')
    context['ti'].xcom_push(key='raw_csv_path', value=raw_csv_path)
