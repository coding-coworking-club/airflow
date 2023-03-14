import os
from datetime import datetime
from pathlib import Path
import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def transform_to_ccclub_db_schema(transform_func, **context):
    # transform schema
    filename = transform_func.__name__.split('_')[-1]
    result_df = transform_func(context)

    # export csv
    today_str = datetime.today().strftime('%Y%m%d')
    result_df.to_csv(f'result/{filename}_{today_str}.csv', index=False)
    result_csv_path = os.path.abspath(f'result/{filename}_{today_str}.csv')
    context['ti'].xcom_push(key='result_csv_path', value=result_csv_path)

    # upload result csv to s3
    s3_hook = S3Hook(aws_conn_id="s3_airflow_data")
    s3_hook.load_file(
        filename=f'result/{filename}_{today_str}.csv',
        key=f'result/{filename}_{today_str}.csv',
        bucket_name="ccclub-airflow-data",
        replace=True
    )
