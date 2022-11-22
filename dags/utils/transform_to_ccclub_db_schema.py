import os
from pathlib import Path
import pandas as pd

from transform.contest import transform_contest


def transform_to_ccclub_db_schema(transform_func, **context):
    # transform schema
    result_df = transform_func(context)

    # export csv
    result_df.to_csv('data/result.csv', index=False)
    result_csv_path = os.path.abspath('data/result.csv')
    context['ti'].xcom_push(key='result_csv_path', value=result_csv_path)
