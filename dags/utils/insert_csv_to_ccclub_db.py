from pathlib import Path
import pandas as pd

from utils.online_judge_to_offical_website import connect_to_local_ccclub_db


def insert_csv_to_ccclub_db(sql, **context):
    csv = context['ti'].xcom_pull(key='csv_path')
    df = pd.read_csv(csv)
    print(df)

    # connect to local ccclub db
    pg_cursor = connect_to_local_ccclub_db()

    # TODO: Insert df to ccclub db
