import os
from pathlib import Path
import pandas as pd


def transform_contest(context):
    raw_csv = context['ti'].xcom_pull(key='raw_csv_path')
    raw_df = pd.read_csv(raw_csv)
    raw_df_length = len(raw_df.index)

    # assemble result dataframe
    result_df = pd.DataFrame()
    result_df['course_id'] = [16] * raw_df_length
    result_df['type'] = [
        'EXERCISE' if '隨堂練習' in i else 'HOMEWORK' for i in raw_df['description']]
    result_df['judge_contest_id'] = raw_df['id']
    result_df['name'] = raw_df['title']
    result_df['start_time'] = raw_df['start_time']
    result_df['end_time'] = raw_df['end_time']
    result_df['create_time'] = raw_df['create_time']
    result_df['update_time'] = raw_df['last_update_time']
    return result_df
