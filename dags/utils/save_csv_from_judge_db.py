import csv
import os
from pathlib import Path

from utils.online_judge_to_offical_website import connect_to_judge_db


def save_csv_from_judge_db(sql, **context):
    # connect to judge db
    pg_cursor = connect_to_judge_db()

    # Execute query from sql file
    query = Path(sql).read_text()
    pg_cursor.execute(query)
    results = pg_cursor.fetchall()

    # save results as csv
    with open("results.csv", "w") as f:
        csv_writer = csv.writer(f)
        for result in results:
            csv_writer.writerow(result)
    csv_path = os.path.abspath('results.csv')
    context['ti'].xcom_push(key='csv_path', value=csv_path)
