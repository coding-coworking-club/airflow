from airflow import DAG
from airflow.models import Variable
from datetime import datetime   # Declare date time
from datetime import timedelta  # Declare the change of time
import pendulum                 # Declare timezone

# Operators
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from utils.save_csv_from_judge_db import save_csv_from_judge_db
from utils.transform_to_ccclub_db_schema import transform_to_ccclub_db_schema
from utils.insert_csv_to_ccclub_db import insert_csv_to_ccclub_db

from transform.contest import transform_contest

# Set the timezone, otherwise it might be UTC
local_tz = pendulum.timezone("Asia/Taipei")

default_args = {
    "owner": "ccclub",
    # "email": ["fake_email@gmail.com", "fake_email_2@gmail.com"],
    "email_on_retry": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "start_date": datetime(2021, 7, 10, 0, 0, 0, 0, local_tz),
    "do_xcom_push": True,
}


with DAG(
    dag_id="insert_judge_data_into_ccclub_db",
    default_args=default_args,
    start_date=datetime(2021, 7, 10, 0, 0, 0, 0, tzinfo=local_tz),
    schedule_interval="0 0 * * *",
    tags=["Medium"],
    catchup=False,
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    select = PythonOperator(
        task_id="save_csv_from_judge_db",
        python_callable=save_csv_from_judge_db,
        op_args=["/opt/airflow/dags/sql/select_contest.sql"],
        provide_context=True,
        dag=dag
    )
    transform = PythonOperator(
        task_id="transform_to_ccclub_db_schema",
        python_callable=transform_to_ccclub_db_schema,
        op_args=[transform_contest],
        provide_context=True,
        dag=dag
    )
    insert = PythonOperator(
        task_id="insert_csv_to_ccclub_db",
        python_callable=insert_csv_to_ccclub_db,
        op_args=["/opt/airflow/dags/sql/insert_contest.sql"],
        provide_context=True,
        dag=dag
    )
    start >> select >> transform >> insert >> end
