from airflow import DAG
from airflow.models import Variable
from datetime import datetime   # Declare date time
from datetime import timedelta  # Declare the change of time
import pendulum                 # Declare timezone

# Operators
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

# Set the timezone, otherwise it might be UTC
local_tz = pendulum.timezone("Asia/Taipei")
    
default_args = {
    'owner': 'ccclub',
    # 'email': ['fake_email@gmail.com', 'fake_email_2@gmail.com'],
    'email_on_retry': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2021, 7, 10, 0, 0, 0, 0, local_tz),
    'do_xcom_push': True,
}

with DAG(
    dag_id='insert_judge_data_into_ccclub_db',
    default_args=default_args,
    start_date=datetime(2021, 7, 10, 0, 0, 0, 0, tzinfo = local_tz),
    schedule_interval="0 0 * * *",
    tags = ['Medium'],
    catchup = False,
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    ssh = SSHOperator(
        task_id='ssh',
        ssh_conn_id='ssh_onlinejudge',
        command='echo $RANDOM',
    )
    postgres = PostgresOperator(
        task_id='postgres',
        postgres_conn_id='postgres_onlinejudge',
        sql='select * from contest limit 10'
    )

    start >> ssh >> postgres >> end
