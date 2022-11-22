from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.postgres_hook import PostgresHook


def connect_to_judge_db():
    # Open SSH tunnel
    ssh_hook = SSHHook(ssh_conn_id="ssh_onlinejudge", keepalive_interval=60)
    tunnel = ssh_hook.get_tunnel(
        5432, remote_host="localhost", local_port=5432)
    tunnel.start()

    # Connect to DB
    pg_hook = PostgresHook(
        postgres_conn_id="postgres_onlinejudge",  # NOTE: host="localhost"
        schema="onlinejudge"
    )
    return pg_hook


def connect_to_local_ccclub_db():
    # Connect to DB
    pg_hook = PostgresHook(
        postgres_conn_id="postgres_local_ccclub",  # NOTE: host="host.docker.internal"
        schema="ccclub"
    )
    return pg_hook
