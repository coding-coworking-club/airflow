from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.postgres_hook import PostgresHook


def query_from_online_jundg_db():
    # Open SSH tunnel
    ssh_hook = SSHHook(ssh_conn_id="ssh_onlinejudge", keepalive_interval=60)
    tunnel = ssh_hook.get_tunnel(5432, remote_host="localhost", local_port=5432)
    tunnel.start()

    # Connect to DB and run query
    pg_hook = PostgresHook(
        postgres_conn_id="postgres_onlinejudge",  # NOTE: host="localhost"
        schema="onlinejudge"
    )
    pg_cursor = pg_hook.get_conn().cursor()
    pg_cursor.execute(
        f"""
         SELECT * FROM contest limit 10;
         """
    )
    results = pg_cursor.fetchall()

    # TODOs
    # 1. Extract SQL scripts and write in .sql file
    # 2. Write SSHHook+PostgresHook to a utils function
    # 3. Add connection_id for local-postgres via HOST=host.docker.internal 
