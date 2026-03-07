import psycopg2
import subprocess
import os

def get_db_conn():
    host = os.environ.get("PG_HOST", "127.0.0.1")
    port = os.environ.get("PG_PORT", "5432")
    user = os.environ.get("PG_USER", "postgres")
    password = os.environ.get("PG_PASSWORD", "postgres")
    dbname = os.environ.get("PG_DATABASE", "postgres")
    
    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname
    )

def run_sql(sql):
    """Run SQL query and return results if any."""
    conn = get_db_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            try:
                if cur.description:
                    return cur.fetchall()
            except psycopg2.ProgrammingError:
                pass
            return None
    finally:
        conn.close()

def run_command(cmd_list):
    """Run a command inside the postgres service container using docker-compose."""
    return subprocess.check_output(
        ["docker-compose", "exec", "-T", "postgres"] + cmd_list
    ).decode()
