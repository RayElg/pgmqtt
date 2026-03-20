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

def run_sql_expect_error(sql, username=None, password=None):
    """Run SQL query expecting an error. Return error message or None if no error."""
    try:
        if username and password:
            # Connect as specific user
            host = os.environ.get("PG_HOST", "127.0.0.1")
            port = os.environ.get("PG_PORT", "5432")
            dbname = os.environ.get("PG_DATABASE", "postgres")
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                dbname=dbname
            )
        else:
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
    except psycopg2.Error as e:
        return str(e)
    except Exception as e:
        return str(e)

def run_command(cmd_list):
    """Run a command inside the postgres service container using docker-compose."""
    return subprocess.check_output(
        ["docker", "compose", "exec", "-T", "postgres"] + cmd_list
    ).decode()


def compose_stop(timeout_secs: int = 15) -> None:
    """Send SIGTERM to the postgres container (graceful shutdown)."""
    subprocess.run(
        ["docker", "compose", "stop", "--timeout", str(timeout_secs), "postgres"],
        capture_output=True,
        timeout=timeout_secs + 15,
    )


def compose_start() -> None:
    """Start the postgres container."""
    subprocess.run(
        ["docker", "compose", "start", "postgres"],
        capture_output=True,
        timeout=30,
    )


def compose_kill() -> None:
    """Send SIGKILL to the postgres container (crash simulation)."""
    subprocess.run(
        ["docker", "compose", "kill", "--signal", "SIGKILL", "postgres"],
        capture_output=True,
        timeout=10,
    )
