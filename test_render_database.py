import psycopg2
from utils import get_db_url


def test_postgress_connection(connection_string):
    try:
        conn = psycopg2.connect(connection_string)

        cur = conn.cursor()

        cur.execute("SELECT version();")
        db_version = cur.fetchone()
        print("Connetion successful!")
        print(f"PostgresSQL dbname: {conn.info.dbname}")

        cur.close()
        conn.close()
        print("Connection closed")
        return True
    except Exception as e:
        print("Connection failed")
        print(e)
        return False

DATABASE_URL = get_db_url()

test_postgress_connection(DATABASE_URL)