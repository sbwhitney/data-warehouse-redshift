import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This procedure drops the tables.

    Args:
        cur: the cursor variable
        conn: the connection variable
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This procedure creates the tables.

    Args:
        cur: the cursor variable
        conn: the connection variable
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This procedure connects to the database and calls the table creation/deletion functions."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()