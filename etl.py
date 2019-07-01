import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This procedure processes song and event files.
    It extracts the song information in order to store it into the staging_songs table.
    Then it extracts the event information in order to store it into the staging_events table.

    Args:
        cur: the cursor variable
        conn: the connection variable
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This procedure create the analytics tables from the staging tables.

    Args:
        cur: the cursor variable
        conn: the connection variable
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This procedure connects to the database and calls the file processing functions."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()