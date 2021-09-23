import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from from s3 files to staging tables
    """
    print('Inserting json file from S3 buckets into staging tables')
    for query in copy_table_queries:
        print('Current Loading: ' + query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load data to dimensional tables by selecting and transforming data from staging tables
    """
    print('Inserting data from staging tables into dimensional tables')
    for query in insert_table_queries:
        print('Current Loading ' + query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Extract songs metadata and user activity data from S3, transform it using a staging table, and load it into dimensional tables for analysis
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()