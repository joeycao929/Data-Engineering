import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        Drop the tables if they already exists in the database
    """
    print('Dropping the table if it exists')
    for query in drop_table_queries:
        print('Current Dropping: {}'.format(query))
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Create tables in the sql_queries.py
    """
    print('Creating the tables in the database')
    for query in create_table_queries:
        print('Current Creating: {}'.format(query))
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Drop Pre-existing Tables \n')
    drop_tables(cur, conn)
    
    print('Start Creating Tables')
    create_tables(cur, conn)


    print('Done!')
    conn.close()
    


if __name__ == "__main__":
    main()