import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    DESCRIPTION:
    Iterates through all drop queries to ensure tables are deleted before creating new tables
    
    INPUT: 
    cur: postgresql cursor for executing sql statements on database
    conn: postgresql connection to database 
    
    OUTPUT:
    No output.
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    DESCRIPTION:
    Iterates through all create table queries to create new tables
    
    INPUT: 
    cur: postgresql cursor for executing sql statements on database
    conn: postgresql connection to database 
    
    OUTPUT:
    No output.
    """    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    DB_NAME=config.get('DB', 'DB_NAME')
    DB_USER=config.get('DB', 'DB_USER')
    DB_PASSWORD=config.get("DB","DB_PASSWORD")
    DB_PORT=config.get("DB","DB_PORT")
    DWH_ENDPOINT=config.get("DWH","DWH_ENDPOINT")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    print ('Connection to Postgresql established...')
    cur = conn.cursor()

    drop_tables(cur, conn)
    print ('Tables dropped...')
    create_tables(cur, conn)
    print ('New tables created...')

    conn.close()


if __name__ == "__main__":
    main()