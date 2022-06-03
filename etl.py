import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    DESCRIPTION:
    Iterates through all load staging table queries to load data from S3 to postgresql database
    
    INPUT: 
    cur: postgresql cursor for executing sql statements on database
    conn: postgresql connection to database 
    
    OUTPUT:
    No output.
    """    
    for query in copy_table_queries:
        print ('Next to run: ', query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    DESCRIPTION:
    Iterates through all insert table queries to insert data from staging tables in star schema
    
    INPUT: 
    cur: postgresql cursor for executing sql statements on database
    conn: postgresql connection to database 
    
    OUTPUT:
    No output.
    """     
    for query in insert_table_queries:
        print ('Next to run: ', query)
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
    
    load_staging_tables(cur, conn)
    print ('Staging tables loaded...')
    insert_tables(cur, conn)
    print ('Data inserted to tables from staging tables...')

    conn.close()


if __name__ == "__main__":
    main()