import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, analytics_queries


def load_staging_tables(cur, conn):
    """
    Executes Copy queries to load information to Redshift Cluster from two S3 buckets
    """
    for query in copy_table_queries: 
        print('\n'.join(('\nLoading STAGING DATA:', query)))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes Copy queries to insert data into the Star Schema
    """
    for query in insert_table_queries: 
        print('\n'.join(('\nInserting into STAR SCHEMA:', query)))
        cur.execute(query)
        conn.commit()
        
        
def run_analytics_queries(cur, conn):
    """
    Executes Some Analytics Queries
    """ 
    for query in analytics_queries: 
        print('\n'.join(('\nExecuting Analytic Query:', query)))
        cur.execute(query)
        conn.commit()


def main():
    """
    First copy into the staging tables with information from the specified S3 buckets and then inserts them into the fact
    and dimension tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
	
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())) 
    print('Connected to AWS Redshift') 
    print(conn)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    run_analytics_queries(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()