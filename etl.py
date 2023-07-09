import configparser
import psycopg2
import logging
from sql_queries import copy_table_queries, insert_table_queries, count_table_queries


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def count_table_rows(cur, conn):
    for query in count_table_queries:
        cur.execute(query)
        rows = cur.fetchall()
        print(query)
        for row in rows:
            print(row)

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    logger.info("connecting to redshift cluster")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    try:
        logger.info("Loading Json data from S3 into staging tables.")
        load_staging_tables(cur, conn)
    except Exception as err:
        logger.info(err)
    
    try:
        logger.info("Inserting values in fact and dimension tables.")
        insert_tables(cur, conn)
    except Exception as err:
        logger.info(err)

    try:
        logger.info("Querying table by counting number of rows in each table.")
        count_table_rows(cur, conn)
    except Exception as err:
        logger.info(err)
        
    conn.close()
    logger.info("Connection is closed.")


if __name__ == "__main__":
    main()