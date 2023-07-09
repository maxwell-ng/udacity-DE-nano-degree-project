import configparser
import psycopg2
import logging
from sql_queries import create_table_queries, drop_table_queries

# set up log configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    logger.info("connecting to redshift cluster.")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    try:
        logger.info("dropping all tables if exists.")
        drop_tables(cur, conn)
    except Exception as err:
        print(err)
        
    try:    
        logger.info("creating new tables.")
        create_tables(cur, conn)
    except Exception as err:
        print(err)
        
    conn.close()
    logger.info("connection closed.")


if __name__ == "__main__":
    main()