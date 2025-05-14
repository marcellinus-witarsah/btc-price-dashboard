import psycopg2
from src.logger import logger
class PostgresDBOps:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None

    def connect(self):
        """ Connect to the PostgreSQL database server """
        try:
            # connecting to the PostgreSQL server
            with psycopg2.connect(**self.db_config) as conn:
                logger.info('Connected to the PostgreSQL server.')
                self.conn = conn
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
    
    def create_table(self, table_name, columns):
        """ Create a table in the PostgreSQL database """
        try:
            with self.conn.cursor() as cursor:
                # Use parameterized queries to prevent SQL injection
                create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
                cursor.execute(create_table_query)
                self.conn.commit()
                logger.info(f'Table {table_name} created successfully.')
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.conn.rollback()
    
    def insert_data(self, table_name, data):
        """ Insert data into the PostgreSQL database """
        try:
            with self.conn.cursor() as cursor:
                # Use parameterized queries to prevent SQL injection
                insert_query = f"INSERT INTO {table_name} (trade_id, symbol, price, qty, side, timestamp) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(insert_query, data)
                self.conn.commit()
                logger.info(f'Data inserted into {table_name} successfully.')
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.conn.rollback()
    
    
    def close_connection(self):
        """ Close the PostgreSQL database connection """
        if self.conn:
            self.conn.close()
            logger.info('PostgreSQL connection closed.')
        else:
            logger.info('No connection to close.')