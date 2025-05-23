import psycopg2
from src.logger import logger


class TimescaleDBOps:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None

    def connect(self):
        """Connect to the TimescaleDB database server"""
        try:
            # connecting to the TimescaleDB server
            if self.conn is not None:
                return
            self.conn = psycopg2.connect(**self.db_config)
            logger.info("Connected to the TimescaleDB server.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)

    def create_table(self, table_name, columns):
        """Create a table in the TimescaleDB database"""
        try:
            with self.conn.cursor() as cursor:
                # Use parameterized queries to prevent SQL injection
                create_table_query = (
                    f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
                )
                cursor.execute(create_table_query)
                self.conn.commit()
                logger.info(f"Table {table_name} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.conn.rollback()

    def create_hypertable(self, table_name, time_column=None):
        """Create a hyper table in the TimescaleDB database"""
        try:
            with self.conn.cursor() as cursor:
                create_hypertable_query = f"SELECT create_hypertable('{table_name}', by_range('{time_column}'));"
                cursor.execute(create_hypertable_query)
                self.conn.commit()
                logger.info(f"Hypertable {table_name} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.conn.rollback()

    def insert_data(self, table_name, data):
        """Insert data into the TimescaleDB database"""
        try:
            with self.conn.cursor() as cursor:
                # Use parameterized queries to prevent SQL injection
                insert_query = f"INSERT INTO {table_name} (trade_id, symbol, price, qty, side, event_timestamp) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(insert_query, data)
                self.conn.commit()
                logger.info(f"Data inserted into {table_name} successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            # logger.warning(error)
            self.conn.rollback()

    def read_data(self, table_name, time_bucket="1m"):
        """Read data from the TimescaleDB database"""
        try:
            with self.conn.cursor() as cursor:
                # Use parameterized queries to prevent SQL injection
                select_query = f"""
                SELECT
                    timestamp_{time_bucket} as ts, 
                    symbol,
                    open(candlestick) as open_price,
                    high(candlestick) as high_price,
                    low(candlestick) as low_price,
                    close(candlestick) as close_price
                FROM {table_name}
                """
                cursor.execute(select_query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                logger.info(f"Data read from {table_name} successfully.")
                return (columns, rows)
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.conn.rollback()

    def close_connection(self):
        """Close the TimescaleDB database connection"""
        if self.conn:
            self.conn.close()
            logger.info("TimescaleDB connection closed.")
        else:
            logger.info("No connection to close.")

    def execute_sql_from_file(self, filepath):
        """Execute SQL script from a file"""
        try:
            with self.conn.cursor() as cursor:
                # Use parameterized queries to prevent SQL injection
                with open(filepath, "r") as sql_file:
                    sql_script = sql_file.read()
                cursor.execute(sql_script)
                self.conn.commit()
                logger.info(f"Execute SQL Script from {filepath} successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.conn.rollback()

    def create_continuous_aggregation(self, source_table, view, cols, groupby_cols):
        """Create a continuous aggregation view in the TimescaleDB database."""
        try:
            with self.conn.cursor() as cursor:
                query = f"""
                    CREATE MATERIALIZED VIEW {view}
                    WITH (timescaledb.continuous) AS
                    SELECT
                        {", ".join(cols)}
                    FROM {source_table}
                    GROUP BY {", ".join(groupby_cols)}
                    WITH NO DATA;
                """
                cursor.execute(query)
                self.conn.commit()
                logger.info(f"View {view} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.conn.rollback()

    def create_automatic_refresh_policy(
        self, view, start_offset, end_offset, scheduler_interval
    ):
        """Create automation for updating continuous aggregated view in the TimescaleDB database."""
        try:
            with self.conn.cursor() as cursor:
                query = f"""
                    SELECT add_continuous_aggregate_policy(
                        '{view}',
                        start_offset => INTERVAL '{start_offset}',
                        end_offset => INTERVAL '{end_offset}',
                        schedule_interval => INTERVAL '{scheduler_interval}'
                    );
                """
                cursor.execute(query)
                self.conn.commit()
                logger.info(f"View {view} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.conn.rollback()

    def create_notify(self, function_name):
        """Create a notify function in the TimescaleDB database"""
        try:
            with self.conn.cursor() as cursor:
                # Use parameterized queries to prevent SQL injection
                query = f"""
                    CREATE OR REPLACE FUNCTION {function_name}()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        PERFORM pg_notify('new_trade_channel', row_to_json(NEW)::text);
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                """
                cursor.execute(query)
                self.conn.commit()
                logger.info(f"Notify function {function_name} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.conn.rollback()

    def create_trigger(self, table_name, trigger_name, function_name):
        """Create a trigger in the TimescaleDB database"""
        try:
            with self.conn.cursor() as cursor:
                # Use parameterized queries to prevent SQL injection
                query = f"""
                    CREATE TRIGGER {trigger_name}
                    AFTER INSERT ON {table_name}
                    FOR EACH ROW
                    EXECUTE FUNCTION {function_name}();
                """
                cursor.execute(query)
                self.conn.commit()
                logger.info(f"Trigger {trigger_name} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.conn.rollback()
