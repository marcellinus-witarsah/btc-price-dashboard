import psycopg2
from psycopg2 import sql
from src.logger import logger


class TimescaleDBOps:
    def __init__(self, db_config):
        self.__db_config = db_config
        self.__conn = None
        self.__connect()

    def __connect(self):
        """Connect to the TimescaleDB database server"""
        try:
            # connecting to the TimescaleDB server
            if self.__conn is not None:
                return
            self.__conn = psycopg2.connect(**self.__db_config)
            logger.info("Connected to the TimescaleDB server.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)

    def create_table(self, table_name: str, columns: dict, primary_key=None):
        """Create a table in the TimescaleDB database"""
        try:
            with self.__conn.cursor() as cursor:
                # Construct SQL String Composition for Columns
                columns = [
                    sql.SQL("{} {}").format(sql.Identifier(column), sql.SQL(data_type))
                    for column, data_type in columns.items()
                ]

                # Construct SQL String Composition for Primary Key
                primary_key = (
                    sql.SQL(", PRIMARY KEY ({})").format(
                        sql.SQL(", ").join(map(sql.Identifier, primary_key))
                    )
                    if primary_key
                    else sql.SQL("")
                )

                # Construct SQL String Composition for Table Creation
                query = sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {table_name} ({columns} {primary_key});"
                ).format(
                    table_name=sql.Identifier(table_name),
                    columns=sql.SQL(", ").join(columns),
                    primary_key=primary_key,
                )
                cursor.execute(query)
                self.__conn.commit()
                logger.info(f"Table {table_name} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def create_hypertable(self, table_name: str, time_column: str):
        """Create a hyper table in the TimescaleDB database"""
        try:
            with self.__conn.cursor() as cursor:
                # Construct SQL String Composition for Hypertable Creation
                query = sql.SQL(
                    "SELECT create_hypertable({table_name}, by_range({time_column}));"
                ).format(
                    table_name=sql.Literal(table_name),
                    time_column=sql.Literal(time_column),
                )
                cursor.execute(query)
                self.__conn.commit()
                logger.info(f"Hypertable {table_name} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def insert_data(self, table_name, columns, values):
        """Insert data into the TimescaleDB database"""
        try:
            with self.__conn.cursor() as cursor:
                query = sql.SQL(
                    "INSERT INTO {table_name} ({columns}) VALUES ({values})"
                ).format(
                    table_name=sql.Identifier(table_name),
                    columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
                    values=sql.SQL(", ").join(sql.Literal(value) for value in values),
                )
                cursor.execute(query)
                self.__conn.commit()
                logger.info(f"Data inserted into {table_name} successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def read_data(self, table_name, time_bucket="1m"):
        """Read data from the TimescaleDB database"""
        try:
            with self.__conn.cursor() as cursor:
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
            self.__conn.rollback()

    def close_connection(self):
        """Close the TimescaleDB database connection"""
        if self.__conn:
            self.__conn.close()
            logger.info("TimescaleDB connection closed.")
        else:
            logger.info("No connection to close.")

    def create_continuous_aggregation(
        self, source_table, view, columns, group_by_columns
    ):
        """Create a continuous aggregation view in the TimescaleDB database."""
        try:
            with self.__conn.cursor() as cursor:
                query = sql.SQL(
                    """
                    CREATE MATERIALIZED VIEW {view}
                    WITH (timescaledb.continuous) AS
                    SELECT
                        {columns}
                    FROM {source_table}
                    GROUP BY {group_by_columns}
                    WITH NO DATA;
                """
                ).format(
                    view=sql.Identifier(view),
                    columns=sql.SQL(", ").join(map(sql.SQL, columns)),
                    source_table=sql.Identifier(source_table),
                    group_by_columns=sql.SQL(", ").join(map(sql.SQL, group_by_columns)),
                )
                cursor.execute(query)
                self.__conn.commit()
                logger.info(f"View {view} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def create_automatic_refresh_policy(
        self, view, start_offset, end_offset, scheduler_interval
    ):
        """Create automation for updating continuous aggregated view in the TimescaleDB database."""
        try:
            with self.__conn.cursor() as cursor:
                query = sql.SQL(
                    """
                    SELECT add_continuous_aggregate_policy(
                        {view},
                        start_offset => INTERVAL {start_offset},
                        end_offset => INTERVAL {end_offset},
                        schedule_interval => INTERVAL {scheduler_interval}
                    )
                """
                ).format(
                    view=sql.Literal(view),
                    start_offset=sql.Literal(start_offset),
                    end_offset=sql.Literal(end_offset),
                    scheduler_interval=sql.Literal(scheduler_interval),
                )
                cursor.execute(query)
                self.__conn.commit()
                logger.info(f"View {view} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def create_notify(self, channel_name, function_name):
        """Create a notify function in the TimescaleDB database"""
        try:
            with self.__conn.cursor() as cursor:
                query = sql.SQL(
                    """
                    CREATE OR REPLACE FUNCTION {function_name}()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        PERFORM pg_notify({channel_name}, row_to_json(NEW)::text);
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                """
                ).format(
                    function_name=sql.Identifier(function_name),
                    channel_name=sql.Literal(channel_name),
                )
                cursor.execute(query)
                self.__conn.commit()
                logger.info(f"Notify function {function_name} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(f"Error creating notify function {function_name}: {error}")
            self.__conn.rollback()

    def create_trigger(self, table_name, trigger_name, function_name):
        """Create a trigger in the TimescaleDB database"""
        try:
            with self.__conn.cursor() as cursor:
                query = sql.SQL(
                    """
                    CREATE TRIGGER {trigger_name}
                    AFTER INSERT ON {table_name}
                    FOR EACH ROW
                    EXECUTE FUNCTION {function_name}();
                """
                ).format(
                    trigger_name=sql.Identifier(trigger_name),
                    table_name=sql.Identifier(table_name),
                    function_name=sql.Identifier(function_name),
                )
                cursor.execute(query)
                self.__conn.commit()
                logger.info(f"Trigger {trigger_name} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def listen_notification(self, channel_name):
        try:
            with self.__conn.cursor() as cursor:
                query = sql.SQL("LISTEN {channel_name}").format(
                    channel_name=sql.Identifier(channel_name)
                )
                cursor.execute(query)
                self.__conn.commit()
                logger.info(f"Listening to notification channel {channel_name}.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(f"Error listening to notification channel {channel_name}: {error}")
            self.__conn.rollback()
