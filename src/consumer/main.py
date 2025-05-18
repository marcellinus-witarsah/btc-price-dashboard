from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.udf import udaf
from src.consumer.ohlc_aggregate_function import OHLCAggregateFunction


def create_events_aggregated_sink(t_env, table_name):
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            event_datetime TIMESTAMP(3),
            symbol STRING,
            qty FLOAT,
            open_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            close_price FLOAT,
            PRIMARY KEY (event_datetime) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgresuser',
            'password' = 'postgrespassword',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)

def create_events_source_kafka(t_env, table_name):
    source_ddl = f"""
        CREATE TABLE {table_name} (
            trade_id STRING, 
            symbol STRING, 
            price FLOAT, 
            qty FLOAT,
            side STRING,
            trade_timestamp BIGINT,
            event_watermark AS TO_TIMESTAMP_LTZ(trade_timestamp, 3),
            WATERMARK for event_watermark as event_watermark - INTERVAL '1' MINUTE
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'broker:29092',
            'topic' = 'btc-trades-topic',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
        

def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # Register User Defined Aggregate Function
    ohlc_udaf = udaf(OHLCAggregateFunction())
    t_env.create_temporary_function("OHLC", ohlc_udaf)

    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env, "trades")
        aggregated_table = create_events_aggregated_sink(t_env, "agg_trades")

        t_env.execute_sql(f"""
        with window_trades as (
        SELECT
            window_start,
            symbol,
            OHLC(price, trade_timestamp) as ohlc
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_watermark), INTERVAL '1' MINUTE)
        )
        GROUP BY window_start, symbol
        )
        SELECT 
            window_start, 
            symbol,
            ohlc.open_price as open_price,
            ohlc.high_price as high_price,
            ohlc.low_price as low_price,
            ohlc.close_price as close_price
        FROM window_trades
        ;
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

    
if __name__ == "__main__":
    main()