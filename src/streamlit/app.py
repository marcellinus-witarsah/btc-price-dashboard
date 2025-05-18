import streamlit as st
from src.logger import logger
from src.utils import load_config
from src.db_ops.postgres_db_ops import PostgresDBOps
import pandas as pd


st.set_page_config(
    page_title="Bitcoin (BTC) Price Dashboard",
    layout="wide",
)


if __name__ == "__main__":

    config = load_config("src/db_ops/postgres.ini", "postgresql")
    db_ops = PostgresDBOps(config)
    db_ops.connect()
    
    columns, rows = db_ops.read_data("trades")
    df = pd.DataFrame(rows, columns=columns)
    
    # Data preprocessing
    # 1. convert `price` and `qty` columns to float64
    df = df.astype(
        {
            "price": "float64",
            "qty": "float64",
        }
    )
    
    # 2. convert `timestamp` column to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    
    # 3. Group data by a minute
    df.set_index("timestamp", inplace=True)
    
    logger.info(df.dtypes)
    logger.info(df.head())
    
    db_ops.close_connection()
