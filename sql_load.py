#deliver stuff to the azure/sql database

import pandas as pd
from datetime import datetime,date as dt
import sqlalchemy as sa
from sqlalchemy import create_engine

def run_sql_load():

    connection_url = sa.engine.URL.create(
        drivername="mysql+pymysql",
        username="root",
        password="",
        host="localhost",
        database="warehouse",
    )

    engine = create_engine(connection_url)

    stock_data = (
        pd.read_csv("./csv_processed/dim_stock.csv")
        .assign(date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
    )

    econ_data = (
        pd.read_csv("./csv_processed/dim_econ.csv")
        .assign(date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
    )

    fact_stock_analysis = (
        pd.read_csv("./csv_processed/fact_stock_analysis.csv")
        .assign(date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
    )

    fact_comparison = (
        pd.read_csv("./csv_processed/fact_comparison.csv")
    )

    #TEMP DEBUG 
    #stock_data.to_csv('./csv_azure/stocks.csv', date_format="%d/%m/%Y %H:%M:%S", index=False)
    #econ_data.to_csv('./csv_azure/econ.csv', date_format="%d/%m/%Y %H:%M:%S", index=False)
    #fact_stock_analysis.to_csv('./csv_azure/fact_stock_analysis.csv', date_format="%d/%m/%Y %H:%M:%S", index=False)
    #fact_comparison.to_csv('./csv_azure/fact_comparison.csv', index=False)

    table_econ = econ_data.to_sql('dim_econ', engine, if_exists="append", index=0)
    table_stock = stock_data.to_sql('dim_stock', engine, if_exists="append", index=0)
    table_fact_comparison = fact_comparison.to_sql('fact_comparison', engine, if_exists="append", index=0)
    table_fact_stock_analysis = fact_stock_analysis.to_sql('fact_stock_analysis', engine, if_exists="append", index=0)



    return "Loaded to SQL"