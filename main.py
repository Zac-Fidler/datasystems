# Importing all that good stuff
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
from utils.setup import *
from utils.dimension_classes import *
from dash import Dash, html, dcc, Input, Output
import plotly.express as px
import dash_ag_grid as dag
import dash_bootstrap_components as dbc  
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from datetime import datetime
from collections import Counter
matplotlib.use('agg')

print("Main Started!")

def todate(date: str):
    return datetime.strptime(date.strip().split(" ")[0], '%d/%m/%Y')

def remove_duplicates_from_list(input_list: list):
    temp = Counter(input_list)
    return list([*temp])
class MainETL():
    def __init__(self):
        self.drop_columns = []
        self.dimension_table = []
        
    def extract(self):
        # Use pandas read_csv to open each CSV file and extract column names from both files to fact table
        # This means I need to combine the two CSV files / Dataframes into one singular fact table dataframe (kill me)
        print("Beginning extraction from CSV to dataframe")
        econ_cols = econ_df.columns.tolist()
        econ_cols.pop()
        stock_cols = stock_df.columns.tolist()
        column_names = econ_cols + stock_cols
        self.fact_table = pd.DataFrame(columns=column_names)

        econ_counter = 1
        for stock_counter in range(0, len(stock_df.index)-1):
            if (todate(stock_df['date'][stock_counter]) >= todate(econ_df['date'][econ_counter-1]) and todate(stock_df['date'][stock_counter]) < todate(econ_df['date'][econ_counter])):
                # TODO: APPEND has been deprecated from pandas. Need to make 2d list of stuff and then concat into new df at the end i guess??
                new_row = {}
                for col in econ_cols:
                    new_row[col] = econ_df[col][econ_counter-1]
                for col in stock_cols:
                    new_row[col] = stock_df[col][stock_counter]
                self.fact_table.append(new_row)
            else:
                econ_counter += 1
                
        # for econ_row in econ_df:
        #     for stock_row in stock_df:
        #         if (todate(stock_df['date'][stock_row]) >= todate(econ_df['date'][econ_row]) and todate(stock_df['date'][stock_row]) < todate(econ_df['date'][econ_row])):
        #             new_row = {}
        #             for col in econ_cols:
        #                 new_row[col] = econ_df[col][econ_row]
        #             for col in stock_cols:
        #                 new_row[col] = stock_df[col][stock_row]
        #             self.fact_table.append(new_row)
        
        print("Extraction to dataframe from CSVs completed!!")
    
    def transform(self):
        print("Beginning transformation!!")
        # Transform fact table data types
        float_cols = ["quarterly_GDP_growth","quarterly_inflation","quarterly_unemployment","quarterly_debt_GDP","household_saving_ratio","share_price","dividend"]
        int_cols = ["share_quantity","earnings"]
        self.fact_table[float_cols] = self.fact_table[float_cols].astype(float)
        self.fact_table[int_cols] = self.fact_table[int_cols].astype(int)
        self.fact_table["date"] = pd.to_datetime(self.fact_table["date"], format="%d/%m/%Y")
        
        # Econ Dim Tables
        dim_country = DimCountry()
        self.drop_columns += dim_country.columns
        self.dimension_table.append(dim_country)
        
        dim_gdp_growth = DimGDPGrowth()
        self.drop_columns += dim_gdp_growth.columns
        # dim_gdp_growth["date"] = pd.to_datetime(dim_gdp_growth["date"], format="%d/%m/%Y")
        self.dimension_table.append(dim_gdp_growth)
        
        dim_inflation = DimInflation()
        self.drop_columns += dim_inflation.columns
        # dim_inflation["date"] = pd.to_datetime(dim_inflation["date"], format="%d/%m/%Y")
        self.dimension_table.append(dim_inflation)
        
        dim_unemployment = DimUnemployment()
        self.drop_columns += dim_unemployment.columns
        # dim_unemployment["date"] = pd.to_datetime(dim_unemployment["date"], format="%d/%m/%Y")
        self.dimension_table.append(dim_unemployment)
        
        dim_debt_gdp = DimDebtGDP()
        self.drop_columns += dim_debt_gdp.columns
        # dim_debt_gdp["date"] = pd.to_datetime(dim_debt_gdp["date"], format="%d/%m/%Y")
        self.dimension_table.append(dim_debt_gdp)
        
        dim_household_saving_ratio = DimHouseholdSavingRatio()
        self.drop_columns += dim_household_saving_ratio.columns
        # dim_household_saving_ratio["date"] = pd.to_datetime(dim_household_saving_ratio["date"], format="%d/%m/%Y")
        self.dimension_table.append(dim_household_saving_ratio)
        
        # Stock Dim Tables
        dim_stock_name = DimStockName()
        self.drop_columns += dim_stock_name.columns
        self.dimension_table.append(dim_stock_name)
        
        dim_share_price = DimSharePrice()
        self.drop_columns += dim_share_price.columns
        self.dimension_table.append(dim_share_price)
        
        dim_share_quantity = DimShareQuantity()
        self.drop_columns += dim_share_quantity.columns
        self.dimension_table.append(dim_share_quantity)
        
        dim_dividend = DimDividend()
        self.drop_columns += dim_dividend.columns
        self.dimension_table.append(dim_dividend)
        
        dim_earnings = DimEarnings()
        self.drop_columns += dim_earnings.columns
        self.dimension_table.append(dim_earnings)
        
        # Note maybe have to set values to date time??
        dim_date = DimDate()
        self.drop_columns += dim_date.columns
        self.dimension_table.append(dim_date)
        
        # Filter out date as it isn't a dimension table for this project
        # list(filter(lambda a: a != "date", self.drop_columns)) 
        # Date no longer included in dimension tables so not necessary to filter out of drop
        
        # Facts being calculated below
        # Stock Growth 
        self.fact_table['Stock Growth'] =  self.fact_table.groupby('country_name')['share_price'].pct_change(periods=1)
        
        # Earnings Growth 
        self.fact_table['Earnings Growth'] =  self.fact_table.groupby('country_name')['earnings'].pct_change(periods=1)
        
        # Stock Dividend Price Ratio aka Dividend Yield
        self.fact_table["Divident Yield"] = (self.fact_table['dividend'] / self.fact_table["share_price"]) * 100
        
        # Stock Market Capitalization
        self.fact_table["Stock Market Cap"] = self.fact_table['share_price'] * self.fact_table["share_quantity"]
        
        # Stock Earnings Per Share
        self.fact_table["Stock Earnings Per Share"] = (self.fact_table['earnings'] / self.fact_table["share_quantity"]) * 100
        
        ### Stock Growth Correlation Coefficients start here 
        
        # Stock vs GDP Growth Coefficients 
        corr = self.fact_table.groupby('country_name').apply(lambda d: d['Stock Growth'].corr(d['quarterly_gdp_growth']))
        self.fact_table['Stock and GDP Growth Coefficient'] = self.fact_table['country_name'].map(corr)

        # Stock Growth vs Country Inflation Coefficients
        corr = self.fact_table.groupby('country_name').apply(lambda d: d['Stock Growth'].corr(d['quarterly_inflation']))
        self.fact_table['Stock and Inflation Coefficient'] = self.fact_table['country_name'].map(corr)
        
        # Stock Growth vs Country Unemployment Coefficients
        corr = self.fact_table.groupby('country_name').apply(lambda d: d['Stock Growth'].corr(d['quarterly_unemployment']))
        self.fact_table['Stock and Unemployment Coefficient'] = self.fact_table['country_name'].map(corr)
        
        # Stock Growth vs Debt as % of GDP Coefficients 
        corr = self.fact_table.groupby('country_name').apply(lambda d: d['Stock Growth'].corr(d['quarterly_debt_gdp']))
        self.fact_table['Stock and Debt to GDP Coefficient'] = self.fact_table['country_name'].map(corr)
        
        # Stock Growth vs Household Savings Coefficients
        corr = self.fact_table.groupby('country_name').apply(lambda d: d['Stock Growth'].corr(d['quarterly_debt_gdp']))
        self.fact_table['Stock and Household Savings Coefficient'] = self.fact_table['country_name'].map(corr)
        
        # Stock Growth vs Earnings Growth Coefficients
        corr = self.fact_table.groupby('country_name').apply(lambda d: d['Stock Growth'].corr(d['quarterly_debt_gdp']))
        self.fact_table['Stock and Household Savings Coefficient'] = self.fact_table['country_name'].map(corr)
        
        ### Replace columns with foreign key IDs. NOTE: Needs to be tested extremememememely badly.
        for dim in self.dimension_tables: # for each dimension table
            # Adds the Foreign Key ID matching up with the value in the fact table
            self.fact_table = pd.merge(self.fact_table, dim.dimension_table, on=dim.columns, how="left")
        # drop all value columns, this results in fact table containing: Foreign Keys + Dates + Facts
        self.fact_table = self.fact_table.drop(columns=self.drop_columns)
        print("Transformation completed!!")
        
        # Test Line to see Fact Table as CSV
        self.fact_table.to_csv("FactTable.csv")

    def load():
        return None

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = dbc.Container([
    html.H1("Visualized Data", className='mb-2', style={'textAlign':'center'}),
])

if __name__ == '__main__':
    app.run_server(debug=False, port=8002)