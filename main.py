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
matplotlib.use('agg')

print("Main Started!")
class MainETL():
    def __init__(self):
        self.drop_columns = []
        self.dimension_table = []
        
    def extract(self):
        # Use pandas read_csv to open each CSV file and extract column names from both files to fact table
        # This means I need to combine the two CSV files / Dataframes into one singular fact table dataframe (kill me)
        econ_cols = econ_df.cols.tolist()
        econ_cols.pop()
        stock_cols = stock_df.cols.tolist()
        column_names = econ_cols + stock_cols
        self.fact_table = pd.DataFrame(columns=column_names)
        for econ_row in econ_df:
            for stock_row in stock_df:
                if (stock_df['date'][stock_row] > econ_df['date'][econ_row] and stock_df['date'][stock_row] < econ_df['date'][econ_row]):
                    new_row = {}
                    for col in econ_cols:
                        new_row[col] = econ_df[col][econ_row]
                    for col in stock_cols:
                        new_row[col] = stock_df[col][stock_row]
                    self.fact_table.append(new_row)
        
        print("Extraction to fact table from CSVs completed!!")
                
    
# Stock dim tables


# Econ dim tables







app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = dbc.Container([
    html.H1("Visualized Data", className='mb-2', style={'textAlign':'center'}),
])

if __name__ == '__main__':
    app.run_server(debug=False, port=8002)