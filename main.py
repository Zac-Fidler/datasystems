# Importing all that good stuff
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
from utils.setup import *
from dash import Dash, html, dcc, Input, Output
import plotly.express as px
import dash_ag_grid as dag
import dash_bootstrap_components as dbc  
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('agg')

print("Main Started!")

# Load ENV variables from file
load_dotenv()
storage_account = os.environ.get("ACCOUNT_STORAGE")
container_name = os.environ.get("CONTAINER_NAME")

# Call setup functions
azureDB = AzureDB()
azureDB.access_container(container_name)
blob_names = azureDB.list_blobs()
blob_list = list(blob_names)
if len(blob_list) > 0: 
    df = azureDB.access_blob_csv(blob_names[0])

    # Dataframe manipulation time😈
    print(df.head)

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = dbc.Container([
    html.H1("Visualized Data", className='mb-2', style={'textAlign':'center'}),
])

if __name__ == '__main__':
    app.run_server(debug=False, port=8002)