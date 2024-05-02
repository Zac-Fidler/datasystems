# Importing all that good stuff
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
from utils.setup import *

# Load ENV variables from file
load_dotenv()
storage_account = os.environ.get("ACCOUNT_STORAGE")
container_name = os.environ.get("CONTAINER_NAME")

# Call setup functions
azureDB = AzureDB()
azureDB.access_container(container_name)
blob_names = azureDB.list_blobs()
df = azureDB.access_blob_csv(blob_names[0])

# Dataframe manipulation time😈
print(df.head)