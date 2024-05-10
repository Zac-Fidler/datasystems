import os, pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
import io
from sqlalchemy import create_engine, text
import pandas as pd

load_dotenv()
username = os.environ.get("USERNAME_AZURE")
password = os.environ.get("PASSWORD")
server = os.environ.get("SERVER")
database = os.environ.get("DATABASE")
connection_string = os.environ.get("AZURE_SQL_CONNECTIONSTRING")
storage_account = os.environ.get("ACCOUNT_STORAGE")

class AzureDB():
  def __init__(self, local_path = "./data", storage_account = storage_account):
    self.local_path = local_path 
    self.account_url = f"https://{storage_account}.blob.core.windows.net"
    self.default_credential = DefaultAzureCredential()
    self.blob_service_client = BlobServiceClient(self.account_url, credential=self.default_credential)
    
  def access_container(self, container_name):
    # Use to create / access a container
    try:
      # Creating if doesn't exist
      print("Attempting to Create Container")
      self.container_client = self.blob_service_client.create_container(container_name) 
      self.container_name = container_name
    except Exception as ex:
      print("Creating container failed... Now attempting to access container")
      self.container_client = self.blob_service_client.get_container_client(container_name) 
      self.container_name = container_name
  
  def delete_container(self):
    print("Deleting Blob Container")
    self.container_client.delete_container(self.container_name)
    print("Done")
    
  def upload_blob(self, blob_name, blob_data = None):
    local_file_name = blob_name
    upload_file_path = os.path.join(self.local_path, local_file_name)
    blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=local_file_name)
    print(f"Uploading {local_file_name} to Azure Storage as blob to {self.container_name} container")
    
    if blob_data is not None:
      blob_client.create_blob_from_text(container_name=self.container_name, blob_name=blob_name, text=blob_data)
    else:
      # Upload created file
      with open(file=upload_file_path, mode="rb") as data:
        blob_client.upload_blob(data)
        
  def list_blobs(self):
    print("Listing Blobs!")
    blob_names = self.container_client.list_blob_names()
    blob_list = []
    for name in blob_names:
      # print("\t" + name)
      blob_list.append(name)
    return blob_list
      
  def download_blob(self, blob_name):
    download_file_path = os.path.join(self.local_path, blob_name)
    print("Downloading to " + download_file_path)
    with open(file=download_file_path, mode="wb") as download_file:
      download_file.write(self.container_client.download_blob(blob_name).readall)
    
  def delete_blob(self, blob_name):
      print("Deleting " + blob_name)
      blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
      blob_client.delete_blob()
      
  def access_blob_csv(self, blob_name):
    # Read CSV directly from Azure to save memory
    try:
      print(f"Accessing {blob_name} directly")
      
      df = pd.read_csv(io.StringIO(self.container_client.download_blob(blob_name).readall().decode('utf-8')))
      return df
    except Exception as ex:
      print(f"Exception: {ex}")