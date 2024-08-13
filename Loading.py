from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import StringIO, BytesIO
import pandas as pd 
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import os

def run_loading():
    load_dotenv()
    customer= pd.read_csv('customer.csv')
    product= pd.read_csv('product.csv')
    transaction= pd.read_csv('transaction.csv')
    Date= pd.read_csv('Date.csv')
    staff= pd.read_csv('staff.csv')
    
    # Retrieve environment variables for Azure Blob Storage
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING') 
        # Check if the container exists and create it if it doesn't
        if not container_client.exists():
            print(f"Container '{container_name}' does not exist.")
            container_client.create_container()
            print(f"Container '{container_name}' created successfully.")
        else:
            print(f"Container '{container_name}' already exists.")
        
        # List of dataframes and corresponding blob names
        
        data = [
            (customer, 'data/customer_table'),
            (product, 'data/product_table'),
            (transaction, 'data/transaction_table'),
            (Date, 'data/date_table'),  # Assuming 'Date' refers to a dataframe named 'date_table'
            (staff, 'data/staff_table')
        
        ]

        # Upload each dataframe as a blob to Azure Blob Storage
        for df, blob_name in data:
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(df.to_csv(index=False), overwrite=True)
            print(f"Uploaded {blob_name} to Azure Blob Storage successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

   

    