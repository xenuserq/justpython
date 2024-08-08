import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import datetime
import pandas as pd
import os

import cleaning
import config

# ----------------------------------------------------------------------------------------------------------
# Prerequistes -
#
# 1. An Azure Cosmos account -
#    https://docs.microsoft.com/azure/cosmos-db/create-cosmosdb-resources-portal#create-an-azure-cosmos-db-account
#
# 2. Microsoft Azure Cosmos PyPi package -
#    https://pypi.python.org/pypi/azure-cosmos/
# ----------------------------------------------------------------------------------------------------------
# Sample - demonstrates the basic CRUD operations on a Item resource for Azure Cosmos
# ----------------------------------------------------------------------------------------------------------

HOST = config.settings['host']
MASTER_KEY = config.settings['master_key']
DATABASE_ID = config.settings['database_id']
CONTAINER_ID = config.settings['container_id']


# Function to upload DataFrame to Cosmos DB
# def upload_dataframe_to_cosmosdb(df, container):
#     for _, row in df.iterrows():
#         item = row.to_dict()
#         container.create_item(body=item)
#     print('dt uploaded')
# def upload_dataframe_to_cosmosdb(df, container):
#     for _, row in df.iterrows():
#         item = row.to_dict()
#         try:
#             container.create_item(body=item)
#         except Exception as e:
#             print(f"An error occurred: {e}")

def upload_dataframe_to_cosmosdb(df, container, limit=None):
    # If limit is provided, limit the DataFrame to the first 'limit' rows
    if limit is not None:
        limited_df = df.head(limit)
    else:
        limited_df = df
    
    for _, row in limited_df.iterrows():
        item = row.to_dict()
        item_id = item['id']  # Ensure 'id' field is in the item
        
        try:
            # Check if the item already exists
            existing_item = list(container.query_items(
                query=f"SELECT * FROM c WHERE c.id = '{item_id}'",
                enable_cross_partition_query=True
            ))
            
            if existing_item:
                # Optionally update the item if it exists
                print(f"Item with id {item_id} already exists. Updating item.")
                container.upsert_item(body=item)  # Upsert (update or insert) item
            else:
                # Insert the item if it does not exist
                print(f"Inserting item with id {item_id}.")
                container.create_item(body=item)
        
        except exceptions.CosmosHttpResponseError as e:
            print(f"Cosmos DB error: {e.message}")
        except Exception as e:
            print(f"An error occurred: {e}")

#retrieve func
def retrieve_data_from_cosmosdb(container, query="SELECT * FROM c"):
    items = []
    try:
        # Query the container
        for item in container.query_items(
            query=query,
            enable_cross_partition_query=True
        ):
            items.append(item)
    except exceptions.CosmosHttpResponseError as e:
        print(f"Cosmos DB error: {e.message}")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return items

#main program
def run_sample():
    client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY}, user_agent="CosmosDBPythonQuickstart", user_agent_overwrite=True)
    try:
        # setup database for this sample
        try:
            db = client.create_database(id=DATABASE_ID)
            print('Database with id \'{0}\' created'.format(DATABASE_ID))

        except exceptions.CosmosResourceExistsError:
            db = client.get_database_client(DATABASE_ID)
            print('Database with id \'{0}\' was found'.format(DATABASE_ID))

        # setup container for this sample
        try:
            container = db.create_container(id=CONTAINER_ID, partition_key=PartitionKey(path='/partitionKey'))
            print('Container with id \'{0}\' created'.format(CONTAINER_ID))

        except exceptions.CosmosResourceExistsError:
            container = db.get_container_client(CONTAINER_ID)
            print('Container with id \'{0}\' was found'.format(CONTAINER_ID))

        #clean dataframe cleaning.py
        #cleaning.cleandt()
        input_file = 'unclean_focus.csv'
        output_file = 'clean_focus.csv'
        cleaning.cleandt(input_file, output_file)
        
        
        #upload dataframe
        #read csv
        df=pd.read_csv(output_file)
        df.index = df.index.map(str)  # Convert index to string
        df.index.name = 'id'  # Set the index name to 'id'
        df.reset_index(inplace=True)  # Move the index into a column
        
        upload_dataframe_to_cosmosdb(df, container, limit=10)  # Specify limit or none for all items

        print("Data upload process completed!")   

        # Retrieve data from Cosmos DB data - List
        data = retrieve_data_from_cosmosdb(container)
        print("Data Download process completed!")
        # Convert data to DataFrame
        df = pd.DataFrame(data)
         
        # select relevant columns
        df = df[['id', 'model', 'year', 'price', 'transmission','fuel type','engine size']]

        print(df.head())
        
      

        
    finally:
            print("\nrun_sample done")


if __name__ == '__main__':
    # Get the directory of the current script
    script_directory = os.path.dirname(os.path.abspath(__file__))
    
    # Change the working directory
    os.chdir(script_directory)
    
    # Verify the current working directory
    current_directory = os.getcwd()
    print(f"Changed working directory to: {current_directory}")
    run_sample()
