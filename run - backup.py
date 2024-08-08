import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import datetime
import pandas as pd

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

def create_items(container):
    print('\nCreating Items\n')

    # Create a SalesOrder object. This object has nested properties and various types including numbers, DateTimes and strings.
    # This can be saved as JSON as is without converting into rows/columns.
    sales_order = get_sales_order("SalesOrder1")
    container.create_item(body=sales_order)

    # As your app evolves, let's say your object has a new schema. You can insert SalesOrderV2 objects without any
    # changes to the database tier.
    sales_order2 = get_sales_order_v2("SalesOrder2")
    container.create_item(body=sales_order2)

def scale_container(container):
    print('\nScaling Container\n')
    
    # You can scale the throughput (RU/s) of your container up and down to meet the needs of the workload. Learn more: https://aka.ms/cosmos-request-units
    try:
        offer = container.read_offer()
        print('Found Offer and its throughput is \'{0}\''.format(offer.offer_throughput))

        offer.offer_throughput += 100
        container.replace_throughput(offer.offer_throughput)

        print('Replaced Offer. Offer Throughput is now \'{0}\''.format(offer.offer_throughput))
    
    except exceptions.CosmosHttpResponseError as e:
        if e.status_code == 400:
            print('Cannot read container throuthput.');
            print(e.http_error_message);
        else:
            raise;

def read_item(container, doc_id, account_number):
    print('\nReading Item by Id\n')

    # We can do an efficient point read lookup on partition key and id
    response = container.read_item(item=doc_id, partition_key=account_number)

    print('Item read by Id {0}'.format(doc_id))
    print('Partition Key: {0}'.format(response.get('partitionKey')))
    print('Subtotal: {0}'.format(response.get('subtotal')))


def read_items(container):
    print('\nReading all items in a container\n')

    # NOTE: Use MaxItemCount on Options to control how many items come back per trip to the server
    #       Important to handle throttles whenever you are doing operations such as this that might
    #       result in a 429 (throttled request)
    item_list = list(container.read_all_items(max_item_count=10))

    print('Found {0} items'.format(item_list.__len__()))

    for doc in item_list:
        print('Item Id: {0}'.format(doc.get('id')))


def query_items(container, account_number):
    print('\nQuerying for an  Item by Partition Key\n')

    # Including the partition key value of account_number in the WHERE filter results in a more efficient query
    items = list(container.query_items(
        query="SELECT * FROM r WHERE r.partitionKey=@account_number",
        parameters=[
            { "name":"@account_number", "value": account_number }
        ]
    ))

    print('Item queried by Partition Key {0}'.format(items[0].get("id")))


def replace_item(container, doc_id, account_number):
    print('\nReplace an Item\n')

    read_item = container.read_item(item=doc_id, partition_key=account_number)
    read_item['subtotal'] = read_item['subtotal'] + 1
    response = container.replace_item(item=read_item, body=read_item)

    print('Replaced Item\'s Id is {0}, new subtotal={1}'.format(response['id'], response['subtotal']))


def upsert_item(container, doc_id, account_number):
    print('\nUpserting an item\n')

    read_item = container.read_item(item=doc_id, partition_key=account_number)
    read_item['subtotal'] = read_item['subtotal'] + 1
    response = container.upsert_item(body=read_item)

    print('Upserted Item\'s Id is {0}, new subtotal={1}'.format(response['id'], response['subtotal']))


def delete_item(container, doc_id, account_number):
    print('\nDeleting Item by Id\n')

    response = container.delete_item(item=doc_id, partition_key=account_number)

    print('Deleted item\'s Id is {0}'.format(doc_id))


def get_sales_order(item_id):
    order1 = {'id' : item_id,
            'partitionKey' : 'Account1',
            'purchase_order_number' : 'PO18009186470',
            'order_date' : datetime.date(2005,1,10).strftime('%c'),
            'subtotal' : 419.4589,
            'tax_amount' : 12.5838,
            'freight' : 472.3108,
            'total_due' : 985.018,
            'items' : [
                {'order_qty' : 1,
                    'product_id' : 100,
                    'unit_price' : 418.4589,
                    'line_price' : 418.4589
                }
                ],
            'ttl' : 60 * 60 * 24 * 30
            }

    return order1


def get_sales_order_v2(item_id):
    # notice new fields have been added to the sales order
    order2 = {'id' : item_id,
            'partitionKey' : 'Account2',
            'purchase_order_number' : 'PO15428132599',
            'order_date' : datetime.date(2005,7,11).strftime('%c'),
            'due_date' : datetime.date(2005,7,21).strftime('%c'),
            'shipped_date' : datetime.date(2005,7,15).strftime('%c'),
            'subtotal' : 6107.0820,
            'tax_amount' : 586.1203,
            'freight' : 183.1626,
            'discount_amt' : 1982.872,
            'total_due' : 4893.3929,
            'items' : [
                {'order_qty' : 3,
                    'product_code' : 'A-123',      # notice how in item details we no longer reference a ProductId
                    'product_name' : 'Product 1',  # instead we have decided to denormalise our schema and include
                    'currency_symbol' : '$',       # the Product details relevant to the Order on to the Order directly
                    'currency_code' : 'USD',       # this is a typical refactor that happens in the course of an application
                    'unit_price' : 17.1,           # that would have previously required schema changes and data migrations etc.
                    'line_price' : 5.7
                }
                ],
            'ttl' : 60 * 60 * 24 * 30
            }

    return order2

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

        #read csv
        df=pd.read_csv("fucus_clean.csv")
        df.index = df.index.map(str)  # Convert index to string
        df.index.name = 'id'  # Set the index name to 'id'
        df.reset_index(inplace=True)  # Move the index into a column
        
        #upload dataframe
        
        upload_dataframe_to_cosmosdb(df, container, limit=10)  # Specify limit or none for all items

        print("Data upload process completed!")   

        # Retrieve data from Cosmos DB data - List
        data = retrieve_data_from_cosmosdb(container)

        # Convert data to DataFrame
        df = pd.DataFrame(data)

        # select relevant columns
        df = df[['id', 'model', 'year', 'price', 'transmission','fuel type','engine size']]

        print(df.head())
        # scale_container(container)
        # create_items(container)
        # read_item(container, 'SalesOrder1', 'Account1')
        # read_items(container)
        # query_items(container, 'Account1')
        # replace_item(container, 'SalesOrder1', 'Account1')
        # upsert_item(container, 'SalesOrder1', 'Account1')
        # delete_item(container, 'SalesOrder1', 'Account1')

        
    finally:
            print("\nrun_sample done")


if __name__ == '__main__':
    run_sample()
