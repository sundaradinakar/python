# This is a sample Python script.

# Press Ctrl+F5 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
from azure.servicebus import ServiceBusClient, ServiceBusMessage

def download_blob_to_dataframe(account_name, account_key, container_name, blob_name):
    try:
        blob_service_client = BlobServiceClient(account_url="{}://{}.blob.core.windows.net".format(
            "https", account_name), credential=account_key)

        blob_client = blob_service_client.get_blob_client(container_name, blob_name)

        blob_data = blob_client.download_blob().readall()

        df = pd.read_csv(io.StringIO(blob_data.decode('utf-8')))

        return df
    except Exception as e:
        print(e)

def upload_file_to_datalake(account_name, account_key, file_system_name, file_path, file_content):
    try:
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", account_name), credential=account_key)

        file_system_client = service_client.get_file_system_client(file_system_name)

        file_client = file_system_client.get_file_client(file_path)

        file_client.upload_data(file_content, overwrite=True)

    except Exception as e:
        print(e)

# Usage
account_name = 'storage account name'
account_key = 'account key for storage'
file_system_name = 'test'
output_file_system_name = 'output'
container_name = 'test'
blob_name = 'student-mat.csv'
file_path = 'student-mat.csv'
#file_content = open('.\student\student-mat.csv', 'rb')

def handle_message(msg):
    # This function will be called when a message is received
    print("Received message: ", msg)
    print('str'+str(msg))
    # Call the functions in main.py here
   # upload_file_to_datalake(account_name, account_key, file_system_name, file_path, file_content)
    df = download_blob_to_dataframe(account_name, account_key, container_name, blob_name)
    print(df.head())
    desc_stats = df.describe()

# Check for missing values
    missing_values = df.isnull().sum()
    
# Perform a groupby operation (replace 'column1' and 'column2' with actual column names in your DataFrame)
    grouped_df = df.groupby('age')['G3'].mean()
    print("Descriptive Statistics:\n", desc_stats)
    print("\nMissing Values:\n", missing_values)
    print("\nGrouped DataFrame:\n", grouped_df)
    msg_text = str(msg)
    upload_file_to_datalake(account_name, account_key, output_file_system_name+'/'+msg_text, file_path, df.to_csv(index=False))
    upload_file_to_datalake(account_name, account_key, output_file_system_name+'/'+msg_text, 'desc_stats.csv', desc_stats.to_csv())
    upload_file_to_datalake(account_name, account_key, output_file_system_name+'/'+msg_text, 'missing_values.csv', missing_values.to_csv())
    upload_file_to_datalake(account_name, account_key, output_file_system_name+'/'+msg_text, 'grouped_df.csv', grouped_df.to_csv())
    print("Files uploaded to Data Lake")

def main():
    connection_str = "put your connection string here for service bus"
    topic_name = "python_topic"
    subscription_name = "pysubscription"

    with ServiceBusClient.from_connection_string(connection_str) as client:
        with client.get_subscription_receiver(topic_name, subscription_name) as receiver:
            for msg in receiver:
                handle_message(msg)
                receiver.complete_message(msg)

if __name__ == "__main__":
    main()
