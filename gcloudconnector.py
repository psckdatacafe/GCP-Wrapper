# List of Services
#   BigQuery 
#   Cloud Pub-Sub (Pub-Only)

import os.path
import json

from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud import storage

from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import pandas as pd

class BigQueryConnector():

    def __init__(self):
        self.__project_id = None
        self.__location = None
        self.__use_credential = None
        self.__credential_path = None
        self.__config_file_path = None
        self.__client = None
        self.__init_status = False
    
    # User does not use the config file
    def init_conn(self,
                            project_id = None,
                            location = "asia-southeast1",
                            use_credential = None,
                            credential_path = None):

        # Intitial Config Variables
        self.__project_id = project_id
        self.__location = location
        self.__use_credential = use_credential

        if self.__use_credential:
            self.__credential_path = credential_path

        self.__client = self.__create_client(self.__use_credential)
        self.__init_status = True
        
    # User uses the config file
    def init_conn_with_config_file(self, config_file_path):
        # Check Config File
        if os.path.isfile(config_file_path):
            # Read Config File
            with open(config_file_path,'r') as f:
                config_dict = json.load(f)
        else:
            raise FileNotFoundError('Config file is not available.')

        # Intitial Variables
        config_dict = config_dict['connect_bigquery']
        self.__project_id = config_dict['project_id']
        self.__location = config_dict['location']
        self.__use_credential = bool(config_dict['use_credential'])
        if self.__use_credential:
            self.__credential_path = config_dict['credential_path']

        self.__client = self.__create_client(self.__use_credential)
        self.__init_status = True

    def __create_client(self, use_credential_file):
        # Using file
        if self.__use_credential:
            if os.path.isfile(self.__credential_path):
                credentials = service_account.Credentials.from_service_account_file(self.__credential_path)
                try:
                    #return bigquery.Client.from_service_account_json(self.__credential_path)
                    return bigquery.Client(credentials=credentials, project=self.__project_id , location=self.__location)
                except:
                    raise FileNotFoundError('Credential file error.')
            else:
                raise FileNotFoundError('Credential file is not available.')

        # Using google service
        else:
            return bigquery.Client(project=self.__project_id , location=self.__location)

    def __check_intitial(self):
        if self.__init_status == False:
            raise RuntimeError('Please initial before apply any method.')

    def query(self, query, output_type = 'df'):
        self.__check_intitial()
        job_config = bigquery.QueryJobConfig()
        query_job = self.__client.query(query, job_config=job_config)  # API request
        if output_type == 'df':
            return query_job.result().to_dataframe()
        elif output_type == 'tupple':
            return query_job.result()
        else:
            return query_job.result().to_dataframe()

    def create_table(self, table_id, schema):
        '''
            Format of table_id: dataset_id.table_id
            Example for schema: 
            schema = [
                bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
            ]    
        '''
        self.__check_intitial()
        table_id = "{}.{}".format(self.__client.project,table_id)
        table = bigquery.Table(table_id, schema=schema)
        table = self.__client.create_table(table)

    def delete_table(self, table_id):
        '''Format of table_id: dataset_id.table_id'''
        self.__check_intitial()
        table_id = "{}.{}".format(self.__client.project,table_id)
        self.__client.delete_table(table_id, not_found_ok=True)

    def list_datasets(self):
        self.__check_intitial()
        return  list(self.__client.list_datasets())

    # def check_table_exist(self, dataset_id,table_id):
    #     '''Format of table_id: dataset_id.table_id'''
    #     self.__check_intitial()
    #     table_reference = self.__client.dataset(dataset_id).table(table_id)
    #     try:
    #         self.__client.get_table(table_reference)
    #         return True
    #     except NotFound:
    #         return False

    def check_table_exist(self, table_id):
        '''Format of table_id: dataset_id.table_id'''
        self.__check_intitial()
        table_id = "{}.{}".format(self.__client.project, table_id)
        try:
            self.__client.get_table(table_id)
            return True
        except NotFound:
            return False

    def list_of_table(self):
        self.__check_intitial()
        datasets = list(self.__client.list_datasets())
        project = self.__client.project

        if datasets:
            for dataset in datasets:  # API request(s)
                print("\t{}".format(dataset.dataset_id))
                dataset_ref = self.__client.dataset(dataset.dataset_id)
                tables = list(self.__client.list_tables(dataset_ref))
                for table in tables:
                    print('\t\t{}'.format(table.table_id))
        else:
            print("{} project does not contain any dataset.".format(project))

    def create_dataset(self, dataset_id):
        self.__check_intitial()
        dataset_id = "{}.{}".format(self.__client.project,dataset_id)

        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self.__location

        dataset = self.__client.create_dataset(dataset)
        print("Created dataset {}.{}".format(self.__client.project, dataset.dataset_id))

    def delete_dataset(self, dataset_id):
        self.__check_intitial()
        dataset_id = "{}.{}".format(self.__client.project,dataset_id)
        self.__client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    def upload_df(self, table_id, data_df):
        self.__check_intitial()
        if self.__use_credential:
            credentials = service_account.Credentials.from_service_account_file(self.__credential_path)
            data_df.to_gbq(table_id, credentials=credentials, location=self.__location, if_exists = 'append', progress_bar=False)
        else:
            data_df.to_gbq(table_id, project_id=self.__project_id, location=self.__location, if_exists = 'append', progress_bar=False)
    
    # View Tabel Session
    def list_view(self, view_dataset_id):
        self.__check_intitial()
        tables = self.__client.list_tables('.'.join([self.__client.project,view_dataset_id]))
        print("Views contained in '{}':".format('.'.join([self.__client.project,view_dataset_id])))
        i=1
        for table in tables:
            if table.table_type == 'VIEW':
                print("{}. {}.{}.{}".format(i,table.project, table.dataset_id, table.table_id))
                i+=1

    def create_view(self, view_dataset_id, view_table_id, view_sql):
        self.__check_intitial()
        view_dataset_ref = self.__client.dataset(view_dataset_id)
        view_ref = view_dataset_ref.table(view_table_id)
        view = bigquery.Table(view_ref)
        view.view_query = view_sql
        view = self.__client.create_table(view)
        print("Successfully created view at {}".format(view.full_table_id))

    def update_view(self, view_dataset_id, view_table_id, view_sql):
        self.__check_intitial()
        view_dataset_ref = self.__client.dataset(view_dataset_id)
        view_ref = view_dataset_ref.table(view_table_id)
        view = bigquery.Table(view_ref)
        view.view_query = view_sql
        view = self.__client.update_table(view, ["view_query"])  
        print("Successfully updated view at {}".format(view.full_table_id))
        
    def delete_view(self, view_table_id):
        self.__check_intitial()
        self.__client.delete_table(view_table_id, not_found_ok=True)
        print("Deleted table '{}'".format(view_table_id))

    def insert_stream_data(self, table_id, rows_to_insert):
        '''
            Format of table_id: dataset_id.table_id
            Example of rows_to_insert : [(u"Peter", 32), (u"Adam", 29)]
            Ref. https://cloud.google.com/bigquery/streaming-data-into-bigquery
        '''
        self.__check_intitial()
        table = self.__client.get_table(table_id)  # Make an API request.
        
        errors = self.__client.insert_rows(table, rows_to_insert)  # Make an API request.
        if errors == []:
            print("New rows have been added.")

    def insert_stream_data_chunk(self, table_id, data_df, max_rows=10000):
        data_rows = data_df.to_dict('split')['data']
        data_chunks = [data_rows[i:i + max_rows] for i in range(0, len(data_rows), max_rows)]  # list of list(s)
        for chunk in data_chunks:
            self.insert_stream_data(table_id=table_id, rows_to_insert=chunk)

    def append_data_from_gcs(self, table_id, uri):
        '''
            Format of table_id: dataset_id.table_id
            Example of uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
            Ref. https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
        '''
        job_config = bigquery.LoadJobConfig()

        # WRITE_EMPTY
        # WRITE_APPEND
        # WRITE_TRUNCATE
        
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.skip_leading_rows = 1
        # The source format defaults to CSV, so the line below is optional.
        job_config.source_format = bigquery.SourceFormat.CSV
        load_job = self.__client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # API request
        print("Starting job {}".format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.

    def load_csv_from_gcs(self, table_id, uri, mode='WRITE_APPEND'):
        '''
            Format of table_id: dataset_id.table_id
            Example of uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"

            Mode:
                WRITE_EMPTY	This    job should only be writing to empty tables.
                WRITE_TRUNCATE      This job will truncate table data and write from the beginning.
                WRITE_APPEND        This job will append to a table.

            Ref. https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html
        '''
        job_config = bigquery.LoadJobConfig()

        if mode == 'WRITE_EMPTY':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        elif mode == 'WRITE_TRUNCATE':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        job_config.skip_leading_rows = 1
        # The source format defaults to CSV, so the line below is optional.
        job_config.source_format = bigquery.SourceFormat.CSV
        load_job = self.__client.load_table_from_uri(
                                                        source_uris  = uri, 
                                                        destination  = table_id, 
                                                        location = self.__location,
                                                        project = self.__client.project,
                                                        job_config = job_config
                                                        
        )  
        # API request
        print("Starting job {}".format(load_job.job_id))
        load_job.result()  # Waits for table load to complete.

    def load_table_from_dataframe(self,dataframe, table_id,mode='WRITE_APPEND', num_retries=2):

        job_config = bigquery.LoadJobConfig()

        if mode == 'WRITE_EMPTY':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        elif mode == 'WRITE_TRUNCATE':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        load_job = self.__client.load_table_from_dataframe(dataframe=dataframe,
                                    destination=table_id, 
                                    num_retries=num_retries, 
                                    location = self.__location,
                                    project = self.__client.project,
                                    job_config=job_config)


        print("Starting job {}".format(load_job.job_id))
        load_job.result()  # Waits for table load to complete.

# ----------------------------------------------------------------------------------------------

# This class support only publish task
class CloudPubConnector():
    def __init__(self):
        self.__project_id = None
        self.__use_credential = None
        self.__credential_path = None
        self.__config_file_path = None
        self.__publisher = None
        self.__init_status = False
    
    # User does not use the config file
    def init_conn(self,
                            project_id = None,
                            use_credential = None,
                            credential_path = None,
                            batch=False, 
                            max_bytes=1, 
                            max_latency=1024,
                            max_messages=500):

        # Intitial Config Variables
        self.__project_id = project_id
        self.__use_credential = use_credential

        if self.__use_credential:
            self.__credential_path = credential_path

        self.__publisher = self.__create_publisher(self.__use_credential, batch, max_bytes, max_latency, max_messages)
        self.__init_status = True
        
    # User uses the config file
    def init_conn_with_config_file(self, config_file_path):
        # Check Config File
        if os.path.isfile(config_file_path):
            # Read Config File
            with open(config_file_path,'r') as f:
                config_dict = json.load(f)['connect_pubsub']
        else:
            raise FileNotFoundError('Config file is not available.')

        # Intitial Variables
        self.__project_id = config_dict['project_id']
        self.__use_credential = bool(config_dict['use_credential'])
        if self.__use_credential:
            self.__credential_path = config_dict['credential_path']

        batch = bool(config_dict['batch'])
        max_bytes = config_dict['max_bytes']
        max_latency = config_dict['max_latency']
        max_messages = config_dict['max_messages']

        self.__publisher = self.__create_publisher(self.__use_credential, batch, max_bytes, max_latency, max_messages)
        self.__init_status = True

    def __create_publisher(self, use_credential_file, batch=False, max_bytes=1024, max_latency=1, max_messages=500):
        # Using file
        if batch == True:
            batch_settings = pubsub_v1.types.BatchSettings(max_bytes=max_bytes,  # Byte (Not exceed 10 megabytes)
                                                                    max_latency=max_latency, # Second
                                                                    max_messages=max_messages)   # A maximum of 1,000 messages in a batch
        if self.__use_credential:
            if os.path.isfile(self.__credential_path):
                try:
                    credentials = service_account.Credentials.from_service_account_file(self.__credential_path)
                    if batch == True:
                        return  pubsub_v1.PublisherClient(credentials=credentials,batch_settings=batch_settings)
                    else:
                        #return pubsub_v1.PublisherClient.from_service_account_json(self.__credential_path)
                        return  pubsub_v1.PublisherClient(credentials=credentials)

                except:
                    raise FileNotFoundError('Credential file error.')
            else:
                raise FileNotFoundError('Credential file is not available.')

        # Using google service
        else:
            if batch == True:
                return pubsub_v1.PublisherClient(batch_settings=batch_settings)
            else: 
                return pubsub_v1.PublisherClient()

    def __check_intitial(self):
        if self.__init_status == False:
            raise RuntimeError('Please initial before apply any method.')

    def publish(self, topic_name, data_str):

        topic_path = self.__publisher.topic_path(self.__project_id, topic_name)
        data_str = data_str.encode("utf-8")
        future = self.__publisher.publish(topic_path, data=data_str)
        print(future.result())

    def publish_with_attribute(self, topic_name, data_str, attr_dict):

        topic_path = self.__publisher.topic_path(self.__project_id, topic_name)
        data_str = data_str.encode("utf-8")
        future = self.__publisher.publish(topic_path, data_str, document=json.dumps(attr_dict))
        print(future.result())

    

# Reference
# https://cloud.google.com/pubsub/docs/publisher
# https://pypi.org/project/google-cloud-pubsub/

# ----------------------------------------------------------------------------------------------

class CouldStorageConnector():

    def __init__(self):
        self.__project_id = None
        self.__location = None
        self.__use_credential = None
        self.__credential_path = None
        self.__config_file_path = None
        self.__storage_client = None
        self.__init_status = False
    
    # User does not use the config file
    def init_conn(self,
                            project_id = None,
                            use_credential = None,
                            credential_path = None):

        # Intitial Config Variables
        self.__project_id = project_id
        self.__use_credential = use_credential

        if self.__use_credential:
            self.__credential_path = credential_path

        self.__storage_client = self.__create_storage_client(self.__use_credential)
        self.__init_status = True
        
    # User uses the config file
    def init_conn_with_config_file(self, config_file_path):
        # Check Config File
        if os.path.isfile(config_file_path):
            # Read Config File
            with open(config_file_path,'r') as f:
                config_dict = json.load(f)['connect_cloudstorage']
        else:
            raise FileNotFoundError('Config file is not available.')

        # Intitial Variables
        self.__project_id = config_dict['project_id']
        self.__use_credential = bool(config_dict['use_credential'])
        if self.__use_credential:
            self.__credential_path = config_dict['credential_path']

        self.__storage_client = self.__create_storage_client(self.__use_credential)
        self.__init_status = True

    def __create_storage_client(self, use_credential_file):
        # Using file
        if self.__use_credential:
            if os.path.isfile(self.__credential_path):
                credentials = service_account.Credentials.from_service_account_file(self.__credential_path)
                try:
                    #return storage.Client.from_service_account_json(self.__credential_path)
                    return storage.Client(credentials=credentials, project=self.__project_id)
                except:
                    raise FileNotFoundError('Credential file error.')
            else:
                raise FileNotFoundError('Credential file is not available.')

        # Using google service
        else:
            return storage.Client(project=self.__project_id)

    def __check_intitial(self):
        if self.__init_status == False:
            raise RuntimeError('Please initial before apply any method.')

    def upload_file(self, bucket_name, source_file_name, destination_blob_name):
        '''
        bucket_name = "your-bucket-name"
        source_file_name = "local/path/to/file"
        destination_blob_name = "storage-object-name"
        '''

        bucket = self.__storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

    def upload_string(self, bucket_name, string_to_upload, type_str, destination_blob_name):
        '''
        bucket_name = "your-bucket-name"
        string_to_upload = "your-string"
        type_str = "text/plain", "text/csv"
        destination_blob_name = "storage-object-name"
        '''

        bucket = self.__storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(string_to_upload, type_str)

# Reference
# https://googleapis.dev/python/storage/latest/index.html

if __name__ == '__main__':
    pass


# Auth Reference
# https://google-auth.readthedocs.io/en/latest/reference/google.oauth2.service_account.html