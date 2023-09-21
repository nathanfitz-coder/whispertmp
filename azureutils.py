import io
#import pandas as pd
from azure.storage.blob import BlobServiceClient #,  BlobClient


class BlobUtil:
    def __init__(self, connection_string, container_name):
        service_client = BlobServiceClient.from_connection_string(connection_string)
        self.client = service_client.get_container_client(container_name)

    def read_string(self, blob_name):
        tgt_blob = self.client.get_blob_client(blob=blob_name)
        stream = tgt_blob.download_blob().readall()
        return stream

    def read_csv(self, blob_name, **kwargs):
        tgt_blob = self.client.get_blob_client(blob=blob_name)
        stream = tgt_blob.download_blob().readall()

        with io.BytesIO(stream) as fo:
            df = pd.read_csv(fo, **kwargs)
        return df

    def read_parquet(self, blob_name, **kwargs):
        tgt_blob = self.client.get_blob_client(blob=blob_name)
        stream = tgt_blob.download_blob().readall()

        with io.BytesIO(stream) as fo:
            df = pd.read_parquet(fo, **kwargs)
        return df

    def read_excel(self, blob_name, **kwargs):
        tgt_blob = self.client.get_blob_client(blob=blob_name)
        stream = tgt_blob.download_blob().readall()

        with io.BytesIO(stream) as fo:
            df = pd.read_excel(fo, **kwargs)
        return df
    
    def upload_csv(self, df, blob_name):
        tgt_blob = self.client.get_blob_client(blob=blob_name)
        csvstring=df.to_csv(index=False)
        tgt_blob.upload_blob(csvstring,overwrite=True)
        return None
    
    def upload_excel(self, df, blob_name):
        tgt_blob = self.client.get_blob_client(blob=blob_name)
        writer = io.BytesIO()
        df.to_excel(writer,index=False)
        tgt_blob.upload_blob(writer.getvalue(),overwrite=True)
        return None

    def upload_parquet(self, df, blob_name):
        tgt_blob = self.client.get_blob_client(blob=blob_name)
        pqtstring=df.to_parquet(index=False)
        tgt_blob.upload_blob(pqtstring,overwrite=True)
        return None
    
    def download_binary_blob(self, blob_name, file_name):
        tgt_blob = self.client.get_blob_client(blob=blob_name)

        with open(file_name, 'wb') as file:
            data = tgt_blob.download_blob()
            file.write(data.readall())

        return None

    def upload_binary_blob(self, blob_name, file_name):
        tgt_blob = self.client.get_blob_client(blob=blob_name)
        with open(file_name, "rb") as data:
            tgt_blob.upload_blob(data, overwrite=True)
        return None

    def find_most_recent_blob(self, dir):
        blob_list = self.client.list_blobs(name_starts_with=dir)
        newest_creation_time=None
        newest_blob=None
        for blob in blob_list:
            if newest_creation_time is None:
                newest_creation_time = blob['creation_time']
                newest_blob = blob['name']
            elif blob['creation_time'] > newest_creation_time:
                newest_creation_time = blob['creation_time']
                newest_blob = blob['name']
        return newest_blob