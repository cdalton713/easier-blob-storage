from unittest import TestCase
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas, ContainerClient, BlobClient
from .client import Client
import os
import dotenv

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


class TestClient(TestCase):

    conn = os.environ.get('CONN')
    container = os.environ.get('CONTAINER')
    Test = Client.from_connection_string(conn, container)

    def test_from_connection_string(self):
        assert self.Test.AZURE_STORAGE_CONTAINER == os.environ.get('CONTAINER')
        assert self.Test.AZURE_STORAGE_CONNECTION_STRING == os.environ.get('CONN')

    def test_list_blobs_in_container(self):
        files = self.Test.list_blobs_in_container()
        x = []
        for i in files:
            x.append(i)

        assert x[0].name == 'Legacy Logo.png'
