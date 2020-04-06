from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas, ContainerClient, BlobClient
from pathlib import Path
import urllib.parse
import os


def _make_url(base_url, *parts: str, **params):
    url = base_url
    for _ in parts:
        url = urllib.parse.urljoin(url, _)
    if params:
        url = '{}?{}'.format(url, urllib.parse.urlencode(params))
    return url


def _create_folder(path, parents=True, exist_ok=True):
    path = Path(path)
    if not path.exists():
        path.mkdir(parents=parents, exist_ok=exist_ok)


class Client(object):

    def __init__(self, storage_account, storage_container, key, protocol='https', endpoint_suffix='core.windows.net'):
        self.AZURE_STORAGE_ACCOUNT = storage_account
        self.AZURE_STORAGE_CONTAINER = storage_container
        self.PROTOCOL = protocol
        self.ENDPOINT_SUFFIX = endpoint_suffix

        self.AZURE_STORAGE_KEY = key
        self.AZURE_STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol={0};AccountName={1};AccountKey={2};EndpointSuffix={3}'.format(
            self.PROTOCOL, self.AZURE_STORAGE_ACCOUNT, self.AZURE_STORAGE_KEY, self.ENDPOINT_SUFFIX)

        self.container_url = _make_url(f'https://{self.AZURE_STORAGE_ACCOUNT}.blob.core.windows.net',
                                       self.AZURE_STORAGE_CONTAINER)

        self.container_client = ContainerClient.from_connection_string(self.AZURE_STORAGE_CONNECTION_STRING, self.AZURE_STORAGE_CONTAINER)

        self.blob_service_client = BlobServiceClient.from_connection_string(self.AZURE_STORAGE_CONNECTION_STRING)
        self.blob_client = None

    @classmethod
    def from_connection_string(cls, connection_string, container_name, credential=None, **kwargs):
        conn = ContainerClient.from_connection_string(connection_string, container_name=container_name,
                                                      credential=credential, **kwargs)


        pass
        #TODO BREAK CONN APART TO WORK WITH THIS CLASS
        return cls(conn.account_name, conn.container_name, conn.credential.account_key)

    def _check_client(self, blob_client=None, blob_path=None, sas_url=None):
        if blob_client:
            return blob_client
        if self.blob_client:
            return self.blob_client
        if not self.blob_client and not blob_path and not sas_url:
            return Exception('blob_path or sas_url required.')

        return self.create_blob_client(blob_path=blob_path, sas_url=sas_url)

    def create_blob_client(self, blob_path=None, sas_url=None):
        if sas_url:
            blob_client = BlobClient.from_blob_url(sas_url)
        elif blob_path and self.AZURE_STORAGE_CONNECTION_STRING and self.AZURE_STORAGE_CONTAINER:
            blob_client = BlobClient.from_connection_string(self.AZURE_STORAGE_CONNECTION_STRING,
                                                            self.AZURE_STORAGE_CONTAINER,
                                                            blob_path)
        else:
            raise Exception('blob_path or sas_url required.')
        return blob_client

    def upload_blob(self, file, blob_path=None, blob_client=None, sas_url=None):
        blob_client = self._check_client(blob_client=blob_client, blob_path=blob_path, sas_url=sas_url)

        with open(Path(file), 'rb') as data:
            blob_client.upload_blob(data)

    def create_sas(self, blob_path, hour_exp: float, **permissions):
        permissions = BlobSasPermissions(read=permissions.pop('read', True), write=permissions.pop('write', True),
                                         delete=permissions.pop('delete', True), add=permissions.pop('add', True),
                                         create=permissions.pop('create', True))

        sas_token = generate_blob_sas(
            self.container_client.account_name,
            self.container_client.container_name,
            blob_name=blob_path,
            permission=permissions,
            account_key=self.container_client.credential.account_key,
            expiry=datetime.utcnow() + timedelta(hours=hour_exp),
            start=datetime.utcnow() - timedelta(minutes=1)
        )

        blob_path = urllib.parse.quote(blob_path)
        sas_url = _make_url(self.container_url, blob_path, sas_token=sas_token)

        return sas_url

    def list_blobs_in_container(self, name_starts_with=None, include=None):
        return self.container_client.list_blobs(name_starts_with=name_starts_with, include=include)

    def set_blob_metadata(self, metadata: [str, dict], blob_path=None, sas_url=None, blob_client=None):
        blob_client = self._check_client(blob_client=blob_client, blob_path=blob_path, sas_url=sas_url)
        blob_client.set_blob_metadata(metadata)

    def clear_blob_metadata(self, blob_path=None, sas_url=None, blob_client=None):
        blob_client = self._check_client(blob_client=blob_client, blob_path=blob_path, sas_url=sas_url)
        blob_client.set_blob_metadata()

    def get_blob_metadata(self, blob_path=None, sas_url=None, blob_client=None):
        blob_client = self._check_client(blob_client=blob_client, blob_path=blob_path, sas_url=sas_url)
        return blob_client.get_blob_properties()

    def download_from_container(self, blob_path: [str, Path], dest: [str, Path], blob_client: str = None,
                                sas_url: str = None,
                                delete_after: bool = False,
                                move_after: [str, Path] = None, ignored_file_types: list = None,
                                only_file_types: list = None, *sub_folders, **set_metadata):

        blob_client = self._check_client(blob_client=blob_client, blob_path=blob_path, sas_url=sas_url)
        blob_path = Path(blob_path)
        blob_ext = blob_path.suffix

        dest_file = Path(dest, *sub_folders, blob_path)
        dest_folder = Path(dest_file).parent

        if only_file_types and isinstance(only_file_types, list):
            [_.upper() for _ in only_file_types]
            if blob_ext.upper()[1:] in only_file_types:
                _create_folder(dest_folder)
                self._download(dest_file, blob_path=blob_path, sas_url=sas_url, blob_client=blob_client)

        elif not ignored_file_types:
            [_.upper() for _ in ignored_file_types]
            if blob_ext.upper()[1:] in ignored_file_types:
                if set_metadata:
                    set_metadata['DOWNLOAD'] = 'IGNORED DUE TO FILETYPE'
        else:
            _create_folder(dest_folder)
            self._download(dest_file, blob_path=blob_path, sas_url=sas_url, blob_client=blob_client)

        if set_metadata:
            self.set_blob_metadata(**set_metadata, blob_path=blob_path, blob_client=blob_client)

        if delete_after:
            self.delete_from_container(blob_path=blob_path, sas_url=sas_url, blob_client=blob_client)
        elif move_after:
            self.move_blob(blob_path, blob_path, sas_url=sas_url, blob_client=blob_client)

    def move_blob(self, blob_path, new_blob_path, dest_container=None, sas_url=None, blob_client=None):
        blob_client = self._check_client(blob_client=blob_client, blob_path=blob_path, sas_url=sas_url)
        self._move_copy_blob_to_container(blob_path, new_blob_path, blob_client, dest_container, 'move')

    def copy_blob(self, blob_path, new_blob_path, dest_container=None, sas_url=None, blob_client=None):
        blob_client = self._check_client(blob_client=blob_client, blob_path=blob_path, sas_url=sas_url)
        self._move_copy_blob_to_container(blob_path, new_blob_path, blob_client, dest_container, 'copy')

    def delete_from_container(self, blob_path=None, sas_url=None, blob_client=None):
        blob_client = self._check_client(blob_client=blob_client, blob_path=blob_path, sas_url=sas_url)
        blob_client.delete_blob()

    def _move_copy_blob_to_container(self, blob_path, new_blob_path, blob_client, dest_container=None, action=None):

        if self.AZURE_STORAGE_CONTAINER == dest_container and blob_path == new_blob_path:
            raise Exception('Old and new blobs have the same name, in the same container.')

        if dest_container:
            dest_blob = self.blob_service_client.get_blob_client(dest_container, new_blob_path)
        else:
            dest_blob = self.blob_service_client.get_blob_client(self.AZURE_STORAGE_CONTAINER, new_blob_path)

        dest_blob.start_copy_from_url(blob_client.sas_url)

        if action == 'move':
            self.delete_from_container(blob_path, blob_client=blob_client)
        elif action == 'copy':
            pass
        else:
            raise Exception('Invalid action.  Use "copy" or "move"')

    def _download(self, dest, blob_path=None, sas_url=None, blob_client=None):
        blob_client = self._check_client(blob_client=blob_client, blob_path=blob_path, sas_url=sas_url)
        with open(dest, 'wb') as download_file:
            download_file.write(blob_client.download_blob().readall())


if __name__ == '__main__':
    s = os.environ.get('CONN')
    lb = Client.from_connection_string(s, os.environ.get('CONTAINER'))

    x = lb.list_blobs_in_container()
    pass