import os
from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import PublicAccess
from django.conf import settings

# Microsoft Azure Blob Storage Documentation (Python)
# https://docs.microsoft.com/en-us/samples/azure/azure-sdk-for-python/storage-blob-samples/
# pip:
#   pip install azure-storage-blob
# requirements (as @ 2022:05:09):
#   azure-storage-blob==12.9.0

class Azure_Blob_Storage:
    """
    This class covers the main processes in managing Azure Bulk Storage Accounts.
    Functions:
        get_files                   | This function returns all files in a specified folder.
        uploadFilesToBlobStorage    | This function uploads one or more files to Azure Blob Storage.
        deleteFileFromBlobStorage   | This function deletes a file from Azure Blob Storage.
        downloadFileFromBlobStorage | This function downloads a file from Azure Blob Stroage.
        createBlobStorageContainer  | This function creates an Azure Blob Stroage Container.
        deleteBlobStorageContainer  | This function deletes an Azure Blob Stroage Container.

    Initilisateion:
        Unless otherwise specified, this class gets the Azure Blob Storage Connection String from setting.AZURE_STORAGE_CONNECTION_STRING

    myBlobStorage = Azure_Blob_Storage(
            azure_storage_connectionstring="my_connection_string"
    )

    #________________________________________________________________
    files = myBlobStorage.get_files(
        dir="../.../.."
        file_name="my_file.txt"
    )

    myBlobStorage.uploadFilesToBlobStorage(
        files=get_files(dir="../.../..",file_name="my_file.txt"),
        container_name="mycontainer"
    )

    myBlobStorage.deleteFileFromBlobStorage(
        file="my_file.txt",
        container_name="mycontainer"
    )

    myBlobStorage.downloadFileFromBlobStorage(
        path="../.../..",
        file="my_file.txt",
        container_name="mycontainer"
    )

    myBlobStorage.createBlobStorageContainer(
        container_name="mycontainer", 
        private=True
    )

    myBlobStorage.createBlobStorageContainer(
        container_name="mycontainer"
    )
    #________________________________________________________________
    """
    def __init__(self, azure_storage_connectionstring=settings.AZURE_STORAGE_CONNECTION_STRING):
        """
        This function gets the Azure Blob Storage Connection String from setting.AZURE_STORAGE_CONNECTION_STRING which can be overridden.

        Args:
            azure_storage_connectionstring | settings.AZURE_STORAGE_CONNECTION_STRING | string (optional)
        
        #________________________________________________________________
        myBlobStorage = Azure_Blob_Storage(
            azure_storage_connectionstring="my_connection_string"
        )
        #________________________________________________________________
        """
        self.azure_storage_connectionstring = azure_storage_connectionstring
        
    def get_files(self, dir, file_name = ""):
        """
        This function returns all files in a specified folder.
        If the file_name parameter is not "" it will only return a single file.

        Return (bool):
            True | Passed
            False | Failed;
                if in debug mode it will print teh exception

        Args: 
            dir | "../.../.." | string
            file_name | "my_file.txt" | string

        #________________________________________________________________
        files = myBlobStorage.get_files(
            dir="../.../.."
            file_name="my_file.txt"
        )
        #________________________________________________________________
        """
        with os.scandir(dir) as entries:
            for entry in entries:
                try:
                    if file_name == "":
                        if entry.is_file() and not entry.name.startswith('.'):
                            yield entry
                    elif entry.name == file_name:
                        yield entry
                except Exception as e:
                    if settings.DEBUG: print(e)

    def uploadFilesToBlobStorage(self, files, container_name):
        """
        This function uploads one or more files to Azure Blob Storage.

        Return (bool):
            True | Passed
            False | Failed;
                if in debug mode it will print teh exception

        Args: 
            files | get_files(dir="../.../..",file_name="my_file.txt") | file
            container_name | "mycontainer" | string

        #________________________________________________________________
        myBlobStorage.uploadFilesToBlobStorage(
            files=get_files(dir="../.../..",file_name="my_file.txt"),
            container_name="mycontainer"
        )
        #________________________________________________________________
        """
        container_client = ContainerClient.from_connection_string(container_name,self.azure_storage_connectionstring)   
        for file in files:
            blob_client = container_client.get_blob_client(file.name)
            with open(file.path, "rb") as data:
                try:
                    blob_client.delete_blob()
                except:
                    pass
                try:
                    blob_client.upload_blob(data)
                except Exception as e:
                    if settings.DEBUG: print(e)
                data.close()
                os.remove(file)

    def deleteFileFromBlobStorage(self, file, container_name):
        """
        This function deletes a file from Azure Blob Storage.

        Return (bool):
            True | Passed
            False | Failed;
                if in debug mode it will print teh exception

        Args:
            file | "my_file.txt" | string
            container_name | "mycontainer" | string

        #________________________________________________________________
        myBlobStorage.deleteFileFromBlobStorage(
            file="my_file.txt",
            container_name="mycontainer"
        )
        #________________________________________________________________
        """
        container_client = ContainerClient.from_connection_string(container_name, self.azure_storage_connectionstring)
        blob_client = container_client.get_blob_client(file)  
        try:
            blob_client.delete_blob()
            return True
        except Exception as e:
            if settings.DEBUG: print(e)
            return False

    def downloadFileFromBlobStorage(self, path, file, container_name):
        """
        This function downloads a file from Azure Blob Stroage.

        Return (bool):
            True | Passed
            False | Failed;
                if in debug mode it will print teh exception

        Args:
            path | "../.../.." | string
            file | "my_file.txt" | string
            container_name | "mycontainer" | string

        #________________________________________________________________
        myBlobStorage.downloadFileFromBlobStorage(
            path="../.../..",
            file="my_file.txt",
            container_name="mycontainer"
        )
        #________________________________________________________________
        """
        path = path + "/" + file
        container_client = ContainerClient.from_connection_string(self.azure_storage_connectionstring, container_name) 
        blob_client = container_client.get_blob_client(file)  
        try:
            with open(path, "wb") as my_blob:
                download_stream = blob_client.download_blob()
                my_blob.write(download_stream.readall())
            return True
        except Exception as e:
            if settings.DEBUG: print(e)
            return False


    def createBlobStorageContainer(self, container_name,private=True):
        """
        This function creates an Azure Blob Stroage Container.

        Return (bool):
            True | Passed
            False | Failed;
                if in debug mode it will print teh exception

        Args:
            container_name | "mycontainer" | string
            private | True | bool (optional)

        #________________________________________________________________
        myBlobStorage.createBlobStorageContainer(
            container_name="mycontainer", 
            private=True
        )
        #________________________________________________________________
        """
        blob_service_client = BlobServiceClient.from_connection_string(self.azure_storage_connectionstring)
        container_client = blob_service_client.get_container_client(container_name)
        try:
            if private:
                container_client.create_container()
            else:
                container_client.create_container(public_access=PublicAccess.Container)
            return True
        except Exception as e:
            if settings.DEBUG: print(e)
            return False

    def deleteBlobStorageContainer(self, container_name,):
        """
        This function deletes an Azure Blob Stroage Container.

        Return (bool):
            True | Passed
            False | Failed;
                if in debug mode it will print teh exception

        Args:
            container_name | "mycontainer" | string

        #________________________________________________________________
        myBlobStorage.createBlobStorageContainer(
            container_name="mycontainer"
        )
        #________________________________________________________________
        """
        blob_service_client = BlobServiceClient.from_connection_string(self.azure_storage_connectionstring)
        container_client = blob_service_client.get_container_client(container_name)
        try:
            container_client.delete_container()
            return True
        except Exception as e:
            if settings.DEBUG: print(e)
            return False

    def __repr__(self):
        return self.azure_storage_connectionstring

    def __str__(self):
        return self.azure_storage_connectionstring
