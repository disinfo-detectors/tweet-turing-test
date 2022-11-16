# tweet_turing.py
#   A collection of functions used for the acquisition and pre-processing
#   pipeline for the Tweet Turing Test project.
#

# imports from Python standard library
import json
import logging
import os

# imports requiring installation
#   connection to Google Cloud Storage
from google.cloud import storage            # pip install google-cloud-storage
from google.oauth2 import service_account   # pip install google-auth

#  data science packages
import demoji                               # pip install demoji
import numpy as np                          # pip install numpy
import pandas as pd                         # pip install pandas


# module-level definitions
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


# functions
def get_json_files(path: str = "./data/"):
    '''TODO description'''
    # get list of JSON files in directory
    file_list = []
    try:
        file_list= os.listdir(path)
    except FileNotFoundError:
        logger.exception(f"get_json_files(): provided path could not be found. path='{path}'")
        return -1

    # create list to store only json files
    json_files = []

    for f in file_list:
        if f.endswith('.json'):
            if (path[-1] not in ["/", "\\"]):   # check if path includes a trailing slash
                trailing_slash = "/"            # TODO: expand for windows backslashes?
            else:
                trailing_slash = ""

            json_files.append(f"{path}{trailing_slash}{f}")
    
    return json_files


def load_local_json(filepath: str, encoding: str = 'utf-8') -> list:
    """Loads a local JSON file. Default encoding is 'utf-8'.
        Returns a list of dicts representing the JSON data."""
    json_data: list = []

    try:
        with open(file=filepath, mode='r', encoding=encoding) as json_file_handle:
            json_data = json.load(json_file_handle)
    except Exception:
        logger.exception(f"load_local_json(): error loading JSON from file '{filepath}'")
        return -1

    return json_data


def load_gcp_json(bucket: storage.Bucket, object_name: str):
    """Wrapper for get_gcp_object_as_json."""
    return get_gcp_object_as_json(bucket=bucket, object_name=object_name)


def merge_json_files(file_list, output_filehandle = None) -> list:
    """TODO description"""
    # initialize empty lists
    result = []
    json_data = []

    # iterate over file_list, extending list `result`
    for f in file_list:
        try:
            with open(file=f, mode='r', encoding='utf-8') as fh:
                json_data = json.load(fh)
        except Exception:
            logger.exception(f"merge_json_files(): error loading JSON from file '{f}'")
            return -1
            
        result.extend(json_data)
    
    # save to a file if output_filehandle was provided
    if output_filehandle is not None:
        # indicates to save the result to a file
        json.dump(result, output_filehandle)
    
    # return the result
    return result


# TODO -> could combine this function with get_json_files, make file extension an argument
def get_csv_files(path: str = "./data/"):
    '''TODO description'''
    # get list of CSV files in directory
    file_list = []
    try:
        file_list= os.listdir(path)
    except FileNotFoundError:
        logger.exception(f"get_csv_files(): provided path could not be found. path='{path}'")
        return -1

    # create list to store only csv files
    csv_files = []

    for f in file_list:
        if f.endswith('.csv'):
            if (path[-1] not in ["/", "\\"]):   # check if path includes a trailing slash
                trailing_slash = "/"            # TODO: expand for windows backslashes?
            else:
                trailing_slash = ""

            csv_files.append(f"{path}{trailing_slash}{f}")
    
    return csv_files


def merge_csv_files(file_list) -> pd.DataFrame:
    """TODO description"""
    # check for no files in file_list
    if (len(file_list) == 0):
        return None
    
    # create dataframe of first CSV
    csv_df: pd.DataFrame = pd.read_csv(file_list[0], encoding='utf-8', low_memory=False)

    # now load and concatenate onto that csv_df as we go
    if (len(file_list) == 1):
        return csv_df

    for file in file_list[1:]:
        new_df: pd.DataFrame = pd.read_csv(file, encoding='utf-8', low_memory=False)
        csv_df = pd.concat([csv_df, new_df])

    return csv_df


def get_gcp_storage_client(project_name: str = "ds-capstone-jmmr", 
                            key_file: str = "../key/service_acct_key.json") -> storage.Client:
    """Creates and returns a storage client object for use with the Google Cloud Storage 
        client library. Enables access to cloud storage buckets within the specified `project_name`.
        Authenticates with the supplied service account key in `key_file`."""
    credentials: service_account.Credentials = None
    
    try:
        credentials = service_account.Credentials.from_service_account_file(key_file)
    except FileNotFoundError:
        logger.exception(f"get_gcp_storage_client(): provided key_file could not be found. key_file='{key_file}'")
        return -1
    except ValueError:
        logger.exception(f"get_gcp_storage_client(): provided key_file didn't use correct format. key_file='{key_file}'")
        return -1
    
    storage_client: storage.Client = storage.Client(project=project_name, credentials=credentials)

    return storage_client


def get_gcp_bucket(storage_client: storage.Client, bucket_name: str = "disinfo-detector-tweet-turing-test") -> storage.Bucket:
    """TODO: description"""
    if (type(storage_client) == int):
        # indicates `get_gcp_storage_client()` had an error
        logger.exception(f"get_gcp_bucket(): provided storage_client had an issue. storage_client='{storage_client}'")
        return -1
    
    return storage_client.get_bucket(bucket_name)


def list_gcp_objects(storage_client: storage.Client, bucket_name: str = "disinfo-detector-tweet-turing-test", 
                        obj_prefix: str = ""):
    """TODO: description"""
    # according to docs, `Bucket.list_blobs(...)` is deprecated, 
    #   with `Client.list_blobs()` called out as its replacement
    blob_list = storage_client.list_blobs(bucket_or_name=bucket_name, prefix=obj_prefix)
    blob_list_str = [obj.name for obj in blob_list if (obj.name != obj_prefix)]

    return blob_list_str


def get_gcp_object_as_json(bucket: storage.Bucket, object_name: str) -> dict:
    """TODO: description"""
    gcp_object: storage.Blob = bucket.get_blob(object_name)

    if (gcp_object == None):
        return None
    
    gcp_object_text: str = gcp_object.download_as_text()

    return json.loads(gcp_object_text)


def get_gcp_object_as_text(bucket: storage.Bucket, object_name: str) -> str:
    """TODO: description"""
    gcp_object: storage.Blob = bucket.get_blob(object_name)

    if (gcp_object == None):
        return None
    
    gcp_object_text: str = gcp_object.download_as_text()

    return gcp_object_text


def get_gcp_object_as_blob(bucket: storage.Bucket, object_name: str) -> storage.Blob:
    """TODO: description"""
    return bucket.blob(object_name)    # using .blob() instead of .get_blob() to avoid downloading too early


def merge_gcp_csv_files(bucket: storage.Bucket, object_list: list) -> pd.DataFrame:
    """TODO: description"""
    # check for no files in file_list
    if (len(object_list) == 0):
        return None
    
    # create dataframe of first CSV
    this_blob: storage.Blob = get_gcp_object_as_blob(bucket, object_list[0])
    csv_df: pd.DataFrame = pd.read_csv(this_blob.open("r", encoding='utf-8'), encoding='utf-8', low_memory=False)
    
    # now load and concatenate onto that csv_df as we go
    if (len(object_list) == 1):
        return csv_df

    for obj in object_list[1:]:
        this_blob = get_gcp_object_as_blob(bucket, obj)
        new_df: pd.DataFrame = pd.read_csv(this_blob.open("r", encoding='utf-8'), encoding='utf-8', low_memory=False)
        csv_df = pd.concat([csv_df, new_df])

    return csv_df


def merge_gcp_json_files(bucket: storage.Bucket, object_list: list):
    """TODO: description"""
    # initialize empty lists
    result = []
    json_data = []

    # iterate over file_list, extending list `result`
    for obj in object_list:
        json_data = get_gcp_object_as_json(bucket=bucket, object_name=obj)
        result.extend(json_data)
    
    # return the result
    return result


def set_gcp_object_from_json(bucket: storage.Bucket, object_name: str, json_data: dict) -> None:
    """TODO: description"""
    new_blob: storage.Blob = bucket.blob(object_name)

    # check if blob already exists
    if (new_blob.exists()):
        pass    # do nothing, overwrite old version

    new_blob.upload_from_string(json.dumps(json_data))





if __name__ == '__main__':
    pass