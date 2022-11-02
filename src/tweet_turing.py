# tweet_turing.py
#   A collection of functions used for the acquisition and pre-processing
#   pipeline for the Tweet Turing Test project.
#
#   Suggested use: `import tweet_turing as tt`

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
def get_json_files(path: str = "./data/") -> list[str]:
    '''TODO description'''
    # get list of JSON files in directory
    file_list: list[str] = []
    try:
        file_list= os.listdir(path)
    except FileNotFoundError:
        logger.exception(f"get_json_files(): provided path could not be found. path='{path}'")
        return -1

    # create list to store only json files
    json_files: list[str] = []

    for f in file_list:
        if f.endswith('.json'):
            if (path[-1] not in ["/", "\\"]):   # check if path includes a trailing slash
                trailing_slash = "/"            # TODO: expand for windows backslashes?
            else:
                trailing_slash = ""

            json_files.append(f"{path}{trailing_slash}{f}")
    
    return json_files


def merge_json_files(file_list: list[str], output_filehandle = None) -> list[dir]:
    """TODO description"""
    # initialize empty lists
    result: list[dict] = []
    json_data: list[dict] = []

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
def get_csv_files(path: str = "./data/") -> list[str]:
    '''TODO description'''
    # get list of CSV files in directory
    file_list: list[str] = []
    try:
        file_list= os.listdir(path)
    except FileNotFoundError:
        logger.exception(f"get_csv_files(): provided path could not be found. path='{path}'")
        return -1

    # create list to store only csv files
    csv_files: list[str] = []

    for f in file_list:
        if f.endswith('.csv'):
            if (path[-1] not in ["/", "\\"]):   # check if path includes a trailing slash
                trailing_slash = "/"            # TODO: expand for windows backslashes?
            else:
                trailing_slash = ""

            csv_files.append(f"{path}{trailing_slash}{f}")
    
    return csv_files


def merge_csv_files(file_list: list[str]) -> pd.DataFrame:
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
        new_df: pd.DataFrame = pd.read_csv(file, encoding='utf-8')
        csv_df = pd.concat([csv_df, new_df])

    return csv_df


def get_gcp_storage_client(project_name: str = "ds-capstone-jmmr", credentials = None):
    pass    # plan to return a storage client, authenticate with supplied credentials


def get_gcp_bucket(bucket_name: str = "disinfo-detector-tweet-turing-test"):
    pass    # plan to return the bucket object


def list_gcp_objects(bucket, prefix: str = "") -> list[str]:
    pass    # plan to return the list of objects in bucket with provided prefix


def get_gcp_object_as_json(bucket, object_name: str) -> list[dict]:
    pass    # plan to get object as blob, download as text, then json.loads to return json


if __name__ == '__main__':
    pass