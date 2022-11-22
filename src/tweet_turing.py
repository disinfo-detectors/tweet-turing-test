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


# constants
csv_column_dtype_mapping: dict = {
    "external_author_id": "string",
    "author": "string",
    "content": "string",
    "region": "string",
    "language": "string",
    "publish_date": "string",   # converted to datetime later
    "harvested_date": "string", # converted to datetime later
    "following": "uint64",
    "followers": "uint64",
    "updates": "uint64",
    "post_type": "string",
    "account_type": "string",
    "retweet": "uint8",
    "account_category": "string",
    "new_june_2018": "uint8",
    "alt_external_id": "string",
    "tweet_id": "string",
    "article_url": "string",
    "tco1_step1": "string",
    "tco2_step1": "string",
    "tco3_step1": "string"
}


authentic_df_eda_dtype_mapping = {
    'author_id': 'string',
    'created_at': 'string',
    'id': 'string',
    'text': 'string',
    'lang': 'string',
    'referenced_tweets': 'object',
    'public_metrics.retweet_count': 'uint64', 
    'public_metrics.reply_count': 'uint64', 
    'public_metrics.like_count': 'uint64', 
    'public_metrics.quote_count': 'uint64',
    'author.location': 'string', 
    'author.name': 'string', 
    'author.username': 'string', 
    'author.public_metrics.followers_count': 'uint64',
    'author.public_metrics.following_count': 'uint64', 
    'author.entities.url.urls': 'object', 
    'author.created_at': 'string',
    'author.verified': 'uint8', 
    'context_annotations': 'object', 
    'entities.annotations': 'object', 
    'entities.mentions': 'object',
    'entities.hashtags': 'object', 
    'entities.urls': 'object',
    'data_source': 'string'
    }


# functions
####################################
#### MERGING AND FILE FUNCTIONS ####
####################################


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


def load_local_json_parquet(filepath: str, engine: str = 'pyarrow') -> list:
    """TODO: description"""
    return pd.read_parquet(filepath, engine=engine)


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
    csv_df: pd.DataFrame = pd.read_csv(
        file_list[0], 
        encoding='utf-8', 
        low_memory=False, 
        dtype=csv_column_dtype_mapping
        )

    # now load and concatenate onto that csv_df as we go
    if (len(file_list) == 1):
        return csv_df

    for file in file_list[1:]:
        new_df: pd.DataFrame = pd.read_csv(
            file, 
            encoding='utf-8', 
            low_memory=False, 
            dtype=csv_column_dtype_mapping
            )
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
    csv_df: pd.DataFrame = pd.read_csv(
        this_blob.open("r", encoding='utf-8'), 
        encoding='utf-8', 
        low_memory=False, 
        dtype=csv_column_dtype_mapping
        )
    
    # now load and concatenate onto that csv_df as we go
    if (len(object_list) == 1):
        return csv_df

    for obj in object_list[1:]:
        this_blob = get_gcp_object_as_blob(bucket, obj)
        new_df: pd.DataFrame = pd.read_csv(
            this_blob.open("r", encoding='utf-8'), 
            encoding='utf-8', 
            low_memory=False, 
            dtype=csv_column_dtype_mapping
            )
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
    
    
def set_gcp_object_from_df_as_parq(bucket: storage.Bucket, object_name: str, df: pd.DataFrame) -> None:
    """TODO: description"""
    new_blob: storage.Blob = bucket.blob(object_name)

    # check if blob already exists
    if (new_blob.exists()):
        pass    # do nothing, overwrite old version

    df.to_parquet(new_blob.open("wb"), engine='pyarrow', index=False)
    

def get_gcp_object_from_parq_as_df(bucket: storage.Bucket, object_name: str) -> pd.DataFrame:
    """TODO: description"""
    gcp_object: storage.Blob = get_gcp_object_as_blob(bucket=bucket, object_name=object_name)
    
    if (gcp_object == None):
        return None
    
    new_df: pd.DataFrame = pd.read_parquet(gcp_object.open("rb"), engine='pyarrow')
    
    return new_df


#################################
#### PREPROCESSING FUNCTIONS ####
#################################


def is_retweet(tweet_series: pd.Series) -> int:
    """Classifies a tweet as a retweet or not based on field `text`.
        Returns 1 if a provided tweet (as pandas Series) is a retweet, returns 0 otherwise.
        Intended to be applied as a mapped function to derive a new dataset feature."""
    # method 1
    return int(tweet_series['text'].startswith('RT @'))


def is_retweet_alt(tweet_series: pd.Series) -> int:
    """Uses an alternate method for determining retweet classification.
        Assumes upstream filter step is performed to remove "NaN" values in field `referenced_tweets`.
        Returns 1 if a provided tweet (as pandas Series) is a retweet, returns 0 otherwise.
        Intended to be applied as a mapped function to derive a new dataset feature."""
    # method 2
    return int(tweet_series['referenced_tweets'][0]['type'] == 'retweeted')


def has_url(tweet_series: pd.Series, search_str: str = 'http') -> int:
    """TODO: description"""
    if (tweet_series['content'] is not None):
        return int(search_str in tweet_series['content'])
    else:
        return 0


def convert_emoji_list(tweet_series: pd.Series) -> list:
    ''' The following converts a text string with emojis into a list of descriptive text strings.
        Duplicate emojis are captured as each emoji converts to 1 text string.'''
    return demoji.findall_list(tweet_series['content'])


def emoji_count(tweet_series: pd.Series) -> int:
    """TODO: description"""
    return len(tweet_series['emoji_text'])


if __name__ == '__main__':
    pass