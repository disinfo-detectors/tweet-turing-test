# tweet_turing_paths.py
#   A collection of constants (dicts, lists, strings, etc.) for locating
#   and storing data files for both local storage and GCP cloud storage.
#   


# setup location of data files
#   files when loading local data
local_data_paths = {
    "govt_entities": "../data/raw/tweets-govt_entities/",
    "individuals": "../data/raw/tweets-individuals/",
    "news_orgs": "../data/raw/tweets-news_orgs/",
    "random": "../data/raw/tweets-random/",
    "troll": "../data/raw/tweets-troll"
}

local_snapshot_paths = {
    "json_snapshot": "../data/snapshot/",
    "csv_snapshot": "../data/snapshot/"
}

#   files when loading data from GCP bucket
gcp_data_paths = {
    "govt_entities": "raw/govt/",
    "individuals": "raw/individuals/",
    "news_orgs": "raw/news/",
    "random": "raw/random/",
    "troll": "raw/troll/"
}

gcp_snapshot_paths = {
    "json_snapshot": "snapshot/",
    "csv_snapshot": "snapshot/"
}

gcp_project_name: str = "ds-capstone-jmmr"
gcp_bucket_name: str = "disinfo-detector-tweet-turing-test"
gcp_key_file: str = "../key/service_acct_key.json"
