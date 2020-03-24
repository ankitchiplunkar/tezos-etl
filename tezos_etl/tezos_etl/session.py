from tezos_etl.tezos_client import TezosClient
import logging
import sys
from google.cloud import storage
from google.oauth2 import service_account


logger = logging.getLogger(__name__)


def setup_logging(settings):
    """
    Add logging format to logger used for debugging and info
    """
    handler = logging.StreamHandler(
        sys.stdout if settings.LOG_STDOUT else sys.stderr)
    formatter = logging.Formatter(settings.LOG_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(settings.LOG_LEVEL)


def setup_node_session(settings):
    return TezosClient(settings.NODE_URL, settings.CHAIN_ID)


def setup_gcs_buckets(settings):
    storage_client = storage.Client.from_service_account_json(
        settings.GOOGLE_APPLICATION_CREDENTIALS)
    raw_bucket = storage_client.get_bucket(settings.RAW_DATA_STORAGE)
    avro_bucket = storage_client.get_bucket(settings.AVRO_DATA_STORAGE)
    return raw_bucket, avro_bucket