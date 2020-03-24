import json
import logging
from tezos_etl.settings import TABLE_NAMES

logger = logging.getLogger(__name__)

BLOCK_CHECKPOINT_FILE = "last_block"


def get_last_block_number(avro_bucket):
    try:
        blob = avro_bucket.get_blob(BLOCK_CHECKPOINT_FILE)
        return int(blob.download_as_string())
    except AttributeError:
        logging.info("last block not found starting from scratch")
        return 0


def update_last_block_number(avro_bucket, last_block_number):
    blob = avro_bucket.blob(BLOCK_CHECKPOINT_FILE)
    blob.upload_from_string(str(last_block_number))


def get_last_block_number_locally(avro_local_location):
    file_name = f"{avro_local_location}/{BLOCK_CHECKPOINT_FILE}"
    try:
        return int(read_from_file(file_name))
    except FileNotFoundError:
        logging.info("last block not found starting from scratch")
        return 0


def update_last_block_number_locally(avro_local_location, last_block_number):
    file_name = f"{avro_local_location}/{BLOCK_CHECKPOINT_FILE}"
    write_to_file(file_name, str(last_block_number))


def write_to_file(file_name, data):
    with open(file_name, "w") as f:
        f.write(data)


def read_from_file(file_name):
    with open(file_name, "r") as f:
        return f.read()


def initialize_block_batch():
    all_batch_data = {}
    for table in TABLE_NAMES:
        all_batch_data.update({table: []})
    return all_batch_data


def update_block_batch(all_batch_data, all_block_data):
    for table in TABLE_NAMES:
        all_batch_data[table] += all_block_data[table]
    return all_batch_data
