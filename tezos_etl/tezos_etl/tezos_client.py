import json
from google.cloud import storage
import requests
import logging
from tezos_etl.transform import (
    transform_operations,
    transform_balance_updates,
    transform_block,
    transform_operation_types,
)
from tezos_etl.load import (
    write_to_avro,
    upload_file_to_gcs,
    bytesio_to_file,
)
from tezos_etl.settings import (
    AVRO_LOCAL_STORAGE,
    RAW_LOCAL_STORAGE,
)
from tezos_etl.manage import (
    read_from_file,
    write_to_file,
)

logger = logging.getLogger(__name__)


class TezosClient():

    def __init__(self, node_url, chain_id, raw_bucket, avro_bucket):
        self.node_url = node_url
        self.chain_id = chain_id
        self.raw_bucket = raw_bucket
        self.avro_bucket = avro_bucket

    def get_block_data(self, block_id="head"):
        url = f"{self.node_url}/chains/{self.chain_id}/blocks/{block_id}/"
        r = requests.get(url)
        r.raise_for_status()
        logging.info(f"Extracted data for block {block_id}")
        return r.json()

    def get_local_block_data(self, block_number):
        file_name = f"{RAW_LOCAL_STORAGE}/{block_number}"
        return json.loads(read_from_file(file_name))

    def get_block_number(self, response):
        return response['header']['level']

    def get_gcs_block_data(self, block_number):
        data_str = self.raw_bucket.blob(str(block_number)).download_as_string()
        return json.loads(data_str)

    def save_response_to_gcs(self, block_number, response):
        blob = self.raw_bucket.get_blob(str(block_number))
        blob.upload_from_string(json.dumps(response))

    def save_response_to_local(self, block_number, response):
        write_to_file(f"{RAW_LOCAL_STORAGE}/{block_number}",
                      json.dumps(response))

    def transform_response(self, response):
        blocks = transform_block(response)
        balance_updates = transform_balance_updates(blocks, response)
        operations = transform_operations(blocks, response)
        all_block_data = transform_operation_types(blocks, response)
        all_block_data.update({
            "Block": blocks,
            "BalanceUpdate": balance_updates,
            "Operation": operations
        })
        logger.debug(f"Transformed block {blocks[0]['level']}")
        return all_block_data

    def upload_avro_files(self, block_number, all_block_data):
        for key, value in all_block_data.items():
            c = write_to_avro(value, key)
            file_name = f"{key}/{block_number}.avro"
            upload_file_to_gcs(self.avro_bucket, file_name, c)

    def save_avro_files_locally(self, file_name, all_block_data):
        for key, value in all_block_data.items():
            c = write_to_avro(value, key)
            final_file_name = f"{AVRO_LOCAL_STORAGE}/{key}/{file_name}.avro"
            bytesio_to_file(c, final_file_name)
        logger.info(f"Saved data for {file_name}")
