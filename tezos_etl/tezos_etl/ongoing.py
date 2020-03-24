from tezos_etl import settings
from tezos_etl.session import (
    setup_logging,
    setup_gcs_buckets,
)
from tezos_etl.tezos_client import TezosClient
from tezos_etl.manage import (
    get_last_block_number,
    get_last_block_number_locally,
    update_last_block_number,
    update_last_block_number_locally,
    initialize_block_batch,
    update_block_batch,
)
import logging


setup_logging(settings)
logger = logging.getLogger(__name__)


raw_bucket, avro_bucket = setup_gcs_buckets(settings)
tezos_client = TezosClient(
    settings.NODE_URL, settings.CHAIN_ID, raw_bucket, avro_bucket)
start_block_number = get_last_block_number_locally(settings.AVRO_LOCAL_STORAGE)
end_block_number = 10
batch_size = settings.BATCH_SIZE

for batch in range(start_block_number, end_block_number, batch_size):
    all_batch_data = initialize_block_batch()
    file_name = f"{batch}_{batch+batch_size-1}.avro"
    for block_number in range(batch, batch+batch_size):
        response = tezos_client.get_local_block_data(block_number)
        all_block_data = tezos_client.transform_response(response)
        all_batch_data = update_block_batch(all_batch_data, all_block_data)
    tezos_client.save_avro_files_locally(file_name, all_batch_data)
    update_last_block_number_locally(settings.AVRO_LOCAL_STORAGE, batch)
    logger.info(f"Finished batch {batch/batch_size}")
