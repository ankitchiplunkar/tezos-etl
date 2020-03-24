from tezos_etl import settings
from tezos_etl.session import (
    setup_logging,
)
from tezos_etl.load import (
    read_from_avro,
    write_to_avro,
    bytesio_to_file,
)
import logging


setup_logging(settings)
logger = logging.getLogger(__name__)

table_names = {
    'Activation': 'activation',
    'BalanceUpdate': 'balance_update',
    'Ballot': 'ballot',
    'Block': 'block',
    'Delegation': 'delegation',
    'DoubleBakingEvidence': 'double_baking_evidence',
    'DoubleEndorsementEvidence': 'double_endorsement_evidence',
    'Endorsement': 'endorsement',
    'Nonce': 'nonce',
    'Operation': 'operation',
    'Origination': 'origination',
    'Proposal': 'proposal',
    'Reveal': 'reveal',
    'Transaction': 'transaction',
}
start_block_number = 0
end_block_number = 870000
batch_size = 1000

for data_type in table_names.keys():
    for batch in range(start_block_number, end_block_number, batch_size):
        final_file_name = f"{settings.BATCH_AVRO_LOCAL_STORAGE}/{data_type}/{data_type.lower()}_{batch}_{batch+batch_size-1}.avro"
        data_list = []
        for b in range(batch, batch+batch_size):
            file_name = f"{settings.AVRO_LOCAL_STORAGE}/{data_type}/{b}.avro"
            data_list += read_from_avro(file_name)
        c = write_to_avro(data_list, data_type)
        bytesio_to_file(c, final_file_name)
        logger.info(f"Successfully wrote file {final_file_name}")
