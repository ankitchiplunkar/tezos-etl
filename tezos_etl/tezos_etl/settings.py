import os
# file to initialize variables and enable to set them up using enviornment variables
# Logging settings
LOG_STDOUT = os.getenv("LOG_STDOUT", "TRUE")
LOG_FORMAT = os.getenv(
    "LOG_FORMAT", "[%(asctime)s][%(levelname)s][%(name)s] %(message)s")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "eth_parser.log")

NODE_URL = os.getenv("NODE_URL", "http://localhost:8732")
CHAIN_ID = os.getenv("CHAIN_ID", "main")

# Gcloud settings
AVRO_DATA_STORAGE = os.getenv("AVRO_DATA_STORAGE", "tezos-avro-data")
RAW_DATA_STORAGE = os.getenv("RAW_DATA_STORAGE", "tezos-raw-data")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS", "tezos-query.json")

# Local settings
AVRO_LOCAL_STORAGE = os.getenv("AVRO_LOCAL_STORAGE", "../avro-data")
RAW_LOCAL_STORAGE = os.getenv("RAW_LOCAL_STORAGE", "../raw-data")
BATCH_AVRO_LOCAL_STORAGE = os.getenv("BATCH_AVRO_LOCAL_STORAGE", "../avro-batch-data")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))


TABLE_NAMES = [
        'Activation',
        'BalanceUpdate',
        'Ballot',
        'Block',
        'Delegation',
        'DoubleBakingEvidence',
        'DoubleEndorsementEvidence',
        'Endorsement',
        'Nonce',
        'Operation',
        'Origination',
        'Proposal',
        'Reveal',
        'Transaction']