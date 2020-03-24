from subprocess import call
DATABASE = 'tezos_etl'
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


def create_table(data_type, table_name):
    cmd = f"""bq load --autodetect --source_format=AVRO --time_partitioning_type=DAY \
            --time_partitioning_field=timestamp --use_avro_logical_types=True \
            {DATABASE}.{table_name} gs://tezos-avro-data/{data_type}/* \
          """
    print(f"Creating table {DATABASE}.{data_type}")
    return call(cmd.split())


def delete_table(table_name):
    cmd = f"bq rm --force=True --table=True {DATABASE}.{table_name}"
    print(f"Deleting table {DATABASE}.{data_type}")
    return call(cmd.split())


if __name__ == '__main__':
    for data_type, table_name in table_names.items():
        create_table(data_type, table_name)
        # delete_table(table_name)
