from datetime import datetime
from safe_cast import (
    safe_int,
    safe_cast,
)


def transform_block(response):
    blocks = []
    number_of_operations = 0
    for operation_group in response['operations']:
        number_of_operations += len(operation_group)
    blocks.append({
        'level': response['header']['level'],
        'proto': response['header']['proto'],
        'predecessor': response['header']['predecessor'],
        'timestamp': convert_timestr_to_timestamp(response['header']['timestamp'])*10**3,
        'validation_pass': response['header']['validation_pass'],
        'operations_hash': response['header']['operations_hash'],
        'fitness': response['header']['fitness'],
        'context': response['header']['context'],
        'protocol': response['protocol'],
        'chain_id': response['chain_id'],
        'block_hash': response['hash'],
        'nonce_hash': get_none_if_key_error(response['metadata'], 'nonce_hash'),
        'consumed_gas': safe_int(get_none_if_key_error(response['metadata'], 'consumed_gas')),
        'baker': get_none_if_key_error(response['metadata'], 'baker'),
        'voting_period_kind': get_none_if_key_error(response['metadata'], 'voting_period_kind'),
        'cycle': get_none_if_type_error(get_none_if_key_error(response['metadata'], 'level'), 'cycle'),
        'cycle_position': get_none_if_type_error(get_none_if_key_error(response['metadata'], 'level'), 'cycle_position'),
        'voting_period': get_none_if_type_error(get_none_if_key_error(response['metadata'], 'level'), 'voting_period'),
        'voting_period_position': get_none_if_type_error(get_none_if_key_error(response['metadata'], 'level'), 'voting_period_position'),
        'expected_commitment': get_none_if_type_error(get_none_if_key_error(response['metadata'], 'level'), 'expected_commitment'),
        'number_of_operation_groups': len(response['operations']),
        'number_of_operations': number_of_operations
    })
    return blocks


def transform_balance_update(blocks, balance_update):
    block = blocks[0]
    return {
        'level': block['level'],
        'timestamp': block['timestamp'],
        'block_hash': block['block_hash'],
        'type': balance_update['type'],
        'operation_hash': balance_update['operation_hash'],
        'kind': balance_update['balance_update']['kind'],
        'contract': get_none_if_key_error(balance_update['balance_update'], 'contract'),
        'change': int(balance_update['balance_update']['change']),
        'delegate': get_none_if_key_error(balance_update['balance_update'], 'delegate'),
        'category': get_none_if_key_error(balance_update['balance_update'], 'category')
    }


def transform_balance_updates(blocks, response):
    transformed_balance_updates = []
    i = 0
    for balance_update in yield_balance_update(response):
        transformed_balance_updates.append(
            transform_balance_update(blocks, balance_update)
        )
        i += 1
    return transformed_balance_updates


def transform_operation(blocks, operation_group_id, operation_id, operation):
    block = blocks[0]
    return {
        'level': block['level'],
        'timestamp': block['timestamp'],
        'block_hash': block['block_hash'],
        'branch': operation['branch'],
        'signature': get_none_if_key_error(operation, 'signature'),
        'operation_hash': operation['hash'],
        'operation_group_id': operation_group_id,
        'operation_id': operation_id
    }


def transform_operations(blocks, response):
    transformed_operations = []
    for operation_group_id, operation_id, operation in yield_operations(response):
        transformed_operations.append(
            transform_operation(blocks, operation_group_id,
                                operation_id, operation)
        )
    return transformed_operations


def transform_operation_types(blocks, response):
    transformed_operation_types = {
        'Endorsement': [],
        'Transaction': [],
        'Delegation': [],
        'Reveal': [],
        'Nonce': [],
        'Activation': [],
        'Origination': [],
        'Proposal': [],
        'DoubleBakingEvidence': [],
        'Ballot': [],
        'DoubleEndorsementEvidence': [],
    }
    for operation_group_id, operation_id, operation in yield_operations(response):
        transformed_operation = transform_operation(blocks, operation_group_id,
                                                    operation_id, operation)
        for content in operation['contents']:
            operation_type = content['kind'].capitalize()
            if operation_type == 'Endorsement':
                transformed_operation_types[operation_type].append(
                    transform_endorsement(content, transformed_operation)
                )
            elif operation_type == 'Transaction':
                transformed_operation_types[operation_type].append(
                    transform_transaction(content, transformed_operation)
                )
            elif operation_type == 'Delegation':
                transformed_operation_types[operation_type].append(
                    transform_delegation(content, transformed_operation)
                )
            elif operation_type == 'Reveal':
                transformed_operation_types[operation_type].append(
                    transform_reveal(content, transformed_operation)
                )
            elif operation_type == 'Seed_nonce_revelation':
                transformed_operation_types['Nonce'].append(
                    transform_nonce(content, transformed_operation)
                )
            elif operation_type == 'Activate_account':
                transformed_operation_types['Activation'].append(
                    transform_activation(content, transformed_operation)
                )
            elif operation_type == 'Origination':
                transformed_operation_types[operation_type].append(
                    transform_origination(content, transformed_operation)
                )
            elif operation_type == 'Proposals':
                transformed_operation_types['Proposal'].append(
                    transform_proposal(content, transformed_operation)
                )
            elif operation_type == 'Double_baking_evidence':
                transformed_operation_types['DoubleBakingEvidence'].append(
                    transform_double_baking_evidence(
                        content, transformed_operation)
                )
            elif operation_type == 'Double_endorsement_evidence':
                transformed_operation_types['DoubleEndorsementEvidence'].append(
                    transform_double_endorsement_evidence(
                        content, transformed_operation)
                )
            elif operation_type == 'Ballot':
                transformed_operation_types[operation_type].append(
                    transform_ballot(content, transformed_operation)
                )
            else:
                print(content)
                print(transformed_operation)
                raise KeyError(
                    f'Operation type {operation_type} not recognized')
    return transformed_operation_types


def transform_endorsement(content, transformed_operation):
    return {
        'level': transformed_operation['level'],
        'timestamp': transformed_operation['timestamp'],
        'block_hash': transformed_operation['block_hash'],
        'operation_hash': transformed_operation['operation_hash'],
        'operation_group_id': transformed_operation['operation_group_id'],
        'operation_id': transformed_operation['operation_id'],
        'delegate': content['metadata']['delegate'],
        'public_key': get_none_if_key_error(content, 'public_key'),
        'slots': content['metadata']['slots']
    }


def transform_transaction(content, transformed_operation):
    return {
        'level': transformed_operation['level'],
        'timestamp': transformed_operation['timestamp'],
        'block_hash': transformed_operation['block_hash'],
        'operation_hash': transformed_operation['operation_hash'],
        'operation_group_id': transformed_operation['operation_group_id'],
        'operation_id': transformed_operation['operation_id'],
        'source': content['source'],
        'destination': content['destination'],
        'fee': int(content['fee']),
        'amount': int(content['amount']),
        'counter': int(content['counter']),
        'gas_limit': int(content['gas_limit']),
        'storage_limit': int(content['storage_limit']),
        'status': content['metadata']['operation_result']['status'],
        'consumed_gas': safe_int(get_none_if_key_error(content['metadata']['operation_result'], 'consumed_gas')),
        'storage_size': safe_int(get_none_if_key_error(content['metadata']['operation_result'], 'storage_size'))
    }


def transform_delegation(content, transformed_operation):
    return {
        'level': transformed_operation['level'],
        'timestamp': transformed_operation['timestamp'],
        'block_hash': transformed_operation['block_hash'],
        'operation_hash': transformed_operation['operation_hash'],
        'operation_group_id': transformed_operation['operation_group_id'],
        'operation_id': transformed_operation['operation_id'],
        'source': content['source'],
        'delegate': get_none_if_key_error(content, 'delegate'),
        'fee': int(content['fee']),
        'counter': int(content['counter']),
        'gas_limit': int(content['gas_limit']),
        'storage_limit': int(content['storage_limit']),
        'status': content['metadata']['operation_result']['status'],
    }


def transform_reveal(content, transformed_operation):
    return {
        'level': transformed_operation['level'],
        'timestamp': transformed_operation['timestamp'],
        'block_hash': transformed_operation['block_hash'],
        'operation_hash': transformed_operation['operation_hash'],
        'operation_group_id': transformed_operation['operation_group_id'],
        'operation_id': transformed_operation['operation_id'],
        'source': content['source'],
        'fee': int(content['fee']),
        'counter': int(content['counter']),
        'gas_limit': int(content['gas_limit']),
        'storage_limit': int(content['storage_limit']),
        'public_key': content['public_key'],
        'status': content['metadata']['operation_result']['status'],
    }


def transform_nonce(content, transformed_operation):
    return {
        'level': transformed_operation['level'],
        'timestamp': transformed_operation['timestamp'],
        'block_hash': transformed_operation['block_hash'],
        'operation_hash': transformed_operation['operation_hash'],
        'operation_group_id': transformed_operation['operation_group_id'],
        'operation_id': transformed_operation['operation_id'],
        'nonce': content['nonce']
    }


def transform_activation(content, transformed_operation):
    return {
        'level': transformed_operation['level'],
        'timestamp': transformed_operation['timestamp'],
        'block_hash': transformed_operation['block_hash'],
        'operation_hash': transformed_operation['operation_hash'],
        'operation_group_id': transformed_operation['operation_group_id'],
        'operation_id': transformed_operation['operation_id'],
        'public_key_hash': content['pkh'],
        'secret': content['secret']
    }


def transform_origination(content, transformed_operation):
    return {
        'level': transformed_operation['level'],
        'timestamp': transformed_operation['timestamp'],
        'block_hash': transformed_operation['block_hash'],
        'operation_hash': transformed_operation['operation_hash'],
        'operation_group_id': transformed_operation['operation_group_id'],
        'operation_id': transformed_operation['operation_id'],
        'source': content['source'],
        'delegate': get_none_if_key_error(content, 'delegate'),
        'manager_public_key': get_none_if_key_error(content, 'managerPubkey'),
        'fee': int(content['fee']),
        'counter': int(content['counter']),
        'gas_limit': int(content['gas_limit']),
        'storage_limit': int(content['storage_limit']),
        'balance': int(content['balance']),
        'status': content['metadata']['operation_result']['status'],
        'originated_contracts': safe_cast(get_none_if_key_error(content['metadata']['operation_result'], 'originated_contracts'), list, []),
    }


def transform_proposal(content, transformed_operation):
    return {
        'level': transformed_operation['level'],
        'timestamp': transformed_operation['timestamp'],
        'block_hash': transformed_operation['block_hash'],
        'operation_hash': transformed_operation['operation_hash'],
        'operation_group_id': transformed_operation['operation_group_id'],
        'operation_id': transformed_operation['operation_id'],
        'source': content['source'],
        'proposals': content['proposals'],
        'period': content['period'],
    }


def transform_double_baking_evidence(content, transformed_operation):
    return {
        'level': transformed_operation['level'],
        'timestamp': transformed_operation['timestamp'],
        'block_hash': transformed_operation['block_hash'],
        'operation_hash': transformed_operation['operation_hash'],
        'operation_group_id': transformed_operation['operation_group_id'],
        'operation_id': transformed_operation['operation_id'],
        'denounced_1_level': content['bh1']['level'],
        'denounced_1_proto': content['bh1']['proto'],
        'denounced_1_predecessor': content['bh1']['predecessor'],
        'denounced_1_timestamp': convert_timestr_to_timestamp(content['bh1']['timestamp']),
        'denounced_1_validation_pass': content['bh1']['validation_pass'],
        'denounced_1_operations_hash': content['bh1']['operations_hash'],
        'denounced_1_fitness': content['bh1']['fitness'],
        'denounced_1_context': content['bh1']['context'],
        'denounced_1_priority': content['bh1']['priority'],
        'denounced_1_proof_of_work_nonce': content['bh1']['proof_of_work_nonce'],
        'denounced_1_signature': content['bh1']['signature'],
        'denounced_2_level': content['bh2']['level'],
        'denounced_2_proto': content['bh2']['proto'],
        'denounced_2_predecessor': content['bh2']['predecessor'],
        'denounced_2_timestamp': convert_timestr_to_timestamp(content['bh2']['timestamp']),
        'denounced_2_validation_pass': content['bh2']['validation_pass'],
        'denounced_2_operations_hash': content['bh2']['operations_hash'],
        'denounced_2_fitness': content['bh2']['fitness'],
        'denounced_2_context': content['bh2']['context'],
        'denounced_2_priority': content['bh2']['priority'],
        'denounced_2_proof_of_work_nonce': content['bh2']['proof_of_work_nonce'],
        'denounced_2_signature': content['bh2']['signature'],
    }


def transform_double_endorsement_evidence(content, transformed_operation):
    return {
        'level': transformed_operation['level'],
        'timestamp': transformed_operation['timestamp'],
        'block_hash': transformed_operation['block_hash'],
        'operation_hash': transformed_operation['operation_hash'],
        'operation_group_id': transformed_operation['operation_group_id'],
        'operation_id': transformed_operation['operation_id'],
        'denounced_1_branch': content['op1']['branch'],
        'denounced_1_signature': content['op1']['signature'],
        'denounced_1_level': content['op1']['operations']['level'],
        'denounced_2_branch': content['op2']['branch'],
        'denounced_2_signature': content['op2']['signature'],
        'denounced_2_level': content['op2']['operations']['level'],
    }


def transform_ballot(content, transformed_operation):
    return {
        'level': transformed_operation['level'],
        'timestamp': transformed_operation['timestamp'],
        'block_hash': transformed_operation['block_hash'],
        'operation_hash': transformed_operation['operation_hash'],
        'operation_group_id': transformed_operation['operation_group_id'],
        'operation_id': transformed_operation['operation_id'],
        'source': content['source'],
        'proposal': content['proposal'],
        'period': content['period'],
        'ballot': content['ballot'],
    }


def yield_balance_update(response):
    temp_balance_updates = []
    # extracting balance updates
    for balance_update in safe_cast(get_none_if_key_error(response['metadata'], 'balance_updates'), list, []):
        temp_balance_updates.append({
            'type': 'block',
            'operation_hash': None,
            'balance_update': balance_update
        })
    for operation_group in response['operations']:
        for operation in operation_group:
            operation_hash = operation['hash']
            for content in operation['contents']:
                for balance_update in safe_cast(get_none_if_key_error(content['metadata'], 'balance_updates'), list, []):
                    temp_balance_updates.append({
                        'type': 'operation',
                        'operation_hash': operation_hash,
                        'balance_update': balance_update
                    })
    for bu in temp_balance_updates:
        yield bu


def yield_operations(response):
    transformed_operations = []
    # extracting operations
    for operation_group_id, operation_group in enumerate(response['operations']):
        for operation_id, operation in enumerate(operation_group):
            yield operation_group_id, operation_id, operation


def get_none_if_key_error(dictionary, key):
    try:
        return dictionary[key]
    except KeyError:
        return None


def get_none_if_type_error(dictionary, key):
    try:
        return dictionary[key]
    except TypeError:
        return None


def convert_timestr_to_timestamp(timestr):
    d = datetime.strptime(timestr, '%Y-%m-%dT%H:%M:%SZ')
    return int(d.timestamp())
