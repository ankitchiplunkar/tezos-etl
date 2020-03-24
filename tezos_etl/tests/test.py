import json
import pytest
from tezos_etl.manage import read_from_file
from tezos_etl.transform import (
    transform_block,
    transform_balance_updates,
    transform_operations,
    transform_operation_types,
)
from tezos_etl.load import write_to_avro

block_number_list = [0, 1, 24, 30, 109, 130, 149, 2307, 4098, 11513, 13298, 32959, 223603, 332624, 360453, 460302, 554813]


def get_response_from_mocks(block_number):
    file_name = f"tests/mocks/{block_number}"
    return read_from_file(file_name)


@pytest.mark.parametrize('block_number', block_number_list)
def test_transform_block(block_number):
    response = json.loads(get_response_from_mocks(block_number))
    blocks = transform_block(response)
    c = write_to_avro(blocks, "Block")


@pytest.mark.parametrize('block_number', block_number_list)
def test_transform_balance_update(block_number):
    response = json.loads(get_response_from_mocks(block_number))
    blocks = transform_block(response)
    balance_updates = transform_balance_updates(blocks, response)
    c = write_to_avro(balance_updates, "BalanceUpdate")


@pytest.mark.parametrize('block_number', block_number_list)
def test_transform_operation(block_number):
    response = json.loads(get_response_from_mocks(block_number))
    blocks = transform_block(response)
    operations = transform_operations(blocks, response)
    c = write_to_avro(operations, "Operation")


@pytest.mark.parametrize('block_number', block_number_list)
def test_transform_operation_types(block_number):
    response = json.loads(get_response_from_mocks(block_number))
    blocks = transform_block(response)
    operation_types = transform_operation_types(blocks, response)
    for key, value in operation_types.items():
        c = write_to_avro(value, key)
