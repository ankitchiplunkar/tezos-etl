import io
import os
import logging
from fastavro import (
    writer,
    parse_schema,
    reader,
)

logger = logging.getLogger(__name__)


def upload_file_to_gcs(bucket, file_name, contents):
    blob = bucket.blob(file_name)
    blob.upload_from_file(contents.getvalue())
    logging.info(f"File {file_name} uploaded")


def write_to_avro(data_list, data_type):
    contents = io.BytesIO()
    schema = get_schema(data_type)
    try:
        writer(contents, schema, data_list)
    except Exception as e:
        print(data_list)
        for key, value in data_list[0].items():
            print(key, type(value))
        raise e
    return contents


def get_schema(data_type):
    schema_file = f"schema/{data_type}.json"
    with open(schema_file, 'r') as schema_file:
        str_schema = schema_file.read()
    schema = parse_schema(eval(str_schema))
    return schema


def bytesio_to_file(bytes_io, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'wb') as f:
        f.write(bytes_io.getvalue())


def read_from_avro(file_path):
    data_list = []
    with open(file_path, 'rb') as fo:
        avro_reader = reader(fo)
        for record in avro_reader:
            data_list.append(record)
    return data_list
