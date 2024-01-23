import json
from helpers import get_stream_records

def read_records(config_path, catalog_path):

  stream_records = get_stream_records(config_path, catalog_path)

  for record in stream_records:
    print(json.dumps(record))

