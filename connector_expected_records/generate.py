import sys
import json
from helpers import get_stream_records

def generate_records(config_path, catalog_path):

  expected_records_path = 'integration_tests/expected_records.jsonl'

  stream_records = get_stream_records(config_path, catalog_path)

  if len(stream_records) == 0:
    print('No records available.')
    sys.exit(1)

  while True:
    records_to_add = input(f'There are {len(stream_records)} records available. Number of records to add: ')

    try:
      records_to_add = int(records_to_add)

      if records_to_add < 0:
        print('Please enter a valid quantity.')
      elif records_to_add > len(stream_records):
        print(f'You cannot add more records than are available. There are {len(stream_records)} records available.')
      else:
        break
    except ValueError:
      print('Please choose a valid quantity.')

  with open(expected_records_path, 'rb') as f:
    f.seek(-1, 2)
    last_char = f.read(1)
    has_newline = last_char == b'\n'

  i = 0
  while i < records_to_add:

    if i == 0 and not has_newline:
      with open(expected_records_path, 'a') as f:
        f.write('\n')

    with open(expected_records_path, 'a') as f:
        f.write(json.dumps(stream_records[i]))
        f.write('\n')
    i += 1