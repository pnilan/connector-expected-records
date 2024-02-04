import sys
import os
import json
import random
from helpers import get_stream_records

def generate_records(config_path, catalog_path, stream, all=False):

  expected_records_path = 'integration_tests/expected_records.jsonl'

  stream_records = get_stream_records(config_path, catalog_path)

  read_records_count = len(stream_records)

  if read_records_count == 0:
    print(f'No records available for {stream} stream.')
    if not all:
      sys.exit(1)

  while True:
    records_to_add = input(f'There are {read_records_count} records available for {stream} stream. Number of records to add: ')

    try:
      records_to_add = int(records_to_add)

      if records_to_add < 0:
        print('Please enter a valid quantity.')
      elif records_to_add > read_records_count:
        print(f'You cannot add more records than are available. There are {read_records_count} records available.')
      else:
        break
    except ValueError:
      print('Please choose a valid quantity.')

  if os.path.exists(expected_records_path):
    with open(expected_records_path, 'rb') as f:
      f.seek(-1, 2)
      last_char = f.read(1)
      has_newline = last_char == b'\n'
  else:
    has_newline = True

  i = 0
  index_set = set()
  while i < records_to_add:

    while True:
      random_index = random.randint(1, records_to_add)

      if random_index not in index_set:
        index_set.add(random_index)
        break

    if i == 0 and not has_newline:
      with open(expected_records_path, 'a') as f:
        f.write('\n')

    with open(expected_records_path, 'a') as f:
        f.write(json.dumps(stream_records[random_index - 1]))
        f.write('\n')
    i += 1