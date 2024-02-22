import json
import sys
from deepdiff import DeepDiff
from helpers import get_stream_records, replace_jsonl_record, replace_file

def update_records(config_path, catalog_path, stream, primary_key_type):

  expected_records_path = 'integration_tests/expected_records.jsonl'

  expected_records = {}

  with open(expected_records_path, 'r') as f:
    for line in f:
      line = json.loads(line)
      if line['stream'] == stream:
        expected_records[line['data'][primary_key_type]] = line['data']

  expected_records_count = len(expected_records)

  if expected_records_count == 0:
    print(f'No existing records for {stream} stream. Run generate command to generate expected records.')
    sys.exit(1)

  stream_records = get_stream_records(config_path, catalog_path)

  if len(stream_records) == 0:
    print('No records in stream.')
    sys.exit(1)

  updated_records_count = 0
  diff_count = 0

  for record in stream_records:
    record1 = expected_records.get(record['data'][primary_key_type])
    record2 = record['data']

    if record1 is None:
      continue

    record_diff = DeepDiff(record1, record2, ignore_order=True)

    if len(record_diff) != 0:
      diff_count += 1
      identifier = record['data'][primary_key_type]

      print(f'{stream} stream record with {primary_key_type}: "{identifier}" has differences:')
      print(record_diff)

      while True:
        replace_record_input = input('Update old record with new one? [Y/n] > ')
        try:
          replace_record_input = replace_record_input.lower()
          if replace_record_input in ['y', 'n']:
            break
          else:
            print('Please enter a valid response.')
        except AttributeError:
          print('Please enter a valid response.')

      if replace_record_input == 'y':
          temp_file_path = 'integration_tests/temp.jsonl'
          replace_jsonl_record(expected_records_path, temp_file_path, primary_key_type, record['data'][primary_key_type], record)
          replace_file(expected_records_path, temp_file_path)
          updated_records_count += 1
          print(f'Updated {stream} record with {primary_key_type}: {record["data"][primary_key_type]}.')
      else:
        print('Skip replacing record.')

    del expected_records[record['data'][primary_key_type]]

  missing_count = len(expected_records)

  if missing_count:
    print(f'Missing records with {primary_key_type}: {list(expected_records.keys())}')

  if diff_count == 0:
    print('No differences found. No records updated. Process completed.')
  else:
    print(f'Updated {updated_records_count} of {expected_records_count} existing expected record(s). Process completed.')






