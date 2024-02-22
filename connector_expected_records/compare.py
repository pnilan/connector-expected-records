import json
import sys
from deepdiff import DeepDiff
from helpers import get_stream_records

def compare_records(config_path, catalog_path, stream, primary_key_type):
  '''
  Compares records in stream to expected records. Currently only returns differences if a record with matching primary key exists in both streams.
  TODO: Return primary_key and count of records that exist in expected records but not in stream.
  '''

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

  diffs = []

  for record in stream_records:
    record1 = expected_records.get(record['data'][primary_key_type])
    record2 = record['data']

    if record1 is None:
      continue

    record_diff = DeepDiff(record1, record2, ignore_order=True)
    del expected_records[record['data'][primary_key_type]]

    if len(record_diff) == 0:
      continue

    diff = {
      'stream': stream,
      primary_key_type: record['data'][primary_key_type],
      'diff': record_diff
    }

    diffs.append(diff)


  missing_records_count = len(expected_records)
  diff_count = len(diffs)

  if diff_count == 0 and missing_records_count == 0:
    print('No differences found.')
  elif diff_count == 0 and missing_records_count > 0:
    print(f'{missing_records_count} records of are missing out of {expected_records_count} expected records.')
  elif diff_count > 0 and missing_records_count == 0:
    print(f'{diff_count} records have differences.')
    for diff in diffs:
      print(diff)
  else:
    print(f'{diff_count} records have differences and {missing_records_count} records are missing out of {expected_records_count} expected records.')
    for diff in diffs:
      print(diff)