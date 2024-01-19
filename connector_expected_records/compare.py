import json
import sys
import subprocess
from deepdiff import DeepDiff
from helpers import get_stream_records
from helpers import find_existing_record_by_pk

def compare_records(config_path, catalog_path, stream, primary_key_type):

  expected_records_path = 'integration_tests/expected_records.jsonl'

  existing_expected_records = []

  with open(expected_records_path, 'r') as f:
    for line in f:
      line = json.loads(line)
      if line['stream'] == stream:
        existing_expected_records.append(line)

  if len(existing_expected_records) == 0:
    print(f'No existing records for {stream} stream. Run generate command to generate expected records.')
    sys.exit(1)

  new_stream_records = get_stream_records(config_path, catalog_path)

  if len(new_stream_records) == 0:
    print('No records in stream.')
    sys.exit(1)

  diffs = []
  exclude_paths = "emitted_at"

  for record in new_stream_records:
    record1 = record
    record2 = find_existing_record_by_pk(existing_expected_records, primary_key_type, record['data'][primary_key_type])

    if record2 is None:
      continue

    record_diff = DeepDiff(record1, record2, ignore_order=True, exclude_paths=exclude_paths)

    if len(record_diff) == 0:
      continue

    diff = {
      'stream': stream,
      primary_key_type: record['data'][primary_key_type],
      'diff': record_diff
    }

    diffs.append(diff)

  if len(diffs) == 0:
    print('No differences found.')
    sys.exit(0)
  else:
    print(f'{len(diffs)} records have differences.')

    for diff in diffs:
      print(json.dumps(diff, indent=2))

    sys.exit(0)