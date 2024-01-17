import subprocess
import json
import os

def find_config():
  config_path = 'secrets/config.json'
  if os.path.exists(config_path):
    return config_path
  else:
    return None

def get_stream_records(config_path, catalog_path):
  read_output = subprocess.run(['python', 'main.py', 'read', '--config', config_path, '--catalog', catalog_path], stdout=subprocess.PIPE, text=True)

  stream_records = []

  for airbyte_message_text in read_output.stdout.splitlines():
    airbyte_message = json.loads(airbyte_message_text)
    if airbyte_message['type'] == 'RECORD':
      stream_records.append(airbyte_message['record'])

  return stream_records

def create_configured_catalog_for_stream(stream):

  if not os.path.exists('integration_tests/configured_catalog.json'):
    print('Missing configured catalog file.')
    return None

  with open('integration_tests/configured_catalog.json', 'r') as f:
    catalog = json.load(f)

  custom_catalog = f'integration_tests/configured_catalog_{stream}.json'

  configured_catalog = {
    "streams": []
  }

  for stream_configuration in catalog['streams']:
    if stream_configuration['stream']['name'] == stream:
      configured_catalog['streams'].append(stream_configuration)
      with open(custom_catalog, 'w') as f:
        json.dump(configured_catalog, f)
      break

  return custom_catalog

def find_existing_record_by_pk(records, primary_key, pk_value):
  for record in records:
    if record['data'][primary_key] == pk_value:
      return record
  return None