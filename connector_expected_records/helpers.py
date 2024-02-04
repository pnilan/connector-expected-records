import subprocess
import json
import jsonlines
import os
from simple_term_menu import TerminalMenu

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

def find_streams():
  source_name = os.path.basename(os.getcwd()).replace('-', '_')
  schema_path = os.path.join(os.getcwd(), source_name, 'schemas')
  stream_names = []
  if os.path.exists(schema_path):
    for file in os.listdir(schema_path):
      if file[-5:] == '.json':
        stream_names.append(file.split('.')[0])
  else:
    print('Schemas folder does not exist.')

  if len(stream_names) == 0:
    print('No available streams.')
    return None
  else:
    stream_names.sort()
    return stream_names

def select_stream(streams):
  terminal_menu = TerminalMenu(streams)
  menu_entry_index = terminal_menu.show()
  print(f'Selected {streams[menu_entry_index]}.')
  return streams[menu_entry_index]

def replace_jsonl_record(expected_records_path, output_path, primary_key_type, primary_key, new_record):
  with jsonlines.open(expected_records_path, 'r') as infile, jsonlines.open(output_path, 'w') as outfile:
    for record in infile:
      if record['data'][primary_key_type] == primary_key:
        outfile.write(new_record)
      else:
        outfile.write(record)

def replace_file(old_file_path, new_file_path):
  if os.path.exists(old_file_path):
    old_file_name = os.path.basename(old_file_path)
    os.remove(old_file_path)
    os.rename(new_file_path, os.path.join(os.path.dirname(new_file_path), old_file_name))
  else:
    print(f'{old_file_path} does not exist.')