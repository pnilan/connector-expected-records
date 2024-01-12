import sys
import os
import subprocess
import argparse
import json

def find_config():
  config_path = 'secrets/config.json'
  if os.path.exists(config_path):
    return config_path
  else:
    return None

def create_catalog(stream):

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

def check_connection(config_path):
  result = subprocess.run(['python', 'main.py', 'check', '--config', config_path], stdout=subprocess.PIPE, text=True)

  for airbyte_message_text in result.stdout.splitlines():
    airbyte_message = json.loads(airbyte_message_text)
    if airbyte_message['type'] == 'CONNECTION_STATUS':
      if airbyte_message['connectionStatus']['status'] == 'SUCCEEDED':
        print('Connection successful.')
      else:
        print('Connection failed.')

def generate_records(config_path, catalog_path):

  expected_records_path = 'integration_tests/expected_records.jsonl'

  result = subprocess.run(['python', 'main.py', 'read', '--config', config_path, '--catalog', catalog_path], stdout=subprocess.PIPE, text=True)

  records = []

  for airbyte_message_text in result.stdout.splitlines():
    airbyte_message = json.loads(airbyte_message_text)
    if airbyte_message['type'] == 'RECORD':
      records.append(airbyte_message['record'])

  if len(records) == 0:
    print('No records available.')
    sys.exit(1)
  else:
    while True:
      records_to_add = input(f'There are {len(records)} records available. Number of records to add: ')

      try:
        records_to_add = int(records_to_add)

        if records_to_add < 0:
          print('Please enter a valid quantity.')
        elif records_to_add > len(records):
          print(f'You cannot add more records than are available. There are {len(records)} records available.')
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
        f.write(json.dumps(records[i]))
        f.write('\n')
    i += 1

def compare_records():
  print('Net yet implemented')


def run(args):

  parent_parser = argparse.ArgumentParser(add_help=False)
  main_parser = argparse.ArgumentParser()
  subparsers = main_parser.add_subparsers(title='commands', dest='command')

  # Accepts check command
  subparsers.add_parser('check', parents=[parent_parser], help='checks that connection to API can be made.')

  # Accepts generate command
  generate_parser = subparsers.add_parser('generate', parents=[parent_parser], help='generates expected records for integration tests for given stream.')
  required_generate_parser = generate_parser.add_argument_group('required named arguments')
  required_generate_parser.add_argument('--stream', required=True, help='name of stream to generate expected records for.')

  # Accepts compare command
  compare_parser = subparsers.add_parser('compare', parents=[parent_parser], help='compares expected records for integration tests for given stream.')
  required_compare_parser = compare_parser.add_argument_group('required named arguments')
  required_compare_parser.add_argument('--stream', required=True, help='name of stream to compare expected records for.')

  parsed_args = main_parser.parse_args(args)
  command = parsed_args.command

  if command == 'check':
    config_path = find_config()

    if config_path is None:
      print('Missing config file.')
      sys.exit(1)
    else:
      check_connection(config_path)
  elif command == 'generate':
    config_path = find_config()
    catalog_path = create_catalog(parsed_args.stream)

    if config_path is None:
      print('Missing config file.')
      sys.exit(1)
    elif catalog_path is None:
      print('Missing catalog file.')
      sys.exit(1)
    else:
      generate_records(config_path, catalog_path)
      os.remove(catalog_path)

  elif command == 'compare':
    config_path = find_config()
    catalog_path = create_catalog(parsed_args.stream)

    if config_path is None:
      print('Missing config file.')
      sys.exit(1)
    elif catalog_path is None:
      print('Missing catalog file.')
      sys.exit(1)
    else:
      compare_records(config_path, catalog_path)
      os.remove(catalog_path)
  else:
    print("Invalid command. Allowable commands: [check, generate, compare]")
    sys.exit(1)

  sys.exit(0)

def main():
  arguments = sys.argv[1:]
  run(arguments)

if __name__ == '__main__':
  main()