import sys
import os
import subprocess
import argparse
import json

def read_json(filepath):
  with open(filepath, "r") as f:
    return json.loads(f.read())

def find_config():
  config_path = 'secrets/config.json'
  if os.path.exists(config_path):
    return config_path
  else:
    return None

def find_catalog(stream):
  catalog_path = f'integration_tests/catalog_{stream}.json'
  if os.path.exists(catalog_path):
    return catalog_path
  else:
    return None

def check_connection(config_path):
  result = subprocess.run(['python', 'main.py', 'check', '--config', config_path], stdout=subprocess.PIPE, text=True)

  for airbyte_message_text in result.stdout.splitlines():
    airbyte_message = json.loads(airbyte_message_text)
    if airbyte_message['type'] == 'CONNECTION_STATUS':
      if airbyte_message['connectionStatus']['status'] == 'SUCCEEDED':
        print('Connection successful.')
      else:
        print('Connection failed.')


def generate_records():
  print('Not yet implemented')

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
    catalog_path = find_catalog(parsed_args.stream)

    if config_path is None:
      print('Missing config file.')
      sys.exit(1)
    elif catalog_path is None:
      print('Missing catalog file.')
      sys.exit(1)
    else:
      generate_records(config_path, catalog_path)

  elif command == 'compare':
    config_path = find_config()
    catalog_path = find_catalog(parsed_args.stream)

    if config_path is None:
      print('Missing config file.')
      sys.exit(1)
    elif catalog_path is None:
      print('Missing catalog file.')
      sys.exit(1)
    else:
      compare_records(config_path, catalog_path)
  else:
    print("Invalid command. Allowable commands: [check, generate, compare]")
    sys.exit(1)

  sys.exit(0)

def main():
  arguments = sys.argv[1:]
  run(arguments)

if __name__ == '__main__':
  main()