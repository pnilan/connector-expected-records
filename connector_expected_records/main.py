import sys
import os
import argparse
from check import check_connection
from generate import generate_records
from compare import compare_records
from helpers import create_configured_catalog_for_stream
from helpers import find_config

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
  required_compare_parser.add_argument('--pk', required=True, help='primary key of stream records.')

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
    catalog_path = create_configured_catalog_for_stream(parsed_args.stream)

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
    catalog_path = create_configured_catalog_for_stream(parsed_args.stream)

    if config_path is None:
      print('Missing config file.')
      sys.exit(1)
    elif catalog_path is None:
      print('Missing catalog file.')
      sys.exit(1)
    else:
      compare_records(config_path, catalog_path, parsed_args.stream, parsed_args.pk)
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