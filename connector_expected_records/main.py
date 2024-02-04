import sys
import os
import argparse
from check import check_connection
from read import read_records
from generate import generate_records
from compare import compare_records
from update import update_records
from helpers import create_configured_catalog_for_stream, find_config, find_streams, select_multiple_streams

def run(args):

  parent_parser = argparse.ArgumentParser(add_help=False)
  main_parser = argparse.ArgumentParser()
  subparsers = main_parser.add_subparsers(title='commands', dest='command')

  # Accepts check command
  subparsers.add_parser('check', parents=[parent_parser], help='checks that connection to API can be made.')

  # Accepts read command
  generate_parser = subparsers.add_parser('read', parents=[parent_parser], help='reads records for a given stream.')
  required_generate_parser = generate_parser.add_argument_group('required named arguments')
  required_generate_parser.add_argument('--stream', required=True, help='name of stream to read.')

  # Accepts generate command
  generate_parser = subparsers.add_parser('generate', parents=[parent_parser], help='generates expected records for given stream.')
  generate_parser_flags = generate_parser.add_argument_group('flags')
  generate_parser_flags.add_argument('--stream', help='name of stream to generate expected records for.')
  generate_parser_flags.add_argument('-a', '--all', action='store_true', help='including "--all" flag will generate expected records for all found streams.')
  generate_parser_flags.add_argument('--select', action='store_true', help='including "--select" flag will enable user selection of streams for which to generate records.')

  # Accepts compare command
  compare_parser = subparsers.add_parser('compare', parents=[parent_parser], help='compares expected records for given stream.')
  required_compare_parser = compare_parser.add_argument_group('required named arguments')
  required_compare_parser.add_argument('--stream', required=True, help='name of stream to compare expected records for.')
  required_compare_parser.add_argument('--pk', required=True, help='primary key of stream records.')

  # Accepts update command
  compare_parser = subparsers.add_parser('update', parents=[parent_parser], help='compares stream records to expected records and enables record replacement on a per-record basis.')
  required_compare_parser = compare_parser.add_argument_group('required named arguments')
  required_compare_parser.add_argument('--stream', required=True, help='name of stream to compare and update records.')
  required_compare_parser.add_argument('--pk', required=True, help='primary key of stream records.')

  # Accepts list command
  subparsers.add_parser('list', parents=[parent_parser], help='lists available streams')

  parsed_args = main_parser.parse_args(args)
  command = parsed_args.command

  if command == 'check':
    config_path = find_config()
    if config_path is None:
      print('Cannot find config file.')
      sys.exit(1)
    else:
      check_connection(config_path)
  elif command == 'read':
    config_path = find_config()
    catalog_path = create_configured_catalog_for_stream(parsed_args.stream)
    if config_path is None:
      print('Cannot find config file.')
      sys.exit(1)
    elif catalog_path is None:
      print('Cannot find catalog file.')
      sys.exit(1)
    else:
      read_records(config_path, catalog_path)
      os.remove(catalog_path)
  elif command == 'generate':
    config_path = find_config()

    # if not parsed_args.all or (parsed_args.stream or parsed_args.select):
      # generate_parser.error('--stream argument is required unless --all or --select is specified.')

    if parsed_args.all:
      stream_catalogs = []

      for stream in find_streams():
        stream_catalog_path = create_configured_catalog_for_stream(stream)
        if stream_catalog_path is None:
          print('Cannot find catalog file.')
          sys.exit(1)
        else:
          stream_catalogs.append([stream, stream_catalog_path])

      for stream_catalog in stream_catalogs:

        continue_to_next_iteration = False

        while True:
          generate_for_stream = input(f'Generate records for stream {stream_catalog[0]}? [Y/n] > ')

          try:
            generate_for_stream = generate_for_stream.lower()
            if generate_for_stream == 'n':
              continue_to_next_iteration = True

              os.remove(stream_catalog[1]) if os.path.exists(stream_catalog[1]) else None

              break
            elif generate_for_stream == 'y':
              break
            else:
              print('Please enter a valid response.')
          except AttributeError:
            print('Please enter a valid response.')

        if continue_to_next_iteration:
          continue

        generate_records(config_path, stream_catalog[1], stream_catalog[0])
        os.remove(stream_catalog[1])
    elif parsed_args.select:
      streams = find_streams()
      selected_streams = select_multiple_streams(streams)

      stream_catalogs = []

      for stream in selected_streams:
        stream_catalog_path = create_configured_catalog_for_stream(stream)
        if stream_catalog_path is None:
          print('Cannot find catalog file.')
          sys.exit(1)
        else:
          stream_catalogs.append([stream, stream_catalog_path])

      for stream_catalog in stream_catalogs:
        generate_records(config_path, stream_catalog[1], stream_catalog[0])
        os.remove(stream_catalog[1])
    else:
      catalog_path = create_configured_catalog_for_stream(parsed_args.stream)
      if catalog_path is None:
        print('Cannot find catalog file.')
        sys.exit(1)
      else:
        generate_records(config_path, catalog_path, parsed_args.stream)
        os.remove(catalog_path)
  elif command == 'compare':
    config_path = find_config()
    catalog_path = create_configured_catalog_for_stream(parsed_args.stream)
    if config_path is None:
      print('Cannot find config file.')
      sys.exit(1)
    elif catalog_path is None:
      print('Cannot find catalog file.')
      sys.exit(1)
    else:
      compare_records(config_path, catalog_path, parsed_args.stream, parsed_args.pk)
      os.remove(catalog_path)
  elif command ==  'update':
    config_path = find_config()
    catalog_path = create_configured_catalog_for_stream(parsed_args.stream)
    if config_path is None:
      print('Cannot find config file.')
      sys.exit(1)
    elif catalog_path is None:
      print('Cannot find catalog file.')
      sys.exit(1)
    else:
      update_records(config_path, catalog_path, parsed_args.stream, parsed_args.pk)
      os.remove(catalog_path)
  elif command == 'list':
    streams = find_streams()
    for stream in streams:
      print(stream)
  else:
    print("Invalid command. Allowable commands: [check, read, generate, compare, update, list]")
    sys.exit(1)

  sys.exit(0)

def main():
  arguments = sys.argv[1:]
  run(arguments)

if __name__ == '__main__':
  main()