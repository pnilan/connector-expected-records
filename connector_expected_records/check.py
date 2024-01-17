import subprocess
import json

def check_connection(config_path):
  result = subprocess.run(['python', 'main.py', 'check', '--config', config_path], stdout=subprocess.PIPE, text=True)

  for airbyte_message_text in result.stdout.splitlines():
    airbyte_message = json.loads(airbyte_message_text)
    if airbyte_message['type'] == 'CONNECTION_STATUS':
      if airbyte_message['connectionStatus']['status'] == 'SUCCEEDED':
        print('Connection successful.')
      else:
        print('Connection failed.')