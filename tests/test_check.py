import pytest
from unittest.mock import patch, MagicMock
from connector_expected_records.check import check_connection


@pytest.mark.parametrize('config_path, expected_output', [
    #  Success Case
    ('secrets/config.json', 'Connection successful.'),
    #  Invalid config
    ('secrets/bad_config.json', 'Connection failed.'),
    #  Missing config
    ('secrets/missing_config.json', 'Missing config file.')
])
def test_check_connection(mock_subprocess_run, config_path, expected_output):


    assert check_connection('config.json') == 'Connection successful.'