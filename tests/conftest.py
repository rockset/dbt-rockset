import pytest
import os
from dotenv import load_dotenv, find_dotenv

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here


@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        'type': 'rockset',
        'threads': 1,
        'database': 'db',
        'api_key': os.getenv('API_KEY'),
        'api_server': os.getenv('API_SERVER'),
        'vi_rrn': os.getenv('VI_RRN'),
        'run_async_iis': os.getenv('USE_ASYNC').lower() == "true",
    }


@pytest.fixture(scope='session', autouse=True)
def load_env():
    env_file = find_dotenv('.env')
    load_dotenv(env_file)
