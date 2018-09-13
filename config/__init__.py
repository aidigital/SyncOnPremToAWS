import os
import configparser

config_env = os.getenv('BI_REPORTING_CONFIG', 'config.ini')
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), config_env))
