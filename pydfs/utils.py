import os
from configparser import ConfigParser

CONFIG_PATH = 'dfs.conf'

LOG_DIR = '/tmp/minion/log'
# create logging directory if not exist
if not os.path.isdir(LOG_DIR):
    os.makedirs(LOG_DIR)

