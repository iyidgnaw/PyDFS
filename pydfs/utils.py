import os
from configparser import ConfigParser

CONFIG_PATH = 'dfs.conf'

LOG_DIR = '/tmp/minion/log'
# create logging directory if not exist
if not os.path.isdir(LOG_DIR):
    os.system('mkdir -p {}'.format(LOG_DIR))

def get_master_config():
    config = ConfigParser()
    config.read_file(open(CONFIG_PATH))
    return {s:dict(config.items(s)) for s in config.sections()}['master']
    