# This file contains all default configuration for all components to read from
# Note: this is the single source of truth about the whole system.
import os

# Master server configuration
BLOCK_SIZE = 10
REPLICATION_FACTOR = 3

# Default values for starting all services
DEFAULT_MINION_PORTS = (8888, 8889, 8890, 8891)
DEFAULT_PROXY_PORT = 2130
DEFAULT_MASTER_PORTS = (2131, 2132, 2133)

LOG_DIR = '/tmp/minion/log'
if not os.path.isdir(LOG_DIR):
    os.makedirs(LOG_DIR)

DATA_DIR = '/tmp/minion/data/'
if not os.path.isdir(DATA_DIR):
    os.makedirs(DATA_DIR)

def clean():
    for f in os.listdir(DATA_DIR):
        os.remove(DATA_DIR+f)
