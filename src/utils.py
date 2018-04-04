import os

# This file contains some helper functions

CONFIG_PATH = 'dfs.conf'

LOG_DIR = '/tmp/minion/log'
# create logging directory if not exist
if not os.path.isdir(LOG_DIR):
    os.makedirs(LOG_DIR)


#  generate a file given path and data
def generate_file(path, data):
    f = open(path, "w+")
    f.write(data)
    f.close()
    return path
