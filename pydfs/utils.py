from configparser import ConfigParser

CONFIG_PATH = 'dfs.conf'

def get_master_config():
    config = ConfigParser()
    config.read_file(open(CONFIG_PATH))
    return {s:dict(config.items(s)) for s in config.sections()}['master']
    