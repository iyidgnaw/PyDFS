import math
import os
import pickle
import random
import signal
import sys
import uuid

import rpyc
from rpyc.utils.server import ThreadedServer

from utils import get_master_config

# restoring master node might not work, but we are not focusing on it for now
def get_state():
    return {'file_table': MasterService.exposed_Master.file_table, \
         'block_mapping': MasterService.exposed_Master.block_mapping}

def set_state(state):
    MasterService.exposed_Master.file_table = state['file_table']
    MasterService.exposed_Master.block_mapping = state['block_mapping']

def int_handler(sig, frame):
    pickle.dump(get_state(), open('fs.img', 'wb'))
    sys.exit(0)

def set_conf():
    conf = get_master_config()
    master = MasterService.exposed_Master

    master.block_size = int(conf['block_size'])
    master.replication_factor = int(conf['replication_factor'])
    minions = conf['minions'].split(',')
    for minion in minions:
        mid, host, port = minion.split(":")
    master.minions[mid] = (host, port)

    # if found saved image of master, restore master state.
    if os.path.isfile('fs.img'):
        set_state(*pickle.load(open('fs.img', 'rb')))

class MasterService(rpyc.Service):
    class exposed_Master(object):
        file_table = {}
        block_mapping = {}
        minions = {}

        block_size = 0
        replication_factor = 0

        def exposed_read(self, fname):
            mapping = self.__class__.file_table[fname]
            return mapping

        def exposed_delete(self, fname):
            del self.__class__.file_table[fname]

        def exposed_write(self, dest, size):
            if self.exists(dest):
                #TODO: Wipe previous value
                pass # ignoring for now, will delete it later

            self.__class__.file_table[dest] = []

            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_blocks(dest, num_blocks)
            return blocks

        def exposed_get_file_table_entry(self, fname):
            if fname in self.__class__.file_table:
                return self.__class__.file_table[fname]
            return None

        def exposed_get_block_size(self):
            return self.__class__.block_size

        def exposed_get_minions(self):
            return self.__class__.minions

        def calc_num_blocks(self, size):
            return int(math.ceil(float(size)/self.__class__.block_size))

        def exists(self, f):
            return f in self.__class__.file_table

        def alloc_blocks(self, dest, num):
            blocks = []
            for _ in range(num):
                block_uuid = uuid.uuid1()
                # TODO: Assigning algorithm.
                nodes_ids = random.sample(self.__class__.minions.keys(),
                                          self.__class__.replication_factor)
                blocks.append((block_uuid, nodes_ids))

                self.__class__.file_table[dest].append((block_uuid, nodes_ids))
            return blocks


if __name__ == "__main__":
    set_conf()
    signal.signal(signal.SIGINT, int_handler)
    t = ThreadedServer(MasterService, port=2131)
    t.start()
