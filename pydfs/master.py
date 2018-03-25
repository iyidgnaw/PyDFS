import logging
import math
import os
import pickle
import random
import signal
import sys
import uuid
from time import sleep
from threading import Thread

import rpyc
from rpyc.utils.server import ThreadedServer

from utils import LOG_DIR

from conf import block_size, replication_factor, minions_conf

MASTER_PORT = 2131

# Issue: State related functions may not work correctly after the Master
# definition changed.
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
    # load and use conf file, restore from dump if possible.
    master = MasterService.exposed_Master
    master.block_size = block_size
    master.replication_factor = replication_factor
    for mid, loc in minions_conf.items():
        host, port = loc.split(":")
        master.minions[mid] = (host, port)
        master.minion_content[mid] = []

    assert len(minions_conf) >= master.replication_factor,\
        'not enough minions to hold {} replications'.format(\
            master.replication_factor)

    # if found saved image of master, restore master state.
    if os.path.isfile('fs.img'):
        set_state(pickle.load(open('fs.img', 'rb')))
    logging.info("Current Config:")
    logging.info("Block size: %d, replication_faction: %d, minions: %s",
                 master.block_size, master.replication_factor,
                 str(master.minions))

class MasterService(rpyc.Service):
    class exposed_Master(object):
        # Map file_name to block_ids
        # {"file_name": [bid1, bid2, bid3]
        file_table = {}
        # Map block_id to where it's saved
        # {"bid": [mid1, mid2, mid3]}
        block_mapping = {}
        # Map mid to what's saved on it
        # {"mid": [bid1, bid2, bid3]}
        minion_content = {}
        # Register the information of every minion
        # TODO: Merge 'minions' and 'minion_content'
        minions = {}

        block_size = 0
        replication_factor = 0
        health_monitoring = 0

        def exposed_read(self, fname):
            if fname in self.__class__.file_table:
                return [(block_id, self.__class__.block_mapping[block_id])
                        for block_id in self.__class__.file_table[fname]]
            return None

        def exposed_delete(self, fname):
            for block_id in self.__class__.file_table[fname]:
                for mid in self.__class__.block_mapping[block_id]:
                    self.__class__.minion_content[mid].remove(block_id)
                del self.__class__.block_mapping[block_id]
            del self.__class__.file_table[fname]

        def exposed_write(self, dest, size):
            if self.exists(dest):
                self.wipe(dest)
                self.exposed_delete(dest)

            self.__class__.file_table[dest] = []

            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_blocks(dest, num_blocks)
            return blocks

        def exposed_get_block_size(self):
            return self.__class__.block_size

        def exposed_get_minions(self):
            return self.__class__.minions

        def exposed_replicate(self, mid):
            for block_id in self.__class__.minion_content[mid]:
                locations = self.__class__.block_mapping[block_id]
                # TODO: Change locations to double linked list
                source_mid = random.choice([x for x in locations if x != mid])
                target_mid = random.choice([x for x in self.__class__.minions if
                    x not in locations])
                # Replicate block from source to target
                self.replicate_block(block_id, source_mid, target_mid)
                # Update information registered on Master
                self.__class__.block_mapping[block_id].append(target_mid)
                self.__class__.minion_content[target_mid].append(block_id)

        def exposed_health_report(self):
            if not self.__class__.health_monitoring:
                Thread(target=self.health_monitor).start()
                self.__class__.health_monitoring = 1
            return self.health_check()

###############################################################################
        # Private functions
###############################################################################
        def alloc_blocks(self, dest, num):
            blocks = []
            for _ in range(num):
                block_uuid = uuid.uuid1()
                # TODO: Assigning algorithm.
                nodes_ids = random.sample(self.__class__.minions.keys(),
                                          self.__class__.replication_factor)

                self.__class__.block_mapping[block_uuid] = nodes_ids
                for mid in nodes_ids:
                    self.__class__.minion_content[mid].append(block_uuid)

                blocks.append((block_uuid, nodes_ids))

                self.__class__.file_table[dest].append(block_uuid)
            return blocks

        def calc_num_blocks(self, size):
            return int(math.ceil(float(size)/self.__class__.block_size))

        def minion_lost_handler(self, status):
            # TODO
            logging.info('1 or more minion dead, status: %s', format(status))

        def health_monitor(self):
            # actively reach out to minions forever
            # SIDE EFFECT: calls minion_lost_handler when
            while 1:
                minions_status = self.health_check()
                if not all(minions_status.values()):
                    self.minion_lost_handler(minions_status)
                sleep(.1)


        def health_check(self):
            # reach out to known minions on file
            # RETURN {minion -> [10]}
            res = {}
            for m, (host, port) in self.__class__.minions.items():
                try:
                    con = rpyc.connect(host, port=port)
                    minion = con.root.Minion()
                    res[m] = 1 if minion.ping() == 'pong' else 0
                except ConnectionRefusedError:
                    res[m] = 0
            return res

        def exists(self, f):
            return f in self.__class__.file_table

        def replicate_block(self, block_id, source, target):
            source_host, source_port = self.__class__.minions[source]
            target_host, target_port = self.__class__.minions[target]
            con = rpyc.connect(source_host, port=source_port)
            minion = con.root.Minion()
            minion.replicate(block_id, target_host, target_port)


        def wipe(self, fname):
            for block_uuid in self.__class__.file_table[fname]:
                node_ids = self.__class__.block_mapping[block_uuid]
                for m in [self.exposed_get_minions()[_] for _ in node_ids]:
                    host, port = m
                    con = rpyc.connect(host, port=port)
                    minion = con.root.Minion()
                    minion.delete(block_uuid)
            return



if __name__ == "__main__":
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'master'),
                        format='%(asctime)s--%(levelname)s:%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    set_conf()
    signal.signal(signal.SIGINT, int_handler)
    t = ThreadedServer(MasterService, port=2131)
    t.start()
