import logging
import math
import os
import pickle
import random
import sys
import uuid
from threading import Thread
from time import sleep

import rpyc
from rpyc.utils.server import ThreadedServer

from conf import block_size, replication_factor, \
    default_minion_ports, default_master_port
from utils import LOG_DIR


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


def set_conf(minion_ports):
    # load and use conf file, restore from dump if possible.
    master = MasterService.exposed_Master
    master.block_size = block_size
    master.replication_factor = replication_factor

    for index, minion_port in enumerate(minion_ports):
        # It is ok to do so because we only test locally
        host = "127.0.0.1"
        port = minion_port
        master.minions[index + 1] = (host, port)
        master.minion_content[index + 1] = []

    assert len(minion_ports) >= master.replication_factor, \
        'not enough minions to hold {} replications'.format( \
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

        # mid to (host,port) mapping
        master_list = []

        block_size = 0
        replication_factor = 0
        health_monitoring = 0

        def exposed_read(self, fname):
            if fname in self.__class__.file_table:
                return [(block_id, self.__class__.block_mapping[block_id])
                        for block_id in self.__class__.file_table[fname]]
            return None

        def exposed_delete(self, fname):
            Thread(target=self.masters_delete, args=[fname]).start()
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

        def exposed_get_minions(self, mid_list):
            return [self.__class__.minions[mid] for mid in mid_list]

        def exposed_get_minion(self, mid):
            return self.__class__.minions[mid]

        def exposed_admin_delete_minion(self, mid):
            # peacefully (intentionally) delete minion
            # where deleted minion's data gets replicated.
            self.exposed_replicate(mid)

            # host, port = self.__class__.minions[mid]
            # con = rpyc.connect(host, port=port)
            # minion = con.root.Minion()
            def sublings_delete_minion(mid):
                for m in self.get_master_siblings():
                    m.delete_minion(mid)

            self.exposed_delete_minion(mid)
            Thread(target=sublings_delete_minion, args=[mid]).start()

        def exposed_delete_minion(self, mid):
            # 'delete minion' in a sense where we only update Master metadata
            del self.__class__.minions[mid]
            for block_id in self.__class__.minion_content[mid]:
                self.__class__.block_mapping[block_id].remove(mid)
                # minion.delete(block_id)
            del self.__class__.minion_content[mid]

        def exposed_add_minion(self, host, port):
            mid = max(self.__class__.minions) + 1
            self.__class__.minions[mid] = (host, port)
            self.__class__.minion_content[mid] = []
            self.flush_attr_entry('minions', mid)
            self.flush_attr_entry('minion_content', mid)

        def exposed_replicate(self, mid):
            for block_id in self.__class__.minion_content[mid]:
                locations = self.__class__.block_mapping[block_id]
                source_mid = random.choice([x for x in locations if x != mid])
                target_mid = random.choice([x for x in self.__class__.minions if
                                            x not in locations])
                # Replicate block from source to target
                self.replicate_block(block_id, source_mid, target_mid)
                # Update information registered on Master
                self.__class__.block_mapping[block_id].append(target_mid)
                self.__class__.minion_content[target_mid].append(block_id)
                self.flush_attr_entry('block_mapping', block_id)
                self.flush_attr_entry('minion_content', target_mid)

        # current state of minion cluster
        def exposed_health_report(self):
            if not self.__class__.health_monitoring:
                Thread(target=self.health_monitor).start()
                self.__class__.health_monitoring = 1
            return self.health_check()

        def exposed_update_attr(self, attr_info, wipe_original=False):
            # update_attr is used by self.flush method.
            # "attr_info" is a tuple: (attr_name, attr_value)
            attr_name, attr_value = attr_info
            attr = getattr(self.__class__, attr_name)
            assert isinstance(attr, dict) and isinstance(attr_value, dict)
            if wipe_original:
                attr = {}
            # update given attribute using the given update dict
            attr.update(attr_value)

        def exposed_update_masters(self, M):
            # M is the new master list
            self.__class__.master_list = M


        ###################################
        # Private functions
        ###################################
        # DEBUG USE ONLY
        # def exposed_eval_this(self, statement):
        #     eval(statement)

        def masters_delete(self, fname):
            for m in self.get_master_siblings():
                m.delete(fname)

        def flush(self, table, entry_key, wipe):
            # flush one entry in the given attr table to other masters
            attr = getattr(self.__class__, table)
            update_dict = {}

            # if 'wipe' flag is on, it means that the entire table is flushed
            update_dict[table] = attr if wipe else {entry_key: attr[entry_key]}

            # "Yo, master brothers and sisters:
            #  this `table[entry_key]` got updated, I'm updating you guys."
            # TODO: parallel this.
            for m in self.get_master_siblings():
                m.update_attr(update_dict, wipe_original=wipe)

        def flush_attr_entry(self, table, entry_key):
            Thread(target=self.flush, args=[table, entry_key, False]).start()

        def flush_attr_table(self, table):
            Thread(target=self.flush, args=[table, None, True]).start()

        def get_master_siblings(self):
            for (host, port) in self.__class__.master_list:
                try:
                    con = rpyc.connect(host, port)
                    m = con.root.Master()
                    yield m
                except ConnectionRefusedError:
                    # do nothing
                    # master does not notify proxy for missing siblings
                    continue

        def alloc_blocks(self, dest, num):
            blocks = []
            for _ in range(num):
                block_uuid = str(uuid.uuid1())
                nodes_ids = random.sample(self.__class__.minions.keys(),
                                          self.__class__.replication_factor)

                self.__class__.block_mapping[block_uuid] = nodes_ids
                for mid in nodes_ids:
                    self.__class__.minion_content[mid].append(block_uuid)

                blocks.append((block_uuid, nodes_ids))

                self.__class__.file_table[dest].append(block_uuid)
            self.flush_attr_entry('file_table', dest)
            return blocks

        def calc_num_blocks(self, size):
            return int(math.ceil(float(size) / self.__class__.block_size))

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
                for m in self.exposed_get_minions(node_ids):
                    host, port = m
                    con = rpyc.connect(host, port=port)
                    minion = con.root.Minion()
                    minion.delete(block_uuid)
            return


def startMasterService(minion_ports, master_port):
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'master'),
                        format='%(asctime)s--%(levelname)s:%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    set_conf(minion_ports)
    # signal.signal(signal.SIGINT, int_handler)
    t = ThreadedServer(MasterService, port=master_port)
    t.start()


def startMasterService_no_minion(master_port):
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'master'),
                        format='%(asctime)s--%(levelname)s:%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    set_conf([])
    # signal.signal(signal.SIGINT, int_handler)
    t = ThreadedServer(MasterService, port=master_port)
    t.start()


if __name__ == "__main__":
    # by default use config.py
    startMasterService(default_minion_ports, default_master_port)
