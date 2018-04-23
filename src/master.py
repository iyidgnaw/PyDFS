import copy
import logging
import math
import os
import random
import uuid
from threading import Thread
from time import sleep

import rpyc
from rpyc.utils.server import ThreadedServer

from conf import BLOCK_SIZE, REPLICATION_FACTOR, \
    DEFAULT_MINION_PORTS, DEFAULT_MASTER_PORTS, LOG_DIR


class MasterService(rpyc.Service):
    class exposed_Master(object):
        # Map file_name to block_ids
        file_table = {} # {'file_name': [bid1, bid2, bid3]}

        # Map block_id to where it's saved
        block_mapping = {} # {'bid': [mid1, mid2, mid3]}

        # Map mid to what's saved on it
        minion_content = {} # {'mid': [bid1, bid2, bid3]}

        # Register the information of every minion
        minions = {} # {'mid': (host, port)}

        master_list = tuple()

        block_size = 0
        replication_factor = 0
        health_monitoring = 0

        def exposed_read(self, fname):
            if fname in self.__class__.file_table:
                return [(block_id, self.__class__.block_mapping[block_id])
                        for block_id in self.__class__.file_table[fname]]
            return None

        def exposed_delete(self, fname):
            def siblings_delete(fname):
                for (h, p) in self.__class__.master_list:
                    try:
                        m = rpyc.connect(h, p)
                        m.root.Master().delete(fname)
                    except ConnectionRefusedError:
                        continue

            Thread(target=siblings_delete, args=[fname]).start()

            for block_id in self.__class__.file_table[fname]:
                for mid in self.__class__.block_mapping[block_id]:
                    m_cont = self.__class__.minion_content[mid]
                    self.__class__.minion_content[mid] = \
                        tuple(filter(lambda x, bid=block_id: x != bid, m_cont))

                del self.__class__.block_mapping[block_id]
            del self.__class__.file_table[fname]

        def exposed_write(self, dest, size):
            if len(self.__class__.minions) < self.__class__.replication_factor:
                return 'not enough minions to hold {} replications'.format(\
                    self.__class__.replication_factor)
            if self.exists(dest):
                self.wipe(dest)
                self.exposed_delete(dest)

            self.__class__.file_table[dest] = tuple()

            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_blocks(dest, num_blocks)
            return blocks

        def exposed_get_block_size(self):
            return self.__class__.block_size

        def exposed_get_minions(self, mid_list):
            return tuple(self.__class__.minions[mid] for mid in mid_list)

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
                for (h, p) in self.__class__.master_list:
                    try:
                        m = rpyc.connect(h, p)
                        m.root.Master().delete_minion(mid)
                    except ConnectionRefusedError:
                        continue

            self.exposed_delete_minion(mid)
            Thread(target=sublings_delete_minion, args=[mid]).start()

        def exposed_delete_minion(self, mid):
            # 'delete minion' in a sense where we only update Master metadata
            del self.__class__.minions[mid]
            for block_id in self.__class__.minion_content[mid]:
                b_map = self.__class__.block_mapping[block_id]
                new_b_map = tuple(filter(lambda x: x != mid, b_map))
                self.__class__.block_mapping[block_id] = new_b_map
                # minion.delete(block_id)
            del self.__class__.minion_content[mid]

        def exposed_add_minion(self, host, port):
            if not self.__class__.minions:
                mid = 0
            else:
                mid = max(self.__class__.minions) + 1
            self.__class__.minions[mid] = (host, port)
            self.__class__.minion_content[mid] = tuple()
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
                self.__class__.block_mapping[block_id] += (target_mid,)
                self.__class__.minion_content[target_mid] += (block_id,)
                self.flush_attr_entry('block_mapping', block_id)
                self.flush_attr_entry('minion_content', target_mid)

        # current state of minion cluster
        def exposed_health_report(self):
            if not self.__class__.health_monitoring:
                Thread(target=self.health_monitor).start()
                self.__class__.health_monitoring = 1
            return self.health_check()

        def exposed_update_attr(self, a_name, a_value, wipe_original=False):
            # update_attr is used by self.flush method.
            # a_name is the table we wish to update
            # a_value is the new values (in the form of tupled dict_items)
            attr = getattr(self.__class__, a_name)
            assert isinstance(attr, dict) and isinstance(a_value, tuple)
            if wipe_original:
                attr = {}
            # update given attribute using the given update values
            attr = dict(tuple(attr.items()) + a_value)

        def exposed_update_masters(self, M):
            # M is the new master list
            self.__class__.master_list = M

        def exposed_new_sibling(self, m):
            # I, the primary master, was introduced to a new sibling
            # I am going to flush all my data onto the new sibling
            host, port = m
            con = rpyc.connect(host, port)
            siblng = con.root.Master()

            for t in ('file_table', 'block_mapping',\
                      'minion_content', 'minions'):
                table = getattr(self.__class__, t)
                siblng.update_attr(t, tuple(table.items()), wipe_original=True)

###############################################################################
        # Private functions
###############################################################################
        def flush(self, table, entry_key, wipe):
            # flush one entry in the given attr table to other masters
            attr = getattr(self.__class__, table)

            # if 'wipe' flag is on, it means that the entire table is flushed
            update_dict = attr if wipe else {entry_key: attr[entry_key]}

            # 'Yo, master brothers and sisters:
            #  this `table[entry_key]` got updated, I'm updating you guys.'
            # TODO: parallel this.
            for (h, p) in self.__class__.master_list:
                try:
                    m = rpyc.connect(h, p)
                    m.root.Master().update_attr(table, \
                        tuple(update_dict.items()), wipe_original=wipe)
                except ConnectionRefusedError:
                    continue

        def flush_attr_entry(self, table, entry_key):
            Thread(target=self.flush, args=[table, entry_key, False]).start()

        def flush_attr_table(self, table):
            Thread(target=self.flush, args=[table, None, True]).start()

        def alloc_blocks(self, dest, num):
            blocks = []
            for _ in range(num):
                block_uuid = str(uuid.uuid1())
                nodes_ids = random.sample(self.__class__.minions.keys(),
                                          self.__class__.replication_factor)

                self.__class__.block_mapping[block_uuid] = nodes_ids
                for mid in nodes_ids:
                    self.__class__.minion_content[mid] += (block_uuid,)

                blocks.append((block_uuid, nodes_ids))

                self.__class__.file_table[dest] += (block_uuid,)
            self.flush_attr_entry('file_table', dest)
            self.flush_attr_table('block_mapping')
            self.flush_attr_table('minion_content')
            return blocks

        def calc_num_blocks(self, size):
            return int(math.ceil(float(size) / self.__class__.block_size))

        def minion_lost_handler(self, status):
            # TODO
            logging.info('1 or more minion dead, status: %s', format(status))
            lost_minions = [mid for mid, value in status.items() if not value]
            for mid in lost_minions:
                self.exposed_admin_delete_minion(mid)
            logging.info('Replicate done')


        def health_monitor(self):
            # actively reach out to minions forever
            # SIDE EFFECT: calls minion_lost_handler when
            while 1:
                minions_status = self.health_check()
                if not all(minions_status.values()):
                    self.minion_lost_handler(minions_status)
                sleep(0.2)

        def health_check(self):
            # reach out to known minions on file
            # RETURN {minion -> [10]}
            res = {}
            minions = copy.deepcopy(self.__class__.minions)
            for m, (host, port) in minions.items():
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


def startMasterService(minion_ports=DEFAULT_MINION_PORTS,
                       master_port=DEFAULT_MASTER_PORTS[0],
                       block_size=BLOCK_SIZE,
                       replication_factor=REPLICATION_FACTOR):
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'master'),
                        format='%(asctime)s--%(levelname)s:%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    # load and use conf file, restore from dump if possible.
    master = MasterService.exposed_Master
    master.block_size = block_size
    master.replication_factor = replication_factor

    # for index, minion_port in enumerate(minion_ports):
        # It is ok to do so because we only test locally
        # host = '127.0.0.1'
        # port = minion_port
        # master.minions[index + 1] = (host, port)
        # master.minion_content[index + 1] = []


    logging.info('Current Config:')
    logging.info('Block size: %d, replication_faction: %d, minions: %s',
                 master.block_size, master.replication_factor,
                 str(master.minions))
    t = ThreadedServer(MasterService, port=master_port)
    t.start()


if __name__ == '__main__':
    # by default use config.py
    startMasterService()
