import logging
import os
import sys
import threading
import uuid
import rpyc

from rpyc.utils.server import ThreadedServer
from conf import LOG_DIR, DATA_DIR

class MinionService(rpyc.Service):
    class exposed_Minion(object):
        block = None
        m_uuid = None
        def __init__(self):
            if not self.__class__.block and not self.__class__.m_uuid:
                self.__class__.blocks = {}
                self.__class__.m_uuid = str(uuid.uuid4())

        def exposed_put(self, block_uuid, data, minions):
            block_addr = DATA_DIR + str(self.__class__.m_uuid) + str(block_uuid)
            with open(block_addr, 'w') as f:

                size = f.write(data)
                if size != len(data):
                    return 1

            logging.info('PUT: %s', block_uuid)
            if minions:
                self.forward(block_uuid, data, minions)
            return 0

        def exposed_ping(self):
            return 'pong'

        def exposed_get(self, block_uuid):
            block_addr = DATA_DIR + str(self.__class__.m_uuid) + str(block_uuid)
            if not os.path.isfile(block_addr):
                return None
            logging.info('GET: %s', block_uuid)
            with open(block_addr, 'r') as f:
                return f.read()

        def exposed_delete(self, block_uuid):
            block_addr = DATA_DIR + str(self.__class__.m_uuid) + str(block_uuid)
            if os.path.isfile(block_addr):
                os.remove(block_addr)
            logging.info('DELETE: %s', block_uuid)

        def exposed_replicate(self, block_uuid, host, port):
            block_addr = DATA_DIR + str(self.__class__.m_uuid) + str(block_uuid)
            if not os.path.isfile(block_addr):
                return
            with open(block_addr, 'r') as f:
                data = f.read()
                con = rpyc.connect(host, port=port)
                target = con.root.Minion()
                target.put(block_uuid, data, [])
###############################################################################
        # Private functions
###############################################################################
        # Previously, for each block, we have data flow like this:
        # Client -> m1 -> m2 -> ...
        # While the problem is when one of the node on the path is dead, all it
        # descendants fail after it. And it's not effective to forward data
        # linearly.
        # TODO: The next step would be how to effectively handle the failure
        # during the forwarding.
        def forward(self, block_uuid, data, minions):
            logging.info('8888: forwarding %s to:%s', block_uuid, str(minions))
            for minion in minions:
                t = threading.Thread(target=self.forward_worker,
                        args=(block_uuid, data, minion,))
                t.daemon = True
                t.start()

        def forward_worker(self, block_uuid, data, minion):
            host, port = minion
            con = rpyc.connect(host, port=port)
            minion = con.root.Minion()
            minion.put(block_uuid, data, None)


def startMinionService(server_port):
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'minion'),
                        format='%(asctime)s--%(levelname)s:%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    t = ThreadedServer(MinionService, port=server_port)
    t.start()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('You need to specify the port number')
    minion_port = int(sys.argv[1])
    startMinionService(minion_port)
