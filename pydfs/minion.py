import logging
import os
import random
import sys
import rpyc

from rpyc.utils.server import ThreadedServer

DATA_DIR = "/tmp/minion/"
LOG_DIR = os.path.join(DATA_DIR, 'log')

class MinionService(rpyc.Service):
    class exposed_Minion(object):
        blocks = {}
        # To test the project on several port on single machine, we need to
        # differ the file_name saved.
        m_uuid = random.choice(range(1000))

        def exposed_put(self, block_uuid, data, minions):
            block_addr = DATA_DIR + str(self.__class__.m_uuid) + str(block_uuid)
            with open(block_addr, 'w') as f:
                f.write(data)
            #logging.info("PUT: %d", block_uuid)
            if minions:
                self.forward(block_uuid, data, minions)


        def exposed_get(self, block_uuid):
            block_addr = DATA_DIR + str(self.__class__.m_uuid) + str(block_uuid)
            if not os.path.isfile(block_addr):
                return None
            #logging.info("GET: %d", block_uuid)
            with open(block_addr, 'r') as f:
                return f.read()

        def exposed_delete(self, block_uuid):
            block_addr = DATA_DIR + str(self.__class__.m_uuid) + str(block_uuid)
            if os.path.isfile(block_addr):
                os.remove(block_addr)
            #logging.info("DELETE: %d", block_uuid)

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
        def forward(self, block_uuid, data, minions):
            logging.info("8888: forwarding %d to:%s", block_uuid, str(minions))
            minion = minions[0]
            minions = minions[1:]
            host, port = minion

            con = rpyc.connect(host, port=port)
            minion = con.root.Minion()
            minion.put(block_uuid, data, minions)

if __name__ == "__main__":
    #TODO: Enable Logging in exposed function.
    if len(sys.argv) < 2:
        print("You need to specify the port number")
    server_port = int(sys.argv[1])
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'master'),
                        format='%(asctime)s--%(levelname)s:%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    t = ThreadedServer(MinionService, port=server_port)
    t.start()
