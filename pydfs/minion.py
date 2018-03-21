import logging
import os
import rpyc

from rpyc.utils.server import ThreadedServer

DATA_DIR = "/tmp/minion/"
LOG_DIR = os.path.join(DATA_DIR, 'log')

class MinionService(rpyc.Service):
    class exposed_Minion(object):
        blocks = {}

        def exposed_put(self, block_uuid, data, minions):
            with open(DATA_DIR+str(block_uuid), 'w') as f:
                f.write(data)
            #logging.info("PUT: %d", block_uuid)
            if minions:
                self.forward(block_uuid, data, minions)


        def exposed_get(self, block_uuid):
            block_addr = DATA_DIR + str(block_uuid)
            if not os.path.isfile(block_addr):
                return None
            #logging.info("GET: %d", block_uuid)
            with open(block_addr) as f:
                return f.read()

        def forward(self, block_uuid, data, minions):
            logging.info("8888: forwarding %d to:%s", block_uuid, str(minions))
            minion = minions[0]
            minions = minions[1:]
            host, port = minion

            con = rpyc.connect(host, port=port)
            minion = con.root.Minion()
            minion.put(block_uuid, data, minions)

        def exposed_delete(self, block_uuid):
            block_addr = DATA_DIR + str(block_uuid)
            if os.path.isfile(block_addr):
                os.remove(block_addr)
            #logging.info("DELETE: %d", block_uuid)

if __name__ == "__main__":
    #TODO: Enable Logging in exposed function.
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'master'),
                        format='%(asctime)s--%(levelname)s:%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    t = ThreadedServer(MinionService, port=8888)
    t.start()
