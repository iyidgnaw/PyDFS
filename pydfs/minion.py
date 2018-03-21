import os
import rpyc

from rpyc.utils.server import ThreadedServer

DATA_DIR = "/tmp/minion/"

class MinionService(rpyc.Service):
    class exposed_Minion(object):
        blocks = {}

        def exposed_put(self, block_uuid, data, minions):
            with open(DATA_DIR+str(block_uuid), 'w') as f:
                f.write(data)
            if minions:
                self.forward(block_uuid, data, minions)


        def exposed_get(self, block_uuid):
            block_addr = DATA_DIR + str(block_uuid)
            if not os.path.isfile(block_addr):
                return None
            with open(block_addr) as f:
                return f.read()

        def forward(self, block_uuid, data, minions):
            print("8888: forwarding to:\n", block_uuid, minions)
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

if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    t = ThreadedServer(MinionService, port=8888)
    t.start()
