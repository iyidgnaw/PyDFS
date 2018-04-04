# This file defines a proxy server as the entry point of the entire system
# Assumption: proxy server never goes down.
import logging
import os
import rpyc
from rpyc.utils.server import ThreadedServer
from utils import LOG_DIR

from conf import default_proxy_port, default_master_port


class ProxyService(rpyc.Service):
    class exposed_Proxy(object):
        master_con = None
        def exposed_get_master(self):
            # return connection to master
            self.check_con()
            return self.master_con.root.Master()


        def check_con(self):
            # TODO disconnection handling
            if not self.__class__.master_con:
                self.__class__.master_con = \
                    rpyc.connect("127.0.0.1", port=default_master_port)


def startProxyService():
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'proxy'),
                        format='%(asctime)s--%(levelname)s:%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    t = ThreadedServer(ProxyService, port=default_proxy_port)
    t.start()

if __name__ == "__main__":
    startProxyService()
