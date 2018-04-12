# This file defines a proxy server as the entry point of the entire system
# Assumption: proxy server never goes down.
# Some Thoughts: In production environments, 'proxy server' can be a cluster.
import logging
from threading import Thread
import os
import rpyc
from rpyc.utils.server import ThreadedServer

from conf import DEFAULT_PROXY_PORT, DEFAULT_MASTER_PORTS, LOG_DIR


class ProxyService(rpyc.Service):
    class exposed_Proxy(object):
        master_con = None
        master_list = None
        master_back_list = []

        def exposed_add_master(self, m):
            self.master_list_append(m)
            self.check_con()
            if len(self.__class__.master_list) > 1:
                self.exposed_get_master().new_sibling(m)

        def exposed_delete_master(self, m):
            if m == self.current_master():
                self.recover_master()
            if m in self.__class__.master_list:
                self.master_list_remove(m)
            if m in self.__class__.master_back_list:
                self.__class__.master_back_list.remove(m)

        def exposed_get_master(self):
            # return connection to master, MAYBE RETURN None.
            if self.check_con():
                return self.__class__.master_con.root.Master()
            return None
#########################################################################
        # Private functions
################################################################################
        def current_master(self):
            if self.__class__.master_list:
                return self.__class__.master_list[0]
            return None

        def check_con(self):
            # Return true if connection is okay
            if self.__class__.master_con:
                return True
            # below code only run once when get_master called first time
            try:
                host, port = self.current_master()
                self.__class__.master_con = rpyc.connect(host, port=port)
                return True
            except ConnectionRefusedError:
                self.recover_master()
                return False

        def recover_master(self):
            # recover_master is called when we would like to discard the
            # current master and find a new master from the master_list
            logging.info('trying to use next master.')
            m = self.current_master
            self.__class__.master_back_list.append(m)
            self.master_list_remove(m)
            while self.__class__.master_list:
                try:
                    host, port = self.current_master()
                    self.__class__.master_con = rpyc.connect(host, port=port)
                    return
                except ConnectionRefusedError:
                    self.recover_master()
            logging.info('Failed to obtain connection to all/any master!')

<<<<<<< HEAD

        def current_master(self):
            if self.__class__.master_list:
                return self.__class__.master_list[0]
            return None

        def master_list_remove(self, m):
            self.__class__.master_list.remove(self, m)
            self.flush_master_list()

        def master_list_append(self, m):
            self.__class__.master_list.append(m)
            self.flush_master_list()

        def flush_master_list(self):
            # force master nodes to have the same master list
            def master_update(master):
                try:
                    con = rpyc.connect(*master)
                    M = self.__class__.master_list[:]
                    M.remove(master)
                    con.root.Master().update_masters(M)
                except ConnectionRefusedError:
                    self.master_list_remove(master)

            for master in self.__class__.master_list:
                Thread(target=master_update, args=(master,)).start()


# def set_conf(master_ports):
#     proxy = ProxyService.exposed_Proxy
#
#     for master_port in master_ports:
#         proxy.master_list.append(master_port)


=======
>>>>>>> 6b3b7e03a6d2de0b971d3ded3289218d2ba2cb12

def startProxyService(proxy_port=DEFAULT_PROXY_PORT,
                      master_list=DEFAULT_MASTER_PORTS):
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'proxy'),
                        format='%(asctime)s--%(levelname)s:%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)

    proxy = ProxyService.exposed_Proxy
    proxy.master_list = [('127.0.0.1', x) for x in master_list]
    t = ThreadedServer(ProxyService, port=proxy_port)
    t.start()

if __name__ == '__main__':
    startProxyService()
