import sys
from multiprocessing import Process

from conf import DEFAULT_MINION_PORTS
from minion import startMinionService
from master import startMasterService
from proxy import startProxyService
###############################################################################
# there are some ADMIN APIs we might want to consider.

def create_minion_node():
    pass

def delete_minion_node(mid):
    pass

def attach_minion_node(port):
    pass

def switch_master_node():
    pass

def create_master_node():
    pass

def attach_master_node():
    pass

def main(args):
    if not args:
        # Fireup everything accroding to conf.py
        pool = []
        pool.append(Process(target=startMasterService))
        for port in DEFAULT_MINION_PORTS:
            pool.append(Process(target=startMinionService, args=(port,)))
        pool.append(Process(target=startProxyService))
        for p in pool:
            p.start()
    else:
        pass


if __name__ == '__main__':
    main(sys.argv[1:])
