from threading import Thread
import rpyc
from minion import startMinionService
from proxy import startProxyService
from master import startMasterService
from client import put
from client import get

def activate_minion(minion_port):
    thread = Thread(target=startMinionService, args=(minion_port,))
    thread.start()

def activate_proxy():
    thread = Thread(target=startProxyService, args=())
    thread.start()

def activate_master():
    thread = Thread(target=startMasterService, args=())
    thread.start()

def simulate_user_operations():
    con = rpyc.connect('localhost', 2130)
    proxy = con.root.Proxy()
    master = proxy.get_master()

    put(master, "./data/a.txt", "pub")
    get(master, "pub")


if __name__ == "__main__":
    # start all minion services
    activate_minion(8888)
    activate_minion(8889)
    activate_minion(8890)

    # start proxy
    activate_proxy()

    # start master
    activate_master()

    # simulate user operation
    simulate_user_operations()
