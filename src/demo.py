import os
from threading import Thread
from minion import startMinionService
from proxy import startProxyService
from master import startMasterService
from client import client


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
    client_service = client(2130)

    path = './sample.txt'
    generate_file(path, "some testing data")

    client_service.put(path, "pub")
    client_service.get("pub")

    os.remove(path)


def generate_file(path, data):
    f = open(path, "w+")
    f.write(data)
    f.close()
    return path


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
