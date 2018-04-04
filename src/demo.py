import os
import time
import socket
from multiprocessing import Process

from client import client
from conf import default_minion_ports, default_proxy_port, default_master_port
from master import startMasterService
from minion import startMinionService
from proxy import startProxyService


#  helper functions
def generate_file(path, data):
    f = open(path, "w+")
    f.write(data)
    f.close()
    return path


class demo:
    def __init__(self, minion_ports, master_port, proxy_port):
        # this var hold reference to all process services
        self.process_ref = []
        self.minion_ports = minion_ports
        self.master_port = master_port
        self.proxy_port = proxy_port

    def activate_minion(self, minion_port):
        p = Process(target=startMinionService, args=(minion_port,))
        p.start()
        self.process_ref.append(p)

    def activate_proxy(self):
        p = Process(target=startProxyService, args=())
        p.start()
        self.process_ref.append(p)

    def activate_master(self, minion_ports, master_port):
        p = Process(target=startMasterService, args=(minion_ports, master_port))
        p.start()
        self.process_ref.append(p)

    def start_all_services(self):

        # minion only knows minion port
        for minion_port in self.minion_ports:
            self.activate_minion(minion_port)

        # start proxy
        # proxy should know master ports
        self.activate_proxy()

        # start master
        # master should know all minion ports
        self.activate_master(self.minion_ports, self.master_port)

    def simulate_user_operations(self):
        client_service = client(self.proxy_port)

        # Generate a file with some data
        path = './sample.txt'
        text = "some testing data"
        generate_file(path, text)

        # perform user operations
        client_service.put(path, "pub")
        result = client_service.get("pub")
        client_service.delete("pub")

        # remove generated file
        os.remove(path)

        # Compare stored and retrieved value
        assert text == result, "Stored and retrieved data are not the same!"

        result = client_service.get("pub")

        # check if delete is working
        assert result == "", "Client deletion not working"
        print("All user operations successful!")

    def cleanup(self):
        for ref in self.process_ref:
            ref.terminate()
        print("All services terminated!")


if __name__ == "__main__":
    # TODO: extract all hard coded value out
    demo_obj = None
    try:
        demo_obj = demo(default_minion_ports,
                        default_master_port, default_proxy_port)

        demo_obj.start_all_services()
        # race condition.
        time.sleep(1)
        demo_obj.simulate_user_operations()
        demo_obj.cleanup()
    except socket.error as e:
        print(e)
        print("Unexpected exception! Check logic")
        demo_obj.cleanup()
