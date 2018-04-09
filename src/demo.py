import os
import random
import socket
import time
from multiprocessing import Process

import rpyc

from client import client
from conf import default_minion_ports, \
    default_proxy_port, \
    default_master_port, \
    replication_factor
from master import startMasterService, startMasterService_no_minion
from minion import startMinionService
from proxy import startProxyService
from utils import generate_file


# This class exposes API for controlling all nodes
class WebServices:
    def __init__(self, minion_ports, master_port, proxy_port):
        # Process reference
        self.minion_process_ref = []
        self.master_process_ref = []
        self.proxy_process_ref = None

        self.minion_ports = minion_ports
        self.master_port = master_port
        self.proxy_port = proxy_port
        # SERVICE LAYER #

    def activate_minion(self, minion_port):
        p = Process(target=startMinionService, args=(minion_port,))
        p.start()
        self.minion_process_ref.append(p)

    def activate_proxy(self, proxy_port):
        p = Process(target=startProxyService, args=(proxy_port, [],))
        p.start()
        self.proxy_process_ref = p

    def activate_master(self, minion_ports, master_port):
        p = Process(target=startMasterService, args=(minion_ports, master_port))
        p.start()
        self.master_process_ref.append(p)

    def activate_master_no_minion(self, master_port):
        p = Process(target=startMasterService_no_minion, args=(master_port,))
        p.start()
        self.master_process_ref.append(p)

    def start_all_services(self):

        # minion only knows minion port
        for minion_port in self.minion_ports:
            self.activate_minion(minion_port)

        # start proxy
        # proxy should know master ports
        self.activate_proxy(self.proxy_port)

        # start master
        # master should know all minion ports
        self.activate_master(self.minion_ports, self.master_port)

        time.sleep(1)

    def cleanup(self):
        for minion_ref in self.minion_process_ref:
            minion_ref.terminate()
        for master_ref in self.master_process_ref:
            master_ref.terminate()
        self.proxy_process_ref.terminate()

        # Process reference
        self.minion_process_ref = []
        self.master_process_ref = []
        self.proxy_process_ref = None
        time.sleep(1)

    # kill k - 1 nodes
    def kill_minions(self, num_minion_to_kill):
        # Randomly kill 2 nodes
        alive_nodes = list(range(0, len(self.minion_process_ref)))
        random.shuffle(alive_nodes)

        print("[Admin] Killing:" + str(num_minion_to_kill) + " minions")
        num_node_down = num_minion_to_kill
        for index in range(num_node_down):
            self.minion_process_ref[alive_nodes[index]].terminate()

    def printHealthReport(self):
        # Validate health report
        Connection = rpyc.connect("localhost", port=default_master_port)
        Master = Connection.root.Master()
        print("Master health report:")

        print(Master.health_report())



class demo:
    def __init__(self):
        self.webservice = WebServices\
            (default_minion_ports, default_master_port, default_proxy_port)

    # Test cases #

    # Test 1: basic DFS functionality
    #  Features tested:
    #       Client: Put, Get, Delete
    def test1(self):
        self.webservice.start_all_services()
        print("Test 1 running.............")
        client_service = client(self.webservice.proxy_port)

        # Generate a file with some data
        path = './test1.txt'
        text = "test1 data"
        dest_name = 'test1'

        generate_file(path, text)

        # perform user operations
        client_service.put(path, dest_name)
        result = client_service.get(dest_name)
        client_service.delete(dest_name)

        # remove generated file
        os.remove(path)

        # Compare stored and retrieved value
        if text != result:
            print("Stored and retrieved data are not the same!")

        # Try to get the file again after deletion
        result = client_service.get(dest_name)

        if result != "":
            print("Client deletion not working")

        print("[Test 1 passed]. Basic client put, get, delete working!")

        self.webservice.cleanup()

    # Test 2: k way replication validation (backup fault tolerant)
    #  Precondition: User has successfully uploaded an file
    #  Steps:
    #     1. k -1 node go offline
    #     2. User retrieves the previously uploaded file
    #     3. User can still get the whole file back
    def test2(self):
        self.webservice.start_all_services()
        print("Test 2 running.............")
        # Precondition test
        client_service = client(self.webservice.proxy_port)

        # Generate a file with some data
        path = './test2.txt'
        text = "test2 data"
        dest_name = 'test2'
        generate_file(path, text)

        # perform user operations
        client_service.put(path, dest_name)
        result = client_service.get(dest_name)

        # remove generated file
        os.remove(path)

        # Compare stored and retrieved value
        if text != result:
            print("Stored and retrieved data are not the same!")

        # End of precondition check

        # Kill k - 1 nodes
        self.webservice.kill_minions(replication_factor - 1)

        self.webservice.printHealthReport()

        retrieved_data = client_service.get(dest_name)
        # Compare stored and retrieved value
        if text != retrieved_data:
            print("Stored and retrieved data are not the same!")

        print("[Test 2 passed] k - 1 minion offline successful!")
        self.webservice.cleanup()

    # Test 3: minion to minion forward fail handling (minion to minion fault)
    #  Steps:
    #     1. Client sends a chunk of data to minion
    #     2. Minion 1 gets the data and replicates to minion 2
    #     3. Minion 2 goes offline
    #     4. Minion 1 should choose a different minion and send the data

    # Test 4: client to minion fail (client to minion fault)
    #  Steps:
    #     1. Client sends a chunk of data to a minion based on allocation scheme
    #     2. The minion goes offline
    #     3. The client should request for new allocation scheme

    # Test 5: dead minion detection (master to minion fault)
    # (heartbeat in progress)
    #  Steps:
    #     1. master should periodically check for dead minions

    # Test 6: master down (proxy to master fault)
    #  Steps:
    #     1. when the main master is down, the backup master should take over
    def test6(self):
        self.webservice.start_all_services()
        print("Test 6 running.............")

        # Precondition test
        client_service = client(self.webservice.proxy_port)

        # Generate a file with some data
        path = './test2.txt'
        text = "test2 data"
        dest_name = 'test2'
        generate_file(path, text)

        # perform user operations
        client_service.put(path, dest_name)
        result = client_service.get(dest_name)

        # remove generated file
        os.remove(path)

        # Compare stored and retrieved value
        if text != result:
            print("Stored and retrieved data are not the same!")

        # End of precondition check

        # start master without minion
        self.webservice.activate_master_no_minion(2132)

        time.sleep(1)

        # connect to proxy to add master
        proxy_con = rpyc.connect('localhost', default_proxy_port)
        proxy = proxy_con.root.Proxy()
        proxy.add_master(2132)

        print("working!")

    def run_all_tests(self):
        self.test1()
        self.test2()
        # self.test6()


###############################

if __name__ == "__main__":
    demo_obj = None
    try:
        demo_obj = demo()

        demo_obj.run_all_tests()

    except socket.error as e:
        print("Unexpected exception! Check logic")
        demo_obj.cleanup()
