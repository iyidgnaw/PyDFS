import os
import random
import socket
import time
from multiprocessing import Process

import rpyc

from client import client
from conf import default_minion_ports, default_proxy_port, default_master_port
from master import startMasterService
from minion import startMinionService
from proxy import startProxyService
from utils import generate_file


class demo:
    def __init__(self, minion_ports, master_port, proxy_port):
        # this var hold reference to all process services

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
        p = Process(target=startProxyService, args=(proxy_port,))
        p.start()
        self.proxy_process_ref = p

    def activate_master(self, minion_ports, master_port):
        p = Process(target=startMasterService, args=(minion_ports, master_port))
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

    def cleanup(self):
        for minion_ref in self.minion_process_ref:
            minion_ref.terminate()
        for master_ref in self.master_process_ref:
            master_ref.terminate()
        self.proxy_process_ref.terminate()

        print("All services terminated!")

    #################################################

    # Test cases #

    # Test 1: basic DFS functionality
    #  Features tested:
    #       Client: Put, Get, Delete
    def test1(self):
        client_service = client(self.proxy_port)

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

        result = client_service.get(dest_name)

        # check if delete is working
        if result != "":
            print("Client deletion not working")

        print("[Test 1 passed]. Basic client put, get, delete working!")

    # Test 2: k way replication validation (backup fault tolerant)
    #  Precondition: User has successfully uploaded an file
    #  Steps:
    #     1. k -1 node go offline
    #     2. User retrieves the previously uploaded file
    #     3. User can still get the whole file back
    def test2(self):

        # Precondition test
        client_service = client(self.proxy_port)

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

        # Notice number of node down has be smaller than k,
        # otherwise all data would be lost

        # Randomly kill 2 nodes
        alive_nodes = [0, 1, 2, 3]
        random.shuffle(alive_nodes)

        num_node_down = 2
        for index in range(num_node_down):
            self.minion_process_ref[alive_nodes[index]].terminate()

        # Validate health report
        Connection = rpyc.connect("localhost", port=default_master_port)
        Master = Connection.root.Master()
        print("Master health report after 2 nodes down:")

        print(Master.health_report())

        retrieved_data = client_service.get(dest_name)
        # Compare stored and retrieved value
        if text != retrieved_data:
            print("Stored and retrieved data are not the same!")

        print("[Test 2 passed] k - 1 minion offline successful!")


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



    def run_all_tests(self):
        self.test1()
        self.test2()


###############################

if __name__ == "__main__":
    demo_obj = None
    try:
        demo_obj = demo(default_minion_ports,
                        default_master_port, default_proxy_port)

        demo_obj.start_all_services()
        # race condition.
        time.sleep(1)
        demo_obj.run_all_tests()
        demo_obj.cleanup()
    except socket.error as e:
        print("Unexpected exception! Check logic")
        demo_obj.cleanup()
