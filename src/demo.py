import os
from multiprocessing import Process

from client import client
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
    def __init__(self):
        # this var hold reference to all process services
        self.process_ref = []

    def activate_minion(self, minion_port):
        p = Process(target=startMinionService, args=(minion_port,))
        p.start()
        self.process_ref.append(p)

    def activate_proxy(self):
        p = Process(target=startProxyService, args=())
        p.start()
        self.process_ref.append(p)

    def activate_master(self, ):
        p = Process(target=startMasterService, args=())
        p.start()
        self.process_ref.append(p)

    def start_all_services(self):
        self.activate_minion(8888)
        self.activate_minion(8889)
        self.activate_minion(8890)

        # start proxy
        self.activate_proxy()

        # start master
        self.activate_master()

    def simulate_user_operations(self):
        client_service = client(2130)

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
        assert result is None, "Client deletion not working"
        print("All user operations successful!")

    def cleanUp(self):
        for ref in self.process_ref:
            ref.terminate()
        print("All services terminated!")


if __name__ == "__main__":
    # TODO: extract all hard coded value out
    try:
        demo = demo()
        demo.start_all_services()
        demo.simulate_user_operations()
        demo.cleanUp()
    except ConnectionRefusedError as e:
        print ("Connection refused. Try it again")
        demo.cleanUp()
    except :
        print("Unexpected exception! Check logic")
        demo.cleanUp()
