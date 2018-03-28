import sys
import rpyc
from master import MASTER_PORT

###############################################################################
       # Connection
###############################################################################
try:
    Connection = rpyc.connect("localhost", port=MASTER_PORT)
    Master = Connection.root.Master()
except ConnectionRefusedError:
    print('Failed to connect to Master.')
    sys.exit(1)
###############################################################################
       # End of Connection
###############################################################################


# there are some ADMIN APIs we might want to consider.

def create_minion_node():
    pass

def delete_minion_node(mid):
    Master.delete_minion(mid)

def attach_minion_node(host, port):
    Master.add_minion(host, port)

def switch_master_node():
    pass

def create_master_node():
    pass

def attach_master_node():
    pass

def main():
    print(Master.health_report())

if __name__ == "__main__":
    main()
