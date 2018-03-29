import os
import sys
import rpyc

class client:
    def __init__(self, proxy_port_num):
        self.con = rpyc.connect('localhost', proxy_port_num)
        self.proxy = self.con.root.Proxy()
        self.master = self.proxy.get_master()

    def send_to_minion(self, block_uuid, data, minions):
        print("sending: " + str(block_uuid) + str(minions))
        main_minion, *replicate_minions = minions
        host, port = main_minion

        con = rpyc.connect(host, port=port)
        minion = con.root.Minion()
        rc = minion.put(block_uuid, data, replicate_minions)
        return rc

    def read_from_minion(self, block_uuid, minion):
        host, port = minion
        con = rpyc.connect(host, port=port)
        minion = con.root.Minion()
        return minion.get(block_uuid)

    def delete_from_minion(self, block_uuid, minion):
        host, port = minion
        con = rpyc.connect(host, port=port)
        minion = con.root.Minion()
        return minion.delete(block_uuid)

    def get(self, fname):
        file_table = self.master.read(fname)
        if not file_table:
            print("404: file not found")
            return None

        result = ''
        for block_uuid, nodes_ids in file_table:
            for m in [self.master.get_minions()[_] for _ in nodes_ids]:
                data = self.read_from_minion(block_uuid, m)
                if data:
                    result += data
                    break
                else:
                    print("No blocks found. Possibly a corrupt file")
        return result


    def put(self, source, dest):
        size = os.path.getsize(source)
        blocks = self.master.write(dest, size)
        with open(source) as f:
            for block_uuid, node_ids in blocks:
                data = f.read(self.master.get_block_size())
                minions = [self.master.get_minions()[_] for _ in node_ids]
                if self.send_to_minion(block_uuid, data, minions):
                    print("Put Operation failed.")
                    break

    def delete(self, fname):
        file_table = self.master.read(fname)
        if not file_table:
            print("404: file not found")
            return

        self.master.delete(fname)
        for block_uuid, node_ids in file_table:
            for m in [self.master.get_minions()[_] for _ in node_ids]:
                self.delete_from_minion(block_uuid, m)

    def backup(self, mid):
        self.master.replicate(int(mid))


def main(args):
    client_service = client(2130)
    if args[0] == "get":
        client_service.get(args[1])
    elif args[0] == "put":
        client_service.put(args[1], args[2])
    elif args[0] == "delete":
        client_service.delete(args[1])
    elif args[0] == "backup":
        client_service.backup(args[1])
    else:
        print("try 'put srcFile destFile OR get file'")


if __name__ == "__main__":
    main(sys.argv[1:])
