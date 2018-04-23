import os
import sys

import rpyc

from conf import DEFAULT_PROXY_PORT

class Client:
    def __init__(self, proxy_port_num):
        self.con = rpyc.connect('localhost', proxy_port_num,
                config={'sync_request_timeout':60})
        self.proxy = self.con.root.Proxy()
        self.master = self.proxy.get_master()

    def send_to_minion(self, block_uuid, data, minions):
        print('[Client] Sending: ' + str(block_uuid) + str(minions))
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

    def read_from_minions(self, block_uuid, minions):
        for minion in minions:
            try:
                return self.read_from_minion(block_uuid, minion)
            except ConnectionRefusedError:
                continue
        print('[Client] No blocks found. Possibly a corrupt file')
        return ''

    def delete_from_minion(self, block_uuid, minion):
        host, port = minion
        con = rpyc.connect(host, port=port)
        minion = con.root.Minion()
        return minion.delete(block_uuid)

    def get(self, fname):
        file_table = self.master.read(fname)
        if not file_table:
            print('[Client] File Not Found')
            return ''

        result = ''
        for block_uuid, node_ids in file_table:
            minions = self.master.get_minions(node_ids)
            result += self.read_from_minions(block_uuid, minions)
        print('[Client] Get from {}: {}'.format(fname, result))
        return result

    def put(self, source, dest):
        print('[Client] Put into {}: {}'.format(dest, source))
        size = os.path.getsize(source)
        blocks = self.master.write(dest, size)
        # when request cannot be satisfied, an error msg is returned.
        if isinstance(blocks, str):
            print(blocks)
            return
        with open(source) as f:
            for block_uuid, node_ids in blocks:
                data = f.read(self.master.get_block_size())
                minions = self.master.get_minions(node_ids)
                if self.send_to_minion(block_uuid, data, minions):
                    print('Put Operation failed.')
                    break

    def delete(self, fname):
        print('[Client] Delete: {}'.format(fname))
        file_table = self.master.read(fname)
        if not file_table:
            print('[Client] File Not Found')
            return

        self.master.delete(fname)
        for block_uuid, node_ids in file_table:
            for m in self.master.get_minions(node_ids):
                self.delete_from_minion(block_uuid, m)

    def close(self):
        self.con.close()


def main(args):
    if not args:
        print('please use "get", "put" or "delete"')
        return
    client_service = Client(DEFAULT_PROXY_PORT)
    if args[0] == 'get':
        client_service.get(args[1])
    elif args[0] == 'put':
        if len(args) < 2:
            print('not enough argument, use "put source dest"')
            return
        client_service.put(args[1], args[2])
    elif args[0] == 'delete':
        client_service.delete(args[1])
    else:
        print('Please specify the operation you want')


if __name__ == '__main__':
    main(sys.argv[1:])
