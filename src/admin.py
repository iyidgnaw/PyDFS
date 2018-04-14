import sys
from multiprocessing import Process
import random
from signal import signal, SIGINT
from rpyc import connect

from conf import DEFAULT_MINION_PORTS, DEFAULT_MASTER_PORTS, DEFAULT_PROXY_PORT
from minion import startMinionService
from master import startMasterService
from proxy import startProxyService
###############################################################################
# there are some ADMIN APIs we might want to consider.

HELP_MSG = '\nAdmin Commands:\n\
    all: setup everything according to conf.py\n\
    ls: print running processes\n\
    add master: create a master process\n\
    kill master: kill a master process\n\
    add minion: create a minion process\n\
    kill minion: kill a minion process\n'

class Admin():
    def __init__(self):
        # port to process mappings
        self.min_pool = {}
        self.master_pool = {}

        self.proxy_process = None
        self.proxy_con = None

    def get_all_processes(self):
        for p in self.min_pool:
            yield self.min_pool[p]
        for p in self.master_pool:
            yield self.master_pool[p]
        yield self.proxy_process

    def print_processes(self, arg):
        print('\nMinions:')
        print(self.min_pool)
        print('\nMasters: ')
        print(self.master_pool)
        print('\nProxy:')
        print(self.proxy_process)

    def kill_pros(self, pros_map, port=None):
    # kill a random process from pros_map if port is not provided
        if not port:
            port = random.choice(list(pros_map.keys()))
        pros_map[port].terminate()
        del pros_map[port]

    def kill_master(self, port=None):
        if not self.master_pool:
            print('no master to kill')
            return
        self.kill_pros(self.master_pool, port)
        if not self.master_pool:
            print('no master left, clean minions')
            for minion in self.min_pool.values():
                minion.terminate()
            self.min_pool = {}

    def kill_minion(self, port=None):
        if not self.min_pool:
            print('no minion to kill')
            return
        self.kill_pros(self.min_pool, port)

    # below methods seem repitative, feel free to change if you have free time
    def create_minion(self, port=None):
        if port in self.min_pool:
            return
        if not self.master_pool:
            print('please create master before creating minion')
            return
        if not port:
            ports_in_use = self.min_pool.keys()
            port = DEFAULT_MINION_PORTS[0]
            while port in ports_in_use:
                port += 1

        self.min_pool[port] = Process(target=startMinionService, args=(port,))
        self.min_pool[port].start()
        self.proxy_con.root.Proxy().get_master().add_minion('localhost', port)
        print('Minion node created at localhost:{}'.format(port))

    def create_master(self, port=None):
        if port in self.master_pool:
            return
        # if port is not provided, tries to find the next free port
        if not port:
            ports_in_use = self.master_pool.keys()
            port = DEFAULT_MASTER_PORTS[0]
            while port in ports_in_use:
                port += 1

        self.master_pool[port] = Process(target=startMasterService, \
            args=([], port))
        self.master_pool[port].start()
        self.proxy_con.root.Proxy().add_master(('localhost', port))
        print('Master node created at localhost:{}'.format(port))


    def create_instance(self, instance):
        if instance == 'master':
            self.create_master()
        elif instance in ('minion', 'worker', 'slave'):
            self.create_minion()
        else:
            print('try adding master or minion instead of', instance)

    def kill_instance(self, instance):
        if instance == 'master':
            self.kill_master()
        elif instance in ('minion', 'worker', 'slave'):
            self.kill_minion()
        else:
            print('try killing master or minion instead of', instance)

    def conf_setup(self):
        # Note that proxy is already fired up by default
        for port in DEFAULT_MASTER_PORTS:
            self.create_master(port)
        for port in DEFAULT_MINION_PORTS:
            self.create_minion(port)


    def main(self, args):
        command_map = {
            'ls': self.print_processes,
            'add': self.create_instance,
            'kill': self.kill_instance
        }

        self.proxy_process = Process(target=startProxyService,\
                                args=(DEFAULT_PROXY_PORT, []))
        self.proxy_process.start()
        self.proxy_con = connect('localhost', DEFAULT_PROXY_PORT)
        print('Proxy node created at localhost:{}'.format(DEFAULT_PROXY_PORT))

        if not args:
            # Fireup everything accroding to conf.py
            self.conf_setup()

        elif args[0] in ('-i', '--interactive'):
            while 1:
                cmd = input('\n>>> ').strip()
                if not cmd:
                    continue
                cmd, arg = cmd.split(' ') if ' ' in cmd else (cmd, None)
                command_map.get(cmd, lambda x: print(self.HELP_MSG))(arg)
        else:
            print('use -i or --interactive to enter interactive mode')

    def stut_down(self, sig, frame):
        for p in self.get_all_processes():
            p.terminate()
            print(p, 'terminated')
        sys.exit(0)

if __name__ == '__main__':
    admin = Admin()
    signal(SIGINT, admin.stut_down)
    admin.main(sys.argv[1:])
        