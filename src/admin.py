import sys
from multiprocessing import Process
import signal
import random
from signal import signal, SIGINT

from conf import DEFAULT_MINION_PORTS, DEFAULT_MASTER_PORTS, DEFAULT_PROXY_PORT
from minion import startMinionService
from master import startMasterService
from proxy import startProxyService
from rpyc import connect
###############################################################################
# there are some ADMIN APIs we might want to consider.

# port to process mappings
min_pool = {}
master_pool = {}

proxy_process = None
proxy_con = None

def get_all_processes():
    for p in min_pool:
        yield min_pool[p]
    for p in master_pool:
        yield master_pool[p]
    yield proxy_process

def print_processes(arg):
    print('\nMinions:')
    print(min_pool)
    print('\nMasters: ')
    print(master_pool)
    print('\nProxy:')
    print(proxy_process)

def kill_pros(pros_map, port=None):
# kill a random process from pros_map if port is not provided
    if not port:
        port = random.choice(list(pros_map.keys()))
    pros_map[port].terminate()
    del pros_map[port]

def kill_master(port=None):
    if not master_pool:
        print('no master to kill')
        return
    kill_pros(master_pool, port)
    if not master_pool:
        print('no master left, clean minions')
        for minion in min_pool.values():
            minion.terminate()
        min_pool = {}

def kill_minion(port=None):
    if not min_pool:
        print('no minion to kill')
        return
    kill_pros(min_pool, port)

# below 4 functions seem repitative, feel free to change if you have free time
def create_minion(port=None):
    if port in min_pool:
        return
    if not master_pool:
        print('please create master before creating minion')
        return
    if not port:
        ports_in_use = min_pool.keys()
        port = DEFAULT_MINION_PORTS[0]
        while port in ports_in_use:
            port += 1

    min_pool[port] = Process(target=startMinionService, args=(port,))
    min_pool[port].start()
    proxy_con.root.Proxy().get_master().add_minion('localhost', port)
    print('Minion node created at localhost:{}'.format(port))

def create_master(port=None):
    if port in master_pool:
        return
    # if port is not provided, tries to find the next free port
    if not port:
        ports_in_use = master_pool.keys()
        port = DEFAULT_MASTER_PORTS[0]
        while port in ports_in_use:
            port += 1

    master_pool[port] = Process(target=startMasterService, args=([],port))
    master_pool[port].start()
    proxy_con.root.Proxy().add_master(('localhost', port))
    print('Master node created at localhost:{}'.format(port))


def create_instance(instance):
    if instance == 'master':
        create_master()
    elif instance in ('minion', 'worker', 'slave'):
        create_minion()
    else:
        print('try adding master or minion instead of', instance)

def kill_instance(instance):
    if instance == 'master':
        kill_master()
    elif instance in ('minion', 'worker', 'slave'):
        kill_minion()
    else:
        print('try killing master or minion instead of', instance)

help_msg = '\nAdmin Commands:\n\
    all: setup everything according to conf.py\n\
    ls: print running processes\n\
    add master: create a master process\n\
    kill master: kill a master process\n\
    add minion: create a minion process\n\
    kill minion: kill a minion process\n'

command_map = {
    'ls': print_processes,
    'add': create_instance,
    'kill': kill_instance
}

def conf_setup():
    # Note that proxy is already fired up by default
    for port in DEFAULT_MASTER_PORTS:
        create_master(port)
    for port in DEFAULT_MINION_PORTS:
        create_minion(port)

def main(args):
    global proxy_process
    global proxy_con
    proxy_process = Process(target=startProxyService, args=(DEFAULT_PROXY_PORT,[]))
    proxy_process.start()
    proxy_con = connect('localhost', DEFAULT_PROXY_PORT)
    print('Proxy node created at localhost:{}'.format(DEFAULT_PROXY_PORT))

    if not args:
        # Fireup everything accroding to conf.py
        conf_setup()

    elif args[0] in ('-i', '--interactive'):
        while 1:
            cmd = input('\n>>> ').strip()
            if not cmd:
                continue
            cmd, arg = cmd.split(' ') if ' ' in cmd else (cmd, None)
            command_map.get(cmd, lambda x:print(help_msg))(arg)
    else:
        print('use -i or --interactive to enter interactive mode')

def stut_down(signal, frame):
    for p in get_all_processes():
        p.terminate()
        print(p, 'terminated')
    sys.exit(0)

if __name__ == '__main__':
    signal(SIGINT, stut_down)
    main(sys.argv[1:])
        