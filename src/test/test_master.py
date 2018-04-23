# pylint: disable=wrong-import-position
import sys
sys.path.append('..')
import rpyc

from conf import DEFAULT_MASTER_PORTS
import test_util
from test_util import suiteRunner

test_dest = 'tmp'
test_size = 20
class MasterTestCase(test_util.TestCase):
    def test_write(self):
        con = rpyc.connect('127.0.0.1', port=DEFAULT_MASTER_PORTS[0])
        master = con.root.Master()
        result = master.write(test_dest, test_size)
        # Make sure get two block tuple according to the size
        self.assertEqual(len(result), 2)
        # Make sure get three mid according to the default replication factor
        self.assertEqual(len(result[0][1]), 3)


    def test_read(self):
        con = rpyc.connect('127.0.0.1', port=DEFAULT_MASTER_PORTS[0])
        master = con.root.Master()
        block_info = master.read(test_dest)
        self.assertEqual(len(block_info), 2)


    def test_delete(self):
        con = rpyc.connect('127.0.0.1', port=DEFAULT_MASTER_PORTS[0])
        master = con.root.Master()
        master.delete(test_dest)
        block_info = master.read(test_dest)
        self.assertEqual(block_info, None)

    def test_add_minion(self):
        con = rpyc.connect('127.0.0.1', port=DEFAULT_MASTER_PORTS[0])
        master = con.root.Master()
        master.add_minion('localhost', 9999)
        self.assertEqual(('localhost', 9999), master.get_minion(4))

    def test_delete_minion(self):
        con = rpyc.connect('127.0.0.1', port=DEFAULT_MASTER_PORTS[0])
        master = con.root.Master()
        master.delete_minion(4)
        self.assertEqual(None, master.get_minion(4))



    #TODO: Add more test cases here

if __name__ == '__main__':
    tests = [MasterTestCase('test_write'), MasterTestCase('test_read'),
             MasterTestCase('test_delete'), MasterTestCase('test_add_minion'),
             MasterTestCase('test_delete_minion')]
    suiteRunner(tests)
