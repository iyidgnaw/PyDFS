# pylint: disable=wrong-import-position
import os
import sys
import rpyc
sys.path.append('..')

from conf import DEFAULT_MINION_PORTS, DATA_DIR
import test_util
from test_util import suiteRunner

test_block_id = '1234567'
test_data = 'YMCA'
test_minion = ('127.0.0.1', DEFAULT_MINION_PORTS[0])
class MinionTestCase(test_util.TestCase):
    #TODO: solve the 'weakly-referenced object no longer exists' problem and
    #extract duplication into setUp()
    def test_put(self):
        con = rpyc.connect('127.0.0.1', port=DEFAULT_MINION_PORTS[0])
        minion = con.root.Minion()
        minion.put(test_block_id, test_data, [])
        self.assertTrue(any([x.endswith('1234567') for x in
            os.listdir(DATA_DIR)]))

    def test_get(self):
        con = rpyc.connect('127.0.0.1', port=DEFAULT_MINION_PORTS[0])
        minion = con.root.Minion()
        result = minion.get(test_block_id)
        self.assertEqual(result, test_data)

    def test_delete(self):
        con = rpyc.connect('127.0.0.1', port=DEFAULT_MINION_PORTS[0])
        minion = con.root.Minion()
        minion.delete(test_block_id)
        self.assertFalse(any([x.endswith('1234567') for x in
            os.listdir(DATA_DIR)]))

    def test_replicate(self):
        pass

if __name__ == '__main__':
    # Create suite because we have to run the following cases in order
    tests = [MinionTestCase('test_put'), MinionTestCase('test_get'),
            MinionTestCase('test_delete')]
    suiteRunner(tests)
