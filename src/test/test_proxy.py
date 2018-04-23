# pylint: disable=wrong-import-position
import unittest
import sys
import test_util
sys.path.append('..')
from client import Client
from conf import DEFAULT_PROXY_PORT

class ProxyTestCase(test_util.TestCase):
    def test_get_master(self):
        #TODO: check the mas_id of binded master
        test_client = Client(DEFAULT_PROXY_PORT)
        master = test_client.master

if __name__ == '__main__':
    unittest.main()
