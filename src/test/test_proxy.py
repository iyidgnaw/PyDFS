import unittest
import test_util

class ProxyTestCase(test_util.TestCase):
    def test_get_master(self):
        #TODO: check the mas_id of binded master
        master = self.__class__.test_client.master

if __name__ == '__main__':
    unittest.main()
