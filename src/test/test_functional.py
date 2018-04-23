# pylint: disable=wrong-import-position
import random
import os
import sys
import time
sys.path.append('..')

from conf import DEFAULT_PROXY_PORT, DEFAULT_MINION_PORTS, DEFAULT_MASTER_PORTS
from client import Client
from test_util import TestCase, suiteRunner


TEST_FILE_NAME = './txt'
TEST_FILE_DATA = 'IQ'
with open(TEST_FILE_NAME, 'w') as f:
    f.write(TEST_FILE_DATA)

class FunctionalTest(TestCase):
    def test_basic(self):
        ''' Baisc functionalt test: put, get, delete'''
        self.client = Client(DEFAULT_PROXY_PORT)
        self.client.put(TEST_FILE_NAME, 'tmp')
        rc = self.client.get('tmp')
        self.assertEqual(rc, TEST_FILE_DATA)
        self.client.delete('tmp')
        rc = self.client.get('tmp')
        self.assertEqual(rc, '')

    def test_randomly_kill_minion(self):
        ''' Kill&Create minion 6 times then try [GET] request'''
        self.client = Client(DEFAULT_PROXY_PORT)
        self.client.put(TEST_FILE_NAME, 'tmp')
        # Need to create one more minion so that we are allowed to kill minion
        self.__class__.admin.create_minion(9999)
        for _ in range(6):
            port = random.choice(DEFAULT_MINION_PORTS)
            self.__class__.admin.kill_minion(port)
            time.sleep(2)
            self.__class__.admin.create_minion(port)
        rc = self.client.get('tmp')
        self.assertEqual(rc, TEST_FILE_DATA)

    def test_fault_tolerant(self):
        ''' Put a test data, then add 4 more minion and kill all the default
        minions. Check the integrity of data'''
        self.client = Client(DEFAULT_PROXY_PORT)
        self.client.put(TEST_FILE_NAME, 'tmp')
        for port in range(8892, 8896):
            self.__class__.admin.create_minion(port)
            time.sleep(0.5)

        for default_port in DEFAULT_MINION_PORTS:
            self.__class__.admin.kill_minion(default_port)
            # Give the system some time to handle the minion lost
            time.sleep(4)
        rc = self.client.get('tmp')
        self.assertEqual(rc, TEST_FILE_DATA)

    def test_kill_current_master(self):
        self.client = Client(DEFAULT_PROXY_PORT)
        self.client.put(TEST_FILE_NAME, 'tmp')
        self.__class__.admin.kill_master(DEFAULT_MASTER_PORTS[0])
        time.sleep(3)
        rc = self.client.get('tmp')
        self.assertEqual(rc, TEST_FILE_DATA)



if __name__ == '__main__':
    # Do not change the order of the following test cases
    tests = [FunctionalTest('test_basic'),
            FunctionalTest('test_randomly_kill_minion'),
            FunctionalTest('test_fault_tolerant')]
            #FunctionalTest('test_kill_current_master')]
    suiteRunner(tests)
    os.remove(TEST_FILE_NAME)
