# pylint: disable=wrong-import-position
import unittest
import sys
sys.path.append('..')

from conf import clean
from conf import DEFAULT_PROXY_PORT
from admin import setupDefaultEnv, tearDownDefaultEnv
from client import Client

class TestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setupDefaultEnv()
        clean()
        cls.test_client = Client(DEFAULT_PROXY_PORT)

    @classmethod
    def tearDownClass(cls):
        tearDownDefaultEnv()
        clean()

def suiteRunner(testcases):
    # Run test cases in order
    suite = unittest.TestSuite()
    suite.addTests(testcases)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
