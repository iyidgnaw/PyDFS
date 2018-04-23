# pylint: disable=wrong-import-position
import unittest
import sys
sys.path.append('..')

from conf import clean
from admin import Admin

class TestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin = Admin()
        cls.admin.conf_setup()
        clean()

    @classmethod
    def tearDownClass(cls):
        for p in cls.admin.get_all_processes():
            p.terminate()
        clean()


def suiteRunner(testcases):
    # Run test cases in order
    suite = unittest.TestSuite()
    suite.addTests(testcases)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
