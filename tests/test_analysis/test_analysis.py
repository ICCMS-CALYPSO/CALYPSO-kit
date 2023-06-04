import sys
import unittest

import numpy as np


class TestValidity(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test01_demo(self):
        self.assertTrue(True)


def main():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestValidity))
    unittest.TextTestRunner(verbosity=2).run(suite)
