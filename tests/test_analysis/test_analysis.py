import sys
import unittest

import numpy as np

from calypsokit.analysis.sim_group import groupby_delta, pairwise


class TestValidity(unittest.TestCase):
    pass


class TestSimGroup(unittest.TestCase):
    def test_01_pairwise(self):
        it = "ABC"
        it_pair = list(pairwise(it))
        self.assertTrue((it_pair[0] == ("A", "B")) and (it_pair[1] == ("B", "C")))

    def test_02_groupby_delta(self):
        it = [0.2, 0.21, 0.22, 0.4, 0.6]
        it_g = list(groupby_delta(it, 0.1))
        self.assertTrue(
            it_g[0] == (0.2, 0.21, 0.22) and (it_g[1] == (0.4,)) and (it_g[2] == (0.6,))
        )
