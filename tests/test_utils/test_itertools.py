import unittest

import numpy as np

from calypsokit.utils.itertools import groupby_delta, pairwise


class TestIterTools(unittest.TestCase):
    def test_01_pairwise(self):
        it = "ABC"
        it_pair = list(pairwise(it))
        self.assertTrue((it_pair[0] == ("A", "B")) and (it_pair[1] == ("B", "C")))

    def test_02_groupby_delta(self):
        it = [1.0, 1.04, 1.1, 3.1, 3.14, 3.4]
        for i, target in zip(
            groupby_delta(it, 0.1), [(1.0, 1.04, 1.1), (3.1, 3.14), (3.4,)]
        ):
            self.assertTrue(np.allclose(i, target))
        it = []
        for i in groupby_delta(it, 1):
            print(i)
