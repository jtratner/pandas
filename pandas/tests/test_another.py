import unittest
from pandas import DataFrame
from pandas.core.common import PerformanceWarning
from pandas.util.testing import assert_produces_warning
import numpy as np
import warnings

class RunTest(unittest.TestCase):
    def test_0000_aa_earlier_test_case(self):
        # test case prompted by #3788
        df = DataFrame([[1, np.array([10, 20, 30])],
                       [1, np.array([40, 50, 60])],
                       [2, np.array([20, 30, 40])]],
                       columns=['category', 'arraydata'])
        grouped = df.groupby('category')
        with assert_produces_warning(PerformanceWarning) as w:
            result = grouped.agg(sum)
            print result
            print grouped.agg(sum)
            print w
            print warnings.filters
