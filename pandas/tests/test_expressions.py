# pylint: disable-msg=W0612,E1101

import unittest
import nose

import operator
from numpy import random, nan
from numpy.random import randn
import numpy as np
from numpy.testing import assert_array_equal

import pandas as pan
from pandas.core.api import Panel, DataFrame, Series, notnull, isnull
from pandas.core import expressions as expr

from pandas.util.testing import (assert_almost_equal,
                                 assert_series_equal,
                                 assert_frame_equal,
                                 assert_panel_equal)
from pandas.util import py3compat

import pandas.util.testing as tm
import pandas.lib as lib

from numpy.testing.decorators import slow

if not expr._USE_NUMEXPR:
    raise nose.SkipTest

_frame  = DataFrame(np.random.randn(10000, 4), columns = list('ABCD'), dtype='float64')
_frame2 = DataFrame(np.random.randn(100, 4),   columns = list('ABCD'), dtype='float64')
_mixed  = DataFrame({ 'A' : _frame['A'].copy(), 'B' : _frame['B'].astype('float32'), 'C' : _frame['C'].astype('int64'), 'D' : _frame['D'].astype('int32') })
_mixed2 = DataFrame({ 'A' : _frame2['A'].copy(), 'B' : _frame2['B'].astype('float32'), 'C' : _frame2['C'].astype('int64'), 'D' : _frame2['D'].astype('int32') })
_integer  = DataFrame(np.random.randint(1, 100, size=(10001, 4)), columns = list('ABCD'), dtype='int64')
_frame_panel = Panel(dict(ItemA = _frame.copy(), ItemB = (_frame.copy() + 3), ItemC=_frame.copy(), ItemD=_frame.copy()))
_integer_panel = Panel(dict(ItemA = _integer, ItemB = (_integer+34).astype('int64')))
_mixed_panel = Panel(dict(ItemA = _mixed, ItemB = (_mixed+3)))

class TestExpressions(unittest.TestCase):

    _multiprocess_can_split_ = False

    def setUp(self):

        self.frame  = _frame.copy()
        self.frame2 = _frame2.copy()
        self.mixed  = _mixed.copy()
        self.mixed2 = _mixed2.copy()
        self.integer = _integer.copy()
        self._MIN_ELEMENTS = expr._MIN_ELEMENTS

    def tearDown(self):
        expr._MIN_ELEMENTS = self._MIN_ELEMENTS

    #TODO: add test for Panel
    #TODO: add tests for binary operations
    @nose.tools.nottest
    def run_arithmetic_test(self, df, other, assert_func, check_dtype=False, test_flex=False):
        """
        tests solely that the result is the same whether or not numexpr is enabled.
        Need to test whether the function does the correct thing elsewhere.
        """
        expr._MIN_ELEMENTS = 0
        operations = ['add', 'sub', 'mul', 'mod', 'truediv', 'floordiv', 'pow']
        if not py3compat.PY3:
            operations.append('div')
        for arith in operations:
            if test_flex:
                op = getattr(df, arith)
            else:
                op = getattr(operator, arith)
            expr.set_use_numexpr(False)
            expected = op(df, other)
            expr.set_use_numexpr(True)
            result = op(df, other)
            try:
                # if check_dtype:
                #     if arith == 'div':
                #         assert expected.dtype.kind == result.dtype.kind
                #     if arith  == 'truediv':
                #         assert result.dtype.kind == 'f'
                assert_func(expected, result)
            except Exception:
                print("Failed test with func %r" % op)
                print("test_flex was %r" % test_flex)
                raise

    def run_frame(self, df, other, **kwargs):
        self.run_arithmetic_test(df, other, assert_frame_equal, test_flex=False, **kwargs)
        self.run_arithmetic_test(df, other, assert_frame_equal, test_flex=True, **kwargs)

    def run_series(self, ser, other, **kwargs):
        self.run_arithmetic_test(ser, other, assert_series_equal, test_flex=False, **kwargs)
        self.run_arithmetic_test(ser, other, assert_almost_equal, test_flex=True, **kwargs)

    def run_panel(self, panel, other, **kwargs):
        self.run_arithmetic_test(panel, other, assert_panel_equal, test_flex=False, **kwargs)
        # self.run_arithmetic_test(panel, other, assert_panel_equal, test_flex=True, **kwargs)

    def test_integer_arithmetic(self):
        self.run_frame(self.integer, self.integer)
        self.run_series(self.integer.icol(0), self.integer.icol(0))

    def test_integer_panel(self):
        self.run_panel(_integer_panel, np.random.randint(1, 100))

    def test_float_arithemtic(self):
        self.run_frame(self.frame, self.frame)
        self.run_series(self.frame.icol(0), self.frame.icol(0))

    def test_float_panel(self):
        self.run_panel(_frame_panel, np.random.randn() + 0.1)

    def test_mixed_arithmetic(self):
        self.run_frame(self.mixed, self.mixed)
        for col in self.mixed.columns:
            self.run_series(self.mixed[col], self.mixed[col])

    def test_mixed_panel(self):
        self.run_panel(_mixed_panel, np.random.randint(1, 100))

    def test_integer_with_zeros(self):
        integer = self.integer * np.random.randint(0, 2, size=np.shape(self.integer))
        self.run_frame(integer, integer)
        self.run_series(integer.icol(0), integer.icol(0))

    def test_integer_panel_with_zeros(self):
        # this probably isn't the greatest test, but whatever
        self.run_panel(_mixed_panel, 0)

    def test_invalid(self):

        # no op
        result   = expr._can_use_numexpr(operator.add, None, self.frame, self.frame, 'evaluate')
        self.assert_(result == False)

        # mixed
        result   = expr._can_use_numexpr(operator.add, '+', self.mixed, self.frame, 'evaluate')
        self.assert_(result == False)

        # min elements
        result   = expr._can_use_numexpr(operator.add, '+', self.frame2, self.frame2, 'evaluate')
        self.assert_(result == False)

        # ok, we only check on first part of expression
        result   = expr._can_use_numexpr(operator.add, '+', self.frame, self.frame2, 'evaluate')
        self.assert_(result == True)

    def test_binary_ops(self):

        def testit():

            for f, f2 in [ (self.frame, self.frame2), (self.mixed, self.mixed2) ]:

                for op, op_str in [('add','+'),('sub','-'),('mul','*'),('div','/'),('pow','**')]:

                    op = getattr(operator,op,None)
                    if op is not None:
                        result   = expr._can_use_numexpr(op, op_str, f, f, 'evaluate')
                        self.assert_(result == (not f._is_mixed_type))

                        result   = expr.evaluate(op, op_str, f, f, use_numexpr=True)
                        expected = expr.evaluate(op, op_str, f, f, use_numexpr=False)
                        assert_array_equal(result,expected.values)
                
                        result   = expr._can_use_numexpr(op, op_str, f2, f2, 'evaluate')
                        self.assert_(result == False)

        
        expr.set_use_numexpr(False)
        testit()
        expr.set_use_numexpr(True)
        expr.set_numexpr_threads(1)
        testit()
        expr.set_numexpr_threads()
        testit()

    def test_boolean_ops(self):


        def testit():
            for f, f2 in [ (self.frame, self.frame2), (self.mixed, self.mixed2) ]:

                f11 = f
                f12 = f + 1
            
                f21 = f2
                f22 = f2 + 1

                for op, op_str in [('gt','>'),('lt','<'),('ge','>='),('le','<='),('eq','=='),('ne','!=')]:

                    op = getattr(operator,op)

                    result   = expr._can_use_numexpr(op, op_str, f11, f12, 'evaluate')
                    self.assert_(result == (not f11._is_mixed_type))

                    result   = expr.evaluate(op, op_str, f11, f12, use_numexpr=True)
                    expected = expr.evaluate(op, op_str, f11, f12, use_numexpr=False)
                    assert_array_equal(result,expected.values)
                    
                    result   = expr._can_use_numexpr(op, op_str, f21, f22, 'evaluate')
                    self.assert_(result == False)

        expr.set_use_numexpr(False)
        testit()
        expr.set_use_numexpr(True)
        expr.set_numexpr_threads(1)
        testit()
        expr.set_numexpr_threads()
        testit()

    def test_where(self):

        def testit():
            for f in [ self.frame, self.frame2, self.mixed, self.mixed2 ]:

                
                for cond in [ True, False ]:

                    c = np.empty(f.shape,dtype=np.bool_)
                    c.fill(cond)
                    result   = expr.where(c, f.values, f.values+1)
                    expected = np.where(c, f.values, f.values+1)
                    assert_array_equal(result,expected)

        expr.set_use_numexpr(False)
        testit()
        expr.set_use_numexpr(True)
        expr.set_numexpr_threads(1)
        testit()
        expr.set_numexpr_threads()
        testit()

if __name__ == '__main__':
    # unittest.main()
    import nose
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)
