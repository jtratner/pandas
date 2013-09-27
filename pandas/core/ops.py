"""
Arithmetic operations for PandasObjects

This is not a public API.
"""
import operator
from functools import partial
import numpy as np
from pandas import compat, lib, tslib
import pandas.index as _index
from pandas.util.decorators import Appender
import pandas.core.common as com
import pandas.core.array as pa
import pandas.computation.expressions as expressions
from pandas.core.common import(bind_method, is_list_like, notnull, isnull,
                               _values_from_object, _np_version_under1p6,
                               _maybe_match_name, _np_version_under1p7)

# -----------------------------------------------------------------------------
# Functions that add arithmetic methods to objects, given arithmetic factory
# methods

def _create_methods(arith_method, radd_func, comp_method, bool_method,
                    use_numexpr, special=False, default_axis='columns'):
    # NOTE: Only frame cares about default_axis, specifically: special methods
    # have default axis None, whereas flex methods have default axis 'columns'
    # if we're not using numexpr, then don't pass a str_rep
    if use_numexpr:
        op = lambda x: x
    else:
        op = lambda x: None
    if special:
        def names(x):
            if x[-1] == "_":
                return "__%s_" % x
            else:
                return "__%s__" % x
    else:
        names = lambda x: x
    radd_func = radd_func or operator.add
    # Inframe, all special methods have default_axis=None, flex methods have default_axis set to the default (columns)
    new_methods = dict(
        add=arith_method(operator.add, names('add'), op('+'), default_axis=default_axis),
        radd=arith_method(radd_func, names('radd'), op('+'), default_axis=default_axis),
        sub=arith_method(operator.sub, names('sub'), op('-'), default_axis=default_axis),
        mul=arith_method(operator.mul, names('mul'), op('*'), default_axis=default_axis),
        truediv=arith_method(operator.truediv, names('truediv'), op('/'),
                             truediv=True, fill_zeros=np.inf, default_axis=default_axis),
        floordiv=arith_method(operator.floordiv, names('floordiv'), op('//'),
                              default_axis=default_axis, fill_zeros=np.inf),
        # Causes a floating point exception in the tests when numexpr
        # enabled, so for now no speedup
        mod=arith_method(operator.mod, names('mod'), default_axis=default_axis,
                         fill_zeros=np.nan),
        pow=arith_method(operator.pow, names('pow'), op('**'), default_axis=default_axis),
        # not entirely sure why this is necessary, but previously was included
        # so it's here to maintain compatibility
        rmul=arith_method(operator.mul, names('rmul'), default_axis=default_axis),
        rsub=arith_method(lambda x, y: y - x, names('rsub'), default_axis=default_axis),
        rtruediv=arith_method(lambda x, y: operator.truediv(y, x), names('rtruediv'),
                              truediv=True, fill_zeros=np.inf, default_axis=default_axis),
        rfloordiv=arith_method(lambda x, y: operator.floordiv(y, x), names('rfloordiv'),
                               default_axis=default_axis, fill_zeros=np.inf),
        rpow=arith_method(lambda x, y: y ** x, names('rpow'), default_axis=default_axis),
        rmod=arith_method(lambda x, y: y % x, names('rmod'), default_axis=default_axis),
    )
    if not compat.PY3:
        new_methods["div"] = arith_method(operator.div, names('div'), op('/'),
                                          truediv=False, fill_zeros=np.inf, default_axis=default_axis)
        new_methods["rdiv"] = arith_method(lambda x, y: operator.div(y, x), names('rdiv'),
                                           truediv=False, fill_zeros=np.inf, default_axis=default_axis)
    else:
        new_methods["div"] = arith_method(operator.truediv, names('div'), op('/'),
                                          truediv=True, fill_zeros=np.inf, default_axis=default_axis)
        new_methods["rdiv"] = arith_method(lambda x, y: operator.truediv(y, x), names('rdiv'),
                                           truediv=False, fill_zeros=np.inf, default_axis=default_axis)
        # Comp methods never had a default axis set
    if comp_method:
        new_methods.update(dict(
            eq=comp_method(operator.eq, names('eq'), op('==')),
            ne=comp_method(operator.ne, names('ne'), op('!='), masker=True),
            lt=comp_method(operator.lt, names('lt'), op('<')),
            gt=comp_method(operator.gt, names('gt'), op('>')),
            le=comp_method(operator.le, names('le'), op('<=')),
            ge=comp_method(operator.ge, names('ge'), op('>=')),
        ))
    if bool_method:
        new_methods.update(dict(
        and_=bool_method(operator.and_, names('and_ [&]'), op('&')),
        # causes the scalar na_cmps test to fail
        # rand_=bool_method(lambda x, y: operator.and_(y, x), names('rand_[&]')),
        or_=bool_method(operator.or_, names('or_ [|]'), op('|')),
        # ror_=bool_method(lambda x, y: operator.or_(y, x), names('ror_ [|]')),
        # For some reason ``^`` wasn't used in original.
        xor=bool_method(operator.xor, names('xor [^]')),
        # rxor=bool_method(lambda x, y: operator.xor(y, x), names('rxor [^]'))
        ))

    new_methods = dict((names(k), v) for k, v in new_methods.items())
    return new_methods


def add_methods(cls, new_methods, force, select, exclude):
    if select and exclude:
        raise TypeError("May only pass either select or exclude")
    methods = new_methods
    if select:
        select = set(select)
        methods = {}
        for key, method in new_methods.items():
            if key in select:
                methods[key] = method
    if exclude:
        for k in exclude:
            new_methods.pop(k, None)

    for name, method in new_methods.items():
        if force or name not in cls.__dict__:
            bind_method(cls, name, method)

#----------------------------------------------------------------------
# Arithmetic
def add_special_arithmetic_methods(cls, arith_method=None, radd_func=None,
                                   comp_method=None, bool_method=None,
                                   use_numexpr=True, force=False, select=None,
                                   exclude=None):
    """
    Adds the full suite of special arithmetic methods (``__add__``, ``__sub__``, etc.) to the class.

    Parameters
    ----------
    arith_method : function (optional)
        factory for special arithmetic methods, with op string:
        f(op, name, str_rep, default_axis=None, fill_zeros=None, **eval_kwargs)
    radd_func :  function (optional)
        Possible replacement for ``operator.add`` for compatibility
    comp_method : function, optional,
        factory for rich comparison - signature: f(op, name, str_rep)
    use_numexpr : bool, default True
        whether to accelerate with numexpr, defaults to True
    force : bool, default False
        if False, checks whether function is defined **on ``cls.__dict__``** before defining
        if True, always defines functions on class base
    select : iterable of strings (optional)
        if passed, only sets functions with names in select
    exclude : iterable of strings (optional)
        if passed, will not set functions with names in exclude
    """
    radd_func = radd_func or operator.add
    # in frame, special methods have default_axis = None, comp methods use 'columns'
    new_methods = _create_methods(arith_method, radd_func, comp_method, bool_method, use_numexpr, default_axis=None,
                                  special=True)

    # inplace operators (I feel like these should get passed an `inplace=True`
    # or just be removed
    new_methods.update(dict(
        __iadd__=new_methods["__add__"],
        __isub__=new_methods["__sub__"],
        __imul__=new_methods["__mul__"],
        __itruediv__=new_methods["__truediv__"],
        __ipow__=new_methods["__pow__"]
    ))
    if not compat.PY3:
        new_methods["__idiv__"] = new_methods["__div__"]

    add_methods(cls, new_methods=new_methods, force=force, select=select, exclude=exclude)


def add_flex_arithmetic_methods(cls, flex_arith_method, radd_func=None,
                                flex_comp_method=None, flex_bool_method=None,
                                use_numexpr=True, force=False, select=None,
                                exclude=None):
    """
    Adds the full suite of flex arithmetic methods (``pow``, ``mul``, ``add``) to the class.

    Parameters
    ----------
    flex_arith_method : function (optional)
        factory for special arithmetic methods, with op string:
        f(op, name, str_rep, default_axis=None, fill_zeros=None, **eval_kwargs)
    radd_func :  function (optional)
        Possible replacement for ``lambda x, y: operator.add(y, x)`` for compatibility
    flex_comp_method : function, optional,
        factory for rich comparison - signature: f(op, name, str_rep)
    use_numexpr : bool, default True
        whether to accelerate with numexpr, defaults to True
    force : bool, default False
        if False, checks whether function is defined **on ``cls.__dict__``** before defining
        if True, always defines functions on class base
    select : iterable of strings (optional)
        if passed, only sets functions with names in select
    exclude : iterable of strings (optional)
        if passed, will not set functions with names in exclude
    """
    radd_func = radd_func or (lambda x, y: operator.add(y, x))
    # in frame, default axis is 'columns', doesn't matter for series and panel
    new_methods = _create_methods(
        flex_arith_method, radd_func, flex_comp_method, flex_bool_method,
        use_numexpr, default_axis='columns', special=False)
    new_methods.update(dict(
        multiply=new_methods['mul'],
        subtract=new_methods['sub'],
        divide=new_methods['div']
    ))

    add_methods(cls, new_methods=new_methods, force=force, select=select, exclude=exclude)

def cleanup_name(name):
    """cleanup special names
    >>> cleanup_name("__rsub__")
    sub
    >>> cleanup_name("rand_")
    and_
    """
    if name[:2] == "__":
        name = name[2:-2]
    if name[0] == "r":
        name = name[1:]
    # readd last _ for operator names.
    if name == "or":
        name = "or_"
    elif name == "and":
        name = "and_"
    return name

# ----------------------------------------------------------------------------
# Arithmetic factory methods

def mask_no_list(obj, mask):
    return obj[mask]

def mask_with_list(obj, mask):
    return np.array(list(obj[mask]))

def _standard_na_op(x, y, op, str_rep=None, fill_zeros=None,
                    convert_mask=False, masker=None, eval_kwargs={}):
    """standard internal operation handler, generally you want to wrap it
    in a partial so it can be called with just two arguments, e.g.
    na_op = partial(_standard_na_op, op=op, str_rep=str_rep, fill_zeros=fill_zeros)

    x : array-like
        object to be operated open
    y : object (e.g., scalar or ndarray-like)
        object to perform arithmetic with
    op : binary arithmetic function function
    str_rep : str (optional)
        string representation of operation to pass to expressions.evaluate.
    fill_zeros : object (optional)
        passes to com._fill_zeros (does nothing if None)
    convert_mask : bool (default False)
        if True, wraps mask with list and does not call upcast_putmask
        if False, does not wrap mask, but calls upcast_putmask
    masker : bool (optional)
        Value to fill in for masked values. Must explicitly pass if using
        convert_mask.
    eval_kwargs : dict (optional)
        Keyword arguments to pass to expressions.evaluate. Note that this must
        be a literal dict, you can't pass as ``**eval_kwargs``
    """
    # generally used for bool methods
    if convert_mask:
        do_mask = lambda obj, mask: np.array(list(obj[mask]))
        assert masker is not None, "With convert_mask, must also specify masker"
    else:
        do_mask = lambda obj, maks: obj[mask]

    try:
        result = expressions.evaluate(op, str_rep, x, y,
                                        raise_on_error=True, **eval_kwargs)
    except TypeError:
        xrav = x.ravel()
        result = np.empty(x.size, dtype=x.dtype)
        if isinstance(y, (np.ndarray, com.ABCSeries)):
            yrav = y.ravel()
            mask = notnull(xrav) & notnull(yrav)
            result[mask] = op(do_mask(xrav, mask), do_mask(yrav, mask))
        else:
            mask = notnull(xrav)
            result[mask] = op(do_mask(xrav, mask), y)
        if convert_mask:
            if not mask.all():
                result[-mask] = masker
        else:
            result, changed = com._maybe_upcast_putmask(result, -mask, np.nan)

        result = result.reshape(x.shape)


# direct copy of original Series _TimeOp
class _TimeOp(object):
    """
    Wrapper around Series datetime/time/timedelta arithmetic operations.
    Generally, you should use classmethod ``maybe_convert_for_time_op`` as an
    entry point.
    """
    fill_value = tslib.iNaT
    wrap_results = staticmethod(lambda x: x)
    dtype = None

    def __init__(self, left, right, name):
        self.name = name

        lvalues = self._convert_to_array(left, name=name)
        rvalues = self._convert_to_array(right, name=name)

        self.is_timedelta_lhs = com.is_timedelta64_dtype(left)
        self.is_datetime_lhs  = com.is_datetime64_dtype(left)
        self.is_integer_lhs = left.dtype.kind in ['i','u']
        self.is_datetime_rhs  = com.is_datetime64_dtype(rvalues)
        self.is_timedelta_rhs = com.is_timedelta64_dtype(rvalues) or (not self.is_datetime_rhs and _np_version_under1p7)
        self.is_integer_rhs = rvalues.dtype.kind in ('i','u')

        self._validate()

        self._convert_for_datetime(lvalues, rvalues)

    def _validate(self):
        # timedelta and integer mul/div

        if (self.is_timedelta_lhs and self.is_integer_rhs) or\
           (self.is_integer_lhs and self.is_timedelta_rhs):

            if self.name not in ('__truediv__','__div__','__mul__'):
                raise TypeError("can only operate on a timedelta and an integer for "
                                "division, but the operator [%s] was passed" % self.name)

        # 2 datetimes
        elif self.is_datetime_lhs and self.is_datetime_rhs:
            if self.name != '__sub__':
                raise TypeError("can only operate on a datetimes for subtraction, "
                                "but the operator [%s] was passed" % self.name)


        # 2 timedeltas
        elif self.is_timedelta_lhs and self.is_timedelta_rhs:

            if self.name not in ('__div__', '__truediv__', '__add__', '__sub__'):
                raise TypeError("can only operate on a timedeltas for "
                                "addition, subtraction, and division, but the operator [%s] was passed" % self.name)

        # datetime and timedelta
        elif self.is_datetime_lhs and self.is_timedelta_rhs:

            if self.name not in ('__add__','__sub__'):
                raise TypeError("can only operate on a datetime with a rhs of a timedelta for "
                                "addition and subtraction, but the operator [%s] was passed" % self.name)

        elif self.is_timedelta_lhs and self.is_datetime_rhs:

            if self.name != '__add__':
                raise TypeError("can only operate on a timedelta and a datetime for "
                                "addition, but the operator [%s] was passed" % self.name)
        else:
            raise TypeError('cannot operate on a series with out a rhs '
                            'of a series/ndarray of type datetime64[ns] '
                            'or a timedelta')

    def _convert_to_array(self, values, name=None):
        """converts values to ndarray"""
        from pandas.tseries.index import DatetimeIndex
        from pandas.tseries.period import PeriodIndex
        from pandas.tseries.offsets import DateOffset
        from pandas.tseries.timedeltas import _possibly_cast_to_timedelta

        coerce = 'compat' if _np_version_under1p7 else True
        if not is_list_like(values):
            values = np.array([values])
        inferred_type = lib.infer_dtype(values)
        if inferred_type in ('datetime64','datetime','date','time'):
            # a datetlike
            if not (isinstance(values, (pa.Array, com.ABCSeries)) and com.is_datetime64_dtype(values)):
                values = tslib.array_to_datetime(values)
            elif isinstance(values, DatetimeIndex):
                values = values.to_series()
        elif inferred_type in ('timedelta', 'timedelta64'):
            # have a timedelta, convert to to ns here
            values = _possibly_cast_to_timedelta(values, coerce=coerce)
        elif inferred_type == 'integer':
            # py3 compat where dtype is 'm' but is an integer
            if values.dtype.kind == 'm':
                values = values.astype('timedelta64[ns]')
            elif isinstance(values, PeriodIndex):
                values = values.to_timestamp().to_series()
            elif name not in ('__truediv__','__div__','__mul__'):
                raise TypeError("incompatible type for a datetime/timedelta "
                                "operation [{0}]".format(name))
        elif isinstance(values[0],DateOffset):
            # handle DateOffsets
            os = pa.array([ getattr(v,'delta',None) for v in values ])
            mask = isnull(os)
            if mask.any():
                raise TypeError("cannot use a non-absolute DateOffset in "
                                "datetime/timedelta operations [{0}]".format(
                                    ','.join([ com.pprint_thing(v) for v in values[mask] ])))
            values = _possibly_cast_to_timedelta(os, coerce=coerce)
        else:
            raise TypeError("incompatible type [{0}] for a datetime/timedelta operation".format(pa.array(values).dtype))

        return values

    def _convert_for_datetime(self, lvalues, rvalues):
        mask = None
        # datetimes require views
        if self.is_datetime_lhs or self.is_datetime_rhs:
            # datetime subtraction means timedelta
            if self.is_datetime_lhs and self.is_datetime_rhs:
                self.dtype = 'timedelta64[ns]'
            else:
                self.dtype = 'datetime64[ns]'
            mask = isnull(lvalues) | isnull(rvalues)
            lvalues = lvalues.view(np.int64)
            rvalues = rvalues.view(np.int64)

        # otherwise it's a timedelta
        else:
            self.dtype = 'timedelta64[ns]'
            mask = isnull(lvalues) | isnull(rvalues)
            lvalues = lvalues.astype(np.int64)
            rvalues = rvalues.astype(np.int64)

            # time delta division -> unit less
            # integer gets converted to timedelta in np < 1.6
            if (self.is_timedelta_lhs and self.is_timedelta_rhs) and\
               not self.is_integer_rhs and\
               not self.is_integer_lhs and\
               self.name in ('__div__', '__truediv__'):
                self.dtype = 'float64'
                self.fill_value = np.nan
                lvalues = lvalues.astype(np.float64)
                rvalues = rvalues.astype(np.float64)

        # if we need to mask the results
        if mask is not None:
            if mask.any():
                def f(x):
                    x = pa.array(x,dtype=self.dtype)
                    np.putmask(x,mask,self.fill_value)
                    return x
                self.wrap_results = f
        self.lvalues = lvalues
        self.rvalues = rvalues

    @classmethod
    def maybe_convert_for_time_op(cls, left, right, name):
        """
        if ``left`` and ``right`` are appropriate for datetime arithmetic with
        operation ``name``, processes them and returns a ``_TimeOp`` object
        that stores all the required values.  Otherwise, it will generate
        either a ``NotImplementedError`` or ``None``, indicating that the
        operation is unsupported for datetimes (e.g., an unsupported r_op) or
        that the data is not the right type for time ops.
        """
        # decide if we can do it
        is_timedelta_lhs = com.is_timedelta64_dtype(left)
        is_datetime_lhs  = com.is_datetime64_dtype(left)
        if not (is_datetime_lhs or is_timedelta_lhs):
            return None
        # rops currently disabled
        if name.startswith('__r'):
            return NotImplemented

        return cls(left, right, name)


# direct copy of series method with slightly different signature
# def _arith_method(op, name, fill_zeros=None):
def _arith_method_SERIES(op, name, str_rep=None, fill_zeros=None, default_axis=None, **eval_kwargs):
    """
    Wrapper function for Series arithmetic operations, to avoid
    code duplication.
    """
    def na_op(x, y):
        try:
            # TODO: Accelerate this with numexpr
            result = op(x, y)
            result = com._fill_zeros(result, y, fill_zeros)

        except TypeError:
            result = pa.empty(len(x), dtype=x.dtype)
            if isinstance(y, (pa.Array, com.ABCSeries)):
                mask = notnull(x) & notnull(y)
                result[mask] = op(x[mask], y[mask])
            else:
                mask = notnull(x)
                result[mask] = op(x[mask], y)

            result, changed = com._maybe_upcast_putmask(result, -mask, pa.NA)

        return result

    def wrapper(left, right, name=name):
        from pandas.core.frame import DataFrame

        time_converted = _TimeOp.maybe_convert_for_time_op(left, right, name)

        if time_converted is None:
            lvalues, rvalues = left, right
            dtype = None
            wrap_results = lambda x: x
        elif time_converted == NotImplemented:
            return NotImplemented
        else:
            lvalues = time_converted.lvalues
            rvalues = time_converted.rvalues
            dtype = time_converted.dtype
            wrap_results = time_converted.wrap_results

        if isinstance(rvalues, com.ABCSeries):

            join_idx, lidx, ridx = left.index.join(rvalues.index, how='outer',
                                                   return_indexers=True)
            rindex = rvalues.index
            name = _maybe_match_name(left, rvalues)
            lvalues = getattr(lvalues, 'values', lvalues)
            rvalues = getattr(rvalues, 'values', rvalues)
            if left.index.equals(rindex):
                index = left.index
            else:
                index = join_idx

                if lidx is not None:
                    lvalues = com.take_1d(lvalues, lidx)

                if ridx is not None:
                    rvalues = com.take_1d(rvalues, ridx)

            arr = na_op(lvalues, rvalues)

            return left._constructor(wrap_results(arr), index=index,
                                     name=name, dtype=dtype)
        elif isinstance(right, DataFrame):
            return NotImplemented
        else:
            # scalars
            if hasattr(lvalues, 'values'):
                lvalues = lvalues.values
            return left._constructor(wrap_results(na_op(lvalues, rvalues)),
                                     index=left.index, name=left.name, dtype=dtype)
    return wrapper

# Original series comp_method with slight tweak to signature
# def _comp_method(op, name, masker=False):
def _comp_method_SERIES(op, name, str_rep=None, masker=False):
    """
    Wrapper function for Series arithmetic operations, to avoid
    code duplication.
    """
    def na_op(x, y):
        if x.dtype == np.object_:
            if isinstance(y, list):
                y = lib.list_to_object_array(y)

            if isinstance(y, (pa.Array, com.ABCSeries)):
                if y.dtype != np.object_:
                    result = lib.vec_compare(x, y.astype(np.object_), op)
                else:
                    result = lib.vec_compare(x, y, op)
            else:
                result = lib.scalar_compare(x, y, op)
        else:

            try:
                result = getattr(x,name)(y)
                if result is NotImplemented:
                    raise TypeError("invalid type comparison")
            except (AttributeError):
                result = op(x, y)

        return result

    def wrapper(self, other):
        from pandas.core.series import Series

        if isinstance(other, com.ABCSeries):
            name = _maybe_match_name(self, other)
            if len(self) != len(other):
                raise ValueError('Series lengths must match to compare')
            return self._constructor(na_op(self.values, other.values),
                                     index=self.index, name=name)
        elif isinstance(other, com.ABCDataFrame):  # pragma: no cover
            return NotImplemented
        elif isinstance(other, (pa.Array, com.ABCSeries)):
            if len(self) != len(other):
                raise ValueError('Lengths must match to compare')
            return self._constructor(na_op(self.values, np.asarray(other)),
                                     index=self.index, name=self.name)
        else:

            mask = isnull(self)

            values = self.values
            other = _index.convert_scalar(values, other)

            if issubclass(values.dtype.type, np.datetime64):
                values = values.view('i8')

            # scalars
            res = na_op(values, other)
            if np.isscalar(res):
                raise TypeError('Could not compare %s type with Series'
                                % type(other))

            # always return a full value series here
            res = _values_from_object(res)

            res = Series(res, index=self.index, name=self.name, dtype='bool')

            # mask out the invalids
            if mask.any():
                res[mask.values] = masker

            return res
    return wrapper


# original Series _bool_method with slight change to signature
def _bool_method_SERIES(op, name, str_rep=None):
    """
    Wrapper function for Series arithmetic operations, to avoid
    code duplication.
    """
    def na_op(x, y):
        try:
            result = op(x, y)
        except TypeError:
            if isinstance(y, list):
                y = lib.list_to_object_array(y)

            if isinstance(y, (pa.Array, com.ABCSeries)):
                if (x.dtype == np.bool_ and
                        y.dtype == np.bool_):  # pragma: no cover
                    result = op(x, y)  # when would this be hit?
                else:
                    x = com._ensure_object(x)
                    y = com._ensure_object(y)
                    result = lib.vec_binop(x, y, op)
            else:
                result = lib.scalar_binop(x, y, op)

        return result

    def wrapper(self, other):
        if isinstance(other, com.ABCSeries):
            name = _maybe_match_name(self, other)
            return self._constructor(na_op(self.values, other.values),
                                     index=self.index, name=name)
        elif isinstance(other, com.ABCDataFrame):
            return NotImplemented
        else:
            # scalars
            return self._constructor(na_op(self.values, other),
                                     index=self.index, name=self.name)
    return wrapper


# original Series _radd_compat method
def _radd_compat_SERIES(left, right):
    radd = lambda x, y: y + x
    # GH #353, NumPy 1.5.1 workaround
    try:
        output = radd(left, right)
    except TypeError:
        cond = (_np_version_under1p6 and
                left.dtype == np.object_)
        if cond:  # pragma: no cover
            output = np.empty_like(left)
            output.flat[:] = [radd(x, right) for x in left.flat]
        else:
            raise

    return output


# Copy of original series _flex_method with slightly changed signature
def _flex_method_SERIES(op, name, str_rep=None, default_axis=None, fill_zeros=None, **eval_kwargs):
    doc = """
    Binary operator %s with support to substitute a fill_value for missing data
    in one of the inputs

    Parameters
    ----------
    other: Series or scalar value
    fill_value : None or float value, default None (NaN)
        Fill missing (NaN) values with this value. If both Series are
        missing, the result will be missing
    level : int or name
        Broadcast across a level, matching Index values on the
        passed MultiIndex level

    Returns
    -------
    result : Series
    """ % name

    @Appender(doc)
    def f(self, other, level=None, fill_value=None):
        if isinstance(other, com.ABCSeries):
            return self._binop(other, op, level=level, fill_value=fill_value)
        elif isinstance(other, (pa.Array, com.ABCSeries, list, tuple)):
            if len(other) != len(self):
                raise ValueError('Lengths must be equal')
            return self._binop(self._constructor(other, self.index), op,
                               level=level, fill_value=fill_value)
        else:
            return self._constructor(op(self.values, other), self.index,
                                     name=self.name)

    f.__name__ = name
    return f

series_flex_funcs = dict(flex_arith_method=_flex_method_SERIES,
                         radd_func=_radd_compat_SERIES,
                         flex_comp_method=_comp_method_SERIES)
series_special_funcs = dict(arith_method=_arith_method_SERIES,
                            radd_func=_radd_compat_SERIES,
                            comp_method=_comp_method_SERIES,
                            bool_method=_bool_method_SERIES)


_arith_doc_FRAME = """
Binary operator %s with support to substitute a fill_value for missing data in
one of the inputs

Parameters
----------
other : Series, DataFrame, or constant
axis : {0, 1, 'index', 'columns'}
    For Series input, axis to match Series index on
fill_value : None or float value, default None
    Fill missing (NaN) values with this value. If both DataFrame locations are
    missing, the result will be missing
level : int or name
    Broadcast across a level, matching Index values on the
    passed MultiIndex level

Notes
-----
Mismatched indices will be unioned together

Returns
-------
result : DataFrame
"""


# Direct copy of _arith_method from DataFrame
def _arith_method_FRAME(op, name, str_rep=None, default_axis='columns', fill_zeros=None, **eval_kwargs):
    def na_op(x, y):
        try:
            result = expressions.evaluate(
                op, str_rep, x, y, raise_on_error=True, **eval_kwargs)
        except TypeError:
            xrav = x.ravel()
            result = np.empty(x.size, dtype=x.dtype)
            if isinstance(y, (np.ndarray, com.ABCSeries)):
                yrav = y.ravel()
                mask = notnull(xrav) & notnull(yrav)
                result[mask] = op(xrav[mask], yrav[mask])
            else:
                mask = notnull(xrav)
                result[mask] = op(xrav[mask], y)

            result, changed = com._maybe_upcast_putmask(result, -mask, np.nan)
            result = result.reshape(x.shape)

        result = com._fill_zeros(result, y, fill_zeros)

        return result

    @Appender(_arith_doc_FRAME % name)
    def f(self, other, axis=default_axis, level=None, fill_value=None):
        from pandas.core.series import Series
        from pandas.core.frame import DataFrame

        if isinstance(other, com.ABCDataFrame):    # Another DataFrame
            return self._combine_frame(other, na_op, fill_value, level)
        elif isinstance(other, com.ABCSeries):
            return self._combine_series(other, na_op, fill_value, axis, level)
        elif isinstance(other, (list, tuple)):
            if axis is not None and self._get_axis_name(axis) == 'index':
                # casted = self._constructor_sliced(other, index=self.index)
                casted = Series(other, index=self.index)
            else:
                # casted = self._constructor_sliced(other, index=self.columns)
                casted = Series(other, index=self.columns)
            return self._combine_series(casted, na_op, fill_value, axis, level)
        elif isinstance(other, np.ndarray):
            if other.ndim == 1:
                if axis is not None and self._get_axis_name(axis) == 'index':
                    # casted = self._constructor_sliced(other, index=self.index)
                    casted = Series(other, index=self.index)
                else:
                    # casted = self._constructor_sliced(other, index=self.columns)
                    casted = Series(other, index=self.columns)
                return self._combine_series(casted, na_op, fill_value,
                                            axis, level)
            elif other.ndim == 2:
                # casted = self._constructor(other, index=self.index,
                                           # columns=self.columns)
                casted = DataFrame(other, index=self.index,
                                 columns=self.columns)
                return self._combine_frame(casted, na_op, fill_value, level)
            else:
                raise ValueError("Incompatible argument shape: %s" %
                                 (other.shape,))
        else:
            return self._combine_const(other, na_op)

    f.__name__ = name

    return f


# Masker unused for now
# Just copy of original _flex_comp_method from DataFrame
def _flex_comp_method_FRAME(op, name, str_rep=None, default_axis='columns', masker=False):

    def na_op(x, y):
        try:
            result = op(x, y)
        except TypeError:
            xrav = x.ravel()
            result = np.empty(x.size, dtype=x.dtype)
            if isinstance(y, (np.ndarray, com.ABCSeries)):
                yrav = y.ravel()
                mask = notnull(xrav) & notnull(yrav)
                result[mask] = op(np.array(list(xrav[mask])),
                                  np.array(list(yrav[mask])))
            else:
                mask = notnull(xrav)
                result[mask] = op(np.array(list(xrav[mask])), y)

            if op == operator.ne:  # pragma: no cover
                np.putmask(result, -mask, True)
            else:
                np.putmask(result, -mask, False)
            result = result.reshape(x.shape)

        return result

    @Appender('Wrapper for flexible comparison methods %s' % name)
    def f(self, other, axis=default_axis, level=None):
        from pandas.core.frame import DataFrame
        from pandas.core.series import Series

        if isinstance(other, com.ABCDataFrame):    # Another DataFrame
            return self._flex_compare_frame(other, na_op, str_rep, level)

        elif isinstance(other, com.ABCSeries):
            return self._combine_series(other, na_op, None, axis, level)

        elif isinstance(other, (list, tuple)):
            if axis is not None and self._get_axis_name(axis) == 'index':
                casted = Series(other, index=self.index)
            else:
                casted = Series(other, index=self.columns)

            return self._combine_series(casted, na_op, None, axis, level)

        elif isinstance(other, np.ndarray):
            if other.ndim == 1:
                if axis is not None and self._get_axis_name(axis) == 'index':
                    casted = Series(other, index=self.index)
                else:
                    casted = Series(other, index=self.columns)

                return self._combine_series(casted, na_op, None, axis, level)

            elif other.ndim == 2:
                casted = DataFrame(other, index=self.index,
                                   columns=self.columns)

                return self._flex_compare_frame(casted, na_op, str_rep, level)

            else:
                raise ValueError("Incompatible argument shape: %s" %
                                 (other.shape,))

        else:
            return self._combine_const(other, na_op)

    f.__name__ = name

    return f
# def _flex_comp_method_FRAME(op, name, str_rep=None, default_axis='columns',
#                             masker=False):

#     na_op = partial(_standard_na_op, op=op, str_rep=str_rep, convert_mask=True,
#                     masker=masker)

#     @Appender('Wrapper for flexible comparison methods %s' % name)
#     def f(self, other, axis=default_axis, level=None):
#         if isinstance(other, com.ABCDataFrame):    # Another DataFrame
#             return self._flex_compare_frame(other, na_op, str_rep, level)

#         elif isinstance(other, com.ABCSeries):
#             return self._combine_series(other, na_op, None, axis, level)

#         elif isinstance(other, (list, tuple)):
#             if axis is not None and self._get_axis_name(axis) == 'index':
#                 casted = self._constructor_sliced(other, index=self.index)
#             else:
#                 casted = self._constructor_sliced(other, index=self.columns)

#             return self._combine_series(casted, na_op, None, axis, level)

#         elif isinstance(other, np.ndarray):
#             if other.ndim == 1:
#                 if axis is not None and self._get_axis_name(axis) == 'index':
#                     casted = self._constructor_sliced(other, index=self.index)
#                 else:
#                     casted = self._constructor_sliced(other, index=self.columns)

#                 return self._combine_series(casted, na_op, None, axis, level)

#             elif other.ndim == 2:
#                 casted = self._constructor(other, index=self.index,
#                                            columns=self.columns)

#                 return self._flex_compare_frame(casted, na_op, str_rep, level)

#             else:
#                 raise ValueError("Incompatible argument shape: %s" %
#                                  (other.shape,))

#         else:
#             return self._combine_const(other, na_op)

#     f.__name__ = name

#     return f


# direct copy of method from pandas/core/frame
def _comp_method_FRAME(func, name, str_rep, masker=False):
    @Appender('Wrapper for comparison method %s' % name)
    def f(self, other):
        if isinstance(other, com.ABCDataFrame):    # Another DataFrame
            return self._compare_frame(other, func, str_rep)
        elif isinstance(other, com.ABCSeries):
            return self._combine_series_infer(other, func)
        else:

            # straight boolean comparisions we want to allow all columns
            # (regardless of dtype to pass thru) See #4537 for discussion.
            return self._combine_const(other, func, raise_on_error=False).fillna(True).astype(bool)

    f.__name__ = name

    return f

frame_flex_funcs = dict(flex_arith_method=_arith_method_FRAME,
                        radd_func=_radd_compat_SERIES,
                        flex_comp_method=_flex_comp_method_FRAME)

frame_special_funcs = dict(arith_method=_arith_method_FRAME,
                           radd_func=_radd_compat_SERIES,
                           comp_method=_comp_method_FRAME,
                           bool_method=_arith_method_FRAME)

def _arith_method_PANEL(op, name, str_rep=None, fill_zeros=None, default_axis=None, **eval_kwargs):
    # work only for scalars
    def na_op(x, y):
        try:
            result = expressions.evaluate(op, str_rep, x, y, raise_on_error=True, **eval_kwargs)
        except TypeError:
            result = op(x, y)

        # handles discrepancy between numpy and numexpr on division/mod by 0
        result = com._fill_zeros(result,y,fill_zeros)
        return result

    def f(self, other):
        if not np.isscalar(other):
            raise ValueError('Simple arithmetic with %s can only be '
                             'done with scalar values' % self._constructor.__name__)

        return self._combine(other, na_op)
    f.__name__ = name
    return f


def _comp_method_PANEL(op, name, str_rep=None, masker=False):
    na_op = partial(_standard_na_op, op=op, str_rep=str_rep, convert_mask=True,
                    masker=masker)

    @Appender('Wrapper for comparison method %s' % name)
    def f(self, other):
        if isinstance(other, self._constructor):
            return self._compare_constructor(other, op)
        elif isinstance(other, (self._constructor_sliced, com.ABCDataFrame, com.ABCSeries)):
            raise Exception("input needs alignment for this object [%s]" %
                            self._constructor)
        else:
            return self._combine_const(other, na_op)

    f.__name__ = name

    return f

panel_special_funcs = dict(arith_method=_arith_method_PANEL,
                           comp_method=_comp_method_PANEL,
                           bool_method=_arith_method_PANEL)
