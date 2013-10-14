.. vim:tw=79

===========================
Index Class - characterized
===========================

Trying to enumerate the interface of the index class:
=====================================================

Absolutely necessary elements of Index interface:
-------------------------------------------------

There are a *ton* of methods and properties of Index, many of which are
related and probably will be implemented with sane defaults on Index.
The goal is to get things down to about 10 methods and properties that
subclasses **must** define.

Properties
~~~~~~~~~~
* values
* ndim
* ``_join_priority``
* ``_has_complex_internals`` --> for groupby, probably will change into a
  getattr
* nlevels
* ``_mpl_repr``
* ``_na_value`` - nan value to use with the Index (this is definitely going to
  be removed.
* ``is_monotonic`` - delegated to ``_engine``, means that Index is always
  increasing
* ``is_lexosrted_for_tuple`` --> always True for non-MI
* ``is_unique`` --> has no dupes (bool + cached)
Type sniffing (may be removed)
* ``inferred_type`` --> cached dynamic property - possibly can hold values like
  'integer', 'floating', 'mixed-integer', 'mixed-integer-float', 'datetime'

Methods
~~~~~~~
* join
* union / +
* intersection / -
* take (ndarray.take-like)
* format (used to represent when on other PandasObjects)
* all
* any
* append
* equals (and potentially also ``is_`` and identical, but might be able to get
  rid of those)
* view() (currently may be called with Index or args to go to underlying
  ndarray. Is deprecated and soon will only be allowed to return shallow copy)
* copy()
* astype() --> Usage strongly discouraged. returns new Index based on type (delegates to object index if
  dtype equivalent to object)
* ``_assert_can_do_setop`` ??
* ``get_names``, ``set_names``, (and underlying ``_get_names``, ``_set_names``
  methods)
* ``rename`` (may or may not be alias to get_names and set_names)
* is_integer, is_floating, is_mixed, is_numeric, holds_integer,
  is_type_compatible, is_all_dates : all are based
  on inferred_type.
* ``_convert_scalar_integer`` --> deprecated and discouraged (workaround for
  Float64Index).
  **TODO: Series needs to be changed to NOT use this method!!**
* to_native_types --> probably will be deprecated
* asof
* asof_locs
* order
* diff
* unique
* get_loc
* map
* isin

NotImplemented/TypeErrors methods:
* sort


Actually deprecated methods that will be gone by 0.14:

* ``tolist``



Properties on Index:
--------------------


* ``values`` --> returns an ndarray
* ``_values_no_copy`` -> returns values but without copying (likely will be
  renamed)
* ``asobject`` --> should return ObjectIndex (or at least Index of boxed
  values) -- often needed for isin
* ``_array_values()`` --> may be replaced entirely by asobject (but may be
  different for integer/float indices)

Properties generally delegated to ndarray on implementation:
------------------------------------------------------------


* ``dtype`` --> numpy dtype for Index
* ``__len__`` --> length of object
* ``shape``


Methods delegated to ndarray
----------------------------

* ``ravel``
* ``searchsorted`` (and I think the implementation is wrong for PeriodIndex)

Methods on Index:
-----------------

* ``_reconstruct`` - build up object from returned ndarray result
* ``__reduce__`` -> thin wrapper around ``_unpickle``

Special/Cythonized Methods and Properties (implementation detail - not really necessary to understand):
-------------------------------------------------------------------------------------------------------

* ``_engine`` --> cached property that handles some accelerated methods under
  the hood, passed ``vgetter()``, a method that gets the (unboxed, ndarray)
  values from the object (currently ``lambda: self.values``)
* ``_groupby``
* ``_arrmap``
* ``_left_indexer_unique``
* ``_left_indexer``
* ``_inner_indexer``
* ``_outer_indexer``


Other Notes on Changes in This PR
=================================

MultiIndex pickles from v0.7.3 and earlier (aka, 'v2' pickles) are no longer
supported. That said, you can easily convert them by using any pandas version
from 0.8, loading your pickle, running ``new_mi =
MultiIndex.from_tuples(list(pickled_mi.levels))`` and then pickling the
resulting object.

Passing ``MultiIndex(levels, labels)`` will now fail b/c of compatibility
issues. Would be tricky (and not worth it IMHO) to re-enable.

Maintains backward compatiiblity with Index(vals, dtype) format [2 pos'n args].
Needs test case(s) though.
