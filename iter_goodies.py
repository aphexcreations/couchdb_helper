#!/usr/bin/env python

"""
Goodies for helping with iterables
"""

from collections import Iterable
from itertools import izip
from itertools import chain


def chunks(r, n):
    """
    Splits an iterable into chunks of n.
    Accepts dicts, lists, tuples, and some types
    of generators.

    Author: Brendon Crawford
    """
    ##
    ## Dict
    ##
    if isinstance(r, dict):
        keys, vals = zip(*r.iteritems())
        out = (
            dict((keys[ii], vals[ii]) for ii in xrange(i, i + n) \
                  if ii < len(r)) \
            for i in xrange(0, len(r), n)
        )
    ##
    ## List or anything that has a length and is sliceable.
    ## This most likely includes a Django QuerySet result
    ##
    elif isinstance(r, Iterable) and \
            hasattr(r, '__len__') and \
            hasattr(r, '__getitem__'):
        out = (r[i:i + n] for i in xrange(0, len(r), n))
    ##
    ## Some other generator that can be converted
    ## to a lsit
    ##
    elif isinstance(r, Iterable):
        ri = list(r)
        out = (ri[i:i + n] for i in xrange(0, len(ri), n))
    ##
    ## Unknown
    ##
    else:
        raise Exception('Cannot chunkify!!')
    return out
