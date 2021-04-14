"""
Indexing Helper Classes and Functions Module

"""
# Copyright 2021 Mounaiban
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

LEFT = -1
RIGHT = 1

def cfunc_default(k, e):
    """
    Default basic comparison function for sorting and searches

    f(k, e) == 0 when k == e
    f(k, e) == -1 when k < e
    f(k, e) == 1 when k > e

    """
    if k < e:
        return -1
    elif k > e:
        return 1
    else:
        return 0

def find_edge(s, i, d=LEFT, cfunc=cfunc_default):
    """
    Find the start or stop slice index of a contiguous run of element
    s[i] in sequence s. Set d=LEFT to find the start of the run,
    or d=RIGHT to find the end of the run.

    Sequence s must be ordered for this function to work correctly.

    find_edge("ABBBBC", 2) => 1
    find_edge("BBBD", 1, d=RIGHT) => 3
    find_edge("K", 0) => 0

    cfunc is the comparator function as follows:
    cfunc(k, e); k is the key, e is the element in s being compared during
    the search.
    
    cfunc(k, e) == 0 when k is regarded as equal to e
    cfunc(k, e) == -1 when k < e
    cfunc(k, e) == 1 when k > e

    """
    len_s = len(s)
    if len_s < 1:
        raise IndexError("nothing to find in empty sequence")
    key = s[i]
    step = None
    if d == LEFT:
        step = i // 2
    else:
        step = (len_s - i) // 2
    last_found = True
    found = True
    # non-recursive binary search in big steps
    while step > 1:
        i += (step * d)
        found = cfunc(key, s[i]) == 0
        if found != last_found:
            d *= -1
        step //= 2
        last_found = found
    # finish with linear search when close to edge
    while 0 <= i < len_s:
        last_found = found
        found = cfunc(key, s[i]) == 0
        if last_found != found:
            break
        i += 1 * d
    if d == LEFT:
        return i+1  # i overshoots the left edge during search
    else:
        return i

def bfind(s, k, cfunc=cfunc_default):
    """
    Generic binary search. Find the index of k in sequence s.
    This function launches the recursive search process, _do_find()

    Returns a 2-tuple like (i, j), where:

    * i is the index of k in s if found, -1 if not found

    * j is the index to be used, if k is to be inserted into s while
      keeping s sorted.

    cfunc is the comparator function as follows:
    cfunc(k, e); k is the key, e is the element in s being compared during
    the search.
    
    cfunc(k, e) == 0 when k is regarded as equal to e
    cfunc(k, e) == -1 when k < e
    cfunc(k, e) == 1 when k > e

    """
    return _do_bfind(s, k, 0, cfunc)

def _do_bfind(s, k, gs0, cfunc):
    """
    Generic binary search launched from bfind().
    This function performs the recursive search process, and is similar to
    the bisect functions in Python standard library, but with support for
    custom comparison functions.

    See bfind() for details on s, k and cfunc.

    gs0 is the index of s[0] for the current recursive call, relative to
    s[0] of the root/first call. For example, given s="ABCDEFG" on the first
    call, when recursing into "EFG", gs0 == 4.

    """
    len_s = len(s)
    if len_s == 0:
        return (-1, gs0)
    i = len_s // 2
    e = s[i]
    if len_s == 1:
        if cfunc(k, e) == 0:
            return (gs0, gs0+1)
        elif cfunc(k, e) == -1:
            return (-1, gs0)
        else:
            return(-1, gs0+1)
    else:
        # len_s >= 2
        if cfunc(k, e) == 0:
            ileft = find_edge(s, i)
            iright = find_edge(s, i, d=RIGHT)
            return (ileft, iright)
        elif cfunc(k, e) == -1:
            return _do_bfind(s[:i], k, gs0, cfunc)
        else:
            return _do_bfind(s[i+1:], k, gs0+i+1, cfunc)

