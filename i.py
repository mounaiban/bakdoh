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

from bisect import bisect_left, bisect_right

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
    i = bisect_left(s, k)
    j = bisect_right(s, k)
    return (i, j)
    # return _do_bfind(s, k, 0, cfunc)

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

class BitDict:
    """
    Helper class for representing name-addressable packed bitfields
    or integers.

    """
    def __init__(self, fields, **kwargs):
        """
        Arguments for creating a BitDict:
        
        * fields: dict with key-value pairs like {k: n, ...},
          where k is the name of the field and n is the number of
          bits of the field. The first field will be represented
          by the highest bits.

        * init: a zero or positive int which sets the initial
          state of the BitDict. Given fields={'A':1, 'B':1, 'C':2},
          when init=0b1101, fields A==0x1, B==0x1, C==0x01.

        * dict_class: a dict-like class; intended to allow easy
          switching to OrderedDict when using older versions of
          Python. Defaults to dict.

        """
        self._dict_class = kwargs.get('dict_class', dict)
            # Please set dict_class to collections.OrderedDict when 
            # using Python 3.1 to 3.5
            # See:
            # StackOverflow. Difference between dictionary and OrderedDict
            # https://stackoverflow.com/questions/34305003/
            # Ramos (2021-03-29). OrderedDict vs. dict in Python:
            # The Right Tool for the Right Job
            # https://realpython.com/python-ordereddict/
        self._do_validate_type(fields)
        self._do_validate_field_types(fields)
        self._do_init_fields(fields)
        self._fields = fields
        a_init = kwargs.get('init', 0)
        self._do_set_from_int(a_init)

    def _do_set_from_int(self, a):
        """
        Initialise all fields from a positive or zero int a.
        Excess bits are discarded. The first field in self._fields
        will take on the highest bits.

        """
        # TODO: Add a _do_from_bytes() method for supporting
        # C-like structure input
        # PROTIP: a is for 'all fields'
        if a < 0:
            details = {
                'msg': 'int must be positive or zero',
                'a': a,
            }
            raise ValueError(details)
        names = self.get_field_names()
        names.reverse()
        for n in names:
            bits = self.get_bit_length(n)
            self._fields[n][1] = a & (1 << bits) - 1
            a >>= bits

    def get_field_names(self):
        return list(self._fields.keys())

    def get_value(self, field):
        """
        Return the big endian int value of a field. Returns None if the
        field does not exist.

        """
        if field in self._fields:
            return self._fields[field][1]

    def get_bit_length(self, field):
        """
        Returns the bit length of the field. Returns None if the field
        does not exist.

        """
        if field in self._fields:
            return self._fields[field][0]

    def set_bit(self, field, pos, b):
        """
        Set a single bit in position pos of a field to b.
        b==1 or b==0; pos=0 sets the least signficant bit in the field.

        """
        if b < 0 or b > 1:
            details = {
                'msg': 'b must be either 1 or 0',
                'b': b,
            }
            raise ValueError(details)
        last_bit = self.get_bit_length(field) - 1
        if pos < 0 or pos > last_bit:
            details = {
                'msg': 'invalid position',
                'pos_min': 0,
                'pos_max': last_bit,
                'pos': pos,
            }
            raise IndexError(details)
        else:
            if b == 1:
                self._fields[field][1] |= (1 << pos)
            elif b == 0:
                self._fields[field][1] &= ~(1 << pos) 

    def set_value(self, field, v):
        """
        Sets the value of the entire field with an int v.
        """
        if v < 0 or v > self.max_value(field):
            details = {
                'msg': 'value out of range',
                'v': v,
                'v_min': 0,
                'v_max': self.max_value(field),
            }
            raise ValueError(details)
        self._fields[field][1] = v

    def max_value(self, field):
        n = self._fields.get(field)[0]
        return (1 << n) - 1

    def _do_validate_type(self, fields):
        """Check if the field dictionary is of an accepted type"""

        if not isinstance(fields, self._dict_class):
            details = {
                'msg': f'specify fields as a {self._dict_class.__name__}',
            }
            raise TypeError(details)

    def _do_validate_field_types(self, fields):
        """
        Check if field lengths are valid.
        Returns None if no errors were found.
        Raises ValueError if at least one field is of an invalid length.

        All bit field must be defined as follows:

        {name: len}, where len is a positive int specifying the number
        of bits

        """
        bad_keys = []
        for k in fields.keys():
            if fields[k] <= 0:
                bad_keys.append(fields[k])
        if len(bad_keys) > 0:
            details = {
                'msg': 'all bit field lenghts must be positive',
                'invalid_fields': bad_keys,
            }
            raise ValueError(details)

    def _do_init_fields(self, fields):
        """
        Converts field specifiers to registers by adding space to
        store bits. Works in place.

        """
        for k in fields.keys():
            fields[k] = [fields[k], 0]

    def __int__(self):
        out = 0
        for k in self._fields.keys():
            out <<= self.get_bit_length(k)
            out |= self.get_value(k)
        return out

    def __len__(self):
        a = 0
        for f in self._fields.values():
            a += f[0]
        return a

