"""
Bakdoh Infohoardi Text Database Access Module

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

import io
from i import bfind

def index_dup(seq, pos=0):
    """
    Return the index of the first duplicate of a seq of items, if any.
    The seq must not have any None elements.

    """
    d0 = None
    if None in seq:
        details = {
            'msg': 'function does not work with sequences containing None'
        }
        raise ValueError(details)
    hist = [None,] * len(seq)
    for i in range(len(seq)):
        if seq[i] in hist:
            return i
        else:
            hist[i] = seq[i]
    return d0

class RecOffsetIndex:
    """
    Keeps track of locations of record markers in a text database file.

    Works only with seekable streams or other addressable data types.

    """
    DEFAULT_REC_END = '\u23f9'
    DEFAULT_REC_START = '\u23fa'
    DEFAULT_REC_META_END = '\u23ed'
    REC_META_END_USED = 0x1
    REC_END_USED = 0x2

    # TODO: Method naming conventions are as follows:
    # * req -> request (non-blocking or threaded operation)
    # * get -> immediately return data
    # * set -> immediately change data

    def __init__(self, **kwargs):
        self.offs = None
        self.rec_end_chr = kwargs.get('rec_end_chr', self.DEFAULT_REC_END)
        self.rec_meta_end_chr = kwargs.get(
            'rec_meta_end_chr', self.DEFAULT_REC_META_END
        )
        self.rec_start_chr = kwargs.get('rec_start_chr', self.DEFAULT_REC_START)
        self._stream_changed = False

    def scan_offs(self, txt, pos=0, endpos=None, n=None, ihfmt=0x00):
        """
        Scan a string or text stream ``txt`` for record markers.

        * pos, endpos => set the start and end positions to scan

        * n => number of records to scan

        * ihfmt => InforHoardi format:

          * 0x0: REC_START only
          * 0x2: REC_START, REC_END only
          * 0x3: REC_START, REC_META_END, REC_END
          * Other formats are not supported (yet)

        """
        # TODO: implement file and record format validation
        # TODO: do this in a separate thread, preferably non-blocking,
        # because this method can be slow
        if isinstance(txt, io.IOBase):
            if not txt.seekable():
                details = {
                    'msg': 'only seekable streams are supported'
                }
                raise TypeError(msg)
        chrs = [self.rec_start_chr,]
        if ihfmt & self.REC_META_END_USED != 0:
            chrs.append(self.rec_meta_end_chr)
        if ihfmt & self.REC_END_USED != 0:
            chrs.append(self.rec_end_chr)
        self.offs = self._do_get_mark_offs(txt, chrs, pos, endpos, n)

    def rec_start(self, off):
        """
        Given offset ``off``, return up the int index of the offset of
        its nearest previous known record start mark in the database file.

        Example:
        Given the following offset dictionary

        {'starts':[1,10,20],'ends':[9,19,29], ...}

        rec_start(7) == 0
        rec_start(10) == 1
        rec_start(11) == 1

        """
        return self._do_index_rec_start(off, self._offs)

    def rec_offs_by_index(self, n):
        """
        Return the n'th known array of offsets found by scan_offs().
        By default, 3-tuples returned with the format:

        (REC_START, REC_META_END, REC_END)

        """
        if self.offs is None:
            details = {
                'msg': 'no offsets found, please get offsets first',
            }
            raise ValueError(details)
        out = []
        for k in self.offs.keys():
            off_list = self.offs[k]
            if n >= len(off_list):
                out.append(None)
            else:
                out.append(self.offs[k][n])
        return out

    def _do_index_rec_start(self, off, offs):
        """
        Find the index of ``off``, or the index of the next lowest
        value if ``off`` is not found, in offs[0]. Return index as int.

        Return None if ``off`` is lower than the lowest value in
        offs[0].

        This method does not work if values in offs[0] are not sorted
        smallest to largest.

        """
        ileft, iright = bfind(offs[self.rec_start_chr], off)
        if ileft >= 0:
            return ileft
        else:
            if iright == 0:
                return None
            else:
                # TODO: Be more in line with slices and bisect by
                # returning an index for use with insert()
                return iright-1

    def _do_get_mark_offs(self, s, chrs, pos=0, endpos=None, n=None):
        """
        Return a dict of offsets of specific characters in chrs in a
        string or text stream s.

        Please ensure that chrs do not contain any duplicates. Only
        seekable streams are supported.

        Result format: {"chr": [off1, ... offn], ...} for each chr in chrs

        Other Arguments
        ---------------
        * pos and endpos are the start and end indices in s to work on

        * n sets a limit of records to process

        Note
        ----
        ``chrs[0]`` is assumed to be the record start marker, and
        the number of instances of this character will be used to
        determine the expected length of the offset lists.

        A record begins with chrs[0], followed by only up to one
        instance of a character in chrs. Exact format specifications
        may vary, check scan_offs() for a list of supported formats.

        If a record is found not to conform to the format, the record
        is abandoned and the next record is skipped to.

        """
        offs = dict()
        if endpos is None:
            endpos = len(s)
        if n is None:
            n = len(s)
            # PROTIP: len(s) is always higher than the number of records,
            # so it can be used as a convenient value for 'no limit'
        for c in chrs:
            if not isinstance(c, str):
                details = {
                    "msg": "use only single chars as markers",
                    "item": c
                }
                raise TypeError(details)
            elif not len(c) == 1:
                details = {
                    "msg": "use only single chars as markers",
                    "item": c
                }
                raise ValueError(details)
            else:
                offs[c] = []

        first_key = tuple(offs.keys())[0]
        i = pos
        while i < endpos and s[i] != chrs[0]:
            # skip to first instance of chrs[0]
            i += 1
        i_ins = 0
        while i < endpos:
            si = s[i]
            if si in chrs:
                offs[si].append(i)
                i_ins = (i_ins + 1) % len(chrs)
            i += 1
            if i_ins == 0:
                if not self._do_ck_len_eq_offs(offs):
                    # malformed record suspected
                    self._do_level_offs(offs)
                    while i < endpos and s[i] != chrs[0]:
                        # skip to next instance of chrs[0]
                        i += 1
                if len(offs[first_key]) >= n:
                    break
        self._do_level_offs(offs)
        return offs

    def _do_ck_len_eq_offs(self, offs):
        """
        Check if all lists in offset dict ``offs`` is the same as the
        length of the first list. Returns True if this is the case,
        False otherwise.

        If offs is empty, True will be returned.

        """
        keys = tuple(offs.keys())
        for k in keys:
            if len(offs[k]) != len(offs[keys[0]]):
                return False
        return True

    def _do_level_offs(self, offs):
        """
        Trim or pad lists in offset dictionary ``offs`` to the same
        length as offs[0] *in place*. Do not return any value.

        """
        keys = tuple(offs.keys())
        for L in keys:
            no = len(offs[keys[0]])
            nl = len(offs[L])
            if nl > no:
                for i in range(nl-no):
                    offs[L].pop()
            elif nl < no:
                offs[L].extend([None,] * (no-nl))

