"""
Bakdoh Infohoardi Test Modules: RecIndexOffset

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

from unittest import TestCase
from infohoardi import index_dup, RecOffsetIndex

class IndexDupTests(TestCase):
    def test_no_dup(self):
        samp = ['a', 'b', 'c', 'd']
        self.assertEqual(index_dup(samp), None)

    def test_one_dup(self):
        samp = ['a', 'b', 'c', 'c', 'd']
        self.assertEqual(index_dup(samp), 3)

    def test_one_dup_noncont(self):
        samp = ['a', 'b', 'c', 'd', 'c']
        self.assertEqual(index_dup(samp), 4)

class ROIDoGetMarkOffsetsTests(TestCase):
    def setUp(self):
        self.roi = RecOffsetIndex()

    def test_empty(self):
        samp = ''
        chrs = ['S','M','E']
        offs = self.roi._do_get_mark_offs(samp, chrs)
        expected = {'S': [], 'M': [], 'E': []}
        self.assertEqual(offs, expected)
        # TODO: decided that empty lists are the best approach
        # for now, as they minimise interference with iterator-based
        # routines.

    def test_3char_back2back(self):
        samp = 'S..M__ES..M__ES..M__E'
        chrs = ['S','M','E']
        expected = {
            'S': [0,7,14],
            'M': [3,10,17],
            'E': [6,13,20]
        }
        offs = self.roi._do_get_mark_offs(samp, chrs)
        self.assertEqual(offs, expected)

    def test_3char_spaced(self):
        samp = 'S..M__E====S..M__E++++++S..M__E'
        chrs = ['S','M','E']
        expected = {
            'S': [0,11,24],
            'M': [3,14,27],
            'E': [6,17,30]
        }
        offs = self.roi._do_get_mark_offs(samp, chrs)
        self.assertEqual(offs, expected)

    def test_1char_back2back(self):
        samp = 'S____S_S_S_'
        chrs = ['S',]
        expected = {'S':[0,5,7,9]}
        offs = self.roi._do_get_mark_offs(samp, chrs)
        self.assertEqual(offs, expected)

    def test_3char_dblmarked_rec_first(self):
        samp = 'S..M..M__ES..M__ES..M__E'
        chrs = ['S','M','E']
        expected = {
            'S': [0,10,17],
            'M': [3,13,20],
            'E': [None,16,23],
        }
        offs = self.roi._do_get_mark_offs(samp, chrs)
        self.assertEqual(offs, expected)

    def test_3char_dblmarked_rec_last(self):
        samp = 'S..M__ES..M__ES..M__'
        chrs = ['S','M','E']
        expected = {
            'S': [0,7,14],
            'M': [3,10,17],
            'E': [6,13,None],
        }
        offs = self.roi._do_get_mark_offs(samp, chrs)
        self.assertEqual(offs, expected)

class ROIDoCkLenEqOffsTests(TestCase):
    def setUp(self):
        self.roi = RecOffsetIndex()

    def test_same_len(self):
        samp = {
            'S': [0,10,20],
            'M': [4,14,24],
            'E': [9,19,29],
        }
        self.assertEqual(self.roi._do_ck_len_eq_offs(samp), True)

    def test_diff_len(self):
        samp = {
            'S': [0,10,20],
            'M': [4,14],
            'E': [9,19,29],
        }
        self.assertEqual(self.roi._do_ck_len_eq_offs(samp), False)

    def test_empty(self):
        samp = dict()
        self.assertEqual(self.roi._do_ck_len_eq_offs(samp), True)

class ROIDoIndexRecStartTests(TestCase):
    def setUp(self):
        self.roi = RecOffsetIndex(rec_start_chr='S')
        self.samp = {'S': [2,4,6]}
        self.samp_even = {'S': [2,4,6,8]}

    def test_found_end(self):
        i = self.roi._do_index_rec_start(6, self.samp)
        self.assertEqual(i, 2)

    def test_found_mid(self):
        i = self.roi._do_index_rec_start(4, self.samp)
        self.assertEqual(i, 1)

    def test_found_start(self):
        i = self.roi._do_index_rec_start(2, self.samp)
        self.assertEqual(i, 0)

    def test_not_found_mid(self):
        i = self.roi._do_index_rec_start(5, self.samp)
        self.assertEqual(i, 1)

    def test_not_found_start(self):
        i = self.roi._do_index_rec_start(3, self.samp)
        self.assertEqual(i, 0)

    def test_past_start(self):
        i = self.roi._do_index_rec_start(1, self.samp)
        self.assertEqual(i, None)

    def test_past_end(self):
        i = self.roi._do_index_rec_start(6, self.samp)
        self.assertEqual(i, 2)

    def test_not_found_mid_even(self):
        i = self.roi._do_index_rec_start(5, self.samp_even)
        self.assertEqual(i, 1)

    def test_not_found_mid_start(self):
        i = self.roi._do_index_rec_start(3, self.samp_even)
        self.assertEqual(i, 0)


class ROIDoLevelOffsTests(TestCase):
    def setUp(self):
        self.roi = RecOffsetIndex()

    def test_same_len(self):
        samp = {
            'S': [0,10,20],
            'M': [4,14,24],
            'E': [9,19,29],
        }
        levelled = samp.copy()
        self.roi._do_level_offs(levelled)
        self.assertEqual(levelled, samp)

    def test_longer_than_d0(self):
        samp = {
            'S': [0,10,20],
            'M': [4,14,24,34,44],
            'E': [9,19,29],
        }
        expected = {
            'S': [0,10,20],
            'M': [4,14,24],
            'E': [9,19,29],
        }
        levelled = samp.copy()
        self.roi._do_level_offs(levelled)
        self.assertEqual(levelled, expected)

    def test_shorter_than_d0(self):
        samp = {
            'S': [0,10,20,30,40],
            'M': [4,14,24],
            'E': [9,19,29],
        }
        expected = {
            'S': [0,10,20,30,40],
            'M': [4,14,24,None,None],
            'E': [9,19,29,None,None],
        }
        levelled = samp.copy()
        self.roi._do_level_offs(levelled)
        self.assertEqual(levelled, expected)

    def test_shorter_longer_than_d0(self):
        samp = {
            'S': [0,10,20],
            'M': [4,14,24,34,44],
            'E': [9,],
        }
        expected = {
            'S': [0,10,20],
            'M': [4,14,24],
            'E': [9,None,None],
        }
        levelled = samp.copy()
        self.roi._do_level_offs(levelled)
        self.assertEqual(levelled, expected)

