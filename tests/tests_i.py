"""
Bakdoh Infohoardi Test Modules: Index Classes and Helper Functions

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

from i import cfunc_default
from i import find_edge, RIGHT
from unittest import TestCase

class CfuncTests(TestCase):
    """Unit Tests for built-in compare functions"""
    def test_cfunc_default_eq(self):
        self.assertEqual(cfunc_default("A", "A"), 0)

    def test_cfunc_default_lt(self):
        self.assertEqual(cfunc_default("A", "B"), -1)

    def test_cfunc_default_gt(self):
        self.assertEqual(cfunc_default("B", "A"), 1)

    def test_cfunc_default_lt(self):
        self.assertEqual(cfunc_default("A", "B"), -1)

    def test_cfunc_default_prefix_lt(self):
        self.assertEqual(cfunc_default("A", "AB"), -1)

    def test_cfunc_default_gt(self):
        self.assertEqual(cfunc_default("B", "A"), 1)

    def test_cfunc_default_prefix_gt(self):
        self.assertEqual(cfunc_default("AB", "A"), 1)

class FindEdgeTests(TestCase):
    """Unit Tests for find_edge()"""

    def test_first_in_even_run_left(self):
        samp = "ABBBBC"
        r = find_edge(samp, 1)
        self.assertEqual(r, 1)

    def test_first_in_even_run_right(self):
        samp = "ABBBBC"
        r = find_edge(samp, 1, d=RIGHT)
        self.assertEqual(r, 5)

    def test_middle_in_even_run_left(self):
        samp = "ABBBBC"
        r = find_edge(samp, 2)
        self.assertEqual(r, 1)

    def test_middle_in_even_run_right(self):
        samp = "ABBBBC"
        r = find_edge(samp, 2, d=RIGHT)
        self.assertEqual(r, 5)

    def test_last_in_even_run_left(self):
        samp = "ABBBBC"
        r = find_edge(samp, 4)
        self.assertEqual(r, 1)

    def test_last_in_even_run_right(self):
        samp = "ABBBBC"
        r = find_edge(samp, 4, d=RIGHT)
        self.assertEqual(r, 5)

    def test_lone_left(self):
        samp = "N"
        r = find_edge(samp, 0)
        self.assertEqual(r, 0)

    def test_lone_right(self):
        samp = "N"
        r = find_edge(samp, 0, d=RIGHT)
        self.assertEqual(r, 1)

    def test_unique_first_right(self):
        samp = "ABBBBC"
        r = find_edge(samp, 0, d=RIGHT)
        self.assertEqual(r, 1)

    def test_unique_first_left(self):
        samp = "ABBBBC"
        r = find_edge(samp, 0)
        self.assertEqual(r, 0)

    def test_unique_first_right(self):
        samp = "ABBBBC"
        r = find_edge(samp, 0, d=RIGHT)
        self.assertEqual(r, 1)
        
    def test_unique_last_left(self):
        samp = "ABBBBC"
        r = find_edge(samp, 5)
        self.assertEqual(r, 5)

    def test_unique_last_right(self):
        samp = "ABBBBC"
        r = find_edge(samp, 5, d=RIGHT)
        self.assertEqual(r, 6)

    def test_unique_mid_left(self):
        samp = "ABBCDDE"
        r = find_edge(samp, 3)
        self.assertEqual(r, 3)
        
    def test_unique_mid_right(self):
        samp = "ABBCDDE"
        r = find_edge(samp, 3, d=RIGHT)
        self.assertEqual(r, 4)
        
    def test_misc(self):
        # TODO: Spin these cases of into separate test methods
        samp = "ABCDEFGGGGGHIJJJJJJK"
        rC_l = find_edge(samp, 2)
        self.assertEqual(rC_l, 2)
        rC_r = find_edge(samp, 2, d=RIGHT)
        self.assertEqual(rC_r, 3)
        rG_l = find_edge(samp, 9)
        self.assertEqual(rG_l, 6)
        rG_r = find_edge(samp, 9, d=RIGHT)
        self.assertEqual(rG_r, 11)
        rI_r = find_edge(samp, 12, d=RIGHT)
        self.assertEqual(rI_r, 13)
        rJ_l = find_edge(samp, 14)
        self.assertEqual(rJ_l, 13)
        rJ_r = find_edge(samp, 14, d=RIGHT)
        self.assertEqual(rJ_r, 19)

