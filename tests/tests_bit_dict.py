"""
Bakdoh Index Helpers Test Modules: BitDict

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
from i import BitDict

class BDDoSetFromInitTests(TestCase):

    # NOTE: _do_set_from_init() is called from __init__()
    def test_init_from_int(self):
        fields = {'R': 5, 'G': 5, 'B': 5}
        bd = BitDict(fields, init=0b110001101101010)
        self.assertEqual(bd._fields['R'][1], 0b11000)
        self.assertEqual(bd._fields['G'][1], 0b11011)
        self.assertEqual(bd._fields['B'][1], 0b01010)

    def test_init_from_int_multi_lengths(self):
        fields = {'weight': 4, 'score': 7, 'verified': 1}
        bd = BitDict(fields, init=0b100110000011)
        self.assertEqual(bd._fields['weight'][1], 0b1001)
        self.assertEqual(bd._fields['score'][1], 0b01000001)
        self.assertEqual(bd._fields['verified'][1], 0b1)

    def test_init_from_zero_length(self):
        fields = {'G1':1, 'G2':0, 'G3':5}
        with self.assertRaises(ValueError):
            bd = BitDict(fields, init=0b1011111)

    def test_zero(self):
        fields = {'R': 5, 'G': 5, 'B': 5}
        bd = BitDict(fields, init=0b110001101101010)
        bd._do_set_from_int(0)
        self.assertEqual(bd._fields['R'][1], 0)
        self.assertEqual(bd._fields['G'][1], 0)
        self.assertEqual(bd._fields['B'][1], 0)

class BDGetValueTests(TestCase):

    def test_get_value(self):
        fields = {'R': 5, 'G': 5, 'B': 5}
        bd = BitDict(fields, init=0b100011000110001)
        self.assertEqual(bd.get_value('R'), 0b10001)
        self.assertEqual(bd.get_value('G'), 0b10001)
        self.assertEqual(bd.get_value('B'), 0b10001)

    def test_get_value_multi_lengths(self):
        fields = {'weight': 4, 'score': 7, 'verified': 1}
        bd = BitDict(fields, init=0b100110000011)
        self.assertEqual(bd.get_value('weight'), 0b1001)
        self.assertEqual(bd.get_value('score'), 0b1000001)
        self.assertEqual(bd.get_value('verified'), 0b1)

class BDSetValueTests(TestCase):

    # TODO: Tests with multiple field lengths
    def setUp(self):
        self.fields_a = {'R': 5, 'G': 5, 'B': 5}

    def test_set_value_left(self):
        bd = BitDict(self.fields_a, init=0b100011000110001)
        bd.set_value('R', 0b11111)
        self.assertEqual(bd.get_value('R'), 0b11111)
        self.assertEqual(bd.get_value('G'), 0b10001)
        self.assertEqual(bd.get_value('B'), 0b10001)

    def test_set_value_mid(self):
        bd = BitDict(self.fields_a, init=0b100011000110001)
        bd.set_value('G', 0b00000)
        self.assertEqual(bd.get_value('R'), 0b10001)
        self.assertEqual(bd.get_value('G'), 0b00000)
        self.assertEqual(bd.get_value('B'), 0b10001)

    def test_set_value_mid_change(self):
        bd = BitDict(self.fields_a, init=0b100011000110001)
        bd.set_value('G', 0b00000)
        bd.set_value('G', 0b11111)
        self.assertEqual(bd.get_value('G'), 0b11111)

    def test_set_value_right(self):
        bd = BitDict(self.fields_a, init=0b100011000110001)
        bd.set_value('B', 0b10101)
        self.assertEqual(bd.get_value('R'), 0b10001)
        self.assertEqual(bd.get_value('G'), 0b10001)
        self.assertEqual(bd.get_value('B'), 0b10101)

    def test_set_value_too_high(self):
        bd = BitDict(self.fields_a, init=0b100011000110001)
        with self.assertRaises(ValueError):
            bd.set_value('G', 0b100000)
        # existing bitfields must not change
        self.assertEqual(bd.get_value('R'), 0b10001)
        self.assertEqual(bd.get_value('G'), 0b10001)
        self.assertEqual(bd.get_value('B'), 0b10001)

    def test_set_value_negative(self):
        bd = BitDict(self.fields_a, init=0b100011000110001)
        with self.assertRaises(ValueError):
            bd.set_value('G', -1)
        self.assertEqual(bd.get_value('R'), 0b10001)
        self.assertEqual(bd.get_value('G'), 0b10001)
        self.assertEqual(bd.get_value('B'), 0b10001)

class BDSetBit(TestCase):

    def test_set_bit(self):
        fields = {'R':5, 'G':5, 'B':5}
        bd = BitDict(fields)
        bd.set_bit('G', 2, 1)
        self.assertEqual(int(bd), 0b10000000)

    def test_set_bit_invalid_b(self):
        fields = {'R':5, 'G':5, 'B':5}
        a = 0b100011000110001
        bd = BitDict(fields, init=a)
        with self.assertRaises(ValueError):
            bd.set_bit('G', 3, 2)
        self.assertEqual(int(bd), a)
        with self.assertRaises(ValueError):
            bd.set_bit('G', 3, -1)
        self.assertEqual(int(bd), a)

    def test_set_bit_invalid_pos(self):
        fields = {'R':5, 'G':5, 'B':5}
        a = 0b100011000110001
        bd = BitDict(fields, init=a)
        with self.assertRaises(IndexError):
            bd.set_bit('G', 6, 1)
        self.assertEqual(int(bd), a)
        with self.assertRaises(IndexError):
            bd.set_bit('G', -1, 1)
        self.assertEqual(int(bd), a)

class BDIntTests(TestCase):

    def test_int(self):
        fields = {'R': 5, 'G': 5, 'B': 5}
        bd = BitDict(fields)
        bd.set_value('B', 0b10001)
        self.assertEqual(int(bd), 0b10001)
        bd.set_value('R', 0b10001)
        self.assertEqual(int(bd), 0b100010000010001)

    def test_len_multi_lengths(self):
        fields = {'weight': 4, 'score': 7, 'verified': 1}
        bd = BitDict(fields)
        bd.set_value('verified', 0b1)
        self.assertEqual(int(bd), 0b1)
        bd.set_value('score', 0b100001)
        self.assertEqual(int(bd), 0b1000011)

class BDLenTests(TestCase):

    def test_len(self):
        fields = {'R': 5, 'G': 5, 'B': 5}
        bd = BitDict(fields)
        self.assertEqual(len(bd), 15)

    def test_len_multi_lengths(self):
        fields = {'weight': 4, 'score': 7, 'verified': 1}
        bd = BitDict(fields)
        self.assertEqual(len(bd), 12)

