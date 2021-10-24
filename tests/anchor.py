"""
Bakdoh TAGS Non-Database-Bound Anchor Object Tests

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

#
# Note
# ====
# Database-bound tests (which rely on a database repository)
# are found in the tests.db module, and are run with the repository
# tests in the tests.tests_tags* modules
#

from tags import Anchor
from unittest import TestCase

class AnchorTests(TestCase):

    def test_eq(self):
        a1 = Anchor('a', 0)
        a2 = Anchor('a', 0)
        self.assertEqual(a1, a2)
    
    def test_eq_different_values(self):
        a1 = Anchor('a', 0)
        a3 = Anchor('a', 100)   # different content
        a4 = Anchor('z', 0)     # different q
        a5 = Anchor('z', 255)   # different content and q
        self.assertNotEqual(a1, a3)
        self.assertNotEqual(a1, a4)
        self.assertNotEqual(a1, a5)

    def test_eq_different_types(self):
        a = Anchor('a', 0)
        self.assertNotEqual(a, 1)
        self.assertNotEqual(a, ('not', 'equal'))
        self.assertNotEqual(a, ['not', 'equal'])
        self.assertNotEqual(a, 'not equal')

