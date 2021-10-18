"""
Bakdoh TAGS Database Tests

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
# How to Use
# ==========
# The tests in this module are designed to test multiple repository
# classes with a single test suite.
#
# Run them by subclassing these tests in a separate module. Each
# series of tests is set up as a class. Create a subclass with
# the RepoClass class attribute assigned to the repository class
# to be tested.
#
# Each series of tests require additional setup before use, please
# see the documentation for each class for usage.
#

from unittest import TestCase, TestSuite
from tags import DB, Anchor

class DBGetTests(TestCase):
    """Database tests for delete, put and set q-value

    To test a repository class, please subclass this and set
    the following:

    1. RepoClass : set to the repository class being tested

    2. Stub methods : override with a working implementation

    """
    RepoClass = None

    def direct_insert(self, repo, x):
        """Inserts anchors and relations directly via the
        repository, preferably using the lowest-level method
        feasible.

        This is a stub method, please override with a working
        implementation.

        Argument Format
        ===============
        * repo : repository object

        * x : an iterable of anchors and/or relations

        """
        raise NotImplementedError("please see DbGetTests for usage")

    def dump(self, repo, term=None):
        """Load anchors directly from the repository, preferably
        using the lowest-level method feasible.

        This is a stub method, please override with a working
        implementation.

        Argument Format
        ===============
        * repo : repository object

        * term : Unix glob-like search term to select the anchors
          and relations to be loaded. If None, return all anchors
          and relations.

        """
        raise NotImplementedError("please see DbWriteTests for usage")

    def setUp(self):
        if not self.RepoClass: self.skipTest('No RepoClass set')
        # TODO: find a way of skipping tests when no RepoClass
        # is set, that doesn't bomb reports with 'skipped test'
        # results

    def test_db_get_a(self):
        testdb = DB(self.RepoClass())
        data = (('a', None), ('z', 99))
        self.direct_insert(testdb, data)
        sample = testdb.get_a('*')
        self.assertEqual(
            list(sample), [Anchor(c, q, db=testdb) for c, q in data]
        )

class DBWriteTests(TestCase):
    """Database tests for delete, put and set q-value

    To test a repository class, please subclass this and set
    the following:

    1. RepoClass : set to the repository class being tested

    2. Stub methods : override with a working implementation

    """
    RepoClass = None

    def dump(self, repo, term=None):
        """Load anchors directly from the repository, preferably
        using the lowest-level method feasible.

        THIS IS A STUB METHOD, PLEASE OVERRIDE WITH A WORKING
        implementation.

        Argument Format
        ===============
        * repo : repository object

        * term : Unix glob-like search term to select the anchors
          and relations to be loaded. If None, return all anchors
          and relations.

        """
        raise NotImplementedError("please see DbWriteTests for usage")

    def setUp(self):
        # TODO: find a way of skipping tests when no RepoClass
        # is set, that doesn't bomb reports with 'skipped test'
        # results
        if not self.RepoClass: self.skipTest('No RepoClass set')

    def test_db_put_a(self):
        """
        Insert ordinary anchors

        """
        testdb = DB(self.RepoClass())
        data = [('a', 1), ('z', 99)]
        for d in data:
            testdb.put_a(*d)
        sample = self.dump(testdb)
        self.assertEqual(sample, data)

    def test_db_put_a_nothing(self):
        """
        Handle attempts to insert empty string anchors

        """
        testdb = DB(self.RepoClass())
        with self.assertRaises(ValueError):
            testdb.put_a('', None)
        ##
        sample = self.dump(testdb)
        self.assertEqual(sample, [])

