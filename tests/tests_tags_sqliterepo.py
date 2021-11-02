"""
Bakdoh TAGS Test Modules: SQLiteRepository

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

from tags import CHAR_WC_1C, CHAR_WC_ZP, SQLiteRepo
from tests.db import DB, DBGetTests, DBWriteTests
from unittest import TestCase

valid_types_a = ([str, int], [str, float], [str, type(None)])
valid_types_rel = (
    [str, str, str, int],
    [str, str, str, float],
    [str, str, str, type(None)],
)

class SlrDbExportTests(TestCase):
    """
    Verify the operation of SQLiteRepo's export function.

    This test relies on the correctness of SQLiteRepo.reltxt().
    Tests for reltxt() must pass for these tests to be valid.

    Unfortunately, tests for export() cannot be run under the
    generic DB class test suite as implementations for export()
    are repository class-specific.

    """
    def setUp(self):
        # Most direct SQLiteRepo insert humanly possible
        self.testdb = DB(SQLiteRepo())
        sc_insert = "INSERT INTO {} VALUES(?,?)".format(SQLiteRepo.table_a)
        reltxt = self.testdb.repo.reltxt
        cs = self.testdb.repo._db_conn.cursor()
        inp = (
            ('a', None),
            ('j', None),
            ('t', 0.0001),
            ('z', -274),
            (reltxt('j', 'a', 'j'), None),
            (reltxt('t', 'a', 't'), -274),
            (reltxt('a', 'z', 'a'), 37)
        )
        cs.executemany(sc_insert, inp)

    def test_export_all_interchange(self):
        out = list(self.testdb.export())
        expected = [
            ('a', None),
            ('j', None),
            ('t', 0.0001),
            ('z', -274),
            ('j', 'a', 'j', None),
            ('t', 'a', 't', -274),
            ('a', 'z', 'a', 37)
        ]
        self.assertEqual(out, expected)

    def test_export_anchor_interchange(self):
        """Export just one anchor and its relations to the
        interchange format

        """
        out = list(self.testdb.export(a='a'))
        expected = [
            ('a', None),
            ('j', 'a', 'j', None),
            ('t', 'a', 't', -274),
            ('a', 'z', 'a', 37)
        ]
        self.assertEqual(out, expected)

class SlrDbImportTests(TestCase):
    """
    Verify the operation of SQLiteRepo's import function.

    This test relies on the correctness of SQLiteRepo.reltxt().
    Tests for reltxt() must pass for these tests to be valid.

    Unfortunately, tests for import_data() cannot be run under the
    generic DB class test suite as import_data() implementations
    are repository class-specific.

    """
    def test_import(self):
        testdb = DB(SQLiteRepo())
        sc_dump = "SELECT * FROM {}".format(SQLiteRepo.table_a)
        cs = testdb.repo._db_conn.cursor()
        inp = (
            ('a',),
            ('j', None),
            ('t', 0.0001),
            ('z', -274),
            ('j', 'a', 'j', None),
            ('t', 'a', 't', -274)
        )
        reltxt = testdb.repo.reltxt
        expected = (
            ('a', None),
            ('j', None),
            ('t', 0.0001),
            ('z', -274),
            (reltxt('j', 'a', 'j'), None),
            (reltxt('t', 'a', 't'), -274)
        )
        testdb.import_data(inp)
        sample = tuple(cs.execute(sc_dump))
        self.assertEqual(sample, expected)

    def test_import_unsupported_format(self):
        """import_data(): reject unsupported formats"""

        testdb = DB(SQLiteRepo())
        sc_dump = "SELECT * FROM {}".format(SQLiteRepo.table_a)
        cs = testdb.repo._db_conn.cursor()
        inp = (
            ('a', 0.1, 0.5, 0.75, 1.1),
            {},
        )
        out = testdb.import_data(inp)
        final = tuple(cs.execute(sc_dump))
        self.assertEqual(final, ())

class SlrDbGetTests(DBGetTests):
    """
    Run the Database Get Tests with a SQLiteRepository.
    Please see DBGetTests in the tests.db module for details

    """
    RepoClass = SQLiteRepo

class SlrDbWriteTests(DBWriteTests):
    """
    Run Database Delete, Put and Set q-value Tests with a
    SQLiteRepository.

    Please see DBWriteTests in the tests.db module for details

    """
    RepoClass = SQLiteRepo

class SLR_ReltextTests(TestCase):
    """Tests for reltext()"""

    def test_reltext_not_exist(self):
        testrep = SQLiteRepo()
        testdb = DB(testrep)
        data = [('a', None), ('z', None)]
        testdb.import_data(data)
        with self.assertRaises(ValueError):
            testdb.repo.reltxt(namee='Rnull', a1e='x', a2e='y', alias='local')

    def test_reltext(self):
        testrep = SQLiteRepo()
        testdb = DB(testrep)
        data = [('a', None), ('z', None)]
        testdb.import_data(data)
        char_rel = testrep._char_rel
        char_alias = testrep._char_alias
        argtests = (
            (
                {
                    'namee': 'Raz',
                    'a1e': 'a',
                    'a2e': 'z',
                    'alias': 'local',
                    'alias_fmt': 0
                },
                'Raz{0}a{0}z'.format(char_rel)
            ),
            (
                {
                    'namee': 'Raz',
                    'a1e': 'a',
                    'a2e': 'z',
                    'alias': 'local',
                    'alias_fmt': 1
                },
                'Raz{0}a{0}{1}2'.format(char_rel, char_alias)
            ),
            (
                {
                    'namee': 'Raz',
                    'a1e': 'a',
                    'a2e': 'z',
                    'alias': 'local',
                    'alias_fmt': 2
                },
                'Raz{0}{1}1{0}z'.format(char_rel, char_alias)
            ),
            (
                {
                    'namee': 'Raz',
                    'a1e': 'a',
                    'a2e': 'z',
                    'alias': 'local',
                    'alias_fmt': 3
                },
                'Raz{0}{1}1{0}{1}2'.format(char_rel, char_alias)
            ),

        ) # format: (kwargs, expected_output)
        # NOTE: this test makes an assumption that ROWIDs are always
        # and strictly in order of insertion of the anchors
        #
        for a in argtests:
            with self.subTest(a=a):
                self.assertEqual(testrep.reltxt(**a[0]), a[1])

class SLR_QClauseTests(TestCase):
    """Tests for _slr_q_clause()"""

    def test(self):
        """Generate SQL expression for q range in WHERE clause"""
        testrep = SQLiteRepo()
        argtests = (
            ({}, ('',[])),
            ({'q_eq':5}, (' AND {} = ?', [5,])),
            ({'q_eq':0}, (' AND {} = ?', [0,])),
            ({'q_gt':1}, (' AND {} > ?', [1,])),
            ({'q_gt':0}, (' AND {} > ?', [0,])),
            ({'q_lt':9}, (' AND {} < ?', [9,])),
            ({'q_lt':0}, (' AND {} < ?', [0,])),
            ({'q_gt':1, 'q_lt':9}, (' AND {0} > ? AND {0} < ?', [1, 9])),
            ({'q_gt':0, 'q_lt':0}, (' AND {0} > ? OR {0} < ?', [0, 0])),
            (
                {'q_gt':1, 'q_lt':9, 'q_lte': 9},
                (' AND {0} > ? AND {0} < ?', [1, 9])
            ),
            ({'q_gte':1, 'q_lte':9}, (' AND {0} >= ? AND {0} <= ?', [1, 9])),
            ({'q_gt':9, 'q_lt':1}, (' AND {0} > ? OR {0} < ?', [9, 1])),
            ({'q_gt':9, 'q_gte':7, 'q_lt':1}, (' AND {0} > ? OR {0} < ?',[9,1])),
            ({'q_gte':9, 'q_lte':1}, (' AND {0} >= ? OR {0} <= ?', [9, 1])),
            (
                {'q_gt':9, 'q_gte':7, 'q_lt':3, 'q_lte':1},
                (' AND {0} > ? OR {0} < ?', [9, 3])
            ),
            (
                {'q_gt':1, 'q_gte':3, 'q_lt':7, 'q_lte':9},
                (' AND {0} > ? AND {0} < ?', [1, 7])
            ),
           (
                {'q_eq':5, 'q_gt':9, 'q_gte':9, 'q_lt':1, 'q_lte':9},
                (' AND {} = ?', [5,])
            ),
        ) # format: (kwargs, expected_output)
          # PROTIP: {0} will be replaced with q-value column name
        for x, y in argtests:
            with self.subTest(args=x):
                expected = (y[0].format(testrep.col_q), y[1])
                self.assertEqual(testrep._slr_q_clause(**x), expected)

    def test_not(self):
        """Generate SQL expression for q range in WHERE clause (q_not)"""
        testrep = SQLiteRepo()
        argtests = (
            ({'q_eq':5, 'q_not': True}, (' AND NOT ({} = ?)', [5,])),
            ({'q_eq':0, 'q_not': True}, (' AND NOT ({} = ?)', [0,])),
        )
        for x, y in argtests:
            with self.subTest(args=x):
                expected = (y[0].format(testrep.col_q), y[1])
                self.assertEqual(testrep._slr_q_clause(**x), expected)

class SLRGetATests(TestCase):
    """Tests for get_a()"""

    def test_get_a_exact_sql_wildcard_escape(self):
        """Get single anchor containing SQL wildcard characters"""
        testdb = DB(SQLiteRepo())
        chars = (SQLiteRepo.CHAR_WC_ZP_SQL, SQLiteRepo.CHAR_WC_1C_SQL)
        data = [(x, 100) for x in chars]
        testdb.import_data(data)
        for c in chars:
            with self.subTest(char=c):
                sample = list(
                    testdb.get_a("{}*".format(c), out_format='interchange')
                )
                self.assertEqual(sample, [(c, 100)])

    def test_get_a_sql_wildcard_escape(self):
        """Get anchors containing SQL wildcard characters using wildcards"""
        testdb = DB(SQLiteRepo())
        t = lambda x: "{0}E{0}".format(x)
        chars = (SQLiteRepo.CHAR_WC_ZP_SQL, SQLiteRepo.CHAR_WC_1C_SQL)
        data = [(t(x), 100) for x in chars]
        testdb.import_data(data)
        for c in chars:
            with self.subTest(char=c):
                sample = list(
                    testdb.get_a("{}*".format(c), out_format='interchange')
                )
                self.assertEqual(sample, [(t(c), 100)])

    def test_get_a_wildcard_escape(self):
        """Get multiple anchors containing TAGS wildcard characters
        using wildcards

        TAGS wildcards are allowed to be stored in DB

        """
        testdb = DB(SQLiteRepo())
        chars = (CHAR_WC_ZP, CHAR_WC_1C)
        fi = lambda x: "&#{};".format(ord(x))
        for c in chars:
            data = [("{}{}".format(c, n), None) for n in range(3)]
            testdb.import_data(data)
            with self.subTest(char=c):
                term = "{}*".format(fi(c))
                samp = list(testdb.get_a(term, out_format='interchange'))
                self.assertEqual(samp, data)

    def test_get_a_special_chars_prefix(self):
        """Get anchors containing escaped prefix special chars

        Only the first char needs to be escaped

        """
        testdb = DB(SQLiteRepo())
        px_chars = testdb.get_special_chars()["PX"]
        fi = lambda x: "&#{};uuu{}u".format(ord(x), x) # format input
        fo = lambda x: "{0}uuu{0}u".format(x)          # format output
        data = [(fi(x), None) for x in px_chars]
        testdb.import_data(data)
        for c in px_chars:
            samp = next(testdb.get_a(fi(c), out_format='interchange'))
            expected = (fo(c), None)
            self.assertEqual(samp, expected)

    def test_get_a_special_chars_mixed(self):
        """Put anchor containing all special and wildcard characters"""
        testdb = DB(SQLiteRepo())
        chardict = testdb.get_special_chars()
        fi = lambda x, y: "".join(("#&{};".format(x), y))
        suffix = "".join((chardict["E"], chardict["F"], chardict["WC"]))
        data = [(fi(x, suffix), -10) for x in chardict["PX"]]
        testdb.import_data(data)
        for d in data:
            samp = list(testdb.export())
            self.assertEqual(samp, data)

class SLRPutATests(TestCase):
    """Tests for put_a()"""

    def test_put_a_special_chars(self):
        """Put anchor containing special reserved characters"""
        testdb = DB(SQLiteRepo())
        ei = lambda x: "&#{};".format(ord(x))
        data = [(ei(x), None) for x in testdb.repo.special_chars["F"]]
        expected = [(x, None) for x in testdb.repo.special_chars["F"]]
        for d in data:
            testdb.put_a(*d)
        samp = list(testdb.export())
        self.assertEqual(samp, expected)

    def test_put_a_special_chars_prefix(self):
        """
        Put anchor containing special prefix characters

        Special prefix characters are allowed to be used as-is in
        anchors, except for the first character.

        """
        testdb = DB(SQLiteRepo())
        fi = lambda x: "&#{0};uuu&#{0};u".format(ord(x)) # format input
        fo = lambda x: "{0}uuu{0}u".format(x)         # format output
        data = [(fi(x), None) for x in testdb.repo.special_chars["PX"]]
        expected = [(fo(x), None) for x in testdb.repo.special_chars["PX"]]
        for d in data:
            testdb.put_a(*d)
        samp = list(testdb.export())
        self.assertEqual(samp, expected)

    def test_put_a_special_chars_wildcards(self):
        """Put anchor containing wildcard characters"""
        testdb = DB(SQLiteRepo())
        chars = (
            CHAR_WC_ZP,
            CHAR_WC_1C,
            SQLiteRepo.CHAR_WC_ZP_SQL,
            SQLiteRepo.CHAR_WC_1C_SQL
        )
        data = [(x, -10) for x in chars]
        for d in data:
            testdb.put_a(*d)
        samp = list(testdb.export())
        self.assertEqual(samp, data)

    def test_put_a_special_chars_mixed(self):
        """Put anchor containing all special and wildcard characters"""
        testdb = DB(SQLiteRepo())
        chardict = testdb.get_special_chars()
        suffix = "".join((chardict["E"], chardict["F"], chardict["WC"]))
        data = [("".join((x, suffix)), -10) for x in chardict["PX"]]
        for d in data:
            testdb.put_a(*d)
        samp = list(testdb.export())
        self.assertEqual(samp, data)

