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

from tags import Anchor, CHAR_REL, CHARS_R_PX, SQLiteRepo
from tests.db import DB, DBGetTests, DBWriteTests
from unittest import TestCase

valid_types_a = ([str, int], [str, float], [str, type(None)])
valid_types_rel = ( 
    [str, str, str, int],
    [str, str, str, float],
    [str, str, str, type(None)],
)

def anchor_from_row(repo, row):
    """
    Return a mock Anchor object or relation containing Anchor
    objects from a SQLiteRepo row.

    NOTE: Aliased relations do not yet work with this function

    """
    # Relations are not validated; non-existent relations can
    # be returned by this function
    ts = list(map(lambda x:type(x), row))
    if len(row) == 2:
        # row format: (anchor_content, anchor_q_val)
        if ts in valid_types_a:
            return (row[0], row[1])
    if len(row) == 4:
        # row format: (rel_name, a_from_content, a_to_content, q)
        if ts in valid_types_rel:
            return (
                row[0],
                direct_select_all(repo, term=row[1])[0][0],
                direct_select_all(repo, term=row[2])[0][0],
                row[3]
            ) # PROTIP: direct_select_all returns only lists so
              # the [0] is needed

def direct_insert(repo, anchors):
    """
    Insert anchors directly into an SQLiteRepo. Uses the same
    arguments as direct_insert(), see tests.db for details.

    Examples:
    Insert anchors: direct_insert(repo, (('a1', 0), ('z1', 1)))
    Insert relation: direct_insert(repo, (('r', 'a1', 'z1', 99),))

    """
    sc = "INSERT INTO {} VALUES (?,?)".format(SQLiteRepo.table_a)
    cus = repo._slr_get_shared_cursor()
    for a in anchors:
        ts = list(map(lambda x:type(x), a))
        if len(a) == 4:
            # relation
            if ts in valid_types_rel:
                cus.execute(sc, (repo.reltxt(a[0], a[1], a[2]), a[3]))
        elif len(a) == 2:
            # anchor
            if ts in valid_types_a:
                cus.execute(sc, (a[0], a[1],))

def direct_select_all(repo, term='*'):
    """
    Return a list of anchors and corresponding q values directly
    from an SQLiteRepo matching an SQL search term.

    If term is not specified, all anchors will be returned.

    NOTE: Aliased relations do not yet work with this function

    """
    # TODO: direct_select_all should no longer take terms and
    # just dump the whole thing... when test dbs are small, we
    # can get away with this. This is to make dump() more repo-
    # agnostic
    sc_ck = "SELECT * FROM {} WHERE {} LIKE ? ESCAPE '{}'".format(
        SQLiteRepo.table_a, SQLiteRepo.col, SQLiteRepo.escape
    )
    
    cus = repo._db_conn.cursor()
    rows = cus.execute(sc_ck, (repo._prep_term(term),))
    out = []
    for r in rows:
        if CHAR_REL in r[0]:
            relrow = r[0].split(CHAR_REL) + [r[1],]
            out.append(anchor_from_row(repo, relrow))
        else:
            out.append(anchor_from_row(repo, r))
    return out

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
        data = [('a', None), ('z', None)]
        direct_insert(testrep, data)
        with self.assertRaises(ValueError):
            testrep.reltxt(namee='Rnull', a1e='x', a2e='y', alias='local')

    def test_reltext(self):
        testrep = SQLiteRepo()
        data = [('a', None), ('z', None)]
        direct_insert(testrep, data)
        argtests = (
            (
                {
                    'namee': 'Raz',
                    'a1e': 'a',
                    'a2e': 'z',
                    'alias': 'local',
                    'alias_fmt': 0
                },
                'Raz{0}a{0}z'.format(CHAR_REL, CHARS_R_PX['A_ID'])
            ),
            (
                {
                    'namee': 'Raz',
                    'a1e': 'a',
                    'a2e': 'z',
                    'alias': 'local',
                    'alias_fmt': 1
                },
                'Raz{0}a{0}{1}2'.format(CHAR_REL, CHARS_R_PX['A_ID'])
            ),
            (
                {
                    'namee': 'Raz',
                    'a1e': 'a',
                    'a2e': 'z',
                    'alias': 'local',
                    'alias_fmt': 2
                },
                'Raz{0}{1}1{0}z'.format(CHAR_REL, CHARS_R_PX['A_ID'])
            ),
            (
                {
                    'namee': 'Raz',
                    'a1e': 'a',
                    'a2e': 'z',
                    'alias': 'local',
                    'alias_fmt': 3
                },
                'Raz{0}{1}1{0}{1}2'.format(CHAR_REL, CHARS_R_PX['A_ID'])
            ),

        ) # format: (kwargs, expected_output)
        # NOTE: this test makes an assumption that ROWIDs are always
        # related to the order which an anchor was inserted into the
        # database.
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
        """Get anchor containing SQL wildcard characters"""
        testrep = SQLiteRepo()
        inputs = testrep._test_sample['WC_SLR']
        for i in inputs:
            direct_insert(testrep, ((i, None),))
            with self.subTest(a=i):
                samp = [x for x in testrep.get_a(i)]
                self.assertEqual(samp, [(i, None),])

    def test_get_a_sql_wildcard_escape(self):
        """Get anchors containing SQL wildcard characters using wildcards"""
        testrep = SQLiteRepo()
        inputs = testrep._test_sample['WC_SLR']
        for i in inputs:
            k = i[0]
            data = [("{}{}".format(i, n), None) for n in range(3)]
            direct_insert(testrep, data)
            with self.subTest(char=k):
                samp = [x for x in testrep.get_a("{}*".format(k))]
                self.assertEqual(samp, data)

    def test_get_a_wildcard_escape(self):
        """Get anchors containing wildcard characters using wildcards"""
        testrep = SQLiteRepo()
        inputs = testrep._test_sample['WC']
        for i in inputs:
            k = i[0]
            data = [("{}{}".format(i, n), None) for n in range(3)]
            direct_insert(testrep, data)
            with self.subTest(char=k):
                samp = [x for x in testrep.get_a("&#{};*".format(ord(k)))]
                self.assertEqual(samp, data)

class SLRPutATests(TestCase):
    """Tests for put_a()"""

    def test_put_a_special_chars(self):
        """Put anchor containing special reserved characters"""
        testrep = SQLiteRepo()
        data = testrep._test_sample["R"]
        #
        for d in data:
            testrep.put_a(d, None)
        ##
        expected = [(r"&#8680;X&#8680;", None),]
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

    def test_put_a_special_chars_prefix(self):
        """
        Put anchor containing special prefix characters

        Special prefix characters are allowed to be used as-is in
        anchors, except for the first character.
        """
        testrep = SQLiteRepo()
        data = testrep._test_sample["R_PX"]
        #
        for d in data:
            testrep.put_a(d, 1)
        ##
        expected = [(r"&#64;X" + "\u0040", 1), (r"&#8714;X" + "\u220a", 1)]
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

    def test_put_a_special_chars_wildcards(self):
        """Put anchor containing wildcard characters"""
        testrep = SQLiteRepo()
        data = testrep._test_sample["WC"]
        data.extend(testrep._test_sample["WC_SLR"])
        #
        for d in data:
            testrep.put_a(d, None)
        ##
        expected = [(x, None) for x in data]
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

