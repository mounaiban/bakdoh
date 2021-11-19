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

from tags import escape, CHAR_WC_1C, CHAR_WC_ZP, SQLiteRepo
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

    This test relies on the correctness of SQLiteRepo._reltext().
    Tests for _reltext() must pass for these tests to be valid.

    Unfortunately, tests for export() cannot be run under the
    generic DB class test suite as implementations for export()
    are repository class-specific.

    """
    def setUp(self):
        # Most direct SQLiteRepo insert humanly possible
        self.testdb = DB(SQLiteRepo())
        sc_insert = "INSERT INTO {} VALUES(?,?)".format(SQLiteRepo.TABLE_A)
        rt = self.testdb.repo._reltext
        cs = self.testdb.repo._db_conn.cursor()
        inp = (
            ('a', None),
            ('j', None),
            ('t', 0.0001),
            ('z', -274),
            (rt('j', 'a', 'j'), None),
            (rt('t', 'a', 't'), -274),
            (rt('a', 'z', 'a'), 37)
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

    This test relies on the correctness of SQLiteRepo._reltext().
    Tests for _reltext() must pass for these tests to be valid.

    Unfortunately, tests for import_data() cannot be run under the
    generic DB class test suite as import_data() implementations
    are repository class-specific.

    """
    def test_import(self):
        testdb = DB(SQLiteRepo())
        sc_dump = "SELECT * FROM {}".format(SQLiteRepo.TABLE_A)
        cs = testdb.repo._db_conn.cursor()
        inp = (
            ('a',),
            ('j', None),
            ('t', 0.0001),
            ('z', -274),
            ('j', 'a', 'j', None),
            ('t', 'a', 't', -274)
        )
        rt = testdb.repo._reltext
        expected = (
            ('a', None),
            ('j', None),
            ('t', 0.0001),
            ('z', -274),
            (rt('j', 'a', 'j'), None),
            (rt('t', 'a', 't'), -274)
        )
        testdb.import_data(inp)
        sample = tuple(cs.execute(sc_dump))
        self.assertEqual(sample, expected)

    def test_import_unsupported_format(self):
        """import_data(): reject unsupported formats"""

        testdb = DB(SQLiteRepo())
        sc_dump = "SELECT * FROM {}".format(SQLiteRepo.TABLE_A)
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
    """Tests for _reltext()"""

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
                    'name': 'Raz',
                    'a_from': 'a',
                    'a_to': 'z',
                    'alias': 'local',
                    'alias_fmt': 0
                },
                'Raz{0}a{0}z'.format(char_rel)
            ),
        ) # format: (kwargs, expected_output)
        for a in argtests:
            with self.subTest(a=a):
                self.assertEqual(testrep._reltext(**a[0]), a[1])

    def test_reltext_alias(self):
        testrep = SQLiteRepo()
        testdb = DB(testrep)
        data = [('a', None), ('z', None)]
        testdb.import_data(data)
        char_rel = testrep._char_rel
        char_alias = testrep._char_alias
        argtests = (
            (
                {
                    'name': 'Raz',
                    'a_from': 'a',
                    'a_to': 'z',
                    'out_format': 1
                },
                'Raz{0}a{0}{1}2'.format(char_rel, char_alias)
            ),
            (
                {
                    'name': 'Raz',
                    'a_from': 'a',
                    'a_to': 'z',
                    'out_format': 2
                },
                'Raz{0}{1}1{0}z'.format(char_rel, char_alias)
            ),
            (
                {
                    'name': 'Raz',
                    'a_from': 'a',
                    'a_to': 'z',
                    'out_format': 3
                },
                'Raz{0}{1}1{0}{1}2'.format(char_rel, char_alias)
            ),

        ) # format: (kwargs, expected_output)
        # NOTE: this test makes an assumption that ROWIDs are always
        # and strictly in order of insertion of the anchors
        #
        for a in argtests:
            with self.subTest(a=a):
                self.assertEqual(testrep._reltext_alias_rowid(**a[0]), a[1])

    def test_reltext_alias_not_exist(self):
        testrep = SQLiteRepo()
        testdb = DB(testrep)
        data = [('a', None), ('z', None)]
        testdb.import_data(data)
        with self.assertRaises(ValueError):
            testdb.repo._reltext_alias_rowid(name='Rnul', a_from='x', a_to='y')

class SLR_QClauseTests(TestCase):
    """Tests for _slr_q_clause()"""

    def test(self):
        """Generate SQL expression for q range in WHERE clause"""
        testrep = SQLiteRepo()
        argtests = (
            ({}, (' ',[])),
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
                expected = (y[0].format(testrep.COL_Q), y[1])
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
                expected = (y[0].format(testrep.COL_Q), y[1])
                self.assertEqual(testrep._slr_q_clause(**x), expected)

class SlrPrepTermTests(TestCase):
    """Tests for _prep_term()"""

    def setUp(self):
        self.testrepo = SQLiteRepo()
        self.escape = self.testrepo.CHAR_ESCAPE
        self.alias = self.testrepo._char_alias

    def test_prep_term_alias(self):
        """Handle ROWID aliases"""
        val = 9001
        term = "{}{}".format(self.alias, val)
        self.assertEqual(self.testrepo._prep_term(term), val)

    def test_prep_term_wildcards(self):
        """Conversion of TAGS wildcards to SQL wildcards"""
        args_outs = (
            {'args': {'term': '*ananas'}, 'out': '%ananas'},
            {'args': {'term': 'ana*nas'}, 'out': 'ana%nas'},
            {'args': {'term': 'ananas*'}, 'out': 'ananas%'},
            {'args': {'term': 'a??na'}, 'out': 'a__na'},
            {
                'args': {'term': '100%an_a'},
                'out': '100{0}%an{0}_a'.format(self.escape)
            },
            {
                'args': {'term': '100%an_a'},
                'out': '100{0}%an{0}_a'.format(self.escape)
            },
        )
        for a in args_outs:
            with self.subTest(term=a['args']):
                self.assertEqual(
                    self.testrepo._prep_term(**a['args']), a['out']
                )

class SlrPrepATests(TestCase):
    """Tests for _prep_a()"""

    def setUp(self):
        self.testrepo = SQLiteRepo()
        self.chardict = self.testrepo.special_chars

    def test_prep_a_special_chars_forbidden(self):
        """Handle forbidden characters in relation names or anchor content.
        """
        ins = (
            "".join((self.chardict["E"], x, self.chardict["WC"]))
            for x in self.chardict["F"]
        )
        outs = (
                "".join((
                    self.chardict["E"], escape(x), self.chardict["WC"]
                )) for x in self.chardict["F"])
        for term, expected in zip(ins, outs):
            with self.subTest(term=term):
                self.assertEqual(self.testrepo._prep_a(term), expected)

    def test_prep_a_special_characters_prefix(self):
        """Handle prefix characters in relation names or anchor content.
        """
        ins = (
            "".join((
                x, self.chardict["E"], self.chardict["WC"]
            )) for x in self.chardict["PX"]
        )
        outs = (
                "".join((escape(x), self.chardict["E"], self.chardict["WC"]
            )) for x in self.chardict["PX"]
        )
        for term, expected in zip(ins, outs):
            with self.subTest(term=term):
                self.assertEqual(self.testrepo._prep_a(term), expected)

class SLRGetATests(TestCase):
    """Tests for get_a()"""

    def test_get_a_alias(self):
        testdb = DB(SQLiteRepo())
        data = (
            ('a', 10),
            ('b', 11),
        )
        testdb.import_data(data)
        expected = ('a', 10) # assuming ROWID strictly follows insertion order
        samp = next(testdb.get_a('@1', out_format='interchange'))
        self.assertEqual(samp, expected)

    def test_get_a_long_content(self):
        """Get Anchors with long-form content

        The preface of the content must be able to retrieve the Anchor

        """
        testrepo = SQLiteRepo()
        testdb = DB(testrepo)
        contents = [
            "{}{}".format(x, "N" * testrepo.preface_length) for x in range(3)
        ]
        init = [(x, 99) for x in contents]
        testdb.import_data(init)
        for c in contents:
            samp_preface = next(testdb.get_a(c[:-1], out_format='interchange'))
            expected_preface = (c[:-1], 99)
            samp_full = next(
                testdb.get_a(c[:-1], length=None, out_format='interchange')
            )
            expected_full = (c, 99)
            self.assertEqual(samp_preface, expected_preface)
            self.assertEqual(samp_full, expected_full)
            # NOTE: The the full contents cannot be used to access
            # the anchor, the preface must be used instead

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
        for c in chars:
            data = [("{}{}".format(c, n), None) for n in range(3)]
            testdb.import_data(data)
            with self.subTest(char=c):
                term = "{}*".format(escape(c))
                samp = list(testdb.get_a(term, out_format='interchange'))
                self.assertEqual(samp, data)

    def test_get_a_special_chars_prefix(self):
        """Get anchors containing escaped prefix special chars

        Only the first char needs to be escaped

        """
        testdb = DB(SQLiteRepo())
        px_chars = testdb.get_special_chars()["PX"]
        fi = lambda x: "{}uuu{}u".format(escape(x), x) # format input
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
        suffix = "".join((chardict["E"], chardict["F"], chardict["WC"]))
        data = [("".join((x, suffix)), -10) for x in chardict["PX"]]
        testdb.import_data(data)
        for d in data:
            samp = list(testdb.export())
            self.assertEqual(samp, data)

class SLRPutATests(TestCase):
    """Tests for put_a()"""

    def test_put_a_long_content(self):
        """Put anchors with long-form content"""
        testrepo = SQLiteRepo()
        testdb = DB(testrepo)
        contents = [
            "{}{}".format(x, "N" * testrepo.preface_length) for x in range(3)
        ]
        data = [(x, None) for x in contents]
        for d in data:
            testdb.put_a(*d)
        samp = list(testdb.export())
        self.assertEqual(samp, data)

    def test_put_a_long_content_same_preface(self):
        """Put anchors with long-form content with the same preface

        The preface of a long-form anchor identifies the anchor
        and cannot be reused.

        """
        testrepo = SQLiteRepo()
        testdb = DB(testrepo)
        contents = [
            "{}{}".format("N" * testrepo.preface_length, x) for x in range(3)
        ]
        data = [(x, None) for x in contents]
        with self.assertRaises(ValueError):
            for d in data:
                testdb.put_a(*d)
        samp = list(testdb.export())
        self.assertEqual(samp, data[:1])

    def test_put_a_long_content_conflicting_preface(self):
        """Put long-form anchor in presence of conflicting anchor

        The preface of a long-form anchor identifies the anchor
        and cannot be identical to an existing anchor. Existing
        anchors must remain intact.

        """
        testrepo = SQLiteRepo()
        testdb = DB(testrepo)
        preface = "N" * testrepo.preface_length
        init = [("{}".format(preface), None),] # anchor equiv. to preface
        data = ("".join((preface, "X")), None) # regular long-form anchor
        testdb.import_data(init)
        with self.assertRaises(ValueError):
            testdb.put_a(*data)
        samp = list(testdb.export())
        self.assertEqual(samp, init)

    def test_put_a_special_chars(self):
        """Put anchor containing special reserved characters"""
        testdb = DB(SQLiteRepo())
        data = [(escape(x), None) for x in testdb.repo.special_chars["F"]]
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
        fi = lambda x: "{0}uuu{0}u".format(escape(x)) # format input
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
        """Put anchor containing all special characters.

        Characters must come out the same way they went in.

        """
        testdb = DB(SQLiteRepo())
        fi = lambda x, y: "".join((escape(x), y))
        chardict = testdb.get_special_chars()
        suffix = "".join((chardict["E"], chardict["F"], chardict["WC"]))
        data = [("".join((p, suffix)), -10) for p in chardict["PX"]]
        for d in data:
            testdb.put_a(*d)
        samp = list(testdb.export())
        self.assertEqual(samp, data)

class SLRPutRelsTests(TestCase):
    """Tests for put_rels()"""

    def test_put_rel_special_chars_wc(self):
        """Put relations containing wildcard characters"""
        testdb = DB(SQLiteRepo())
        init = (
            ('a***', 10),
            ('b???', 20),
        )
        testdb.import_data(init)
        testdb.put_rel('R?*', 'a***', 'b???', None)
        final = [
            ('a***', 10),
            ('b???', 20),
            ('R?*', 'a***', 'b???', None)
        ]
        samp = list(testdb.export())
        self.assertEqual(samp, final)

class SLRGetRelsTests(TestCase):
    """Tests for get_rels()"""

    def test_get_rel_special_chars_wc(self):
        """Get relations containing wildcard characters"""
        testdb = DB(SQLiteRepo())
        init = (
            ('a***', 10),
            ('b???', 20),
            ('R?*', 'a***', 'b???', None)
        )
        testdb.import_data(init)
        expected = ('R?*', 'a***', 'b???', None)
        samp = next(testdb.get_rels(a_from='a&#42;*', out_format='interchange'))
        self.assertEqual(samp, expected)

class SLRSetQTests(TestCase):
    """Tests for setting q-values"""

    def test_set_a_q_long_content(self):
        """Set Anchor q-value with long-form content

        The preface of the content must be able to set the Anchor's q-value

        """
        testrepo = SQLiteRepo()
        testdb = DB(testrepo)
        suffix = "N" * testrepo.preface_length
        init = [
            ("{}{}".format("Z", suffix), 0),
            ('Z', 0),
        ]
        expected = [
            ("{}{}".format("Z", suffix), 99),
            ('Z', 0),
        ]
        testdb.import_data(init)
        testdb.set_a_q("{}{}".format("Z", suffix[:-1]), 99)
        samp = list(testdb.export())
        self.assertEqual(samp, expected)

    def test_set_a_q_special_chars_wc(self):
        testdb = DB(SQLiteRepo())
        init = (
            ('a***', 10),
            ('b???', 20),
        )
        testdb.import_data(init)
        expected = [
            ('a***', 10),
            ('b???', 888),
        ]
        testdb.set_a_q('b???', 888, wildcards=False)
        samp = list(testdb.export())
        self.assertEqual(samp, expected)

    def test_set_rel_q_special_chars_wc(self):
        testdb = DB(SQLiteRepo())
        init = (
            ('a***', 10),
            ('b???', 20),
            ('R?*', 'a***', 'b???', None)
        )
        testdb.import_data(init)
        expected = [
            ('a***', 10),
            ('b???', 20),
            ('R?*', 'a***', 'b???', 888)
        ]
        testdb.set_rel_q('R?*', 'a***', 'b???', 888, wildcards=False)
        samp = list(testdb.export())
        self.assertEqual(samp, expected)


class SLRIncrQTests(TestCase):
    """Tests for setting q-values"""

    def test_incr_a_q_special_chars_wc(self):
        testdb = DB(SQLiteRepo())
        init = (
            ('a***', 10),
            ('b???', 20),
        )
        testdb.import_data(init)
        expected = [
            ('a***', 15),
            ('b???', 20),
        ]
        testdb.incr_a_q('a***', 5, wildcards=False)
        samp = list(testdb.export())
        self.assertEqual(samp, expected)

    def test_incr_rel_q_special_chars_wc(self):
        testdb = DB(SQLiteRepo())
        init = (
            ('a***', 10),
            ('b???', 20),
            ('R?*x', 'a***', 'b???', 80)
        )
        testdb.import_data(init)
        expected = [
            ('a***', 10),
            ('b???', 20),
            ('R?*x', 'a***', 'b???', 79)
        ]
        testdb.incr_rel_q('R?*x', 'a***', 'b???', -1, wildcards=False)
        samp = list(testdb.export())
        self.assertEqual(samp, expected)

