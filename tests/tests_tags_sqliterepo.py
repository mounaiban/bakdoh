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

from tags import SQLiteRepo, CHAR_REL, CHARS_R_PX
from unittest import TestCase

def direct_insert(repo, data):
    """
    Insert anchors directly into an SQLiteRepo

    Argument 'data' contains anchors and corresponding q
    (quantity) value in a format like: ((anchor_0, q_0),
    (anchor_1, q_1), ...)

    """
    sc = "INSERT INTO {} VALUES (?,?)".format(SQLiteRepo.table_a)
    cus = repo._slr_get_cursor()
    cus.executemany(sc, data)

def direct_select_all(repo, term='%'):
    """
    Return a list of anchors and corresponding q values directly
    from an SQLiteRepo matching an SQL search term.

    If term is not specified, all anchors will be returned.

    """
    sc_ck = "SELECT * FROM {} WHERE {} LIKE ?".format(
        SQLiteRepo.table_a, SQLiteRepo.col
    )
    cus = repo._slr_get_cursor()
    rows = cus.execute(sc_ck, (term,))
    return [x for x in rows]

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
            ({}, ''),
            ({'q':5}, ' AND {} = 5'),
            ({'q':0}, ' AND {} = 0'),
            ({'q_gt':1}, ' AND {} > 1'),
            ({'q_gt':0}, ' AND {} > 0'),
            ({'q_lt':9}, ' AND {} < 9'),
            ({'q_lt':0}, ' AND {} < 0'),
            ({'q_gt':1, 'q_lt':9}, ' AND {0} > 1 AND {0} < 9'),
            ({'q_gt':0, 'q_lt':0}, ' AND {0} > 0 OR {0} < 0'), # edge case
            ({'q_gt':1, 'q_lt':9, 'q_lte': 9}, ' AND {0} > 1 AND {0} < 9'),
            ({'q_gte':1, 'q_lte':9}, ' AND {0} >= 1 AND {0} <= 9'),
            ({'q_gt':9, 'q_lt':1}, ' AND {0} > 9 OR {0} < 1'),
            ({'q_gt':9, 'q_gte':9, 'q_lt':1}, ' AND {0} > 9 OR {0} < 1'),
            ({'q_gte':9, 'q_lte':1}, ' AND {0} >= 9 OR {0} <= 1'),
            (
                {'q_gt':9, 'q_gte':9, 'q_lt':1, 'q_lte':1},
                ' AND {0} > 9 OR {0} < 1'
            ),
            (
                {'q_gt':1, 'q_gte':1, 'q_lt':9, 'q_lte':9},
                ' AND {0} > 1 AND {0} < 9'
            ),
            (
                {'q':5, 'q_gt':9, 'q_gte':9, 'q_lt':1, 'q_lte':9},
                ' AND {} = 5'
            ),
        ) # format: (kwargs, expected_output)
          # PROTIP: {0} will be replaced with q-value column name
        for x in argtests:
            with self.subTest(args=x[0]):
                expected = x[1].format(testrep.col_q)
                self.assertEqual(testrep._slr_q_clause(**x[0]), expected)

    def test_not(self):
        """Generate SQL expression for q range in WHERE clause (q_not)"""
        testrep = SQLiteRepo()
        argtests = (
            ({'q':5, 'q_not': True}, ' AND NOT ({} = 5)'),
            ({'q':0, 'q_not': True}, ' AND NOT ({} = 0)'),
        )
        for x in argtests:
            with self.subTest(args=x[0]):
                expected = x[1].format(testrep.col_q)
                self.assertEqual(testrep._slr_q_clause(**x[0]), expected)

class SLRDeleteATests(TestCase):
    """Tests for delete_a()"""

    def test_delete_a_exact(self):
        """Delete anchor by exact name"""
        testrep = SQLiteRepo()
        data = [('a', None), ('n', None), ('z', None)]
        direct_insert(testrep, data)
        ##
        testrep.delete_a('n')
        expected = [('a', None), ('z', None)]
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

    def test_delete_a_wildcard_prefix(self):
        """
        Delete anchor with prefix wildcard

        Relations must be left intact

        """
        testrep = SQLiteRepo()
        data = [
            ('a0', None),
            ('a1', None),
            ('n', None),
            ('z', None),
            (testrep.reltxt('aRnz', 'n', 'z'), None),
        ]
        direct_insert(testrep, data)
        ##
        testrep.delete_a('a*')
        expected = [('n', None), ('z', None), (testrep.reltxt('aRnz', 'n', 'z'), None)]
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

class SLRGetATests(TestCase):
    """Tests for get_a()"""

    def test_get_a_exact(self):
        """
        Get anchor by exact name

        Relations must not be returned

        """
        testrep = SQLiteRepo()
        data = (
            ('a', None),
            ('n', None),
            ('z', None),
            (testrep.reltxt('aRan', 'a', 'n'), None),
            (testrep.reltxt('nRna', 'n', 'a'), None),
            (testrep.reltxt('zRzn', 'z', 'n'), None),
        )
        direct_insert(testrep, data)
        ##
        expected_a = [('a', None),]
        expected_n = [('n', None),]
        expected_z = [('z', None),]
        samp_a = [x for x in testrep.get_a('a')]
        samp_n = [x for x in testrep.get_a('n')]
        samp_z = [x for x in testrep.get_a('z')]
        self.assertEqual(expected_a, samp_a)
        self.assertEqual(expected_n, samp_n)
        self.assertEqual(expected_z, samp_z)

    def test_get_a_q_exact(self):
        """Get anchor with quantity by exact name"""
        testrep = SQLiteRepo()
        data = (('a', 1), ('n', 10), ('z', 100))
        direct_insert(testrep, data)
        ##
        expected_a = [('a', 1),]
        expected_n = [('n', 10),]
        expected_z = [('z', 100),]
        samp_a = [x for x in testrep.get_a('a')]
        samp_n = [x for x in testrep.get_a('n')]
        samp_z = [x for x in testrep.get_a('z')]
        self.assertEqual(samp_a, expected_a)
        self.assertEqual(samp_n, expected_n)
        self.assertEqual(samp_z, expected_z)

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

    def test_get_q_eq(self):
        """Get anchors by exact quantity"""
        testrep = SQLiteRepo()
        data = (('a', 1), ('n', 2.5), ('z', 2.5))
        direct_insert(testrep, data)
        ##
        expected_one = [('a', 1)]
        expected_tpf = [('n', 2.5), ('z', 2.5)]
        samp_one = [x for x in testrep.get_a('*', q=1)]
        samp_tpf = [x for x in testrep.get_a('*', q=2.5)]
        self.assertEqual(samp_one, expected_one)
        self.assertEqual(samp_tpf, expected_tpf)

    def test_get_q_eq_not(self):
        """Get anchors by negated exact quantity"""
        testrep = SQLiteRepo()
        data = (('a', 1), ('n', 2.5), ('z', 2.5))
        direct_insert(testrep, data)
        ##
        expected_one = [('n', 2.5), ('z', 2.5)]
        expected_tpf = [('a', 1)]
        samp_one = [x for x in testrep.get_a('*', q_not=True, q=1)]
        samp_tpf = [x for x in testrep.get_a('*', q_not=True, q=2.5)]
        self.assertEqual(samp_one, expected_one)
        self.assertEqual(samp_tpf, expected_tpf)

    def test_get_q_range(self):
        """Get anchors by quantity range"""
        testrep = SQLiteRepo()
        data = (('a', 0), ('g', 0.2), ('m', 0.4), ('s', 0.6), ('z', None))
        direct_insert(testrep, data)
        ##
        tests = (
            ({'a': '*', 'q_gt': 0.1}, [('g', 0.2), ('m', 0.4), ('s', 0.6)]),
            ({'a': '*', 'q_lt': 0.6}, [('a', 0), ('g', 0.2), ('m', 0.4)]),
            ({'a': '*', 'q_gt': 0.2, 'q_lt': 0.6}, [('m', 0.4)]),
            (
                {'a': '*', 'q_gte': 0.2, 'q_lte': 0.6},
                [('g', 0.2), ('m', 0.4), ('s', 0.6)]
            ),
            ({'a': '*', 'q_gt': 0.2, 'q_lt': 0.6}, [('m', 0.4)]),
            ({'a': '*', 'q_gt': 0.4, 'q_lt': 0.2}, [('a', 0), ('s', 0.6)]),
        ) # format: (kwargs, expected_result)
        ##
        for t in tests:
            with self.subTest(kwargs=t):
                samp = [x for x in testrep.get_a(**t[0])]
                self.assertEqual(samp, t[1])

    def test_get_q_range_not(self):
        """Get anchors by negated quantity range"""
        testrep = SQLiteRepo()
        data = (('a', 0), ('g', 0.2), ('m', 0.4), ('s', 0.6), ('z', None))
        direct_insert(testrep, data)
        ##
        tests = (
            ({'a': '*', 'q_not': True, 'q_gt': 0.1}, [('a', 0)]),
            ({'a': '*', 'q_not': True, 'q_lt': 0.4}, [('m', 0.4), ('s', 0.6)]),
            (
                {'a': '*', 'q_not': True, 'q_gt': 0.1, 'q_lt': 0.5},
                [('a', 0), ('s', 0.6)]
            ),
            (
                {'a': '*', 'q_not': True, 'q_gte': 0.2, 'q_lte': 0.6},
                [('a', 0)]
            ),
            (
                {'a': '*', 'q_not': True, 'q_gt': 0.4, 'q_lt': 0.2},
                [('g', 0.2), ('m', 0.4)]
            ),
        ) # format: (kwargs, expected_result)
        ##
        for t in tests:
            with self.subTest(kwargs=t):
                samp = [x for x in testrep.get_a(**t[0])]
                self.assertEqual(samp, t[1])

    def test_get_q_zerovsnone(self):
        """
        Get zero quantity anchors excluding anchors without quantity

        """
        testrep = SQLiteRepo()
        data = (('a', 0), ('z', None))
        direct_insert(testrep, data)
        ##
        expected = [('a', 0)]
        samp = [x for x in testrep.get_a('*', q=0)]
        self.assertEqual(samp, expected)

    def test_get_a_wildcard_onechar_together(self):
        """Get multiple anchors with one character wildcard (3 in a row)"""
        testrep = SQLiteRepo()
        data = (('abbba', None), ('accca', None), ('adddda', None))
        direct_insert(testrep, data)
        ##
        expected = [('abbba', None), ('accca', None)]
        samp = [x for x in testrep.get_a('a???a')]
        self.assertEqual(samp, expected)

    def test_get_a_wildcard_onechar_interlace(self):
        """Get multiple anchors with one character wildcard (interlaced)"""
        testrep = SQLiteRepo()
        data = (('ababa', None), ('aeiou', None), ('azaza', None))
        direct_insert(testrep, data)
        ##
        expected = [('ababa', None), ('azaza', None)]
        samp = [x for x in testrep.get_a('a?a?a')]
        self.assertEqual(samp, expected)

    def test_get_a_q_wildcard_infix(self):
        """Get multiple anchors with infix wildcard"""
        testrep = SQLiteRepo()
        data = (('ajj', None), ('n1_e', 1), ('n_2e', 10))
        direct_insert(testrep, data)
        ##
        expected = [('n1_e', 1), ('n_2e', 10)]
        samp = [x for x in testrep.get_a('n*e')]
        self.assertEqual(samp, expected)

    def test_get_a_q_wildcard_prefix(self):
        """Get multiple anchors with prefix wildcard"""
        testrep = SQLiteRepo()
        data = (('ajj', None), ('n1_e', 1), ('n_2e', 10))
        direct_insert(testrep, data)
        ##
        expected = [('ajj', None),]
        samp = [x for x in testrep.get_a('*j')]
        self.assertEqual(samp, expected)

    def test_get_a_q_wildcard_suffix(self):
        """Get multiple anchors with suffix wildcard"""
        testrep = SQLiteRepo()
        data = (('a1', None), ('a2', 10), ('za', 100))
        direct_insert(testrep, data)
        ##
        expected = [('a1', None), ('a2', 10)]
        samp = [x for x in testrep.get_a('a*')]
        self.assertEqual(samp, expected)

class SLRPutATests(TestCase):
    """Tests for put_a()"""

    def test_put_a(self):
        """Put anchor (both with or without quantity)"""
        testrep = SQLiteRepo()
        data = [('a', None), ('z', 1000)]
        ##
        for d in data:
            testrep.put_a(a=d[0], q=d[1])
        samp = direct_select_all(testrep)
        self.assertEqual(samp, data)

    def test_put_a_non_numerical_q(self):
        """Put anchor with non-numerical quantity (not allowed)"""
        testrep = SQLiteRepo()
        ##
        with self.assertRaises(TypeError):
            testrep.put_a(a='a', q='VALUE')
            testrep.put_a(a='a', q=(1,10,100))
        ##
        expected = []
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

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

class SLRDeleteRelsTests(TestCase):
    """Tests for delete_rels()"""

    def test_delete_rels_exact_afrom(self):
        """Delete relations with exact name"""
        testrep = SQLiteRepo()
        data = [('a', None), ('z', None), (testrep.reltxt('Raz', 'a', 'z'), None)]
        direct_insert(testrep, data)
        ##
        testrep.delete_rels(a_from='a')
        ##
        expected = [('a', None), ('z', None)]
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

    def test_delete_rels_exact_afrom_namesake(self):
        """
        Delete relations by exact destination anchor name (namesakes exist)

        Unrelated relations with same name as must be left intact

        """
        testrep = SQLiteRepo()
        data = [
            ('a', None),
            ('z', None),
            ('Raz', None),
            (testrep.reltxt('Raz', 'a', 'z'), None)
        ]
        direct_insert(testrep, data)
        ##
        testrep.delete_rels(a_from='a')
        ##
        expected = [('a', None), ('z', None), ('Raz', None)]
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

    def test_delete_rels_exact_ato(self):
        """Delete relations by exact destination anchor name"""
        testrep = SQLiteRepo()
        data = [('a', None), ('z', None), (testrep.reltxt('Raz', 'a', 'z'), None)]
        direct_insert(testrep, data)
        ##
        testrep.delete_rels(a_to='z')
        ##
        expected = [('a', None), ('z', None)]
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

class SLRGetRelsTests(TestCase):
    """Tests for get_rels()"""

    def test_get_rels_name_exact(self):
        """Get relations with exact name"""
        testrep = SQLiteRepo()
        aa = ('a', None)
        az = ('z', None)
        anchors = (aa, az)
        rels = [('Raz0', aa, az, None), ('Raz1', aa, az, None)]
        rels_sql = [(testrep.reltxt(n, af[0], at[0]), q) for n, af, at, q in rels]
        direct_insert(testrep, anchors)
        direct_insert(testrep, rels_sql)
        ##
        samp = [x for x in testrep.get_rels(name='Raz0')]
        expected = [rels[0],]
        self.assertEqual(samp, expected)

    def test_get_rels_a_exact(self):
        """Get relations by exact anchor (with or without quantity)"""
        testrep = SQLiteRepo()
        aa = ('a', None)
        az = ('z', None)
        anchors = (aa, az)
        rels = [('Raz0', aa, az, None), ('Raz1', aa, az, None)]
        rels_sql = [(testrep.reltxt(n, af[0], at[0]), q) for n, af, at, q in rels]
        direct_insert(testrep, anchors)
        direct_insert(testrep, rels_sql)
        ##
        samp = [x for x in testrep.get_rels(name='Raz0')]
        expected = [rels[0],]
        self.assertEqual(samp, expected)
        samp = [x for x in testrep.get_rels(name='Raz1')]
        expected = [rels[1],]

    def test_get_rels_a_eq_q_range(self):
        """Get relations by exact anchors and quantity range"""
        testrep = SQLiteRepo()
        aa = ('a', None)
        az = ('z', None)
        data = (
            aa,
            az,
            (testrep.reltxt('RazN', 'a', 'z'), None),
            (testrep.reltxt('Raz0', 'a', 'z'), 0),
            (testrep.reltxt('Raz2', 'a', 'z'), 0.2),
            (testrep.reltxt('Raz4', 'a', 'z'), 0.4),
            (testrep.reltxt('Rza6', 'z', 'a'), 0.6),
            (testrep.reltxt('Rza8', 'z', 'a'), 0.8),
        )
        direct_insert(testrep, data)
        ##
        tests = (
            (
                {'a_from': 'a', 'a_to': 'z', 'q_gt': 0.1},
                [('Raz2', aa, az, 0.2), ('Raz4', aa, az, 0.4)]
            ),
            (
                {'a_from': 'a', 'a_to': 'z', 'q_lt': 0.4},
                [('Raz0', aa, az, 0), ('Raz2', aa, az, 0.2)]
            ),
            (
                {'a_from': 'z', 'a_to': 'a', 'q_lte': 0.8},
                [('Rza6', az, aa, 0.6), ('Rza8', az, aa, 0.8)]
            ),
        ) # format: (kwargs, expected_result)
        ##
        for t in tests:
            with self.subTest(kwargs=t[0]):
                samp = [x for x in testrep.get_rels(**t[0])]
                self.assertEqual(samp, t[1])

    def test_get_rels_a_eq_q_not_range(self):
        """Get relations by exact anchors and quantity range"""
        testrep = SQLiteRepo()
        aa = ('a', None)
        az = ('z', None)
        data = (
            aa,
            az,
            (testrep.reltxt('RazN', 'a', 'z'), None),
            (testrep.reltxt('Raz0', 'a', 'z'), 0),
            (testrep.reltxt('Raz2', 'a', 'z'), 0.2),
            (testrep.reltxt('Raz4', 'a', 'z'), 0.4),
            (testrep.reltxt('Rza6', 'z', 'a'), 0.6),
            (testrep.reltxt('Rza8', 'z', 'a'), 0.8),
        )
        direct_insert(testrep, data)
        ##
        tests = (
            (
                {'a_from': 'a', 'a_to': 'z', 'q_gt': 0.1, 'q_not': True},
                [('Raz0', aa, az, 0),]
            ),
            (
                {'a_from': 'a', 'a_to': 'z', 'q_lt': 0.1, 'q_not': True},
                [('Raz2', aa, az, 0.2), ('Raz4', aa, az, 0.4)]
            ),
            (
                {'a_from': 'z', 'a_to': 'a', 'q_lte': 0.8, 'q_not': True},
                []
            ),
        ) # format: (kwargs, expected_result)
        ##
        for t in tests:
            with self.subTest(kwargs=t[0]):
                samp = [x for x in testrep.get_rels(**t[0])]
                self.assertEqual(samp, t[1])

    def test_get_rels_name_wildcard_suffix(self):
        """Get relations by name suffix wildcard (with/without quantity)"""
        testrep = SQLiteRepo()
        aa = ('a', None)
        an = ('n', None)
        az = ('z', None)
        anchors = (aa, an, az)
        rels = (
            ('Ran0', aa, an, None),
            ('Raz1', aa, az, 11),
            ('Rna2', an, aa, 22),
        )
        rels_sql = [(testrep.reltxt(n, af[0], at[0]), q) for n, af, at, q in rels]
        direct_insert(testrep, anchors)
        direct_insert(testrep, rels_sql)
        ##
        samp = [x for x in testrep.get_rels(name='Ra*')] # all rels from a
        expected = [rels[0], rels[1]]
        self.assertEqual(samp, expected)

    def test_get_rels_a_from_wildcard_suffix(self):
        """Get relations by source anchor wildcard (with/without quantity)"""
        testrep = SQLiteRepo()
        a0 = ('a0', None)
        a1 = ('a1', None)
        z = ('z', None)
        anchors = (a0, a1, z)
        rels = (
            ('Ra0z0', a0, z, None),
            ('Ra1z1', a1, z, 11),
            ('Rza12', z, a1, 22),
            ('Ra1a03', a1, a0, 33),
        )
        rels_sql = [(testrep.reltxt(n, af[0], at[0]), q) for n, af, at, q in rels]
        direct_insert(testrep, anchors)
        direct_insert(testrep, rels_sql)
        ##
        samp = [x for x in testrep.get_rels(a_from='a*')] # rels from a0 & a1
        expected = [rels[0], rels[1], rels[3]]
        self.assertEqual(samp, expected)

    def test_get_rels_a_to_wildcard_suffix(self):
        """Get relations by target anchor wildcard (with/without quantity)"""
        testrep = SQLiteRepo()
        a0 = ('a0', None)
        a1 = ('a1', None)
        z = ('z', None)
        anchors = (a0, a1, z)
        rels = (
            ('Ra0a10', a0, a1, None),
            ('Ra1z1', a1, z, 11),
            ('Rza02', z, a0, 22),
            ('Rza13', z, a1, 33),
        )
        rels_sql = [(testrep.reltxt(n, f[0], t[0]), q) for n, f, t, q in rels]
        direct_insert(testrep, anchors)
        direct_insert(testrep, rels_sql)
        ##
        samp = [x for x in testrep.get_rels(a_to='a*')] # rels from a0 & a1
        expected = [rels[0], rels[2], rels[3]]
        self.assertEqual(samp, expected)

class SLRPutRelTests(TestCase):
    """Tests for put_rel()"""

    def test_put_rel(self):
        """Put new relation"""
        testrep = SQLiteRepo()
        data = (('a', None), ('z', 1))
        direct_insert(testrep, data)
        #
        testrep.put_rel('Raz', 'a', 'z')
        ##
        term = testrep.reltxt('Raz', 'a', 'z')
        samp = direct_select_all(testrep, term)
        self.assertEqual(len(samp), 1)

    def test_put_rel_multi(self):
        """Put multiple relations between same anchors in the same direction"""
        testrep = SQLiteRepo()
        data = (('a', None), ('z', 1))
        direct_insert(testrep, data)
        #
        testrep.put_rel('Raz0', 'a', 'z')
        testrep.put_rel('Raz1', 'a', 'z')
        testrep.put_rel('Raz2', 'a', 'z')
        ##
        term = testrep.reltxt('Raz%', 'a', 'z')
        samp = direct_select_all(testrep, term=term)
        self.assertEqual(len(samp), 3)

    def test_put_rel_q(self):
        """Put new relation with quantitative value"""
        testrep = SQLiteRepo()
        data = (('a', None), ('z', 1))
        direct_insert(testrep, data)
        #
        testrep.put_rel('Raz', 'a', 'z', 3.142)
        ##
        term = testrep.reltxt('Raz', 'a', 'z')
        samp = direct_select_all(testrep, term)
        expected = (term, 3.142)
        self.assertEqual(samp[0], expected)

    def test_put_rel_non_numerical_q(self):
        """Put new relation with non-numeric quantity (not allowed)"""
        testrep = SQLiteRepo()
        data = (('a', None), ('z', 1))
        direct_insert(testrep, data)
        #
        with self.assertRaises(TypeError):
            testrep.put_rel('Raz', 'a', 'z', ':(')
        ##
        term = testrep.reltxt('Raz', 'a', 'z')
        samp = direct_select_all(testrep, term)
        self.assertEqual(len(samp), 0)

    def test_put_rel_duplicate(self):
        """
        Put duplicate relations (not allowed)

        Duplicate relations are repeated relations between the same
        anchors with the same name in the same direction.

        """
        testrep = SQLiteRepo()
        data = (('a', None), ('z', 1))
        direct_insert(testrep, data)
        #
        testrep.put_rel('Raz', 'a', 'z')
        ##
        with self.assertRaises(ValueError):
            testrep.put_rel('Raz', 'a', 'z')
        ##
        term = testrep.reltxt('Raz', 'a', 'z')
        samp = direct_select_all(testrep, term)
        self.assertEqual(len(samp), 1)

    def test_put_rel_self_link(self):
        """Put relation linking anchor to itself (not allowed)"""
        testrep = SQLiteRepo()
        data = (('a', None),)
        direct_insert(testrep, data)
        ##
        with self.assertRaises(ValueError):
            testrep.put_rel('Raa', 'a', 'a')
        ##
        term = testrep.reltxt('Raa', 'a', 'a')
        samp = direct_select_all(testrep, term)
        self.assertEqual(len(samp), 0)

class SLRIncrAQTests(TestCase):

    def test_incr_a_q_exact(self):
        """Increment assigned quantity by exact anchor name"""
        testrep = SQLiteRepo()
        data = [('a', 1), ('z', 100)]
        direct_insert(testrep, data)
        #
        testrep.incr_a_q('a', 1)
        expected = [('a', 2), ('z', 100)]
        ##
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

    def test_incr_a_q_exact_neg_d(self):
        """Decrement assigned quantity by exact anchor name (when d<0)"""
        testrep = SQLiteRepo()
        sc_in = "INSERT INTO {} VALUES(?, ?)".format(testrep.table_a)
        cus = testrep._slr_get_cursor()
        data = [('a', 1), ('z', 100)]
        cus.executemany(sc_in, data)
        #
        testrep.incr_a_q('a', -1)
        expected = [('a', 0), ('z', 100)]
        ##
        sc_ck = "SELECT * FROM {}".format(
            testrep.table_a, testrep.col
        )
        r = [x for x in cus.execute(sc_ck)]
        self.assertEqual(r, expected)

class SLRSetAQTests(TestCase):

    def test_set_a_q_exact(self):
        """Assign quantity to an anchor by exact name"""
        testrep = SQLiteRepo()
        data = (('a', None), ('z', None))
        direct_insert(testrep, data)
        #
        q_expected = 1
        testrep.set_a_q('a', q_expected)
        ##
        samp = direct_select_all(testrep, term='a')
        self.assertEqual(samp[0], ('a', q_expected))

class SLRIncrRelQTests(TestCase):

    def test_incr_rel_q_exact(self):
        """Increment quantity assigned to relation by exact name and anchors"""
        testrep = SQLiteRepo()
        rel_a_z_0 = testrep.reltxt('Raz0', 'a', 'z')
        rel_a_z_1 = testrep.reltxt('Raz1', 'a', 'z')
        data = (('a', 100), ('z', 100), (rel_a_z_0, 1), (rel_a_z_1, 100))
            # two anchors and a relation between them
        direct_insert(testrep, data)
        #
        testrep.incr_rel_q('Raz0', 'a', 'z', 1)
        ##
        expected = [('a', 100), ('z', 100), (rel_a_z_0, 2), (rel_a_z_1, 100)]
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

    def test_incr_rel_q_exact_neg_d(self):
        """
        Decrement quantity assigned to relation by exact name and anchors
        (where d<0)

        """
        testrep = SQLiteRepo()
        rel_a_z_0 = testrep.reltxt('Raz0', 'a', 'z')
        rel_a_z_1 = testrep.reltxt('Raz1', 'a', 'z')
        data = (('a', 100), ('z', 100), (rel_a_z_0, 1), (rel_a_z_1, 100))
            # two anchors and a relation between them
        direct_insert(testrep, data)
        #
        testrep.incr_rel_q('Raz0', 'a', 'z', -1)
        ##
        expected = [('a', 100), ('z', 100), (rel_a_z_0, 0), (rel_a_z_1, 100)]
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

class SLRSetRelQTests(TestCase):

    def test_set_rel_q_exact(self):
        """Assign quantity to relation by exact name and anchors"""
        testrep = SQLiteRepo()
        rel_a_z = testrep.reltxt('Raz', 'a', 'z')
        data = (('a', None), ('z', None), (rel_a_z, None))
            # two anchors and a relation between them
        direct_insert(testrep, data)
        #
        q_expected = 1
        testrep.set_rel_q('Raz', 'a', 'z', q_expected)
        ##
        r = direct_select_all(testrep, rel_a_z)
        self.assertEqual(r[0], (rel_a_z, q_expected))

