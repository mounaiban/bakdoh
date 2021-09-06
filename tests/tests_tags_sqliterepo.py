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

from tags import reltxt, SQLiteRepo, SYMBOLS
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

class SLRGetATests(TestCase):
    """Tests for get_a()"""

    def test_get_a_exact(self):
        """Get anchor by exact name"""
        testrep = SQLiteRepo()
        data = (('a', None), ('n', None), ('z', None))
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
        ta = ""
        for x in SYMBOLS.values():
            ta = "".join((ta, x))
        #
        testrep.put_a(ta, None)
        ##
        ae = ""
        for x in SYMBOLS.values():
            ae = "".join((ae, r"\u{:04x}".format(ord(x))))
        expected = [(ae, None)]
        samp = direct_select_all(testrep)
        self.assertEqual(samp, expected)

    def test_put_a_q_special_chars(self):
        """Put anchor with quantity containing special reserved characters"""
        testrep = SQLiteRepo()
        ta = ""
        for x in SYMBOLS.values():
            ta = "".join((ta, x))
        #
        testrep.put_a(ta, 1)
        ##
        sc_ck = "SELECT * FROM {}".format(testrep.table_a)
        cus = testrep._slr_get_cursor()
        rows = cus.execute(sc_ck)
        ae = ""
        for x in SYMBOLS.values():
            ae = "".join((ae, r"\u{:04x}".format(ord(x))))
        expected = [(ae, 1)]
        samp = [x for x in rows]
        self.assertEqual(samp, expected)

class SLRGetRelsTests(TestCase):
    """Tests for get_rels()"""

    def test_get_rels_name_exact(self):
        """Get relations with exact name"""
        testrep = SQLiteRepo()
        anchors = (('a', None), ('z', None))
        rels = (
            (reltxt('Raz0', 'a', 'z'), None),
            (reltxt('Raz1', 'a', 'z'), None),
        )
        direct_insert(testrep, anchors)
        direct_insert(testrep, rels)
        ##
        samp = [x for x in testrep.get_rels(name='Raz0')]
        expected = [rels[0],]
        self.assertEqual(samp, expected)

    def test_get_rels_a_exact(self):
        """Get relations by exact anchor (with or without quantity)"""
        testrep = SQLiteRepo()
        anchors = (('a', None), ('z', None))
        rels = (
            (reltxt('Raz0', 'a', 'z'), None),
            (reltxt('Rza1', 'z', 'a'), 10),
        )
        direct_insert(testrep, anchors)
        direct_insert(testrep, rels)
        ##
        samp = [x for x in testrep.get_rels(name='Raz0')]
        expected = [rels[0],]
        self.assertEqual(samp, expected)
        samp = [x for x in testrep.get_rels(name='Raz1')]
        expected = [rels[1],]

    def test_get_rels_name_wildcard_suffix(self):
        """Get relations by name suffix wildcard (with/without quantity)"""
        testrep = SQLiteRepo()
        anchors = (('a', None), ('n', None), ('z', None))
        rels = (
            (reltxt('Ran0', 'a', 'n'), None),
            (reltxt('Raz1', 'a', 'z'), 11),
            (reltxt('Rna2', 'n', 'a'), 22),
        )
        direct_insert(testrep, anchors)
        direct_insert(testrep, rels)
        ##
        samp = [x for x in testrep.get_rels(name='Ra*')] # all rels from a
        expected = [rels[0], rels[1]]
        self.assertEqual(samp, expected)

    def test_get_rels_a_from_wildcard_suffix(self):
        """Get relations by source anchor wildcard (with/without quantity)"""
        testrep = SQLiteRepo()
        anchors = (('a0', None), ('a1', None), ('z', None))
        rels = (
            (reltxt('Ra0z0', 'a0', 'z'), None),
            (reltxt('Ra1z1', 'a1', 'z'), 11),
            (reltxt('Rza12', 'z', 'a1'), 22),
            (reltxt('Ra1a03', 'a1', 'a0'), 33),
        )
        direct_insert(testrep, anchors)
        direct_insert(testrep, rels)
        ##
        samp = [x for x in testrep.get_rels(a_from='a*')] # rels from a0 & a1
        expected = [rels[0], rels[1], rels[3]]
        self.assertEqual(samp, expected)

    def test_get_rels_a_to_wildcard_suffix(self):
        """Get relations by target anchor wildcard (with/without quantity)"""
        testrep = SQLiteRepo()
        anchors = (('a0', None), ('a1', None), ('z', None))
        rels = (
            (reltxt('Ra0a10', 'a0', 'a1'), None),
            (reltxt('Ra1z1', 'a1', 'z'), 11),
            (reltxt('Rza02', 'z', 'a0'), 22),
            (reltxt('Rza13', 'z', 'a1'), 33),
        )
        direct_insert(testrep, anchors)
        direct_insert(testrep, rels)
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
        term = reltxt('Raz', 'a', 'z')
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
        term = reltxt('Raz%', 'a', 'z')
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
        term = reltxt('Raz', 'a', 'z')
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
        term = reltxt('Raz', 'a', 'z')
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
        term = reltxt('Raz', 'a', 'z')
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
        term = reltxt('Raa', 'a', 'a')
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
        rel_a_z_0 = reltxt('Raz0', 'a', 'z')
        rel_a_z_1 = reltxt('Raz1', 'a', 'z')
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
        rel_a_z_0 = reltxt('Raz0', 'a', 'z')
        rel_a_z_1 = reltxt('Raz1', 'a', 'z')
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
        rel_a_z = reltxt('Raz', 'a', 'z')
        data = (('a', None), ('z', None), (rel_a_z, None))
            # two anchors and a relation between them
        direct_insert(testrep, data)
        #
        q_expected = 1
        testrep.set_rel_q('Raz', 'a', 'z', q_expected)
        ##
        r = direct_select_all(testrep, rel_a_z)
        self.assertEqual(r[0], (rel_a_z, q_expected))

