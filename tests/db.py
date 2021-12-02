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
# The test cases in this module are designed to test multiple repository
# classes with minimal setup.
#
# Test cases can be re-run with a different repository class, with
# the following process:
#
# 1. Create a subclass of the test case classes in this module.
#    please be aware that DBTests is not a test case; it serves
#    as a stub and an informal specification.
#
# 2. Implement the methods defined in DBTests in the new
#    subclass, for each test case.
#
# 3. Place the test case subclass in a module with all the other
#    tests where it can be discovered by Python unittest.
#
# Most tests in this module are data-driven; tests are defined by test
# data files or Python dicts. The format is at the bottom of this file.
#
# TODO: Find a way to eliminate the need to re-implement methods
# for each test case
#

import builtins
from unittest import TestCase, TestSuite
from tags import DB, Anchor

class DBTests(TestCase):
    # DB test masterclass
    RepoClass = None

    def direct_insert(self, db, x):
        """Inserts anchors and relations directly via the
        repository, preferably using the lowest-level method
        feasible.

        This is a stub method, please override with a working
        implementation.

        Argument Format
        ===============
        * db: database object

        * x : an iterable of anchors and/or relations:

          If an element is a 2-tuple like (a, q), insert an
          anchor 'a' with a numeric q-value of 'q'.

          If an element is a 4-tuple like (n, af, at, q), insert
          a relation named 'n', from anchor 'af' to anchor 'at',
          with a numeric q-value of 'q'.

          Elements 'a', 'af', 'at' and 'n' are strings, element 'q'
          is an int, a float or None.

        """
        raise NotImplementedError("please see DbTests for usage")


    def dump(self, db):
        """Returns the entire contents of the repository,
        preferably using the lowest-level method feasible.

        This is a stub method, please override with a working
        implementation.

        Return Format
        =============
        Anchors and relations must be returned in a list, in
        the interchange format:

        * Anchors must be returned as 2-tuple like (a, q), where:

          'a' is the Anchor's content

          'q' is the q-value of the Anchor

        * Relations must be returned in a 4-tuple like
          (n, afo, ato, q), where:

          'n' is the name of the relation,

          'afo' and 'ato' are source and destination anchors
          (as Anchor objects),

          'q' is the numeric q-value of the relation

        Argument Format
        ===============
        * db: database object

        * term : Unix glob-like search term to select the anchors
          and relations to be loaded. If None, return all anchors
          and relations.

        """
        raise NotImplementedError("please see DbTests for usage")

    def _run_tests(self, data):
        """
        Runs a test defined by 'data' on a mock database, where 'data'
        is a dict containing a test plan as shown:

        TAGS Database Test Case Data Format
        ===================================
        This is a summary of the format used in Database test plans:

        {
            test_0_name: {
                 "method": method_name,
                 "init": [anchor_0, ... anchor_n],
                 "args_outs": [
                     {
                       "subtest_name": subtest_name,
                       "args": args,
                       "exception": ex,
                       "warning": w,
                       "out": out,
                       "final": final,
                     },
                     ...
                 ]
             },
            ...
            test_n_name: test,
        }

        For each item in args_outs, the method nominated by 'method_name'
        is run with arguments from 'args' on a temporary, mock database with
        contents defined by init.

        The output of the method call is compared with 'out'. When a warning
        or exception is expected, its type is checked against ex or w.

        The Python dict may be used in lieu of an object.

        Notes
        =====
        * init: Initial state of the database before each test, specified in
          the 'interchange' format:

          (content, q) for anchors

          (name, a_from_content, a_to_content, q) for relations

        * args_outs: an object containing arguments in "args", and
          expected returned values in "out", with optional warning or
          exception.

        * "out" is the expected return values of calling 'method_name'
            with arguments from 'args'.

        * "final" is the expected state of the whole mock database after
          the test, specified in the 'interchange' format as with "init".

        * In "args_outs", "warning" and "exception" cannot be used together.
          if both are present, only "exception" will be used.

        * There is currently no check to ensure that any "subtest_name"
          is only used once per test case. Test names may be re-used by
          accident, so watch out for additional name matches when
          searching for a failed test.

        """
        # TODO: How to port this format to ECMA-404/JSON?
        # TODO: Support multiple successive calls per "args_outs"

        if not self.RepoClass: self.skipTest('No RepoClass set')
            # TODO: find a way of skipping tests when no RepoClass
            # is set, that doesn't bomb reports with 'skipped test'
            # results
            #
            # PROTIP: If you get a TypeError, check if:
            # * The args are valid and of the correct type
            #   (number, string, etc...)
            # * A comma follows a lone case in args_out:
            #   'args_out': ({...}) is wrong,
            #   'args_out': ({...},) is correct
        for test in data.keys():
            td = data[test]
            args_outs = td['args_outs']
            if type(args_outs) not in (list, tuple):
                raise TypeError(
                    'args_out in test {} must be list or tuple'.format(test)
                )
            for c in args_outs:
                testdb = DB(self.RepoClass())
                testdb.import_data(td['init'])
                args = c['args']
                ex = None
                m = testdb.__getattribute__(td['method'])
                n = c.get('subtest_name')
                wa = None
                if 'exception' in c:
                    ex = getattr(builtins, c.get('exception', ''))
                elif 'warning' in c:
                    wa = getattr(builtins, c.get('warning', ''))
                with self.subTest(test=test, subtest=n, method=m, args=args):
                    if ex:
                        with self.assertRaises(ex):
                            result = m(**args)
                    elif wa:
                        with self.assertWarns(ex):
                            result = m(**args)
                    else:
                        result = m(**args)
                        if 'out' in c:
                            # PROTIP: check if 'out' matches returned value
                            # from method call
                            if hasattr(result, '__iter__'):
                                self.assertEqual(list(result), c['out'])
                            else:
                                self.assertEqual(result, c['out'])
                    if 'final' in c:
                        # PROTIP: check if the final state of the mock
                        # database is the same as defined by 'final'
                        self.assertEqual(
                            list(testdb.export()), c['final']
                        )

class DBGetTests(DBTests):
    """
    Database tests for getter methods (get_a, get_rel, ...).
    Please see the comments at the beginning of this module for
    usage instructions.

    """
    def test_db_count_a(self):
        test_data = {
            'count_a_basic': {
                'method': 'count_a',
                'init': (
                    ('anna', None),
                    ('annz', 0.1),
                    ('naan', 0.25), # yum
                    ('nana', 0.333),
                    ('zaaa', 0.5)
                ),
                'args_outs': (
                    {
                        'subtest_name': 'count_a_exact',
                        'args': {'a': 'anna'},
                        'out': 1
                    },
                    {
                        'subtest_name': 'count_a_exact_not_found_casesen',
                        'args': {'a': 'aNNa'},
                        'out': 0
                    },
                    {
                        'subtest_name': 'count_a_q_eq',
                        'args': {'q_eq': 0.333},
                        'out': 1
                    },
                    {
                        'subtest_name': 'count_a_q_gte',
                        'args': {'q_gte': 0.333},
                        'out': 2
                    },
                    {
                        'subtest_name': 'count_a_q_lte',
                        'args': {'q_lte': 0.333},
                        'out': 3
                    },
                    {
                        'subtest_name': 'count_a_q_not',
                        'args': {'q_lte': 0.1, 'q_not': True},
                        'out': 3
                    },
                    {
                        'subtest_name': 'count_a_q_range',
                        'args': {'q_lte': 0.5, 'q_gte': 0.1},
                        'out': 4
                    },
                    {
                        'subtest_name': 'count_a_q_range_not',
                        'args': {'q_lt': 0.5, 'q_gt': 0.1, 'q_not': True},
                        'out': 2
                    },
                )
            },
        }
        self._run_tests(test_data)

    def test_db_exists_rels(self):
        test_data = {
            'exists_rels_basic': {
                'method': 'exists_rels',
                'init': (
                    ('annz', None),
                    ('znna', None),
                    ('azs', 'annz', 'znna', 0.3),
                    ('azm', 'annz', 'znna', 0.5),
                    ('azL', 'annz', 'znna', 0.7),
                ),
                'args_outs': (
                    {
                        'subtest_name': 'exists_rels_exact',
                        'args': {
                            'name': 'azs', 'a_from': 'annz', 'a_to': 'znna'
                        },
                        'out': True
                    },
                    {
                        'subtest_name': 'exists_rels_exact_no_rel',
                        'args': {'a_from': 'znna', 'a_to': 'annz'},
                        'out': False
                    },
                    {
                        'subtest_name': 'exists_rels_exact_no_anchors',
                        'args': {'a_from': 'h404', 'a_to': 'h403'},
                        'out': False
                    },
                    {
                        'subtest_name': 'exists_rels_exact_casesen',
                        'args': {
                            'name': 'Azs', 'a_from': 'annz', 'a_to': 'znna',
                        },
                        'out': False
                    },
                    {
                        'subtest_name': 'exists_rels_q_eq',
                        'args': {
                            'a_from': 'annz', 'a_to': 'znna', 'q_eq': 0.3
                        },
                        'out': True
                    },
                    {
                        'subtest_name': 'exists_rels_q_range_a',
                        'args': {
                            'a_from': 'annz',
                            'a_to': 'znna',
                            'q_lt': 0.75,
                            'q_gt': 0.25
                        },
                        'out': True
                    },
                    {
                        'subtest_name': 'exists_rels_q_range_b',
                        'args': {
                            'a_from': 'annz',
                            'a_to': 'znna',
                            'q_lt': 1.75,
                            'q_gt': 1.25
                        },
                        'out': False
                    },
                ),
            },
        }
        self._run_tests(test_data)

    def test_db_get_a(self):
        test_data = {
            'get_a_exact': {
                'meta': {
                    'description': {
                        'en-au': 'get_a() by exact content',
                    'comments': ['relations must not be returned']
                    },
                },
                'method': 'get_a',
                'init': (
                    ('a', None),
                    ('n', 1.414),
                    ('z', 255),
                    ('a', 'a', 'z', None),
                    ('z', 'z', 'a', 512)
                ),
                'args_outs': (
                    {
                        'args': {'a': 'a', 'out_format': 'interchange'},
                        'out': [('a', None),]
                    },
                    {
                        'args': {'a': 'n', 'out_format': 'interchange'},
                        'out': [('n', 1.414),]
                    },
                    {
                        'args': {'a': 'z', 'out_format': 'interchange'},
                        'out': [('z', 255),]
                    },
                ),
            },
            'get_a_wc': {
                'meta': {
                    'description': {
                        'en-au': 'get_a() by content wildcard',
                    },
                },
                'method': 'get_a',
                'init': (
                    ('ababa', None),
                    ('bbbbb', 1),
                    ('cbabc', 2),
                    ('cbaba', 2.5),
                    ('daaaa', 3),
                ),
                'args_outs': (
                    {
                        'args': {'a': 'c*', 'out_format': 'interchange'},
                        'out': [('cbabc', 2), ('cbaba', 2.5)]
                    },
                    {
                        'args': {'a': '*a', 'out_format': 'interchange'},
                        'out': [
                            ('ababa', None),
                            ('cbaba', 2.5),
                            ('daaaa', 3)
                        ]
                    },
                    {
                        'args': {'a': '*a*', 'out_format': 'interchange'},
                        'out': [
                            ('ababa', None),
                            ('cbabc', 2),
                            ('cbaba', 2.5),
                            ('daaaa', 3)
                        ]
                    },
                    {
                        'args': {'a': 'c*a', 'out_format': 'interchange'},
                        'out': [('cbaba', 2.5),]
                    },
                    {
                        'args': {'a': '?b?b?', 'out_format': 'interchange'},
                        'out': [
                            ('ababa', None),
                            ('bbbbb', 1),
                            ('cbabc', 2),
                            ('cbaba', 2.5),
                        ]
                    },
                ),
            },
            'get_a_q_exact': {
                'meta': {
                    'description': {
                        'en-au': 'get_a() by exact q-value',
                    },
                    'comments': ['relations must not be returned']
                },
                'method': 'get_a',
                'init': (
                    ('a', None),
                    ('j', 1.414),
                    ('t', 255),
                    ('z', 255),
                    ('a', 'a', 'z', None),
                    ('j', 'z', 'a', 512)
                ),
                'args_outs': (
                    {
                        'args': {
                            'a': '*', 'q_eq': 255, 'out_format': 'interchange'
                        },
                        'out': [('t', 255), ('z', 255)]
                    },
                    {
                        'args': {
                            'a': '*', 'q_eq': 1.414, 'out_format': 'interchange'
                        },
                        'out': [('j', 1.414),]
                    }
                ),
            },
            'get_a_q_zerovsnone': {
                'meta': {
                    'description': {
                        'en-au': 'get_a() anchors with a value of 0',
                    },
                    'comments': [
                        'relations must not be returned',
                        'zero is different from None',
                    ]
                },
                'method': 'get_a',
                'init': (
                    ('a', None),
                    ('n', 0),
                    ('z', 0),
                    ('a', 'a', 'z', None),
                    ('z', 'z', 'a', 512)
                ),
                'args_outs': (
                    {
                        'args': {
                            'a': '*', 'q_eq': 0, 'out_format': 'interchange'
                        },
                        'out': [('n', 0), ('z', 0)]
                    },
                ),
            },
            'get_a_q_range': {
                'meta': {
                    'description': {
                        'en-au': 'get_a() by q value range',
                    },
                },
                'method': 'get_a',
                'init': (('a', 10), ('n', 20), ('z', 30),),
                'args_outs': (
                    {
                        'args': {
                            'a': '*', 'q_gt': 20, 'out_format': 'interchange'
                        },
                        'out': [('z', 30),]
                    },
                    {
                        'args': {
                            'a': '*', 'q_lt': 20, 'out_format': 'interchange'
                        },
                        'out': [('a', 10),]
                    },
                    {
                        'args': {
                            'a': '*', 'q_gte': 20, 'out_format': 'interchange'
                        },
                        'out': [('n', 20), ('z', 30),]
                    },
                    {
                        'args': {
                            'a': '*', 'q_lte': 20, 'out_format': 'interchange'
                        },
                        'out': [('a', 10), ('n', 20),]
                    },
                ),
            },
            'get_a_q_not_range': {
                'meta': {
                    'description': {
                        'en-au': 'get_a() by negated q value or range',
                    },
                },
                'method': 'get_a',
                'init': (
                    ('a', -10),
                    ('g', -5),
                    ('m', 5),
                    ('s', 10),
                    ('z', None)
                ),
                'args_outs': (
                    {
                        'args': {
                            'a': '*',
                            'q_eq': -5,
                            'q_not': True,
                            'out_format': 'interchange'
                        },
                        'out': [('a', -10), ('m', 5), ('s', 10)],
                    },
                    {
                        'args': {
                            'a': '*',
                            'q_gt': 5,
                            'q_not': True,
                            'out_format': 'interchange'
                        },
                        'out': [('a', -10), ('g', -5), ('m', 5),],
                    },
                    {
                        'args': {
                            'a': '*',
                            'q_lt': -5,
                            'q_not': True,
                            'out_format': 'interchange',
                        },
                        'out': [('g', -5), ('m', 5), ('s', 10),],
                    },
                    {
                        'args': {
                            'a': '*',
                            'q_lt': -5,
                            'q_gt': 5,
                            'q_not': True,
                            'out_format': 'interchange'
                        },
                        'out': [('g', -5), ('m', 5),],
                    },
                    {
                        'args': {
                            'a': '*',
                            'q_lte': -5,
                            'q_gte': 5,
                            'q_not': True,
                            'out_format': 'interchange'
                        },
                        'out': []
                    },
                    {
                        'args': {
                            'a': '*',
                            'q_lt': -5,
                            'q_gt': 5,
                            'q_not': True,
                            'out_format': 'interchange'
                        },
                        'out': [('g', -5), ('m', 5),],
                    },
                ),
            },
        }
        self._run_tests(test_data)

    def test_db_get_a_casesen(self):
        test_data = {
            'get_a_exact_casesen': {
                'method': 'get_a',
                'init': (
                    ('azz', None),
                    ('AzZ', 10),
                    ('azZ', None),
                ),
                'args_outs': (
                    {
                        'subtest_name': 'get_a_exact_casesen',
                        'args': {'a': 'azz'},
                        'out': [('azz', None),]
                    },
                    {
                        'subtest_name': 'get_a_exact_q_casesen',
                        'args': {'a': 'AzZ', 'q': 10},
                        'out': [('azz', None),]
                    }
                )
            },
            'get_a_exact_casesen_no_wc': {
                'method': 'get_a',
                'init': (
                    ('azAZ', None),
                    ('az*Z', 20),
                    ('az?z', 10),
                ),
                'args_outs': (
                    {
                        'subtest_name': 'get_a_exact_q_casesen_no_wc_A',
                        'args': {'a': 'az*Z', 'wildcards': False},
                        'subtest_name': 'get_a_exact_casesen_no_wc',
                        'out': [('az*Z', 20),]
                    },
                    {
                        'subtest_name': 'get_a_exact_q_casesen_no_wc_B',
                        'args': {'a': 'az?Z', 'wildcards': False},
                        'subtest_name': 'get_a_exact_casesen_no_wc',
                        'out': [('az?Z', 10),]
                    },
                )
            }
        }

    def test_db_get_rels(self):
        test_data = {
            'get_rels_exact': {
                'meta': {
                    'description': {
                        'en-au': 'get_rels() by name or anchor content',
                    'comments': ['anchors must not be returned']
                    },
                },
                'method': 'get_rels',
                'init': (
                    ('a', 0),
                    ('n', 0),
                    ('a', 'a', 'n', None),
                    ('n', 'n', 'a', 2),
                    ('z', 'n', 'a', 3)
                ),
                'args_outs': (
                    {
                        'args': {'name': 'a', 'out_format': 'interchange'},
                        'out': [('a', 'a', 'n', None),]
                    },
                    {
                        'args': {'name': 'z', 'out_format': 'interchange'},
                        'out': [('z', 'n', 'a', 3),]
                    },
                    {
                        'args': {'a_from': 'n', 'out_format': 'interchange'},
                        'out': [
                            ('n', 'n', 'a', 2),
                            ('z', 'n', 'a', 3),
                        ]
                    },
                ),
            },
            'get_rels_a_q_range': {
                'meta': {
                    'description': {
                        'en-au': 'get_rels() by anchor content, q range',
                    'comments': ['anchors must not be returned']
                    },
                },
                'method': 'get_rels',
                'init': (
                    ('a', 0),
                    ('z', 1),
                    ('rN', 'a', 'z', None),
                    ('r0', 'a', 'z', 0),
                    ('r2', 'a', 'z', 2),
                    ('r4', 'a', 'z', 4),
                    ('r6', 'z', 'a', 6),
                    ('r8', 'z', 'a', 8)
                ),
                'args_outs': (
                    {
                        'args': {
                            'a_from': 'a',
                            'a_to': 'z',
                            'q_gt': 1,
                            'out_format': 'interchange'
                        },
                        'out': [('r2', 'a', 'z', 2), ('r4', 'a', 'z', 4),]
                    },
                    {
                        'args': {
                            'a_from': 'a',
                            'a_to': 'z',
                            'q_gt': 1,
                            'q_not': True,
                            'out_format': 'interchange'
                        },
                        'out': [('r0', 'a', 'z', 0),]
                    },
                    {
                        'args': {
                            'a_from': 'a',
                            'a_to': 'z',
                            'q_lt': 1,
                            'q_not': True,
                            'out_format': 'interchange'
                        },
                        'out': [('r2', 'a', 'z', 2), ('r4', 'a', 'z', 4),]
                    },
                    {
                        'args': {
                            'a_from': 'a',
                            'a_to': 'z',
                            'q_lte': 8,
                            'q_not': True
                        },
                        'out': []
                    },
                    {
                        'args': {
                            'a_from': 'a',
                            'a_to': 'z',
                            'q_lt': 4,
                            'out_format': 'interchange'
                        },
                        'out': [('r0', 'a', 'z', 0), ('r2', 'a', 'z', 2),]
                    },
                    {
                        'args': {
                            'a_from': 'z',
                            'a_to': 'a',
                            'q_lte': 8,
                            'out_format': 'interchange'
                        },
                        'out': [('r6', 'z', 'a', 6), ('r8', 'z', 'a', 8),]
                    },
                ),
            },
            'get_rels_name_wc': {
                'meta': {
                    'description': {
                        'en-au': 'get_rels() by name wildcard',
                    'comments': ['anchors must not be returned']
                    },
                },
                'method': 'get_rels',
                'init': (
                    ('aaaa', 1),
                    ('jwqa', 2),
                    ('tqqz', 3),
                    ('zwwz', 4),
                    ('aaaa', 'aaaa', 'zwwz', 1),
                    ('jwqa', 'jwqa', 'aaaa', 2),
                    ('tqqz', 'tqqz', 'jwqa', 3),
                    ('zwwz', 'zwwz', 'tqqz', 4)
                ),
                'args_outs': (
                    {
                        'args': {'name': '*a', 'out_format':'interchange'},
                        'out': [
                            ('aaaa', 'aaaa', 'zwwz', 1),
                            ('jwqa', 'jwqa', 'aaaa', 2)
                        ]
                    },
                    {
                        'args': {'name': '*q*', 'out_format': 'interchange'},
                        'out': [
                            ('jwqa', 'jwqa', 'aaaa', 2),
                            ('tqqz', 'tqqz', 'jwqa', 3)
                        ]
                    },
                    {
                        'args': {'name': '?w??', 'out_format': 'interchange'},
                        'out': [
                            ('jwqa', 'jwqa', 'aaaa', 2),
                            ('zwwz', 'zwwz', 'tqqz', 4)
                        ]
                    },
                    {
                        'args': {'name': 'z*', 'out_format': 'interchange'},
                        'out': [('zwwz', 'zwwz', 'tqqz', 4)]
                    },
                    {
                        'args': {'a_from': '*a', 'out_format': 'interchange'},
                        'out': [
                            ('aaaa', 'aaaa', 'zwwz', 1),
                            ('jwqa', 'jwqa', 'aaaa', 2)
                        ]
                    },
                    {
                        'args': {'a_to': '*z', 'out_format': 'interchange'},
                        'out': [
                            ('aaaa', 'aaaa', 'zwwz', 1),
                            ('zwwz', 'zwwz', 'tqqz', 4)
                        ]
                    },
                ),
            },
        }
        self._run_tests(test_data)

    def test_db_get_rels_casesen(self):
        """get_rels(): case sensitivity of exact name/content lookups"""
        test_data = {
            'get_rels_exact_cases': {
                'method': 'get_rels',
                'init': (
                    ('azz', 0),
                    ('Azz', 0),
                    ('zaa', 10),
                    ('Zaa', 10),
                    ('raZ', 'azz', 'Zaa', 1),
                    ('rAz', 'Azz', 'zaa', 2),
                    ('rza', 'zaa', 'azz', 3),
                    ('raz', 'azz', 'zaa', 4),
                ),
                'args_outs': (
                    {
                        'subtest_name': 'get_rels_exact_casesen_A',
                        'args': {
                            'name': 'raZ',
                            'a_from': 'azz',
                            'a_to': 'Zaa',
                            'out_format': 'interchange'
                        },
                        'out': [('raZ', 'azz', 'Zaa', 1),]
                    },
                    {
                        'subtest_name': 'get_rels_exact_casesen_B',
                        'args': {
                            'name': 'rAz',
                            'a_from': 'Azz',
                            'a_to': 'zaa',
                            'out_format': 'interchange'
                        },
                        'out': [('rAz', 'Azz', 'zaa', 2),]
                    },
                ),
            },
        }
        self._run_tests(test_data)

class DBWriteTests(DBTests):
    """Database tests for delete, put and set q-value
    Please see the comments at the beginning of this module for
    usage instructions.

    """
    def test_db_delete_a(self):
        """delete_a(): Delete anchors"""

        test_data = {
            'delete_a_exact': {
                'meta': {
                    'description': {
                        'en-au': 'delete_a() by exact content',
                    },
                },
                'method': 'delete_a',
                'init': (('a', None), ('n', None), ('z', None),),
                'args_outs': (
                    {
                        'args': {'a': 'a'},
                        'final': [('n', None), ('z', None)]
                    },
                ),
            },
            'delete_a_exact_casesen': {
                'meta': {
                    'description': {
                        'en-au': 'delete_a() by exact content',
                    },
                },
                'method': 'delete_a',
                'init': (('a', None), ('A', None), ('z', None),),
                'args_outs': (
                    {
                        'args': {'a': 'a'},
                        'final': [('A', None), ('z', None)]
                    },
                ),
            },
            'delete_a_wc': {
                'meta': {
                    'description': {
                        'en-au': 'delete_a() by wildcards',
                    },
                },
                'method': 'delete_a',
                'init': (
                    ('aaaa', None),
                    ('azzz', None),
                    ('bzzi', None),
                    ('vzzi', None),
                ),
                'args_outs': (
                    {
                        'args': {'a': 'a*'},
                        'final': [('bzzi', None), ('vzzi', None)]
                    },
                    {
                        'args': {'a': '*z*'},
                        'final': [('aaaa', None),]
                    },
                    {
                        'args': {'a': '*i'},
                        'final': [('aaaa', None), ('azzz', None)]
                    },
                    {
                        'args': {'a': '?z?i'},
                        'final': [('aaaa', None), ('azzz', None)]
                    },
                ),
            },
        }
        self._run_tests(test_data)

    def test_db_incr_a_q(self):
        """incr_a_q(): Increment anchor q value"""

        test_data = {
            'incr_a_q': {
                'meta': {
                    'description': {
                        'en-au': 'incr_a_q() specific anchor',
                    },
                    'comments': ['relations must be unchanged',],
                },
                'method': 'incr_a_q',
                'init': (('a', 0), ('z', 0), ('r', 'a', 'z', None)),
                'args_outs': (
                    {
                        'args': {'a': 'a', 'd': -10},
                        'final': [('a', -10), ('z', 0), ('r', 'a', 'z', None),]
                    },
                    {
                        'args': {'a': 'a', 'd': 10},
                        'final': [('a', 10), ('z', 0), ('r', 'a', 'z', None),]
                    },
                ),
            },
            'incr_a_q_q_exact': {
                'meta': {
                    'description': {
                        'en-au': 'incr_a_q() by exact q value',
                    },
                    'comments': ['relations must be unchanged',],
                },
                'method': 'incr_a_q',
                'init': (
                    ('a', 0),
                    ('g', 1.4),
                    ('m', 2.8),
                    ('s', 0.72),
                    ('z', 0),
                    ('r', 'a', 'z', None)
                ),
                'args_outs': (
                    {
                        'args': {'a': '*', 'd': 0.36, 'q_eq':0},
                        'final': [
                            ('a', 0.36),
                            ('g', 1.4),
                            ('m', 2.8),
                            ('s', 0.72),
                            ('z', 0.36),
                            ('r', 'a', 'z', None)
                        ]
                    },
                )
            },
            'incr_a_q_wc': {
                'meta': {
                    'description': {
                        'en-au': 'incr_a_q() by anchor wildcard',
                    },
                    'comments': ['relations must be unchanged',],
                },
                'method': 'incr_a_q',
                'init': (
                    ('tfa', 20),
                    ('tfb', 30),
                    ('tra', 40),
                    ('trb', 50),
                    ('ma', 1000),
                    ('k', 0),
                    ('tfa', 'tfa', 'k', None)
                ),
                'args_outs': (
                    {
                        'args': {'a': '?f?', 'd': -5},
                        'final': [
                            ('tfa', 15),
                            ('tfb', 25),
                            ('tra', 40),
                            ('trb', 50),
                            ('ma', 1000),
                            ('k', 0),
                            ('tfa', 'tfa', 'k', None)
                        ]
                    },
                    {
                        'args': {'a': 't*', 'd': 273},
                        'final': [
                            ('tfa', 293),
                            ('tfb', 303),
                            ('tra', 313),
                            ('trb', 323),
                            ('ma', 1000),
                            ('k', 0),
                            ('tfa', 'tfa', 'k', None)
                        ]
                    },
                )
            }

        }
        self._run_tests(test_data)

    def test_db_put_a(self):
        """put_a(): Insert anchors"""

        test_data = {
            'put_a': {
                'meta': {
                    'description': {
                        'en-au': 'put_a() without q',
                    },
                },
                'method': 'put_a',
                'init': (),
                'args_outs': (
                    {
                        'args': {'a': 'a'},
                        'final': [('a', None),]
                    },
                ),
            },
            'put_a_q': {
                'meta': {
                    'description': {
                        'en-au': 'put_a() with q',
                    },
                },
                'method': 'put_a',
                'init': (),
                'args_outs': (
                    {
                        'args': {'a': 'a', 'q': 1.414},
                        'final': [('a', 1.414),]
                    },
                    {
                        'args': {'a': 'a', 'q': 1},
                        'final': [('a', 1),]
                    },
                ),
            },
        }
        self._run_tests(test_data)

    def test_db_put_a_invalid(self):
        """put_a(): Handling invalid or malformed anchors"""

        test_data = {
            'put_a_non_num_q': {
                'meta': {
                    'description': {
                        'en-au': 'put_a() with non-numeric q',
                    },
                },
                'method': 'put_a',
                'init': (),
                'args_outs': (
                    {
                        'args': {'a': 'a', 'q': 'VALUE'},
                        'exception': 'TypeError',
                        'final': []
                    },
                    {
                        'args': {'a': 'a', 'q': (1,10,100)},
                        'exception': 'TypeError',
                        'final': []
                    },
                ),
            },
            'put_a_nothing': {
                'meta': {
                    'description': {
                        'en-au': 'put_a(): empty string anchor',
                    },
                },
                'method': 'put_a',
                'init': (),
                'args_outs': (
                    {
                        'args': {'a': '',},
                        'exception': 'ValueError',
                        'final': []
                    },
                ),
            }
        }
        self._run_tests(test_data)

    def test_db_set_a_q(self):
        """set_rel_q: set anchor q-value"""
        test_data = {
            'set_a_q': {
                'meta': {
                    'description': {
                        'en-au': 'set_a_q() on specific relation',
                    },
                    'comment': ['relations must be left unchanged'],
                },
                'method': 'set_a_q',
                'init': (('a', 1), ('z', 0), ('z', 'a', 'z', 0)),
                'args_outs': (
                    {
                        'args': {'s': 'z', 'q': 2},
                        'final': [('a', 1), ('z', 2), ('z', 'a', 'z', 0)]
                    },
                    {
                        'args': {'s': 'z', 'q': -2},
                        'final': [('a', 1), ('z', -2), ('z', 'a', 'z', 0)]
                    },
                    {
                        'args': {'s': 'z', 'q': None},
                        'final': [('a', 1), ('z', None), ('z', 'a', 'z', 0)]
                    },
                ),
            },
            'set_a_q_q_wc': {
                'meta': {
                    'description': {
                        'en-au': 'set_a_q() by q-value and wildcard',
                    },
                    'comments': ['relations must be left unchanged',],
                },
                'method': 'set_a_q',
                'init': (
                    ('vx1', 0),
                    ('vx2', 0),
                    ('ax1', 1.0),
                    ('ax2', 2.0),
                    ('vg', 0.5),
                    ('r', 0),
                    ('v', 'vg', 'r', 0)
                ),
                'args_outs': (
                    {
                        'args': {'s': 'v*', 'q': 0.1, 'q_eq': 0},
                        'final': [
                            ('vx1', 0.1),
                            ('vx2', 0.1),
                            ('ax1', 1.0),
                            ('ax2', 2.0),
                            ('vg', 0.5),
                            ('r', 0),
                            ('v', 'vg', 'r', 0)
                        ]
                    },
                    {
                        'args': {'s': '*', 'q': 0.1, 'q_eq': 0},
                        'final': [
                            ('vx1', 0.1),
                            ('vx2', 0.1),
                            ('ax1', 1.0),
                            ('ax2', 2.0),
                            ('vg', 0.5),
                            ('r', 0.1),
                            ('v', 'vg', 'r', 0)
                        ]
                    },
                )
            },
            'set_a_q_wc': {
                'meta': {
                    'description': {
                        'en-au': 'set_a_q() by anchor wildcard',
                    },
                    'comments': ['relations must be left unchanged',],
                },
                'method': 'set_a_q',
                'init': (
                    ('vx1', 1.5),
                    ('vx2', 3.3),
                    ('ax1', 1.0),
                    ('ax2', 2.0),
                    ('vg', 0.5),
                    ('r', 0),
                    ('v', 'vg', 'r', None)
                ),
                'args_outs': (
                    {
                        'args': {'s': 'v*', 'q': 0},
                        'final': [
                            ('vx1', 0),
                            ('vx2', 0),
                            ('ax1', 1.0),
                            ('ax2', 2.0),
                            ('vg', 0),
                            ('r', 0),
                            ('v', 'vg', 'r', None)
                        ]
                    },
                    {
                        'args': {'s': '?x?', 'q': 0},
                        'final': [
                            ('vx1', 0),
                            ('vx2', 0),
                            ('ax1', 0),
                            ('ax2', 0),
                            ('vg', 0.5),
                            ('r', 0),
                            ('v', 'vg', 'r', None)
                        ]
                    },
                )
            }
        }
        self._run_tests(test_data)

    def test_db_set_a_q_invalid(self):
        """set_a_q: handle invalid arguments

        Anchors and relations must be left unchanged
        """
        test_data = {
            'set_a_q_invalid': {
                'meta': {
                    'description': {
                        'en-au': 'set_a_q() on specific relation',
                    },
                    'comment': ['relations must be left unchanged'],
                },
                'method': 'set_a_q',
                'init': (('a', 1), ('z', None), ('z', 'a', 'z', 0)),
                'args_outs': (
                    {
                        'args': {'s': 'z', 'q': 'str'},
                        'exception': 'TypeError',
                        'final': [('a', 1), ('z', None), ('z', 'a', 'z', 0)]
                    },
                    {
                        'args': {'s': 'a', 'q': []},
                        'exception': 'TypeError',
                        'final': [('a', 1), ('z', None), ('z', 'a', 'z', 0)]
                    },
                ),
            },
        }
        self._run_tests(test_data)

    def test_db_set_a_q_casesen(self):
        test_data = {
            'set_a_q_casesen': {
                'method': 'set_a_q',
                'init': (
                    ('aaa', None),
                    ('aAa', None)
                ),
                'args_outs': (
                    {
                        'subtest_name': 'set_a_exact_q_casesen',
                        'args': {'s': 'aaa', 'q': 10, 'format': 'interchange'},
                        'final': [('aaa', 10), ('aAa', None)]
                    },
                )
            },
            'set_a_q_casesen_no_wc': {
                'method': 'set_a_q',
                'init': (
                    ('z??z', None),
                    ('z??Z', None),
                    ('zAZz', None),
                    ('z*Zz', None),
                    ('z*zZ', None),
                ),
                'args_outs': (
                    {
                        'subtest_name': 'set_a_exact_q_casesen_no_wc_A',
                        'args': {'s': 'z??z', 'q': 3.14159, 'wildcards': False},
                        'final': [
                            ('z??z', 3.14159),
                            ('z??Z', None),
                            ('zAZz', None),
                            ('z*Zz', None),
                            ('z*zZ', None),
                        ]
                    },
                    {
                        'subtest_name': 'set_a_exact_q_casesen_no_wc_B',
                        'args': {'s': 'z*Zz', 'q': 3.14159, 'wildcards': False},
                        'final': [
                            ('z??z', None),
                            ('z??Z', None),
                            ('zAZz', None),
                            ('z*Zz', 3.14159),
                            ('z*zZ', None),
                        ]
                    },
                )
            }
        }
        self._run_tests(test_data)

    def test_db_delete_rel(self):
        """delete_rels(): Delete relations"""

        test_data = {
            'delete_rels': {
                'meta': {
                    'description': {
                        'en-au': 'delete_rels() by anchor content',
                    },
                    'comments': ['anchors must be left intact',],
                },
                'method': 'delete_rels',
                'init': (
                    ('a', 1),
                    ('z', 2),
                    ('Rza0', 88),
                    ('Rza1', 99),
                    ('Raz0', 'a', 'z', 0),
                    ('Raz1', 'a', 'z', 1),
                    ('Rza0', 'z', 'a', 0),
                    ('Rza1', 'z', 'a', 1),
                ),
                'args_outs': (
                    {
                        'args': {'a_from': 'a'},
                        'final': [
                            ('a', 1),
                            ('z', 2),
                            ('Rza0', 88),
                            ('Rza1', 99),
                            ('Rza0', 'z', 'a', 0),
                            ('Rza1', 'z', 'a', 1)
                        ]
                    },
                    {
                        'args': {'a_to': 'a'},
                        'final': [
                            ('a', 1),
                            ('z', 2),
                            ('Rza0', 88),
                            ('Rza1', 99),
                            ('Raz0', 'a', 'z', 0),
                            ('Raz1', 'a', 'z', 1)
                        ]
                    },
                ),
            },
            'delete_rels_exact_casesen': {
                'method': 'delete_rels',
                'init': (
                    ('aAaa', 1),
                    ('AaAA', 10),
                    ('zZzz', 2),
                    ('ZZZZ', 20),
                    ('ZAZA', 'ZZZZ', 'AaAA', 0),
                    ('zaza', 'zZzz', 'aAaa', 1),
                ),
                'args_outs': (
                    {
                        'args': {
                            'name': 'zaza',
                            'a_from': 'zZzz',
                            'a_to':'aAaa',
                        },
                        'final': [
                            ('aAaa', 1),
                            ('AaAA', 10),
                            ('zZzz', 2),
                            ('ZZZZ', 20),
                            ('ZAZA', 'ZZZZ', 'AaAA', 0),
                        ]
                    },
                ),
            }
        }
        self._run_tests(test_data)

    def test_db_incr_rel_q(self):
        """incr_rel_q: increment relation q-value"""
        test_data = {
            'incr_rel_q': {
                'meta': {
                    'description': {
                        'en-au': 'incr_rel_q() on specific relation',
                    },
                    'comments': ['anchors must be left untouched'],
                },
                'method': 'incr_rel_q',
                'init': (('a', 1), ('z', 2), ('z', 'a', 'z', 0)),
                'args_outs': (
                    {
                        'args': {
                            'name': 'z', 'a_from':'a', 'a_to':'z', 'd': 10
                        },
                        'final': [('a', 1), ('z', 2), ('z', 'a', 'z', 10)]
                    },
                    {
                        'args': {
                            'name': 'z', 'a_from':'a', 'a_to':'z', 'd': -10
                        },
                        'final': [('a', 1), ('z', 2), ('z', 'a', 'z', -10)]
                    },
                ),
            },
            'incr_rel_q_q_wc': {
                'meta': {
                    'description': {
                        'en-au': 'incr_rel_q() by wildcard and q-value',
                    },
                    'comment': ['anchors must be left unchanged'],
                },
                'method': 'incr_rel_q',
                'init': (
                    ('qx1', 10),
                    ('qx2', 20),
                    ('ca1', 30),
                    ('cb1', 40),
                    ('cb2', 50),
                    ('Lx0', 'ca1', 'qx1', 10),
                    ('Lx1', 'ca1', 'qx2', 20),
                    ('Ln0', 'ca1', 'cb1', 30),
                    ('Ln1', 'ca1', 'cb2', 40),
                ),
                'args_outs': (
                    {
                        'args': {
                            'name': 'Ln*',
                            'a_from': '*',
                            'a_to': '*',
                            'q_lte': 30,
                            'd': 5
                        },
                        'final': [
                            ('qx1', 10),
                            ('qx2', 20),
                            ('ca1', 30),
                            ('cb1', 40),
                            ('cb2', 50),
                            ('Lx0', 'ca1', 'qx1', 10),
                            ('Lx1', 'ca1', 'qx2', 20),
                            ('Ln0', 'ca1', 'cb1', 35),
                            ('Ln1', 'ca1', 'cb2', 40),
                        ]
                    },
                    {
                        'args': {
                            'name': '*',
                            'a_from': '*1',
                            'a_to': '*1',
                            'q_lte': 20,
                            'd': 5
                        },
                        'final': [
                            ('qx1', 10),
                            ('qx2', 20),
                            ('ca1', 30),
                            ('cb1', 40),
                            ('cb2', 50),
                            ('Lx0', 'ca1', 'qx1', 15),
                            ('Lx1', 'ca1', 'qx2', 20),
                            ('Ln0', 'ca1', 'cb1', 30),
                            ('Ln1', 'ca1', 'cb2', 40),
                        ]
                    },
                ),
            },
            'incr_rel_q_wc': {
                'meta': {
                    'description': {
                        'en-au': 'incr_rel_q() by wildcard',
                    },
                    'comment': ['anchors must be left unchanged'],
                },
                'method': 'incr_rel_q',
                'init': (
                    ('qx1', 10),
                    ('qx2', 20),
                    ('ca1', 30),
                    ('cb1', 40),
                    ('cb2', 50),
                    ('Lx0', 'ca1', 'qx1', 10),
                    ('Lx1', 'ca1', 'qx2', 20),
                    ('Ln0', 'ca1', 'cb1', 30),
                    ('Ln1', 'ca1', 'cb2', 40),
                ),
                'args_outs': (
                    {
                        'args': {
                            'name': 'Lx*', 'a_from': '*', 'a_to': '*', 'd': 5
                        },
                        'final': [
                            ('qx1', 10),
                            ('qx2', 20),
                            ('ca1', 30),
                            ('cb1', 40),
                            ('cb2', 50),
                            ('Lx0', 'ca1', 'qx1', 15),
                            ('Lx1', 'ca1', 'qx2', 25),
                            ('Ln0', 'ca1', 'cb1', 30),
                            ('Ln1', 'ca1', 'cb2', 40),
                        ]
                    },
                    {
                        'args': {
                            'name': '*', 'a_from': '*1', 'a_to': '*1', 'd': 5
                        },
                        'final': [
                            ('qx1', 10),
                            ('qx2', 20),
                            ('ca1', 30),
                            ('cb1', 40),
                            ('cb2', 50),
                            ('Lx0', 'ca1', 'qx1', 15),
                            ('Lx1', 'ca1', 'qx2', 20),
                            ('Ln0', 'ca1', 'cb1', 35),
                            ('Ln1', 'ca1', 'cb2', 40),
                        ]
                    },
                ),
            },
        }
        self._run_tests(test_data)

    def test_db_put_rel(self):
        """put_rel(): Link anchors with relations"""

        test_data = {
            'put_rel': {
                'meta': {
                    'description': {
                        'en-au': 'put_rel() by exact anchor content',
                    },
                },
                'method': 'put_rel',
                'init': (('a', 1), ('z', 2)),
                'args_outs': (
                    {
                        'args': {'rel': 'Raz', 'a_from':'a', 'a_to':'z'},
                        'final': [('a', 1), ('z', 2), ('Raz', 'a', 'z', None)]
                    },
                    {
                        'args': {
                            'rel': 'Raz', 'a_from':'a', 'a_to':'z', 'q': 12
                        },
                        'final': [('a', 1), ('z', 2), ('Raz', 'a', 'z', 12)]
                    },
                    {
                        'args': {
                            'rel': 'Raz', 'a_from':'a', 'a_to':'z', 'q': 1.2
                        },
                        'final': [('a', 1), ('z', 2), ('Raz', 'a', 'z', 1.2)]
                    },
                ),
            },
            'put_rel_multi': {
                'meta': {
                    'description': {
                        'en-au': 'put_rel(): multi rels between same anchors',
                    },
                },
                'method': 'put_rel',
                'init': (
                    ('a', 1),
                    ('z', 2),
                    ('R0', 'a', 'z', None),
                    ('R1', 'z', 'a', None)
                ),
                'args_outs': (
                    {
                        'args': {'rel': 'R2', 'a_from':'a', 'a_to':'z', 'q':4},
                        'final': [
                            ('a', 1),
                            ('z', 2),
                            ('R0', 'a', 'z', None),
                            ('R1', 'z', 'a', None),
                            ('R2', 'a', 'z', 4)
                        ]
                    },
                    {
                        'args': {'rel': 'R3', 'a_from':'z', 'a_to':'a', 'q':8},
                        'final': [
                            ('a', 1),
                            ('z', 2),
                            ('R0', 'a', 'z', None),
                            ('R1', 'z', 'a', None),
                            ('R3', 'z', 'a', 8)
                        ]
                    },
                ),
            },
        }
        self._run_tests(test_data)

    def test_db_put_rel_invalid(self):
        """put_rel(): handling of invalid and malformed relations

        Invalid relations must not be created

        """
        test_data = {
            'put_rel_non_num_q': {
                'meta': {
                    'description': {
                        'en-au': 'put_rel() with non-numerical q',
                    },
                },
                'method': 'put_rel',
                'init': (('a', 1), ('z', 2)),
                'args_outs': (
                    {
                        'args': {
                            'rel':'r', 'a_from':'a', 'a_to':'z', 'q':':('
                        },
                        'exception': 'TypeError',
                        'final': [('a', 1), ('z', 2),]
                    },
                ),
            },
            'put_rel_nothing': {
                'meta': {
                    'description': 'put_rel(): with empty strings',
                },
                'method': 'put_rel',
                'init': (('a', None), ('z', None)),
                'args_outs': (
                    {
                        'args': {'rel': '', 'a_from': 'a', 'a_to': 'z'},
                        'exception': 'ValueError',
                        'final': [('a', None), ('z', None)],
                    },
                    {
                        'args': {'rel': 'r', 'a_from': '', 'a_to': 'z'},
                        'exception': 'ValueError',
                        'final': [('a', None), ('z', None)],
                    },
                    {
                        'args': {'rel': 'r', 'a_from': 'a', 'a_to': ''},
                        'exception': 'ValueError',
                        'final': [('a', None), ('z', None)],
                    },
                    {
                        'args': {'rel': 'r', 'a_from': '', 'a_to': ''},
                        'exception': 'ValueError',
                        'final': [('a', None), ('z', None)],
                    },
                ),
            },
            'put_rel_duplicate': {
                'method': 'put_rel',
                'init': (
                    ('a', 1), ('z', 2), ('r', 'a', 'z', None),
                ),
                'args_outs': (
                    {
                        'subtest_name': 'put_rel_duplicate_no_q',
                        'args': {'rel':'r', 'a_from':'a', 'a_to':'z'},
                        'exception': 'ValueError',
                        'final': [('a', 1), ('z', 2), ('r', 'a', 'z', None)]
                    },
                    {
                        'subtest_name': 'put_rel_duplicate_set_q',
                        'args': {'rel':'r', 'a_from':'a', 'a_to':'z', 'q': 99},
                        'exception': 'ValueError',
                        'final': [('a', 1), ('z', 2), ('r', 'a', 'z', None)]
                    },
                ),
            },
            'put_rel_self_link': {
                'method': 'put_rel',
                'init': (('a', 1),),
                'args_outs': (
                    {
                        'subtest_name': 'put_rel_self_link_no_q',
                        'args': {'rel':'r', 'a_from':'a', 'a_to':'a'},
                        'exception': 'ValueError',
                        'final': [('a', 1),]
                    },
                    {
                        'subtest_name': 'put_rel_self_link_set_q',
                        'args': {'rel':'r', 'a_from':'a', 'a_to':'a', 'q': 99},
                        'exception': 'ValueError',
                        'final': [('a', 1),]
                    },
                ),
            }
        }
        self._run_tests(test_data)

    def test_db_set_rel_q(self):
        """set_rel_q: set relation q-value"""
        test_data = {
            'set_rel_q': {
                'method': 'set_rel_q',
                'init': (('a', 1), ('z', 2), ('z', 'a', 'z', 0)),
                'args_outs': (
                    {
                        'subtest_name': 'set_rel_q_exact_rel_pos',
                        'args': {
                            'name': 'z', 'a_from':'a', 'a_to':'z', 'q': 2
                        },
                        'final': [('a', 1), ('z', 2), ('z', 'a', 'z', 2)]
                    },
                    {
                        'subtest_name': 'set_rel_q_exact_rel_neg',
                        'args': {
                            'name': 'z', 'a_from':'a', 'a_to':'z', 'q': -2
                        },
                        'final': [('a', 1), ('z', 2), ('z', 'a', 'z', -2)]
                    },
                    {
                        'subtest_name': 'set_rel_q_exact_rel_none',
                        'args': {
                            'name': 'z', 'a_from':'a', 'a_to':'z', 'q': None
                        },
                        'final': [('a', 1), ('z', 2), ('z', 'a', 'z', None)]
                    },
                ),
            },
            'set_rel_q_wc': {
                'method': 'set_rel_q',
                'init': (
                    ('qx1', 0),
                    ('qx2', 0),
                    ('ca1', 0),
                    ('cb1', 0),
                    ('cb2', 0),
                    ('Lx0', 'ca1', 'qx1', 0),
                    ('Lx1', 'ca1', 'qx2', 0),
                    ('Ln0', 'ca1', 'cb1', 0),
                    ('Ln1', 'ca1', 'cb2', 0),
                ),
                'args_outs': (
                    {
                        'subtest_name': 'set_rel_q_wc_name_prefix',
                        'args': {
                            'name': 'Lx*', 'a_from': '*', 'a_to': '*', 'q': 100
                        },
                        'final': [
                            ('qx1', 0),
                            ('qx2', 0),
                            ('ca1', 0),
                            ('cb1', 0),
                            ('cb2', 0),
                            ('Lx0', 'ca1', 'qx1', 100),
                            ('Lx1', 'ca1', 'qx2', 100),
                            ('Ln0', 'ca1', 'cb1', 0),
                            ('Ln1', 'ca1', 'cb2', 0),
                        ]
                    },
                    {
                        'subtest_name': 'set_rel_q_a_content_suffix',
                        'args': {
                            'name': '*', 'a_from': '*1', 'a_to': '*1', 'q': 100
                        },
                        'final': [
                            ('qx1', 0),
                            ('qx2', 0),
                            ('ca1', 0),
                            ('cb1', 0),
                            ('cb2', 0),
                            ('Lx0', 'ca1', 'qx1', 100),
                            ('Lx1', 'ca1', 'qx2', 0),
                            ('Ln0', 'ca1', 'cb1', 100),
                            ('Ln1', 'ca1', 'cb2', 0),
                        ]
                    },
                ),
            },
        }
        self._run_tests(test_data)

    def test_set_rel_q_invalid(self):
        """set_rel_q(): Handle invalid arguments

        Anchors and relations must be left unchanged
        """
        test_data = {
            'set_rel_q_invalid': {
                'method': 'set_rel_q',
                'init': (('a', 1), ('z', 2), ('z', 'a', 'z', 0)),
                'args_outs': (
                    {
                        'subtest_name': 'set_rel_q_exact_rel_pos_invalid_L',
                        'exception': 'TypeError',
                        'args': {
                            'name': 'z', 'a_from':'a', 'a_to':'z', 'q': []
                        },
                        'final': [('a', 1), ('z', 2), ('z', 'a', 'z', 0)]
                    },
                    {
                        'subtest_name': 'set_rel_q_exact_rel_neg_invalid_S',
                        'exception': 'TypeError',
                        'args': {
                            'name': 'z', 'a_from':'a', 'a_to':'z', 'q': 'notnum'
                        },
                        'final': [('a', 1), ('z', 2), ('z', 'a', 'z', 0)]
                    },
                ),
            },
        }

    def test_set_rel_q_casesen(self):
        test_data = {
            'set_rel_q_casesen': {
                'method': 'set_rel_q',
                'init': (
                    ('zZz', None),
                    ('Zzz', None),
                    ('rZz', 'zZz', 'Zzz', None),
                    ('rzZ', 'Zzz', 'zZz', None),
                ),
                'args_outs': (
                    {
                        'subtest_name': 'set_rel_q_exact_casesen_A',
                        'args': {
                            'name': 'rZz',
                            'a_from': 'zZz',
                            'a_to': 'Zzz',
                            'out_format': 'interchange',
                            'q': 10
                        },
                        'final': (
                            ('zZz', None),
                            ('Zzz', None),
                            ('rZz', 'zZz', 'Zzz', 10),
                            ('rzZ', 'Zzz', 'zZz', None),
                        )
                    },
                    {
                        'subtest_name': 'set_rel_q_exact_casesen_B',
                        'args': {
                            'name': 'rzZ',
                            'a_from': 'Zzz',
                            'a_to': 'zZz',
                            'out_format': 'interchange',
                            'q': 10
                        },
                        'final': (
                            ('zZz', None),
                            ('Zzz', None),
                            ('rZz', 'zZz', 'Zzz', None),
                            ('rzZ', 'Zzz', 'zZz', 10),
                        )
                    }
                )
            }
        }

    def test_self_test_run_tests(self):
        test_data = {
            '_run_tests_with_ex': {
                'method': 'delete_rels',
                'init': (('a', 1), ('z', 2)),
                'args_outs': (
                    {
                        'subtest_name': '_run_tests_ex_invalid_args',
                        'args': {},
                        'exception': 'ValueError',
                        'final': [('a', 1), ('z', 2),]
                    },
                ),
            },
        }
        self._run_tests(test_data)

