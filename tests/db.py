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
        Anchors and relations must be returned in a list.

        * Anchors must be returned as an Anchor object

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
                 "meta": {
                     "comments": [comment_0, ... comment_n],
                     "descripton": {
                         lang_0: desc_0,
                         ...
                         lang_n: desc_n,
                     }
                 },
                 "method": method_name,
                 "init": [anchor_0, ... anchor_n],
                 "args_outs": [
                     {
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
          accordance to the argument format accepted by direct_insert().
          The entire array is used as the 'x' argument.
         
        * args_outs: an object containing arguments in "args", and
          expected returned values in "out", with optional warning or
          exception.
        
        * "out" is the expected return values of calling 'method_name'
            with arguments from 'args'.
        
        * "final" is the expected state of the whole mock database after
          the test.
       
        * In "args_outs", "warning" and "exception" cannot be used together.
          if both are present, only "exception" will be used.
        
        """
        # TODO: How to port this format to ECMA-404/JSON?
        # TODO: Support multiple successive calls per "args_outs"

        if not self.RepoClass: self.skipTest('No RepoClass set')
            # TODO: find a way of skipping tests when no RepoClass
            # is set, that doesn't bomb reports with 'skipped test'
            # results
            #
            # PROTIP: If you get a TypeError at args = c['args'],
            # check if:
            # * The args are valid and of the correct type
            #   (number, string, etc...)
            # * A comma follows a lone case in args_out:
            #   'args_out': ({...}) is wrong,
            #   'args_out': ({...},) is correct
        for test in data.keys():
            t = data[test]
            for c in t['args_outs']:
                testdb = DB(self.RepoClass())
                self.direct_insert(testdb, t['init'])
                args = c['args']
                ex = None
                m = testdb.__getattribute__(t['method'])
                wa = None
                if 'exception' in c:
                    ex = getattr(builtins, c.get('exception', ''))
                elif 'warning' in c:
                    wa = getattr(builtins, c.get('warning', ''))
                with self.subTest(test_name=test, method=m, args=args):
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
                        self.assertEqual(self.dump(testdb), c['final'])

class DBGetTests(DBTests):
    """
    Database tests for getter methods (get_a, get_rel, ...).
    Please see the comments at the beginning of this module for
    usage instructions.

    """

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
                    {'args': {'a': 'a'}, 'out': [Anchor('a', None),]},
                    {'args': {'a': 'n'}, 'out': [Anchor('n', 1.414),]},
                    {'args': {'a': 'z'}, 'out': [Anchor('z', 255),]},
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
                        'args': {'a': 'c*'},
                        'out': [Anchor('cbabc', 2), Anchor('cbaba', 2.5)]
                    },
                    {
                        'args': {'a': '*a'},
                        'out': [
                            Anchor('ababa', None),
                            Anchor('cbaba', 2.5),
                            Anchor('daaaa', 3)
                        ]
                    },
                    {
                        'args': {'a': '*a*'},
                        'out': [
                            Anchor('ababa', None),
                            Anchor('cbabc', 2),
                            Anchor('cbaba', 2.5),
                            Anchor('daaaa', 3)
                        ]
                    },
                    {
                        'args': {'a': 'c*a'},
                        'out': [Anchor('cbaba', 2.5),]
                    },
                    {
                        'args': {'a': '?b?b?'},
                        'out': [
                            Anchor('ababa', None),
                            Anchor('bbbbb', 1),
                            Anchor('cbabc', 2),
                            Anchor('cbaba', 2.5),
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
                        'args': {'a': '*', 'q_eq': 255},
                        'out': [Anchor('t', 255), Anchor('z', 255)]
                    },
                    {
                        'args': {'a': '*', 'q_eq': 1.414},
                        'out': [Anchor('j', 1.414),]
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
                        'args': {'a': '*', 'q_eq': 0},
                        'out': [Anchor('n', 0), Anchor('z', 0)]
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
                        'args': {'a': '*', 'q_gt': 20},
                        'out': [Anchor('z', 30),]
                    },
                    {
                        'args': {'a': '*', 'q_lt': 20},
                        'out': [Anchor('a', 10),]
                    },
                    {
                        'args': {'a': '*', 'q_gte': 20},
                        'out': [Anchor('n', 20), Anchor('z', 30),]
                    },
                    {
                        'args': {'a': '*', 'q_lte': 20},
                        'out': [Anchor('a', 10), Anchor('n', 20),]
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
                        'args': {'a': '*', 'q_eq': -5, 'q_not': True},
                        'out': [
                            Anchor('a', -10),
                            Anchor('m', 5),
                            Anchor('s', 10)
                        ],
                    },
                    {
                        'args': {'a': '*', 'q_gt': 5, 'q_not': True},
                        'out': [
                            Anchor('a', -10),
                            Anchor('g', -5),
                            Anchor('m', 5),
                        ],
                    },
                    {
                        'args': {'a': '*', 'q_lt': -5, 'q_not': True},
                        'out': [
                            Anchor('g', -5),
                            Anchor('m', 5),
                            Anchor('s', 10),
                        ],
                    },
                    {
                        'args': {
                            'a': '*', 'q_lt': -5, 'q_gt': 5, 'q_not': True
                        },
                        'out': [Anchor('g', -5), Anchor('m', 5),],
                    },
                    {
                        'args': {
                            'a': '*', 'q_lte': -5, 'q_gte': 5, 'q_not': True
                        },
                        'out': []
                    },
                    {
                        'args': {
                            'a': '*', 'q_lt': -5, 'q_gt': 5, 'q_not': True
                        },
                        'out': [Anchor('g', -5), Anchor('m', 5),],
                    },
                ),
            },
        }
        self._run_tests(test_data)

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
                        'args': {'name': 'a'},
                        'out': [('a', Anchor('a', 0), Anchor('n', 0), None),]
                    },
                    {
                        'args': {'name': 'z'},
                        'out': [('z', Anchor('n', 0), Anchor('a', 0), 3),]
                    },
                    {
                        'args': {'a_from': 'n'},
                        'out': [
                            ('n', Anchor('n', 0), Anchor('a', 0), 2),
                            ('z', Anchor('n', 0), Anchor('a', 0), 3),
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
                        'args': {'a_from': 'a', 'a_to': 'z', 'q_gt': 1},
                        'out': [
                            ('r2', Anchor('a', 0), Anchor('z', 1), 2),
                            ('r4', Anchor('a', 0), Anchor('z', 1), 4),
                        ]
                    },
                    {
                        'args': {
                            'a_from': 'a',
                            'a_to': 'z',
                            'q_gt': 1,
                            'q_not': True
                        },
                        'out': [
                            ('r0', Anchor('a', 0), Anchor('z', 1), 0),
                        ]
                    },
                    {
                        'args': {
                            'a_from': 'a',
                            'a_to': 'z',
                            'q_lt': 1,
                            'q_not': True
                        },
                        'out': [
                            ('r2', Anchor('a', 0), Anchor('z', 1), 2),
                            ('r4', Anchor('a', 0), Anchor('z', 1), 4),
                        ]
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
                        'args': {'a_from': 'a', 'a_to': 'z', 'q_lt': 4},
                        'out': [
                            ('r0', Anchor('a', 0), Anchor('z', 1), 0),
                            ('r2', Anchor('a', 0), Anchor('z', 1), 2),
                        ]
                    },
                    {
                        'args': {'a_from': 'z', 'a_to': 'a', 'q_lte': 8},
                        'out': [
                            ('r6', Anchor('z', 1), Anchor('a', 0), 6),
                            ('r8', Anchor('z', 1), Anchor('a', 0), 8),
                        ]
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
                        'args': {'name': '*a'},
                        'out': [
                            ('aaaa', Anchor('aaaa',1), Anchor('zwwz',4), 1),
                            ('jwqa', Anchor('jwqa',2), Anchor('aaaa',1), 2)
                        ]
                    },
                    {
                        'args': {'name': '*q*'},
                        'out': [
                            ('jwqa', Anchor('jwqa',2), Anchor('aaaa',1), 2),
                            ('tqqz', Anchor('tqqz',3), Anchor('jwqa',2), 3)
                        ]
                    },
                    {
                        'args': {'name': '?w??'},
                        'out': [
                            ('jwqa', Anchor('jwqa',2), Anchor('aaaa',1), 2),
                            ('zwwz', Anchor('zwwz',4), Anchor('tqqz',3), 4)
                        ]
                    },
                    {
                        'args': {'name': 'z*'},
                        'out': [
                            ('zwwz', Anchor('zwwz',4), Anchor('tqqz',3), 4)
                        ]
                    },
                    {
                        'args': {'a_from': '*a'},
                        'out': [
                            ('aaaa', Anchor('aaaa',1), Anchor('zwwz',4), 1),
                            ('jwqa', Anchor('jwqa',2), Anchor('aaaa',1), 2)
                        ]
                    },
                    {
                        'args': {'a_to': '*z'},
                        'out': [
                            ('aaaa', Anchor('aaaa',1), Anchor('zwwz',4), 1),
                            ('zwwz', Anchor('zwwz',4), Anchor('tqqz',3), 4)
                        ]
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
                        'final': [Anchor('n', None), Anchor('z', None)]
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
                        'final': [Anchor('bzzi', None), Anchor('vzzi', None)]
                    },
                    {
                        'args': {'a': '*z*'},
                        'final': [Anchor('aaaa', None),]
                    },
                    {
                        'args': {'a': '*i'},
                        'final': [Anchor('aaaa', None), Anchor('azzz', None)]
                    },
                    {
                        'args': {'a': '?z?i'},
                        'final': [Anchor('aaaa', None), Anchor('azzz', None)]
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
                        'final': [
                            Anchor('a', -10),
                            Anchor('z', 0),
                            ('r', Anchor('a', -10), Anchor('z', 0), None),
                        ]
                    },
                    {
                        'args': {'a': 'a', 'd': 10},
                        'final': [
                            Anchor('a', 10),
                            Anchor('z', 0),
                            ('r', Anchor('a', 10), Anchor('z', 0), None),
                        ]
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
                            Anchor('a', 0.36),
                            Anchor('g', 1.4),
                            Anchor('m', 2.8),
                            Anchor('s', 0.72),
                            Anchor('z', 0.36),
                            ('r', Anchor('a', 0.36), Anchor('z', 0.36), None)
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
                            Anchor('tfa', 15),
                            Anchor('tfb', 25),
                            Anchor('tra', 40),
                            Anchor('trb', 50),
                            Anchor('ma', 1000),
                            Anchor('k', 0),
                            ('tfa', Anchor('tfa', 15), Anchor('k', 0), None)
                        ]
                    },
                    {
                        'args': {'a': 't*', 'd': 273},
                        'final': [
                            Anchor('tfa', 293),
                            Anchor('tfb', 303),
                            Anchor('tra', 313),
                            Anchor('trb', 323),
                            Anchor('ma', 1000),
                            Anchor('k', 0),
                            ('tfa', Anchor('tfa', 293), Anchor('k', 0), None)
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
                        'final': [Anchor('a', None),]
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
                        'final': [Anchor('a', 1.414),]
                    },
                    {
                        'args': {'a': 'a', 'q': 1},
                        'final': [Anchor('a', 1),]
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
                'init': (('a', 1), ('z', None), ('z', 'a', 'z', 0)),
                'args_outs': (
                    {
                        'args': {'s': 'z', 'q': 2},
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', 2),
                            ('z', Anchor('a', 1), Anchor('z', 2), 0)
                        ]
                    },
                    {
                        'args': {'s': 'z', 'q': -2},
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', -2),
                            ('z', Anchor('a', 1), Anchor('z', -2), 0)
                        ]
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
                            Anchor('vx1', 0.1),
                            Anchor('vx2', 0.1),
                            Anchor('ax1', 1.0),
                            Anchor('ax2', 2.0),
                            Anchor('vg', 0.5),
                            Anchor('r', 0),
                            ('v', Anchor('vg', 0.5), Anchor('r', 0), 0)
                        ]
                    },
                    {
                        'args': {'s': '*', 'q': 0.1, 'q_eq': 0},
                        'final': [
                            Anchor('vx1', 0.1),
                            Anchor('vx2', 0.1),
                            Anchor('ax1', 1.0),
                            Anchor('ax2', 2.0),
                            Anchor('vg', 0.5),
                            Anchor('r', 0.1),
                            ('v', Anchor('vg', 0.5), Anchor('r', 0.1), 0)
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
                            Anchor('vx1', 0),
                            Anchor('vx2', 0),
                            Anchor('ax1', 1.0),
                            Anchor('ax2', 2.0),
                            Anchor('vg', 0),
                            Anchor('r', 0),
                            ('v', Anchor('vg', 0), Anchor('r', 0), None)
                        ]
                    },
                    {
                        'args': {'s': '?x?', 'q': 0},
                        'final': [
                            Anchor('vx1', 0),
                            Anchor('vx2', 0),
                            Anchor('ax1', 0),
                            Anchor('ax2', 0),
                            Anchor('vg', 0.5),
                            Anchor('r', 0),
                            ('v', Anchor('vg', 0.5), Anchor('r', 0), None)
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
                            Anchor('a', 1),
                            Anchor('z', 2),
                            Anchor('Rza0', 88),
                            Anchor('Rza1', 99),
                            ('Rza0', Anchor('z', 2), Anchor('a', 1), 0),
                            ('Rza1', Anchor('z', 2), Anchor('a', 1), 1)
                        ]
                    },
                    {
                        'args': {'a_to': 'a'},
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', 2),
                            Anchor('Rza0', 88),
                            Anchor('Rza1', 99),
                            ('Raz0', Anchor('a', 1), Anchor('z', 2), 0),
                            ('Raz1', Anchor('a', 1), Anchor('z', 2), 1)
                        ]
                    },
                ),
            },
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
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', 2),
                            ('z', Anchor('a', 1), Anchor('z', 2), 10)
                        ]
                    },
                    {
                        'args': {
                            'name': 'z', 'a_from':'a', 'a_to':'z', 'd': -10
                        },
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', 2),
                            ('z', Anchor('a', 1), Anchor('z', 2), -10)
                        ]
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
                            Anchor('qx1', 10),
                            Anchor('qx2', 20),
                            Anchor('ca1', 30),
                            Anchor('cb1', 40),
                            Anchor('cb2', 50),
                            ('Lx0', Anchor('ca1',30), Anchor('qx1',10), 10),
                            ('Lx1', Anchor('ca1',30), Anchor('qx2',20), 20),
                            ('Ln0', Anchor('ca1',30), Anchor('cb1',40), 35),
                            ('Ln1', Anchor('ca1',30), Anchor('cb2',50), 40),
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
                            Anchor('qx1', 10),
                            Anchor('qx2', 20),
                            Anchor('ca1', 30),
                            Anchor('cb1', 40),
                            Anchor('cb2', 50),
                            ('Lx0', Anchor('ca1',30), Anchor('qx1',10), 15),
                            ('Lx1', Anchor('ca1',30), Anchor('qx2',20), 20),
                            ('Ln0', Anchor('ca1',30), Anchor('cb1',40), 30),
                            ('Ln1', Anchor('ca1',30), Anchor('cb2',50), 40),
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
                            Anchor('qx1', 10),
                            Anchor('qx2', 20),
                            Anchor('ca1', 30),
                            Anchor('cb1', 40),
                            Anchor('cb2', 50),
                            ('Lx0', Anchor('ca1',30), Anchor('qx1',10), 15),
                            ('Lx1', Anchor('ca1',30), Anchor('qx2',20), 25),
                            ('Ln0', Anchor('ca1',30), Anchor('cb1',40), 30),
                            ('Ln1', Anchor('ca1',30), Anchor('cb2',50), 40),
                        ]
                    },
                    {
                        'args': {
                            'name': '*', 'a_from': '*1', 'a_to': '*1', 'd': 5
                        },
                        'final': [
                            Anchor('qx1', 10),
                            Anchor('qx2', 20),
                            Anchor('ca1', 30),
                            Anchor('cb1', 40),
                            Anchor('cb2', 50),
                            ('Lx0', Anchor('ca1',30), Anchor('qx1',10), 15),
                            ('Lx1', Anchor('ca1',30), Anchor('qx2',20), 20),
                            ('Ln0', Anchor('ca1',30), Anchor('cb1',40), 35),
                            ('Ln1', Anchor('ca1',30), Anchor('cb2',50), 40),
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
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', 2),
                            ('Raz', Anchor('a', 1), Anchor('z', 2), None)
                        ]
                    },
                    {
                        'args': {
                            'rel': 'Raz', 'a_from':'a', 'a_to':'z', 'q': 12
                        },
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', 2),
                            ('Raz', Anchor('a', 1), Anchor('z', 2), 12)
                        ]
                    },
                    {
                        'args': {
                            'rel': 'Raz', 'a_from':'a', 'a_to':'z', 'q': 1.2
                        },
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', 2),
                            ('Raz', Anchor('a', 1), Anchor('z', 2), 1.2)
                        ]
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
                            Anchor('a', 1),
                            Anchor('z', 2),
                            ('R0', Anchor('a', 1), Anchor('z', 2), None),
                            ('R1', Anchor('z', 2), Anchor('a', 1), None),
                            ('R2', Anchor('a', 1), Anchor('z', 2), 4)
                        ]
                    },
                    {
                        'args': {'rel': 'R3', 'a_from':'z', 'a_to':'a', 'q':8},
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', 2),
                            ('R0', Anchor('a', 1), Anchor('z', 2), None),
                            ('R1', Anchor('z', 2), Anchor('a', 1), None),
                            ('R3', Anchor('z', 2), Anchor('a', 1), 8)
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
                        'final': [Anchor('a', 1), Anchor('z', 2),]
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
                        'final': [Anchor('a', None), Anchor('z', None)],
                    },
                    {
                        'args': {'rel': 'r', 'a_from': '', 'a_to': 'z'},
                        'exception': 'ValueError',
                        'final': [Anchor('a', None), Anchor('z', None)],
                    },
                    {
                        'args': {'rel': 'r', 'a_from': 'a', 'a_to': ''},
                        'exception': 'ValueError',
                        'final': [Anchor('a', None), Anchor('z', None)],
                    },
                    {
                        'args': {'rel': 'r', 'a_from': '', 'a_to': ''},
                        'exception': 'ValueError',
                        'final': [Anchor('a', None), Anchor('z', None)],
                    },
                ),
            },
            'put_rel_duplicate': {
                'meta': {
                    'description': {
                        'en-au': 'put_rel(): duplicate relations',
                    },
                },
                'method': 'put_rel',
                'init': (
                    ('a', 1), ('z', 2), ('r', 'a', 'z', None),
                ),
                'args_outs': (
                    {
                        'args': {'rel':'r', 'a_from':'a', 'a_to':'z'},
                        'exception': 'ValueError',
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', 2),
                            ('r', Anchor('a', 1), Anchor('z', 2), None)
                        ]
                    },
                    {
                        'args': {'rel':'r', 'a_from':'a', 'a_to':'z', 'q': 99},
                        'exception': 'ValueError',
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', 2),
                            ('r', Anchor('a', 1), Anchor('z', 2), None)
                        ]
                    },
                ),
            },
            'put_rel_self_link': {
                'meta': {
                    'description': {
                        'en-au': 'put_rel(): self-linking relations',
                    },
                },
                'method': 'put_rel',
                'init': (
                    ('a', 1),
                ),
                'args_outs': (
                    {
                        'args': {'rel':'r', 'a_from':'a', 'a_to':'a'},
                        'exception': 'ValueError',
                        'final': [
                            Anchor('a', 1),
                        ]
                    },
                    {
                        'args': {'rel':'r', 'a_from':'a', 'a_to':'a', 'q': 99},
                        'exception': 'ValueError',
                        'final': [
                            Anchor('a', 1),
                        ]
                    },
                ),
            }
        }
        self._run_tests(test_data)

    def test_db_set_rel_q(self):
        """set_rel_q: set relation q-value"""
        test_data = {
            'set_rel_q': {
                'meta': {
                    'description': {
                        'en-au': 'set_rel_q() on specific relation',
                    },
                    'comment': ['anchors must be left unchanged'],
                },
                'method': 'set_rel_q',
                'init': (('a', 1), ('z', 2), ('z', 'a', 'z', None)),
                'args_outs': (
                    {
                        'args': {
                            'name': 'z', 'a_from':'a', 'a_to':'z', 'q': 2
                        },
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', 2),
                            ('z', Anchor('a', 1), Anchor('z', 2), 2)
                        ]
                    },
                    {
                        'args': {
                            'name': 'z', 'a_from':'a', 'a_to':'z', 'q': -2
                        },
                        'final': [
                            Anchor('a', 1),
                            Anchor('z', 2),
                            ('z', Anchor('a', 1), Anchor('z', 2), -2)
                        ]
                    },
                ),
            },
            'set_rel_q_wc': {
                'meta': {
                    'description': {
                        'en-au': 'set_rel_q() by wildcard',
                    },
                    'comment': ['anchors must be left unchanged'],
                },
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
                        'args': {
                            'name': 'Lx*', 'a_from': '*', 'a_to': '*', 'q': 100
                        },
                        'final': [
                            Anchor('qx1', 0),
                            Anchor('qx2', 0),
                            Anchor('ca1', 0),
                            Anchor('cb1', 0),
                            Anchor('cb2', 0),
                            ('Lx0', Anchor('ca1',0), Anchor('qx1',0), 100),
                            ('Lx1', Anchor('ca1',0), Anchor('qx2',0), 100),
                            ('Ln0', Anchor('ca1',0), Anchor('cb1',0), 0),
                            ('Ln1', Anchor('ca1',0), Anchor('cb2',0), 0),
                        ]
                    },
                    {
                        'args': {
                            'name': '*', 'a_from': '*1', 'a_to': '*1', 'q': 100
                        },
                        'final': [
                            Anchor('qx1', 0),
                            Anchor('qx2', 0),
                            Anchor('ca1', 0),
                            Anchor('cb1', 0),
                            Anchor('cb2', 0),
                            ('Lx0', Anchor('ca1',0), Anchor('qx1',0), 100),
                            ('Lx1', Anchor('ca1',0), Anchor('qx2',0), 0),
                            ('Ln0', Anchor('ca1',0), Anchor('cb1',0), 100),
                            ('Ln1', Anchor('ca1',0), Anchor('cb2',0), 0),
                        ]
                    },
                ),
            },
        }
        self._run_tests(test_data)

    def test_self_test_run_tests(self):
        test_data = {
            '_run_tests_with_ex': {
                'meta': {
                    'comments': ['delete_rels() cannot be used without args'],
                    'description': {
                        'en-au': 'test _run_test() exceptions',
                    },
                },
                'method': 'delete_rels',
                'init': (('a', 1), ('z', 2)),
                'args_outs': (
                    {
                        'args': {},
                        'exception': 'ValueError',
                        'final': [ Anchor('a', 1), Anchor('z', 2), ]
                    },
                ),
            },
        }
        self._run_tests(test_data)

