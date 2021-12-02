"""
Bakdoh Totally Approachable Graph System (TAGS) Module

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

import sqlite3
from json import JSONEncoder
from html import unescape
from itertools import chain
from warnings import warn

# Reserved symbols used as TAGS wildcards (same as Unix glob)
CHAR_WC_1C = "\u003f" # single char only: Question Mark
CHAR_WC_ZP = "\u002a" # zero or more chars: Asterisk

# NOTE: Escape sequences are used for the sake of precision,
# based on the awareness that multiple forms of the symbols
# are in use in different locales and languages.

def escape(c):
    """Convert a character to an escape sequence according to spec"""
    return "&#{};".format(ord(c)) # currently: decimal-coded HTML entities

def escape_dict(chars, maketrans=False):
    """
    Create a translation dict from the string of characters "chars"
    which replaces instances of said characters with decimal HTML
    Entitites (ampersand-code-semicolon escape sequences).

    When ``maketrans`` is set to True, the returned dict is formatted
    for use with str.translate(). Such dict's may not be readable to
    untrained humans.

    """
    if maketrans: f = ord
    else: f = lambda x:x
    return dict(zip((f(v) for v in chars), (escape(v) for v in chars)))

class Anchor:
    # Anchor (graph node) class. Includes navigation methods.
    def __init__(self, content, q=None, **kwargs):
        # kwargs accepted: db, init_sync
        db = kwargs.get('db')
        if db is not None and type(db) is not DB:
            raise TypeError("cannot use non-TAGS database as db")
        self.db = db
        self.content = content
        self.q = q
        init_sync = kwargs.get('init_sync', True)
        if self.db and init_sync: self.reload()

    def __eq__(self, other):
        """Anchor comparison: two Anchors are of equal value when
        both content and q are of equal value

        """
        if type(other) != type(self): return False
        return (self.q == other.q) and (self.content == other.content)

    def __repr__(self):
        return "{}('{}',q={})".format(
            self.__class__.__name__, self.content, self.q
        )

    def _ck_db_writable(self):
        if not self.db.allow_put_self:
            raise RuntimeError('db allow_put_self not set')
        if not self.db.repo.writable:
            raise RuntimeError('db is not writable')

    def reload(self):
        """Refresh the q value of the anchor from the database
        and return the q value

        """
        if self.db:
            try:
                it = self.db.get_a(
                    self.content, out_format=0x3, wildcards=False
                )
                self.q = next(it)[1]
                return self.q
            except StopIteration:
                # if Anchor is not found
                pass

    def link(self, name, a_to, q):
        # Create a relation from this anchor.
        self._ck_db_writable()
        self.db.put_rel(name, self.content, a_to, q)

    def unlink(self, name, a_to):
        # Remove relations from this anchor. Wildcards are supported
        self._ck_db_writable()
        self.db.delete_rels(name=name, a_from=self.content, a_to=a_to)

    def put_self(self):
        """Writes the Anchor to the connected database, if present.

        If the Anchor is not in the DB, create the anchor. Else,
        update the q-value instead.

        """
        self._ck_db_writable()
        try:
            detect = next(self.db.repo.get_a(self.content, wildcards=False))
            if not detect:
                self.db.put_a(self.content, self.q)
            else:
                if self.q is None: return
                self.db.set_a_q(self.content, self.q, wildcards=False)
        except StopIteration:
            # Anchor not found in db
            self.db.put_a(self.content, self.q)

    def rels_out(self, s=CHAR_WC_ZP):
        # get names of relations linked from this anchor matching a pattern
        return self.db.get_rel_names(s, a_from=self.content)

    def rels_in(self, s=CHAR_WC_ZP):
        # get names of relations linked to this anchor matching a pattern
        return self.db.get_rel_names(s, a_to=self.content)

    def related_to(self, rel=CHAR_WC_ZP):
        # Get anchors in relations matching rel linked from this anchor
        return (
            (a[0],a[2],a[3])
            for a in self.db.get_rels(name=rel, a_from=self.content)
        )

    def related_from(self, rel=CHAR_WC_ZP):
        # Get anchors in relations matching rel linked to this anchor
        return (
            (a[0],a[1],a[3])
            for a in self.db.get_rels(name=rel, a_to=self.content)
        )

class DB:
    """
    Interface class to access TAGS databases.

    This class provides access to a graph database via an abstract
    interface, allowing multiple databases of different implementations
    and backing stores to be used together without any need for
    awareness of implementation details for the most features.

    In order to access a database, wrap this class around a Repository
    class which will access the backing store which loads and saves
    database content to storage.

    This class may also be used as a reference for implementing
    repositories.

    Notes
    =====
    Case sensitivity (for text scripts with case) is still not
    finalised at this stage.

    For now, operations are case-sensitive only when relation names,
    source and destination are fully specified without wildcards.

    """
    num_args = ('q', 'q_eq', 'q_gt', 'q_gte', 'q_lt', 'q_lte')
    default_out_format = 0x7

    def __init__(self, repo, **kwargs):
        """
        To use, prepare a repository 'repo' beforehand, then wrap
        a DB around it, for example:

        d = DB(SQLiteRepo('example.tags.sqlite3'))

        alternatively:

        r = SQLiteRepo('example.tags.sqlite3')
        d = DB(r)

        SQLiteRepository is a TAGS built-in class which loads and
        stores TAGS databases in SQLite 3 database files.

        Keyword Arguments
        =================
        * allow_put_self : When set to True, this allows linked Anchors to
          automatically request themselves to be saved to the database
          if they are not found in the database, or when the q-value
          is updated.

        """
        self.repo = repo
        self.allow_put_self = kwargs.get('allow_put_self', False)

    def __repr__(self):
        return "{}(repo={})".format(self.__class__.__name__, self.repo)

    def _ck_args_isnum(self, ck_args=None, **kwargs):
        if ck_args is None: ck_args = self.num_args
        for a in kwargs:
            if a in ck_args:
                if type(kwargs[a]) not in (int, float):
                    raise TypeError('argument {} must be a number'.format(a))

    def _ck_args_str_not_empty(self, ck_args=('a', 'a_from', 'a_to'), **kwargs):
        for a in kwargs:
            if a in ck_args:
                if type(a) is not str:
                    raise TypeError('argument {} must be string').format(a)
                elif not kwargs[a]:
                    raise ValueError(
                        'argument {} cannot be empty string'.format(a)
                    )

    def count_a(self, a='*', **kwargs):
        """
        Count anchors matching ``a``.

        Accepts the same arguments and wildcard syntax as get_rels()
        """
        return self.repo.count_a(a, **kwargs)

    def delete_a(self, a, **kwargs):
        """
        Delete anchors matching ``a``.

        Accepts the same arguments and wildcard syntax as get_rels(),
        please see the documentation for that method for details.

        Accepted arguments: s, q_eq, (q_gt or q_gte), (q_lt or q_lte).
        See get_a() for details on how to use the q-arguments.

        Wildcards
        =========
        See get_a(); this method uses the same wildcard syntax.

        If any name or anchor contains an asterisk or question mark,
        use the HTML entities '&ast;' and '&quest;' instead.

        Examples
        ========
        * delete_a('apple') : delete the anchor 'apple'

        * delete_a('app*') : delete all anchors starting with 'app'

        * delete_a('ch????') : delete all six-character anchors
          starting with 'ch'

        Notes
        =====
        * If any name or anchor contains an asterisk, or question mark,
          use the escape sequence &ast; and &quest; instead.

        * Use wildcards with caution, as with any other delete operation
          in any database system.

        """
        self.repo.delete_a(a)

    def delete_rels(self, **kwargs):
        """
        Delete relations by anchor, name or wildcard.

        Accepts the same arguments and wildcard syntax as get_rels(),
        please see the documentation for that method for details.

        Accepted arguments: a_from, a_to, name, q_eq, (q_gt or q_gte),
        (q_lt or q_lte)

        At least either a_to or a_from must be specified.

        Wildcards
        =========
        See get_a(); this method uses the same wildcard syntax.

        If any name or anchor contains an asterisk or question mark,
        use the HTML entities '&ast;' and '&quest;' instead.

        Examples
        ========
        * delete_rels(a_from='apple') : delete all relations from the
          anchor 'apple'

        * delete_rels(a_to='berry') : delete all relations to anchor 'berry'

        * delete_rels(a_to='app*') :  delete all relations pointing to
          anchors starting with 'app'

        * delete_rels(a_from='apple', a_to='berry') : delete all relations
          from the anchor 'apple' to the anchor 'berry'

        * delete_rels(name='rel*', a_from='apple', a_to='blackberry')
          Delete all relations from the anchor 'apple' to the
          'berry' with names starting with 'rel'

        Notes
        =====
        * If any name or anchor contains an asterisk, or question mark,
          use the escape sequence &ast; and &quest; instead.

        * Use wildcards with caution, as with any other delete operation
          in any database system.

        """
        self.repo.delete_rels(**kwargs)

    def exists_rels(self, name='*', a_from='*', a_to='*', **kwargs):
        """
        Return True if there is an existing relation with name matching
        ``name`` from anchor ``a_from`` to anchor ``a_to``.

        Accepts the same arguments and wildcard syntax as get_rels(),
        please see the documentation for that method for details.
        """
        return self.repo.exists_rels(name, a_from, a_to, **kwargs)

    def export(self, a='*', relname='*', **kwargs):
        """
        Return an iterator containing anchors by content or wildcard,
        and associated relations in the interchange format.

        See import_data() for a brief description of the format.

        Set the "out_format" argument to 'json' to write a JSON
        string ready to be exported to a file or any other stream.

        """
        # TODO: enable selective export by q-values
        fmt = kwargs.get('out_format', 'interchange')
        fmt_i = 'interchange'
        anchs = self.get_a(a, out_format=fmt_i, length=None)
        rels_a = self.get_rels(name=relname, a_from=a, out_format=fmt_i)
        rels_b = ()
        if a != '*':
            rels_b = self.get_rels(name=relname, a_to=a, out_format=fmt_i)
        if fmt == 'json':
            temp = {
                'a': list(anchs),
                'rels': list(chain(rels_a, rels_b))
            }
            je = JSONEncoder()
            return je.encode(temp)
        elif fmt == 'interchange':
            return chain(anchs, rels_a, rels_b)
        else:
            raise ValueError('output format "{}" unsupported'.format(fmt))

    def get_a(self, a, **kwargs):
        """
        Return an iterator containing anchors by content or wildcard.

        Arguments
        =========
        * a: anchor content or wildcard

        * out_format: sets the output format; accepted values are
          the integers 1, 3, 7 or the string "interchange":

          1: a, return anchor content only

          3: (a, q), return content and q-value in tuple

          7: Anchor(a,q), return content and q-value as Anchor object

          "interchange": same as 3 for anchors, use for export/import

          The default format is 7.

        * q_eq : return only relations with a q-value equals to 'q_eq'

        * q_gt : return only relations with a q-value greater than 'q_gt';
         cannot be used with 'q_eq' or 'q_gte'

        * q_gte : return only relations with a q-value greater than or equal
          to 'q_gte'; cannot be used with 'q_eq' or 'q_gt'

        * q_lt : return only relations with a q-value smaller than 'q_lt';
         cannot be used with 'q_eq' or 'q_lte'

        * q_lte : return only relations with a q-value smaller than or
          equal to 'q_lte'; cannot be used with 'q_eq' or 'q_lt'

        Wildcards
        =========
        The following Unix glob-like wildcards are accepted:

        * asterisk (*) : zero or more characters

        * question mark (?) : any one character

        If any name or anchor contains an asterisk or question mark,
        use the HTML entities '&ast;' and '&quest;' instead.

        Examples
        ========
        * get_a('apple') : get the anchor 'apple'

        * get_a('be*') : get all anchors starting with 'be'

        * get_a('ch????') : get all anchors with six characters starting
          with 'ch'

        """
        def format_bare(rout):
            return (rout[0], rout[1])

        def format_obj(rout):
            return Anchor(rout[0], rout[1], db=self, init_sync=False)

        def format_conly(rout):
            # content only
            return rout[0]

        fmt_fns = {0x1: format_conly, 0x3: format_bare, 0x7: format_obj}
        fmt = kwargs.get('out_format', self.default_out_format)
        if fmt == 'interchange': fmt = 0x3
        f = fmt_fns[fmt]
        return (f(r) for r in self.repo.get_a(a, **kwargs))

    def get_rel_names(self, s, **kwargs):
        # Return an iterator of relation names in use between
        # two anchors, a_from and a_to, matching s
        return (r[0] for r in self.repo.get_rel_names(s, **kwargs))

    def get_rels(self, **kwargs):
        """
        Return an iterator containing relations by name, anchor, or
        wildcard. Each relation is presented as a tuple of four
        elements:

        (relation_name, from_anchor, to_anchor, quantity)

        Arguments
        =========
        * a_from : return only relations from anchors matching 'a_from'

        * a_to : return only relations towards anchors matching 'a_from'

        * name : return only relations matching 'name'

        * out_format: sets the output format; accepted values are
          the integers 1, 3, 7 or the string "interchange":

          1: (name, a_from, a_to, q) return anchor content only

          3: (name, (a_from, a_from_q), (a_to, a_to_q), q)
             return anchor content and q-value

          7: (name, Anchor(a,q), Anchor(a,q), q)
             return anchors as Anchor objects

          "interchange": same as 1 for relations, use for export/import

          The default format is 7.

        * q_eq : return only relations with a q-value equals to 'q_eq'

        * q_gt : return only relations with a q-value greater than 'q_gt';
          cannot be used with 'q_eq' or 'q_gte'

        * q_gte : return only relations with a q-value greater than or equal
          to 'q_gte'; cannot be used with 'q_eq' or 'q_gt'

        * q_lt : return only relations with a q-value smaller than 'q_lt';
          cannot be used with 'q_eq' or 'q_lte'

        * q_lte : return only relations with a q-value smaller than or
          equal to 'q_lte'; cannot be used with 'q_eq' or 'q_lt'

        Wildcards
        =========
        See get_a(); this method uses the same wildcard syntax.

        If any name or anchor contains an asterisk or question mark,
        use the HTML entities '&ast;' and '&quest;' instead.

        Examples
        ========
        * get_rels(a_from='apple') : relations from the anchor 'apple'

        * get_rels(a_to='berry') : relations to anchor 'berry'

        * get_rels(a_to='ap*') : relations to anchors starting with 'ap'

        * get_rels(a_to='ch????') : relations pointing to anchors with
          six characters and starting with 'ch'

        * get_rels(a_from='apple', a_to='berry') : relations from anchor
          'apple' to anchor 'berry'

        * get_rels(name='mashup*', a_from='apple', a_to='berry)
          relations from the anchor 'apple' to the anchor 'berry'
          with names starting with 'mashup'

        """

        fmt = kwargs.get('out_format', self.default_out_format)
        if fmt == 'interchange': fmt=0x1
        return (
            (
                n,
                next(self.get_a(f, out_format=fmt, wildcards=False)),
                next(self.get_a(t, out_format=fmt, wildcards=False)),
                q
            )
            for n, f, t, q in self.repo.get_rels(**kwargs)
        )

    def get_special_chars(self):
        """Returns a dict of characters that have some special meaning
        to the database or repository. The characters are sorted by
        types, addressable by the following keys:

        * "E" (escape): characters used in escape sequences in wildcards,
          names or content

        * "F" (forbidden): delimiter characters that cannot be stored
          anywhere in anchors or relation names

        * "PX" (prefix): characters used for accessing special or virtual
          anchors; these cannot be used as the first character of any
          anchor or relation name, and must be escaped when querying

        * "WC" (wildcard): characters used as wildcards, including
          '*', '?' and all internal wildcards.

        The dict is mainly used for unit testing.

        """
        return self.repo.special_chars

    def get_stats(self):
        return {
            'preface_length': self.repo.preface_length,
            'db_path': self.repo.db_path,
            'writable': self.repo.writable,
        }

    def import_data(self, data):
        """
        Imports anchors and relations into the database from a
        tuple or list-based specification in the interchange format
        as follows:

        * (anchor_content, anchor_q) for Anchors

        * (rel_name, anchor_from, anchor_to, rel_q) for Relations
          Both anchor_from and anchor_to must exist in the database
          at time of insertion.

        All inputs must be wrapped in a single list or tuple, specified
        as the "data" argument.

        Example: (('a1', 0), ('a2', 9001), ('r', 'a1', 'a2', None))

        The examples above are shown as tuples, but lists may be
        used instead.

        Returns a report of unsuccessful imports as a dict. The report
        is under the "not_imported" key, which contains a list of
        2-tuples like: (input, error).

        This method is so-called because "import" is a reserved keyword.

        """
        report = {
            'not_imported': [],
        }
        for d in data:
            err_un = TypeError('unsupported format')
            try:
                if type(d) not in (tuple, list):
                    report['not_imported'].append((d, err_un))
                elif len(d) == 1:
                    self.put_a(d[0], None)
                elif len(d) == 2:
                    self.put_a(d[0], d[1])
                elif len(d) == 4:
                    self.put_rel(d[0], d[1], d[2], d[3])
                else:
                    report['not_imported'].append((d, err_un))
            except Exception as ex:
                report['not_imported'].append((d, ex))
        return report

    def incr_a_q(self, a, d, **kwargs):
        """
        Increment or decrement a quantity assigned to anchor 'a'
        by d. When d<0, the quantity is decreased.

        If no value has been assigned to the anchor, this method
        has no effect.

        """
        self._ck_args_isnum(d=d)
        self.repo.incr_a_q(a, d, **kwargs)

    def incr_rel_q(self, name, a_from, a_to, d, **kwargs):
        """
        Increment or decrement a quantity assigned to a relation between
        a_from and a_to by d. When d<0, the quantity is decreased.

        If no value has been assigned to the relation, this method has
        no effect.

        """
        self._ck_args_isnum(d=d, **kwargs)
        self.repo.incr_rel_q(name, a_from, a_to, d, **kwargs)

    def put_a(self, a, q=None):
        """
        Create an anchor containing str 'a' and an optional
        numerical quantity value 'q'.

        Anchors of the same content can only be inserted once
        per database.

        """
        self._ck_args_str_not_empty(a=a)
        return self.repo.put_a(a, q)

    def put_rel(self, rel, a_from, a_to, q=None):
        """
        Create a relation between anchors 'a_from and 'a_to', with
        an optional numerical quantity value 'q'.

        Only one relation for each combination of name, source anchor
        and destination anchor may be present in the database at
        any given time.

        """
        # TODO: returning information about the anchor/relation
        # from invoking put_rel() or put_a() may be helpful
        if a_from == a_to:
            raise ValueError('cannot link {} to itself'.format(a_from))
        self._ck_args_str_not_empty(
            ck_args=('rel', 'a_from', 'a_to'),
            rel = rel,
            a_from = a_from,
            a_to = a_to
        )
        return self.repo.put_rel(rel, a_from, a_to, q=q)

    def set_a_q(self, s, q, **kwargs):
        """
        Assign a numerical quantity q to an anchor 's'.

        """
        if q is not None:
            self._ck_args_isnum(q=q, **kwargs)
        self.repo.set_a_q(s, q, **kwargs)

    def set_rel_q(self, name, a_from, a_to, q, **kwargs):
        """
        Assign a numerical quantity q to a relation between anchors
        a_from and a_to.

        """
        if q is not None:
            self._ck_args_isnum(q=q, **kwargs)
        self.repo.set_rel_q(name, a_from, a_to, q, **kwargs)

class SQLiteRepo:
    """
    Repository to manage a TAGS database in storage, using SQLite 3

    """
    CHARS_DB_DEFAULT = {
        'CHAR_F_REL_SQL': "\u21e8", # relation marker (Arrow to the right)
        'CHAR_PX_AL_SQL': "\u0040", # alias marker (At-sign)
        'CHAR_PX_T_SQL': "\u220a",  # type marker (Small element-of symbol)
    }
    CHAR_ESCAPE = '\\'
    CHAR_WC_1C_SQL = "\u005f" # Underscore
    CHAR_WC_ZP_SQL = "\u0025" # Percent Sign
    CHARS_WC = ""  # populated at runtime, see Setup below
    COL_CONFIG_KEY = "key"
    COL_CONFIG_VALUE = "v"
    COL_CONTENT = "content"
    COL_Q = "q"
    LIMITS_DEFAULT = {
        'PREFACE_LENGTH': 128,
        'MAX_RESULTS': 32,
    }
    TABLE_A = "a"
    TABLE_CONFIG = "config"
    TRANS_WC = {
        CHAR_WC_1C: CHAR_WC_1C_SQL,
        CHAR_WC_ZP: CHAR_WC_ZP_SQL,
        CHAR_WC_1C_SQL: "{}{}".format(CHAR_ESCAPE, CHAR_WC_1C_SQL),
        CHAR_WC_ZP_SQL: "{}{}".format(CHAR_ESCAPE, CHAR_WC_ZP_SQL)
    }
    # Setup
    for c in TRANS_WC.keys():
        CHARS_WC = "".join((CHARS_WC, c))
    TRANS_WC = str.maketrans(TRANS_WC)

    def __init__(self, db_path=None, mode="rwc", **kwargs):
        """
        Examples
        ========
        repo = SQLiteRepository() => new in-memory DB
        repo = SQLiteRepository('intel.sqlite3') => use intel.sqlite3 file
        repo = SQLiteRepository('intel.sqlite3', mode='ro') => use intel.sqlite3
            file in read-only mode.

        Arguments
        =========
        * db_path: Filesystem path to the SQLite database file. If no
          path is specified, an in-memory database is created.

        * mode: SQLite database access mode. Use "rwc" to create
          databases, "rw" for read-write access and "ro" for
          read-only access. For details, see Part 3.3 in 'Uniform
          Resource Identifiers' from the SQLite documentation
          <https://sqlite.org/uri.html>

        Notes
        =====
        * db_path is used as-is; there is no preprocessing to escape
          reserved characters and clean up backslashes. For details,
          see Part 3.1 in 'Uniform Resource Identifiers' from the SQLite
          documentation <https://sqlite.org/uri.html>.

        * All in-memory databases are read-write

        """
        # TODO: Allow user to set database-local special characters
        if db_path is None:
            db_path = ":memory:"
            mode = 'memory'
        self.special_chars = {
            "E": self.CHAR_ESCAPE, "F": "", "PX": "", "WC": self.CHARS_WC
        }
        self.db_path = db_path
        self.preface_length: int
        self.uri = "file:{}?mode={}".format(db_path, mode)
        self.writable = (mode == 'memory') or ('w' in mode)
        self._char_al: str[1]
        self._char_rel: str[1]
        self._chars_px = ""
        self._db_conn = sqlite3.connect(self.uri, uri=True)
        self._db_cus = self._db_conn.cursor()
        self._max_results: int
        self._trans_f = {}
        self._trans_px = {}
        self._subclause_preface: str
        # Setup: detect and create SQLite tables
        try:
            self._slr_ck_tables()
        except sqlite3.OperationalError as x:
            if 'no such table' in x.args[0]:
                limits_temp = self.LIMITS_DEFAULT.copy()
                limits_temp['PREFACE_LENGTH'] = kwargs.pop(
                    'preface_length', self.LIMITS_DEFAULT['PREFACE_LENGTH']
                )
                self._slr_create_tables()
                self._slr_dict_to_config(self.CHARS_DB_DEFAULT)
                self._slr_dict_to_config(limits_temp)
        # Setup: set config from SQLite file
        config_chars = self._slr_config_to_dict('CHAR_%')
        config_limits = self._slr_config_to_dict('MAX_%')
        for k in config_chars:
            if k.startswith('CHAR_F'):
                c = config_chars[k]
                self._trans_f[ord(c)] = escape(c)
                self.special_chars['F'] = "".join((c, self.special_chars['F']))
            if k.startswith('CHAR_PX'):
                c = config_chars[k]
                self._chars_px = "".join((self._chars_px, c))
                self._trans_px[ord(c)] = escape(c)
                self.special_chars['PX'] = self._chars_px
        self.preface_length = self._slr_config_to_dict('PRE%')['PREFACE_LENGTH']
        if 'preface_length' in kwargs:
            warn(
                'preface_length set to {} by DB'.format(self.preface_length),
                RuntimeWarning
            )
        self._char_alias = config_chars['CHAR_PX_AL_SQL']
        self._char_rel = config_chars['CHAR_F_REL_SQL']
        self._max_results = config_limits['MAX_RESULTS']
        self._subclause_preface = "substr({}, 1, {})".format(
            self.COL_CONTENT, self.preface_length
        )

    def __repr__(self):
        return "{}({}, uri={})".format(
            self.__class__.__name__, self.db_path, self.uri
        )

    def _index_prefix(self, s, px_list):
        """
        Return the index of the end of the prefix in string 's', if
        it begins with any prefix in the list 'px_list'.

        The int '0' (zero) will be returned instead if no prefix is
        found.

        """
        for p in px_list:
            if s.startswith(p): return (len(p))
        return 0

    def _prep_a(self, a, **kwargs):
        """
        Prepare text for insertion into the DB, or for lookup
        (SQL SELECT) queries.

        ROWID aliases (like '@9001') are converted into integers.

        Arguments
        =========
        * wildcards : when True, converts TAGS wildcards to
          SQL wildcards.

        """
        # NeXt-generation Version
        if not kwargs.get('wildcards', True):
            out = "".join((a[0].translate(self._trans_px), a[1:]))
            return out.translate(self._trans_f)
        else:
            if a.startswith(self._char_alias):
                alias = a[1:]
                if alias.isdigit(): return int(alias)
                else: return a
            else:
                i = self._index_prefix(a, self._trans_px.values())
                out = a.translate(self.TRANS_WC)
                out = "".join((out[:i], unescape(out[i:])))
                return out.translate(self._trans_f)

    def _prep_a_rel(self, a, **kwargs):
        # prep_a for relations
        if a.startswith(self._char_alias):
            cont = next(self._get_a_by_alias(self._prep_a(a)))[0]
            if self._char_rel in cont:
                ex = ValueError(
                    'relations between relations not yet supported'
                )
                raise(ex)
            else: return cont
        else:
            return self._prep_a(a, **kwargs)

    def _slr_ck_anchors_exist(self, anchors):
        """
        Check if anchors exist. Can also be used for relations, given
        relations are herein specially-formed anchors.

        Returns a 2-tuple like (result, anchor) where:

        * result : True if all anchors exist

        * anchor : either None if all anchors exist, or the first
          anchor that was found not to exist.

        Arguments
        =========
        Any keyword argument with a key starting with 'a' is regarded
        as an anchor. All anchors will be checked.

        """
        for a in anchors:
            if self.count_a(a[:self.preface_length], wildcards=False) <= 0:
                return (False, a)
        return (True, None)

    def _slr_ck_tables(self):
        """
        Check if the anchor table has been created in an SQLite
        database file.

        Raises sqlite3.OperationalError if the anchor table has not been
        created, and/or cannot be created.

        """
        sc_ck = """
            SELECT COUNT(*) FROM {0}
            WHERE {1} LIKE '%' AND {2} LIKE '%'
            LIMIT 1
        """.format(self.TABLE_A, self.COL_CONTENT, self.COL_Q)
        cs = self._slr_get_shared_cursor()
        cs.execute(sc_ck)

    def _slr_create_tables(self):
        """
        Prepare the anchor table in a new SQLite database file

        """
        sc_table_a = """
            CREATE TABLE IF NOT EXISTS {}({} UNIQUE NOT NULL, {})
            """.format(self.TABLE_A, self.COL_CONTENT, self.COL_Q)
        sc_table_c = """
            CREATE TABLE IF NOT EXISTS {}({} UNIQUE NOT NULL, {} NOT NULL)
            """.format(
                self.TABLE_CONFIG,
                self.COL_CONFIG_KEY,
                self.COL_CONFIG_VALUE
            )
        cs = self._slr_get_shared_cursor()
        cs.execute(sc_table_a)
        cs.execute(sc_table_c)
        self._db_conn.commit()

    def _slr_get_shared_cursor(self):
        if not self._db_cus:
            self._db_cus = self._db_conn.cursor()
        return self._db_cus

    def _slr_get_rowids(self, a, **kwargs):
        """Returns SQLite ROWIDs for anchors matching a"""
        prologue = "SELECT ROWID, {} from {} ".format(
            self.COL_CONTENT, self.TABLE_A
        )
        wildcards = kwargs.pop('wildcards', self._has_wildcards(a))
        term = self._prep_a(a, wildcards=wildcards)
        sc, _ = self._slr_sql_script(
            prologue=prologue,
            preface=True,
            with_rels=False,
            wildcards=wildcards
        )
        cs = kwargs.get('cursor', self._db_conn.cursor())
        return cs.execute(sc, (term,))

    def _slr_get_last_insert_rowid(self):
        sc_rowid = "SELECT last_insert_rowid()"""
        cs = self._db_conn.cursor()
        return next(cs.execute(sc_rowid))[0]

    def _slr_config_to_dict(self, term='%'):
        """Reads database config table into dict"""
        sc = "SELECT {0},{1} FROM {2} WHERE {0} LIKE ?".format(
            self.COL_CONFIG_KEY, self.COL_CONFIG_VALUE, self.TABLE_CONFIG
        )
        cs = self._db_conn.cursor()
        rows = cs.execute(sc, (term,))
        out = {}
        for r in rows: out[r[0]] = r[1]
        return out

    def _slr_dict_to_config(self, confdict):
        """Writes a dict to the database config table"""
        sc = "INSERT INTO {} VALUES(?,?)".format(self.TABLE_CONFIG)
        cs = self._slr_get_shared_cursor()
        for k in confdict:
            cs.execute(sc, (k, confdict[k]))
        self._db_conn.commit()

    def _slr_insert_into_a(self, item, q):
        """
        Inserts an item into the anchor table. This method is intended
        to be called by put_a() and put_rel().

        Raises sqlite3.IntegrityError when an attempt is made to put
        duplicate anchors or relations.

        """
        if q is not None:
            if type(q) not in (int, float):
                raise TypeError('q must be a number')
        sc = 'INSERT INTO {} VALUES(?, ?)'.format(self.TABLE_A)
        cs = self._slr_get_shared_cursor()
        cs.execute(sc, (item, q))
        self._db_conn.commit()
        return {'_sql_rowid': self._slr_get_last_insert_rowid()}

    def _slr_sql_script(
                self, prologue, preface, with_rels, wildcards, **kwargs
            ):
        # prologue is the first part of the SQL script,
        # e.g.: SELECT count(*) FROM a, # UPDATE a SET content = ?, ...
        #
        # param order of finish script is like:
        # [start,] [length,] search_term, *qparams...
        #
        # TODO: switch to named style in generated scripts
        #
        sc = "{} WHERE ".format(prologue, self.TABLE_A)
        target: str
        if preface and not wildcards:
            target = self._subclause_preface
        else:
            target = self.COL_CONTENT
        if not with_rels:
            sc = "".join((sc, "{} NOT LIKE '%{}%' AND ".format(
                            target, self._char_rel
                        )))
        if wildcards:
            sc = "".join((sc, "{} LIKE ? ESCAPE '{}' ".format(
                                target, self.CHAR_ESCAPE
                            )))
        else:
            sc = "".join((sc, "{} = ? ".format(target)))
        qparams = ()
        if kwargs:
            qc, qparams = self._slr_q_clause(**kwargs)
            sc = "".join((sc, qc))
        qlimit = ()
        if 'limit' in kwargs:
            qlimit = (kwargs.get("limit", 0),)
            sc = "".join((sc, "LIMIT ?"))
        return (sc, [x for x in chain(qparams, qlimit)])

    def _slr_q_clause(self, **kwargs):
        """
        Returns an tuple like (q_clause, params) where

        * q_clause is a string for searching anchors by
          q values for use with SQL WHERE clauses,

        * params is a list of real numbers, in the correct
          order, to be used with the clause.

        Arguments
        =========
        * q_eq: Specify exact quantity. Cannot be used with any
          other argument.

        * q_gt, q_gte: Specify lower bound (greater than x);
          q_gt=x becomes q > x; q_gte=x becomes q >= x.
          Both arguments cannot be used together or with q.

        * q_lt, q_lte: Specify upper bound (less than x);
          q_lt=x becomes q < x; q_lte becomes q <= x.
          Both arguments cannot be used together or with q.

        * q_not: when set to True, prepare a clause for
          selecting anchors NOT within the range specified.

        Some arguments will be ignored if mutually exclusive
        arguments are used together. The order of precedence
        is as follows: q, (q_gt or q_lt), (q_gte or q_lte)

        Note
        ====
        If the lower bound is higher than the upper bound,
        a range exclusion expression will be returned.

        e.g. the clause returned when q_gt=4, q_lt=3 is
        " q > ? OR q < ?", equivalent to
        " AND NOT (q > ? AND q < ?)"

        """
        andor = ""
        clause = " "
        lbe = None  # lower bound or exact value
        lbe_expr = ""
        params = []
        ub = None  # upper bound
        ub_expr = ""
        if 'q_eq' in kwargs:
            # exact
            lbe = kwargs['q_eq']
            lbe_expr = "{} = ?".format(self.COL_Q)
            params.append(lbe)
        else:
            # lower bound
            if 'q_gt' in kwargs:
                lbe = kwargs['q_gt']
                lbe_expr = "{} > ?".format(self.COL_Q)
                params.append(lbe)
            elif 'q_gte' in kwargs:
                lbe = kwargs['q_gte']
                lbe_expr = "{} >= ?".format(self.COL_Q)
                params.append(lbe)
            # upper bound
            if 'q_lt' in kwargs:
                ub = kwargs['q_lt']
                ub_expr = "{} < ?".format(self.COL_Q)
                params.append(ub)
            elif 'q_lte' in kwargs:
                ub = kwargs['q_lte']
                ub_expr = "{} <= ?".format(self.COL_Q)
                params.append(ub)
            if lbe is not None and ub is not None:
                if lbe < ub:
                    andor = " AND "
                else:
                    andor = " OR "
        if lbe is not None or ub is not None:
            clause = "{}{}{}".format(lbe_expr, andor, ub_expr)
            if kwargs.get('q_not', False):
                return (" AND NOT ({})".format(clause), params)
            else:
                return ("".join((" AND ", clause)), params)
        else:
            return (clause, params)

    def _get_a_by_alias(self, alias, **kwargs):
        """Return an Anchor's preface associated with an alias

        Aliases are 'virtual anchors' that reference other anchors
        or relations, and are prefixed by an at "@" symbol (U+0040).

        The SQLite Repository supports integer and alphanumeric
        aliases.

        Integer aliases like "@9001" return anchors or relations
        based on their assigned SQL ROWID.

        Aliases are an experimental feature which may be removed or
        replaced by another incompatible feature.

        """
        if type(alias) is int:
            sc_lu = """
                SELECT substr({}, ?, ?), {} FROM {} WHERE ROWID = ?
                """.format(self.COL_CONTENT, self.COL_Q, self.TABLE_A)
            cs = kwargs.get('cursor', self._db_conn.cursor())
            return cs.execute(sc_lu, (1, self.preface_length, alias))
        else:
            raise(NotImplementedError('alphanumeric aliases not supported'))

    def _has_wildcards(self, a):
        return True in map(lambda x: x in a, self.CHARS_WC)

    def _reltext(self, name='*', a_from='*', a_to='*', **kwargs):
        """
        Return a string representing a relation from ``a_from`` to
        ``a_to``, with name ``name``. This method is used for building
        search terms (which may contain wildcards) for looking up
        relations too.

        """
        # NeXt-generation Version
        # supported kwargs: wildcards
        return '{}{}{}{}{}'.format(
            self._prep_a(name, **kwargs),
            self._char_rel,
            self._prep_a_rel(a_from, **kwargs),
            self._char_rel,
            self._prep_a_rel(a_to, **kwargs),
        )

    def get_a(self, a, **kwargs):
        """Handle DB request to return an iterator of anchors.
        Accepts the same arguments as DB.get_a() with some differences;
        please see the documentation for the method for details.

        SQLite Repository-Specific Features
        ===================================
        * Anchors can be accessed with a local alias, by using a search
          term starting with the at-sign '@', followed by the anchor's ROWID.
          For example, if the anchor 'durian' has a ROWID of 7, then '@7'
          returns 'durian'.

        """
        # TODO: supported kwargs: start, length, wildcards, preface
        start = kwargs.get('start', 1)
        length = kwargs.get('length', self.preface_length)
        params_length: tuple
        prologue: str
        term: str
        wildcards = kwargs.pop('wildcards', self._has_wildcards(a))
        if a.startswith(self._char_alias):
            return self._get_a_by_alias(self._prep_a(a))
        else:
            term = self._prep_a(a, wildcards=wildcards)
        if length is None:
            params_length = (start,)
            prologue = "SELECT substr({}, ?), {} FROM {} ".format(
                self.COL_CONTENT, self.COL_Q, self.TABLE_A
            )
        else:
            params_length = (start, length)
            prologue = "SELECT substr({}, ?, ?), {} FROM {} ".format(
                self.COL_CONTENT, self.COL_Q, self.TABLE_A
            )
        sc, params_q = self._slr_sql_script(
            prologue=prologue,
            preface=len(term) <= self.preface_length,
            with_rels=False,
            wildcards=wildcards,
            **kwargs
        )
        params = [x for x in chain(params_length, (term,), params_q)]
        cs = kwargs.get('cursor', self._db_conn.cursor())
        return ((unescape(c), q) for c, q in cs.execute(sc, params))

    def put_a(self, a, q=None):
        """Handle DB request to put an anchor into the SQLite
        backing store

        """
        ck = self._slr_ck_anchors_exist((a,))
        if ck[0]:
            apre = a[:self.preface_length]
            raise ValueError('anchor starting with {} exists'.format(apre))
        return self._slr_insert_into_a(self._prep_a(a, wildcards=False), q)

    def set_a_q(self, a, q, **kwargs):
        """Handle DB request to assign a numerical quantity to an
        anchor. Called from DB.set_a_q()

        """
        wildcards = kwargs.pop('wildcards', self._has_wildcards(a))
        term = self._prep_a(a, wildcards=wildcards)
        prologue = "UPDATE {} SET {} = ? ".format(self.TABLE_A, self.COL_Q)
        params = [q, term]
        sc, params_q = self._slr_sql_script(
            prologue=prologue,
            preface=len(term) <= self.preface_length,
            with_rels=False,
            wildcards=wildcards,
            **kwargs
        )
        cs = self._slr_get_shared_cursor()
        params.extend(params_q)
        cs.execute(sc, params)

    def incr_a_q(self, a, d, **kwargs):
        """Handle DB request to increment/decrement a numerical
        quantity of an anchor. Called from DB.incr_a_q()

        """
        wildcards = kwargs.pop('wildcards', self._has_wildcards(a))
        term = self._prep_a(a, wildcards=wildcards)
        prologue = "UPDATE {0} SET {1} = {1}+? ".format(self.TABLE_A, self.COL_Q)
        params = [d, term]
        sc, params_q = self._slr_sql_script(
            prologue=prologue,
            preface=len(term) <= self.preface_length,
            with_rels=False,
            wildcards=wildcards,
            **kwargs
        )
        cs = self._slr_get_shared_cursor()
        params.extend(params_q)
        cs.execute(sc, params)

    def exists_rels(self, name='*', a_from='*', a_to='*', **kwargs):
        """Check if one or more relations exist. Wildcards are accepted.
        """
        # This rather grammatically incorrect name was chosen as
        # to stay in line with the naming style of the other methods
        # in this class.
        #
        term = self._reltext(name, a_from, a_to)
        # TODO: find a more elegant way to prevent incorrect length
        # and preface settings from reaching _slr_sql_script()
        kwargs['length'] = None
        prologue = "SELECT NULL FROM {} ".format(self.TABLE_A)
        sc, params_q = self._slr_sql_script(
            prologue=prologue,
            preface=False,
            with_rels=True,
            wildcards=kwargs.pop('wildcards', self._has_wildcards(term)),
            limit=1,
            **kwargs
        )
        params = [x for x in chain((term,), params_q)]
        cs = kwargs.get('cursor', self._db_conn.cursor())
        try:
            return next(cs.execute(sc, params))[0] is None
        except StopIteration:
            return False

    def count_a(self, a='*', **kwargs):
        """Count the number of Anchors matching ``a``"""
        wildcards = kwargs.pop('wildcards', self._has_wildcards(a))
        term = self._prep_a(a, wildcards=wildcards)
        params = [term,]
        prologue = "SELECT count(*) FROM {} ".format(self.TABLE_A)
        sc, params_q = self._slr_sql_script(
            prologue=prologue,
            with_rels=False,
            wildcards=wildcards,
            preface=(len(a) <= self.preface_length) and not wildcards,
            **kwargs
        )
        params.extend(params_q)
        cs = kwargs.get('cursor', self._db_conn.cursor())
        return next(cs.execute(sc, params))[0]

    def delete_a(self, a, **kwargs):
        """Handle DB request to delete anchors. Accepts the same arguments
        as DB.delete_a(). Please see the documentation of that method
        for usage.

        """
        # TODO: Allow delete by quantity or quantity range?
        if a == CHAR_WC_ZP:
            raise ValueError("cowardly refusing to delete all anchors")
        wildcards = kwargs.pop('wildcards', self._has_wildcards(a))
        term = self._prep_a(a, wildcards=wildcards)
        prologue = "DELETE FROM {} ".format(self.TABLE_A)
        sc = self._slr_sql_script(
            prologue=prologue,
            preface=len(a) <= self.preface_length and not wildcards,
            with_rels=False,
            wildcards=wildcards,
        )[0]
        cs = self._slr_get_shared_cursor()
        cs.execute(sc, (term,))
        self._db_conn.commit()

    def put_rel(self, name, a1, a2, q=None, **kwargs):
        """Handle DB request to create anchors. Accepts the same arguments
        as DB.put_rel(). Please see the documentation of that method
        for usage.

        SQLite Repository-Specific Features
        ===================================
        * Arguments: alias, alias_format, prep_a

        * The anchor preparation process may be bypassed by setting
          the 'prep_a' argument to False.

        """
        ck = self._slr_ck_anchors_exist((a1, a2))
        if not ck[0]:
            raise ValueError('anchor {} not found'.format(ck[1]))
        rtxt = self._reltext(name, a1, a2, wildcards=False)
        try:
            return self._slr_insert_into_a(rtxt, q)
        except sqlite3.IntegrityError as x:
            if 'UNIQUE constraint failed' in x.args[0]:
                raise ValueError('relation already exists')

    def set_rel_q(self, name, a_from, a_to, q, **kwargs):
        """Handle DB request to set the numerical quantity assigned
        to the relationship named 'name' from anchor 'a_from' to
        'a_to'. Called from DB.set_rel_q(). Please see the documentation
        for that method for usage.

        """
        term = self._reltext(name, a_from, a_to, **kwargs)
        prologue = "UPDATE {} SET {} = ? ".format(self.TABLE_A, self.COL_Q)
        params = [q, term]
        sc, params_q = self._slr_sql_script(
            prologue=prologue,
            preface=False,
            with_rels=True,
            wildcards=kwargs.pop('wildcards', self._has_wildcards(term)),
            **kwargs
        )
        cs = self._slr_get_shared_cursor()
        params.extend(params_q)
        cs.execute(sc, params)

    def incr_rel_q(self, name, a_from, a_to, d, **kwargs):
        """Handle DB request to increment/decrement the numerical
        quantity assigned to the relationship named 'name' from anchor
        'a_from' to 'a_to'. Called from DB.incr_rel_q() please see the
        documentation for that method for usage.

        """
        term = self._reltext(name, a_from, a_to, **kwargs)
        prologue = "UPDATE {0} SET {1}={1}+? ".format(self.TABLE_A, self.COL_Q)
        params = [d, term]
        sc, params_q = self._slr_sql_script(
            prologue=prologue,
            preface=False,
            with_rels=True,
            wildcards=kwargs.pop('wildcards', self._has_wildcards(term)),
            **kwargs
        )
        cs = self._slr_get_shared_cursor()
        params.extend(params_q)
        cs.execute(sc, params)

    def delete_rels(self, **kwargs):
        """Handle DB request to delete relations. Accepts the same arguments
        as DB.delete_rels(). Please see the documentation of that method
        for usage.

        """
        # TODO: Allow delete by quantity or quantity range?
        a_from = kwargs.get('a_from', CHAR_WC_ZP)
        a_to = kwargs.get('a_to', CHAR_WC_ZP)
        name = kwargs.get('name', CHAR_WC_ZP)
        if a_to == CHAR_WC_ZP and a_from == CHAR_WC_ZP:
            raise ValueError("at least one of a_to or a_from must not be '*'")
        term = self._reltext(name, a_from, a_to)
        wildcards = kwargs.get('wildcards', self._has_wildcards(term))
        prologue = "DELETE FROM {} ".format(self.TABLE_A)
        sc = self._slr_sql_script(prologue, False, True, wildcards, **kwargs)[0]
        cs = self._db_conn.cursor()
        cs.execute(sc, (term,))
        self._db_conn.commit()

    def get_rel_names(self, s, **kwargs):
        """Handle DB request to return an iterator of names of relations
        in use from anchor 'a_from' to anchor 'a_to'. Accepts the same
        arguments as DB.get_rel_names. Please see the documentation
        of that method for usage.

        SQLite Repository-Specific Features
        ===================================
        * cursor : specify a SQLite cursor for performing the lookup;
          using a particular cursor is only really needed in exceptional
          circumstances.

        """
        prologue = """
            SELECT DISTINCT substr({0}, 0, instr({0}, '{1}')) FROM {2}
            """.format(self.COL_CONTENT, self._char_rel, self.TABLE_A)
        term = self._reltext(
            s, kwargs.get('a_from', CHAR_WC_ZP), kwargs.get('a_to', CHAR_WC_ZP)
        )
        sc = self._slr_sql_script(
            prologue,
            preface=False,
            with_rels=True,
            wildcards=self._has_wildcards(term),
            **kwargs
        )[0]
        cs = kwargs.get('cursor', self._slr_get_shared_cursor())
        return cs.execute(sc, (term,))

    def get_rels(self, **kwargs):
        """Handle DB request to return an iterator of relations.
        Accepts the same arguments as DB.get_rels(). Please see the
        documentation of that method for usage.

        """
        term = self._reltext(
            kwargs.get('name', '*'),
            kwargs.get('a_from', '*'),
            kwargs.get('a_to', '*')
        )
        # TODO: find a more elegant way to prevent incorrect length
        # and preface settings from reaching _slr_sql_script()
        kwargs['length'] = None
        prologue = "SELECT {}, {} FROM {}".format(
            self.COL_CONTENT, self.COL_Q, self.TABLE_A
        )
        sc, params_q = self._slr_sql_script(
            prologue=prologue,
            preface=False,
            with_rels=True,
            wildcards=self._has_wildcards(term),
            **kwargs
        )
        params = [x for x in chain((term,), params_q)]
        cs = kwargs.get('cursor', self._db_conn.cursor())
        return (
            r[0].split(self._char_rel)+[r[1],] for r in cs.execute(sc, params)
        )

