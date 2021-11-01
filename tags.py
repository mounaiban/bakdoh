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

# Reserved symbols used as TAGS wildcards (same as Unix glob)
CHAR_WC_1C = "\u002a" # single char only: Asterisk
CHAR_WC_ZP = "\u003f" # zero or more chars: Question mark

# NOTE: Escape sequences are used for the sake of precision,
# based on the awareness that multiple forms of the symbols
# are in use in different locales and languages.

def uesc_dict(d):
    """
    Return a translation dict which replaces nominated characters
    with HTML Entitites (ampersand-code-semicolon escape sequences).

    Values of dict d represent nominated characters. Keys from d
    no not affect output.

    """
    out = {}
    for v in d.values():
        out[ord(v)] = r"&#{};".format(ord(v))
    return out

class Anchor:
    # Anchor (graph node) class. Includes navigation methods.
    def __init__(self, content, q=None, **kwargs):
        # kwargs accepted: db
        self.db = kwargs.get('db')
        self.content = content
        self.q = q
        do_get_q = (self.db is not None) and (q is None)
        if do_get_q: self.get_q()

    def __eq__(self, other):
        """Anchor comparison: two Anchors are of equal value when
        both content and q are of equal value

        """
        if type(other) != type(self): return False
        return (self.q == other.q) and (self.content == other.content)

    def __repr__(self):
        return "Anchor({},q={})".format(self.content, self.q)

    def get_q(self):
        """Refresh the q value of the anchor from the database
        and return the q value

        """
        self.q = next(self.db.repo.get_a(self.content))[1]
        return self.q

    def rels_out(self, s):
        # get names of relations linked from this anchor matching a pattern
        return self.db.get_rel_names(s, a_from=self.content)

    def rels_in(self, s):
        # get names of relations linked to this anchor matching a pattern
        return self.db.get_rel_names(s, a_to=self.content)

    def related_to(self, rel):
        # Get anchors in relations matching rel linked from this anchor
        return (
            (a[0],a[2],a[3])
            for a in self.db.get_rels(name=rel, a_from=self.content)
        )

    def related_from(self, rel):
        # Get anchors in relations matching rel linked to this anchor
        return (
            (a[0],a[1],a[3])
            for a in self.db.get_rels(name=rel, a_to=self.content)
        )

class DB:
    # Database interface class for loading and storing anchors to a
    # database. Connect a backing store via a repository class
    # (such as SQLiteRepo) to start using databases.
    #
    # This class also serves as a reference interface for repositories
    #
    # TODO: DB case sensitivity is still not finalised at this stage;
    # for now:
    #
    # * get_a() is case-sensitive only when no wildcards are used
    #
    # * get_rels() is case-sensitive only when source and destination
    #   anchors are specified, along with the relation name, all without
    #   wildcards.
    #
    # * both methods are case-insensitive under all other circumstances
    #

    num_args = ('q', 'q_eq', 'q_gt', 'q_gte', 'q_lt', 'q_lte')
    default_out_format = 0x7

    def __init__(self, repo, **kwargs):
        # Prepare repository 'repo' beforehand, then set up a DB
        # like: DB(repo)
        #
        # Please see the documentation for the repository class
        # for details on how to create or load databases
        self.repo = repo

    def _ck_args_isnum(self, ck_args=None, **kwargs):
        if ck_args is None: ck_args = self.num_args
        for a in kwargs:
            if a in ck_args:
                if type(kwargs[a]) not in (int, float):
                    raise TypeError('argument {} must be a number'.format(a))

    def _ck_strargs_not_empty(self, ck_args=('a', 'a_from', 'a_to'), **kwargs):
        for a in kwargs:
            if a in ck_args:
                if type(a) is not str:
                    raise TypeError('argument {} must be string').format(a)
                elif not kwargs[a]:
                    raise ValueError(
                        'argument {} cannot be empty string'.format(a)
                    )

    def delete_a(self, a, **kwargs):
        """
        Delete anchors matching 'a'.

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
        anchs = self.get_a(a, out_format=fmt_i)
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
            return Anchor(rout[0], rout[1], db=self)

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
        self._ck_strargs_not_empty(a=a)
        self.repo.put_a(a, q)

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
        self._ck_strargs_not_empty(
            ck_args=('rel', 'a_from', 'a_to'),
            rel = rel,
            a_from = a_from,
            a_to = a_to
        )
        self.repo.put_rel(rel, a_from, a_to, q=q)

    def set_a_q(self, s, q, **kwargs):
        """
        Assign a numerical quantity q to an anchor 's'.

        """
        self._ck_args_isnum(q=q, **kwargs)
        self.repo.set_a_q(s, q, **kwargs)

    def set_rel_q(self, name, a_from, a_to, q, **kwargs):
        """
        Assign a numerical quantity q to a relation between anchors
        a_from and a_to.

        """
        self._ck_args_isnum(q=q, **kwargs)
        self.repo.set_rel_q(name, a_from, a_to, q, **kwargs)

class SQLiteRepo:
    """
    Repository to manage a TAGS database in storage, using SQLite 3

    """
    CHAR_WC_ZP_SQL = "\u005f" # Question Mark
    CHAR_WC_1C_SQL = "\u0025" # Percent Sign
    chars_wc_sql = (CHAR_WC_ZP_SQL, CHAR_WC_1C_SQL)
    CHARS_DB_DEFAULT = {
        'CHAR_F_REL_SQL': "\u21e8", # relation marker (Arrow to the right)
        'CHAR_PX_AL_SQL': "\u0040", # alias marker (At-sign)
        'CHAR_PX_T_SQL': "\u220a",  # type marker (Small element-of symbol)
    }
    table_a = "a"
    table_config = "config"
    col = "content"
    col_q = "q"
    col_config_key = "key"
    col_config_value = "v"
    limit = 32
    escape = '\\'
    trans_wc = {
        CHAR_WC_1C: CHAR_WC_1C_SQL,
        CHAR_WC_ZP: CHAR_WC_ZP_SQL,
        CHAR_WC_1C_SQL: "{}{}".format(escape, CHAR_WC_1C_SQL),
        CHAR_WC_ZP_SQL: "{}{}".format(escape, CHAR_WC_ZP_SQL)
    }

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
            db_path = "test.sqlite3"
            mode = 'memory'
        uri = "file:{}?mode={}".format(db_path, mode)
        self._char_rel = None
        self._chars_px = ""
        self._chars_f_escape = {}
        self._db_path = db_path
        self._db_conn = sqlite3.connect(uri, uri=True)
        self._db_cus = None
        self._config = None
        self._test_chars = {"PX": "", "F": "",}
            # standard test char patterns:
            # PX => special prefix chars, F => special characters
        self.trans_wc = str.maketrans(self.trans_wc)
        try:
            self._slr_ck_tables()
        except sqlite3.OperationalError as x:
            if 'no such table' in x.args[0]:
                self._slr_create_tables()
                self._slr_dict_to_config(self.CHARS_DB_DEFAULT)
        self._config = self._slr_config_to_dict()
        for k in self._config:
            if k.startswith('CHAR_F'):
                c = self._config[k]
                self._chars_f_escape[ord(c)] = "&#{};".format(ord(c))
                self._test_chars['F'] = c
            if k.startswith('CHAR_PX'):
                self._chars_px = "".join((self._chars_px, self._config[k]))
                self._test_chars['PX'] = self._config[k]
            self._char_rel = self._config['CHAR_F_REL_SQL']
            self._char_alias = self._config['CHAR_PX_AL_SQL']

    def _prep_term(self, term):
        """
        Return an str of a ready-to-use form of a search term.
        TAGS wildcards are converted to SQL wildcards, HTML Entities
        used as escape codes are decoded, local aliases are converted
        to integer ROWIDs.

        """
        if term.startswith(self._config['CHAR_PX_AL_SQL']):
            return int(term[1:])
        else: return unescape(term.translate(self.trans_wc))

    def _prep_a(self, a):
        """
        Prepare an anchor for insertion into database, converting
        reserved characters into escape codes as necessary
        """
        out = ""
        p = a[0]
        if p in self._chars_px:
            out = "".join((r"&#{};".format(ord(p)), a[1:]))
        else:
            out = a
        return out.translate(self._chars_f_escape)

    def _slr_ck_anchors_exist(self, **kwargs):
        """
        Check if anchors exist, before creating relations

        Arguments
        =========
        a1e, a2e: anchors, with all special characters escaped
        or anchor aliases, like @n, where n is the anchor's ROWID
        in the SQLite backend.

        """
        anchors = (kwargs['a1e'], kwargs['a2e'])
        sc_ck_alias = "SELECT COUNT(*) FROM {} WHERE ROWID = ?".format(
            self.table_a
        )
        sc_ck = "SELECT COUNT(*) FROM {} WHERE {} = ?".format(
            self.table_a, self.col
        )
        for a in anchors:
            term = None
            if a.startswith(self._char_rel):
                sc = sc_ck_alias
                term = int(a[1:])
            else:
                sc = sc_ck
                term = a
            cs = self._slr_get_shared_cursor()
            r = next(cs.execute(sc, (term,)))
            if r[0] <= 0:
                raise ValueError('both anchors must exist')

    def _slr_ck_rel_self(self, **kwargs):
        """
        Prevent self-linking of anchors, by checking if both anchors
        nominated for put_rel() are the same.

        """
        if kwargs['a1e'] == kwargs['a2e']:
            raise ValueError('self-linking relations not allowed')

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
        """.format(self.table_a, self.col, self.col_q)
        cs = self._slr_get_shared_cursor()
        cs.execute(sc_ck)

    def _slr_create_tables(self):
        """
        Prepare the anchor table in a new SQLite database file

        """
        sc_table_a = """
            CREATE TABLE IF NOT EXISTS {}({} UNIQUE NOT NULL, {})
            """.format(self.table_a, self.col, self.col_q)
        sc_table_c = """
            CREATE TABLE IF NOT EXISTS {}({} UNIQUE NOT NULL, {} NOT NULL)
            """.format(
                self.table_config,
                self.col_config_key,
                self.col_config_value
            )
        cs = self._slr_get_shared_cursor()
        cs.execute(sc_table_a)
        cs.execute(sc_table_c)

    def _slr_set_q(self, ae, q, is_rel=False, **kwargs):
        # PROTIP: also works with relations; relations are special anchors
        suggest_wc = True in map(lambda x: x in ae, self.chars_wc_sql)
        sc_set = "UPDATE {} SET {} = ? ".format(self.table_a, self.col_q)
        sc = "".join((sc_set, self._slr_a_where_clause(
            is_rel=is_rel, wildcards=kwargs.get('wildcards', suggest_wc)
        )))
        params = [q, ae]
        if kwargs:
            sc_q, qparams = self._slr_q_clause(**kwargs)
            sc = "".join((sc, sc_q))
            params.extend(qparams)
        cus = self._slr_get_shared_cursor()
        cus.execute(sc, params)
        self._db_conn.commit()

    def _slr_incr_q(self, ae, d, is_rel=False, **kwargs):
        sc_incr = "UPDATE {0} SET {1}={1}+? ".format(self.table_a, self.col_q)
        sc = "".join((sc_incr, self._slr_a_where_clause(is_rel=is_rel)))
        params = [d, ae]
        if kwargs:
            sc_q, qparams = self._slr_q_clause(**kwargs)
            sc = "".join((sc, sc_q))
            params.extend(qparams)
        cus = self._slr_get_shared_cursor()
        cus.execute(sc, params)
        self._db_conn.commit()

    def _slr_get_a(self, ae, **kwargs):
        """Helper method used by get_a() and get_rels() to fetch anchors
        and relations from the SQLite backing store.

        Please see DB.get_rels() for usage

        Supported kwargs: cursor, is_rel, q_eq, (q_gt or q_gte),
        (q_lt or q_lte), q_not

        """
        suggest_wc = True in map(lambda x: x in ae, self.chars_wc_sql)
        params = [ae,]
        sc_select = "SELECT {}, {} FROM {} ".format(
            self.col, self.col_q, self.table_a,
        )
        sc_where = self._slr_a_where_clause(
            is_rel=kwargs.get('is_rel', False),
            is_alias=kwargs.get('is_alias', False),
            wildcards=kwargs.get('wildcards', suggest_wc)
        )
        sc = "".join((sc_select, sc_where))
        if kwargs:
            sc_q = self._slr_q_clause(**kwargs)
            sc = "".join((sc, sc_q[0]))
            params.extend(sc_q[1])
        # NOTE: the number of parameters required by the
        # statement can vary from one to three, depending
        # on the arguments in use.
        cs = kwargs.get('cursor', self._db_conn.cursor())
        return ((unescape(c), q) for c, q in cs.execute(sc, params))

    def _slr_get_shared_cursor(self):
        if not self._db_cus:
            self._db_cus = self._db_conn.cursor()
        return self._db_cus

    def _slr_get_rowids(self, a, **kwargs):
        """Returns SQLite ROWIDs for anchors matching a"""
        sc_rowid = "SELECT ROWID, {} from {} ".format(self.col, self.table_a)
        sc = "".join((sc_rowid, self._slr_a_where_clause(),))
        term = self._prep_term(a)
        cs = kwargs.get('cursor', self._db_conn.cursor())
        return cs.execute(sc, (term,))

    def _slr_config_to_dict(self):
        """Reads database config table into dict"""
        sc = "SELECT {},{} FROM {}".format(
            self.col_config_key, self.col_config_value, self.table_config
        )
        cs = self._db_conn.cursor()
        rows = cs.execute(sc)
        out = {}
        for r in rows: out[r[0]] = r[1]
        return out

    def _slr_dict_to_config(self, confdict):
        """Writes a dict to the database config table"""
        sc = "INSERT INTO {} VALUES(?,?)".format(self.table_config)
        cs = self._slr_get_shared_cursor()
        for k in confdict:
            cs.execute(sc, (k, confdict[k]))

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
        sc = 'INSERT INTO {} VALUES(?, ?)'.format(self.table_a)
        cs = self._slr_get_shared_cursor()
        cs.execute(sc, (item, q))
        self._db_conn.commit()

    def _slr_a_where_clause(self, is_rel=False, is_alias=False, wildcards=True):
        """
        Returns an SQL WHERE clause for SELECT, DELETE and UPDATE
        operations on anchors and relations.

        When is_rel is True, the clause includes relations.

        When is_alias is True, the clause selects anchors by SQLite
        ROWID

        """
        # TODO: Should is_rel be called with_rels?

        out = "WHERE "
        if is_alias:
            out = "".join((out, "ROWID = ? "))
        else:
            if wildcards:
                out = "".join((out, "{} LIKE ? ESCAPE '{}' ".format(
                    self.col, self.escape)))
            else:
                out = "".join((out, "{} = ?".format(self.col)))
        if not is_rel:
            out = "".join((out, "AND {} NOT LIKE '%{}%' ".format(
                        self.col, self._char_rel)))
        return out

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
        clause = ""
        lbe = None  # lower bound or exact value
        lbe_expr = ""
        params = []
        ub = None  # upper bound
        ub_expr = ""
        if 'q_eq' in kwargs:
            # exact
            lbe = kwargs['q_eq']
            lbe_expr = "{} = ?".format(self.col_q)
            params.append(lbe)
        else:
            # lower bound
            if 'q_gt' in kwargs:
                lbe = kwargs['q_gt']
                lbe_expr = "{} > ?".format(self.col_q)
                params.append(lbe)
            elif 'q_gte' in kwargs:
                lbe = kwargs['q_gte']
                lbe_expr = "{} >= ?".format(self.col_q)
                params.append(lbe)
            # upper bound
            if 'q_lt' in kwargs:
                ub = kwargs['q_lt']
                ub_expr = "{} < ?".format(self.col_q)
                params.append(ub)
            elif 'q_lte' in kwargs:
                ub = kwargs['q_lte']
                ub_expr = "{} <= ?".format(self.col_q)
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

    def reltxt(self, namee, a1e, a2e, alias=None, alias_fmt=3):
        """
        Return a string representing a relation with name namee from
        anchor a1e to a2e. This function is also used for building
        search terms containing wildcards.

        Note that special symbols are not escaped: malformed relations
        are not suppressed. Please escape these symbols before using
        this function.

        Keyword Arguments
        =================
        * alias - when set to 'local', local aliased relations are
                  created instead. On SQLite repositories, anchors
                  are referenced by a short code like @n, where n is
                  the anchor's SQLite ROWID.

        * alias_fmt - set to the following int values to create
                      partial or fully aliased relations:

                      1: use alias for a2e only

                      2: use alias for a1e only

                      3: use alias for both anchors

                      Results for undocumented values are
                      undocumented.

        Note
        ====
        Aliased relations are space-efficient, and are practically
        required for longer anchors, but not searchable.

        """
        # NOTE: The 'e' suffix in the argument names means that
        # the argument is 'expected to be already escaped'
        #
        # TODO: remove support for aliased relations, and swap with
        # verbose<==>aliased converter?
        #
        af = a1e
        at = a2e
        if alias == 'local':
            try:
                rid1 = next(self._slr_get_rowids(a1e))[0]
                rid2 = next(self._slr_get_rowids(a2e))[0]
            except StopIteration:
                # TODO: need a clearer error message
                raise ValueError('both anchors must have ROWIDs')
            if rid1 and rid2:
                if alias_fmt & 1:
                    at = "{}{}".format(self._char_alias, rid2)
                if alias_fmt & 2:
                    af = "{}{}".format(self._char_alias, rid1)
        return '{}{}{}{}{}'.format(
            namee, self._char_rel, af, self._char_rel, at
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

          Aliases are an experimental feature which may be removed in
          future releases.

        """
        is_alias = a.startswith(self._char_rel)
        return self._slr_get_a(
            self._prep_term(a), is_alias=is_alias, is_rel=False, **kwargs
        )

    def put_a(self, a, q=None):
        """Handle DB request to put an anchor into the SQLite
        backing store

        """
        self._slr_insert_into_a(self._prep_a(a), q)

    def set_a_q(self, a, q, **kwargs):
        """Handle DB request to assign a numerical quantity to an
        anchor. Called from DB.set_a_q()

        """
        self._slr_set_q(self._prep_term(a), q, **kwargs)

    def incr_a_q(self, a, d, **kwargs):
        """Handle DB request to increment/decrement a numerical
        quantity of an anchor. Called from DB.incr_a_q()

        """
        self._slr_incr_q(self._prep_term(a), d, **kwargs)

    def delete_a(self, a):
        """Handle DB request to delete anchors. Accepts the same arguments
        as DB.delete_a(). Please see the documentation of that method
        for usage.

        """
        # TODO: Allow delete by quantity or quantity range?

        sc_delete = "DELETE FROM {} ".format(self.table_a)
        sc = "".join((sc_delete, self._slr_a_where_clause()))
        cs = self._slr_get_shared_cursor()
        cs.execute(sc, (self._prep_term(a),))
        self._db_conn.commit()

    def put_rel(self, name, a1, a2, q=None, **kwargs):
        """Handle DB request to create anchors. Accepts the same arguments
        as DB.put_rel(). Please see the documentation of that method
        for usage.

        SQLite Repository-Specific Features
        ===================================
        * Arguments: alias, alias_fmt, prep_a

        * The anchor preparation process may be bypassed by setting
          the 'prep_a' argument to False.

        """
        ck_fns = (
            self._slr_ck_rel_self,
            self._slr_ck_anchors_exist,
        )
        a1e = None
        a2e = None
        namee = None
        if kwargs.get('prep_a', True):
            a1e = self._prep_a(a1)
            a2e = self._prep_a(a2)
            namee = self._prep_a(name)
        else:
            a1e = a1
            a2e = a2
            namee = name
        for f in ck_fns:
            f(namee=namee, a1e=a1e, a2e=a2e)
        rtxt = self.reltxt(namee, a1e, a2e, alias=kwargs.get('alias'))
        try:
            self._slr_insert_into_a(rtxt, q)
        except sqlite3.IntegrityError as x:
            if 'UNIQUE constraint failed' in x.args[0]:
                raise ValueError('relation already exists')

    def set_rel_q(self, name, a_from, a_to, q, **kwargs):
        """Handle DB request to set the numerical quantity assigned
        to the relationship named 'name' from anchor 'a_from' to
        'a_to'. Called from DB.set_rel_q(). Please see the documentation
        for that method for usage.

        """
        ae_from = self._prep_term(a_from)
        ae_to = self._prep_term(a_to)
        namee = self._prep_term(name)
        term = self.reltxt(namee, ae_from, ae_to)
        self._slr_set_q(term, q, is_rel=True, **kwargs)

    def incr_rel_q(self, name, a_from, a_to, d, **kwargs):
        """Handle DB request to increment/decrement the numerical
        quantity assigned to the relationship named 'name' from anchor
        'a_from' to 'a_to'. Called from DB.incr_rel_q() please see the
        documentation for that method for usage.

        """
        ae_from = self._prep_term(a_from)
        ae_to = self._prep_term(a_to)
        namee = self._prep_term(name)
        term = self.reltxt(namee, ae_from, ae_to)
        self._slr_incr_q(term, d, is_rel=True, **kwargs)

    def delete_rels(self, **kwargs):
        """Handle DB request to delete relations. Accepts the same arguments
        as DB.delete_rels(). Please see the documentation of that method
        for usage.

        """
        # TODO: Allow delete by quantity or quantity range?

        a_to = kwargs.get('a_to')
        a_from = kwargs.get('a_from')
        if a_to is None and a_from is None:
            raise ValueError("at least one of a_to or a_from is required")
        if a_from is None:
            ae_from = '%'
        else:
            ae_from = self._prep_term(a_from)
        if a_to is None:
            ae_to = '%'
        else:
            ae_to = self._prep_term(a_to)
        name = kwargs.get('name')
        if name is None:
            namee = '%'
        else:
            namee = self._prep_term(name)
        sc_delete = "DELETE FROM {} ".format(self.table_a)
        sc = "".join((sc_delete, self._slr_a_where_clause(is_rel=True)))
        cs = self._slr_get_shared_cursor()
        term = self.reltxt(namee, ae_from, ae_to)
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
        sc_relnames = """
                SELECT DISTINCT substr({0}, 0, instr({0}, '{1}')) FROM {2}
            """.format(self.col, self._char_rel, self.table_a)
        sc_where = self._slr_a_where_clause(is_rel=True)
        sc = "".join((sc_relnames, sc_where))
        term = self._prep_term(self.reltxt(
            s,
            kwargs.get('a_from', CHAR_WC_ZP),
            kwargs.get('a_to', CHAR_WC_ZP)
        ))
        cs = kwargs.get('cursor', self._slr_get_shared_cursor())
        return cs.execute(sc, (term,))

    def get_rels(self, **kwargs):
        """Handle DB request to return an iterator of relations.
        Accepts the same arguments as DB.get_rels(). Please see the
        documentation of that method for usage.

        """
        # TODO: SQLiteRepo may not handle case sensitivity correctly
        # with relations, due to the way the LIKE statement works.
        # Need to investigate that.

        nameargs = ('name', 'a_from', 'a_to')
        for n in nameargs:
            if n not in kwargs:
                kwargs[n] = '%'
            else:
                kwargs[n] = self._prep_term(kwargs[n])
        term = self.reltxt(kwargs['name'], kwargs['a_from'], kwargs['a_to'])
        rels = self._slr_get_a(term, is_rel=True, **kwargs)
        return (r[0].split(self._char_rel)+[r[1],] for r in rels)

