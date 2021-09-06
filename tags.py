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

SYMBOLS = {
    "Q": "\u003d",      # Equals sign
    "QUESTION": "\u003f",
    "PERCENT": "\u0025",
    "REL_START": "\u003a",  # Colon
    "REL_REF": "\u2192",    # Arrow right symbol
    "TYPEOF": "\u2208",     # Subset of symbol
    "WILDCARD": "\u002a",   # Asterisk
} # symbols with special meanings cannot be used directly in names of anchors

# NOTE: Escape sequences are used for the sake of precision,
# based on the awareness that multiple forms of the symbols
# in use in different locales and languages.

def uesc_dict(d):
    """
    Return a translation dict which replaces nominated characters
    with their Unicode escape sequences.

    Values of dict d represent nominated characters. Keys from d
    no not affect output.

    """
    out = {}
    for v in d.values():
        out[ord(v)] = r"\u{:04x}".format(ord(v))
    return out

def reltxt(namee, a1e, a2e):
    """
    Return a string representing a relation with name namee between
    anchor a1e and a2e. This function is also used for building
    search terms containing wildcards.

    Note that special symbols are not escaped: malformed relations
    are not suppressed. Please escape these symbols before using
    this function.

    """
    # NOTE: The 'e' suffix in the argument names means that
    # the argument is 'expected to be already escaped'
    return '{}{}{}{}{}'.format(
        namee,
        SYMBOLS['REL_START'],
        a1e,
        SYMBOLS['REL_REF'],
        a2e,
    )

class SQLiteRepo:
    """
    Repository to manage a TAGS database in storage, using SQLite 3

    """
    table_a = "a"
    col = "item"
    col_q = "q"
    limit = 32
    uescs = uesc_dict(SYMBOLS)
    uescs_g = uescs.copy()

    # The uescs_g dict is used for get queries, where percent and
    # question marks are allowed as wildcards
    uescs_g[ord(SYMBOLS['WILDCARD'])] = "%"
    uescs_g.pop(ord(SYMBOLS['QUESTION']))

    def __init__(self, db_path=None, mode="rwc"):
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
        if db_path is None:
            db_path = "test.sqlite3"
            mode = 'memory'
        uri = "file:{}?mode={}".format(db_path, mode)
        self._db_path = db_path
        self._db_conn = sqlite3.connect(uri, uri=True)
        self._db_cus = None
        try:
            self._slr_ck_tables()
        except sqlite3.OperationalError as x:
            if 'no such table' in x.args[0]:
                self._slr_create_tables()

    def _ck_q_isnum(self, **kwargs):
        argnames = ('q', 'd')
        for a in argnames:
            try:
                if type(kwargs[a]) not in (int, float):
                    raise TypeError('argument {a} must be a number')
            except KeyError:
                pass

    def _test_get_symbols(self):
        # TODO: get a sample of all reserved symbols in a string separated
        # by spaces
        # TODO: for unit tests only (idea to put test functions in classes
        # outside of unit tests inspired by YTdownloader)
        out = ""
        for x in SYMBOLS.values():
            out = "".join((out, "{} ".format(x)))
        return out[:-1]

    def _slr_ck_anchors_exist(self, **kwargs):
        """
        Check if anchors exist, before creating relations

        Arguments
        =========
        a1e, a2e: anchors, with all special characters in
        SYMBOLS escaped

        """
        sc_ck = 'SELECT COUNT(*) FROM {0} WHERE {1} = ? OR {1} = ?'.format(
            self.table_a, self.col
        )
        cs = self._slr_get_cursor()
        r = [x for x in cs.execute(sc_ck, (kwargs['a1e'], kwargs['a2e'],))]
        rows = r[0][0]
        if rows != 2:
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
        cs = self._slr_get_cursor()
        cs.execute(sc_ck)

    def _slr_create_tables(self):
        """
        Prepare the anchor table in a new SQLite database file

        """
        sc = 'CREATE TABLE IF NOT EXISTS {}({} UNIQUE NOT NULL, {})'.format(
            self.table_a, self.col, self.col_q
        )
        cs = self._slr_get_cursor()
        cs.execute(sc)

    def _slr_set_q(self, ae, q):
        # TODO: also works with relations, as relations are special anchors
        sc_up = "UPDATE {} SET {} = ? WHERE {} LIKE ?".format(
            self.table_a, self.col_q, self.col
        )
        cus = self._slr_get_cursor()
        cus.execute(sc_up, (q, ae))
        self._db_conn.commit()

    def _slr_incr_q(self, ae, d):
        sc_incr = "UPDATE {0} SET {1} = {1}+? WHERE {2} LIKE ?".format(
            self.table_a, self.col_q, self.col
        )
        cus = self._slr_get_cursor()
        cus.execute(sc_incr, (d, ae))
        self._db_conn.commit()

    def _slr_get_cursor(self):
        if not self._db_cus:
            self._db_cus = self._db_conn.cursor()
        return self._db_cus

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
        cs = self._slr_get_cursor()
        cs.execute(sc, (item, q))
        self._db_conn.commit()

    def get_a(self, a):
        """
        Get an iterator containing anchors matching a. Use '*' as a
        wildcard.

        """
        # TODO: Check applicability of '?' wildcards
        term = a.translate(self.uescs_g)
        sc = """
            SELECT {1}, {2} FROM
            (SELECT {1}, {2} FROM {0} WHERE {1} NOT LIKE '%{3}%')
            WHERE {1} LIKE ?
        """.format(self.table_a, self.col, self.col_q, SYMBOLS['REL_START'])
        cs = self._slr_get_cursor()
        return cs.execute(sc, (term,))

    def put_a(self, a, q=None):
        """
        Put an anchor a with optional numerical quantity value q.

        """
        d = a.translate(self.uescs)
        self._slr_insert_into_a(d, q)

    def set_a_q(self, a, q):
        """
        Assign a numerical quantity q to an anchor a.

        """
        self._ck_q_isnum(q=q)
        ae = a.translate(self.uescs_g)
        self._slr_set_q(ae, q)

    def incr_a_q(self, a, d):
        """
        Increment or decrement a quantity assigned to anchor a by
        d. When d<0, the quantity is decreased.

        If no value has been assigned to the anchor, this method has
        no effect.

        """
        self._ck_q_isnum(d=d)
        ae = a.translate(self.uescs_g)
        self._slr_incr_q(ae, d)

    def delete_a(self, a):
        """
        Delete anchors matching a. Specify the exact name of an
        anchor, or use the asterisk '*' as a wildcard (with caution).

        If the name of the anchor contains an asterisk, use the
        escape sequence \\u002a instead.

        """
        d = a.translate(self.uescs_g)
        sc = """
            DELETE FROM {0} WHERE {1} NOT LIKE '%{2}%' AND {1} LIKE ?
        """.format(self.table_a, self.col, SYMBOLS['REL_START'])
        cs = self._slr_get_cursor()
        cs.execute(sc, (d,))
        self._db_conn.commit()

    def put_rel(self, name, a1, a2, q=None):
        """
        Create a relation between anchors a1 and a2, with an
        optional numerical quantity value q.

        """
        ck_fns = (
            self._slr_ck_rel_self,
            self._slr_ck_anchors_exist,
        )
        a1e = a1.translate(self.uescs)
        a2e = a2.translate(self.uescs)
        namee = name.translate(self.uescs)
        for f in ck_fns:
            f(namee=namee, a1e=a1e, a2e=a2e)
        rtxt = reltxt(namee, a1e, a2e)
        try:
            self._slr_insert_into_a(rtxt, q)
        except sqlite3.IntegrityError as x:
            if 'UNIQUE constraint failed' in x.args[0]:
                raise ValueError('relation already exists')

    def set_rel_q(self, name, a_from, a_to, q):
        """
        Assign a numerical quantity q to a relation between anchors
        a_from and a_to.

        """
        ck_fns = (
            self._slr_ck_anchors_exist,
            self._ck_q_isnum,
        )
        ae_from = a_from.translate(self.uescs_g)
        ae_to = a_to.translate(self.uescs_g)
        namee = name.translate(self.uescs_g)
        for f in ck_fns:
            f(namee=namee, a1e=ae_from, a2e=ae_to, q=q)
        term = reltxt(name, ae_from, ae_to)
        self._slr_set_q(term, q)

    def incr_rel_q(self, name, a_from, a_to, d):
        """
        Increment or decrement a quantity assigned to a relation between
        a_from and a_to by d. When d<0, the quantity is decreased.

        If no value has been assigned to the relation, this method has
        no effect.

        """
        ck_fns = (
            self._slr_ck_anchors_exist,
            self._ck_q_isnum,
        )
        ae_from = a_from.translate(self.uescs_g)
        ae_to = a_to.translate(self.uescs_g)
        namee = name.translate(self.uescs_g)
        for f in ck_fns:
            f(namee=namee, a1e=ae_from, a2e=ae_to, d=d)
        term = reltxt(name, ae_from, ae_to)
        self._slr_incr_q(term, d)

    def delete_rels(self, **kwargs):
        """
        Delete relations by anchor, name or wildcard.

        Examples
        ========
        delete_rels(a_from='apple')
        Delete all relations from the anchor 'apple'

        delete_rels(a_to='blackberry')
        Delete all relations pointing to anchor 'apple'

        delete_rels(a_to='anti*')
        Delete all relations pointing to anchors starting with
        'anti-'

        delete_rels(a_from='apple', a_to='blackberry')
        Delete all relations from the anchor 'apple' to the
        anchor 'blackberry'

        delete_rels(name='mashup*', a_from='apple', a_to='blackberry')
        Delete all relations from the anchor 'apple' to the
        anchor 'blackberry' with names starting with 'mashup-'

        Notes
        =====
        * At least either a_to or a_from must be specified.

        * If any name or anchor contains an asterisk, use the escape
          sequence \\u002a instead.

        * Use wildcards with caution, as with any other delete operation
          in any database system.

        """
        a_to = kwargs.get('a_to')
        a_from = kwargs.get('a_from')
        if a_to is None and a_from is None:
            raise ValueError("at least one of a_to or a_from is required")
        if a_from is None:
            ae_from = '%'
        else:
            ae_from = a_from.translate(self.uescs_g)
        if a_to is None:
            ae_to = '%'
        else:
            ae_to = a_to.translate(self.uescs_g)
        name = kwargs.get('name')
        if name is None:
            namee = '%'
        else:
            namee = name.translate(self.uescs_g)
        sc = 'DELETE FROM {} WHERE {} LIKE ?'.format(self.table_a, self.col)
        cs = self._slr_get_cursor()
        term = reltxt(namee, ae_from, ae_to)
        cs.execute(sc, (term,))
        self._db_conn.commit()

    def get_rels(self, **kwargs):
        """
        Return an iterator containing relations by name, anchor, or
        wildcard.

        Examples
        ========
        get_rels(a_from='apple')
        Return all relations from the anchor 'apple'

        get_rels(a_to='blackberry')
        Return all relations pointing to anchor 'apple'

        get_rels(a_to='anti*')
        Return all relations pointing to anchors starting with
        'anti-'

        get_rels(a_from='apple', a_to='blackberry')
        Return all relations from the anchor 'apple' to the
        anchor 'blackberry'

        get_rels(name='mashup*', a_from='apple', a_to='blackberry')
        Return all relations from the anchor 'apple' to the
        anchor 'blackberry' with names starting with 'mashup-'

        Note
        ====
        * If any name or anchor contains an asterisk, use the escape
          sequence \\u002a instead.

        """
        argnames = ('name', 'a_from', 'a_to')
        for n in argnames:
            if n not in kwargs:
                kwargs[n] = '%'
            else:
                kwargs[n] = kwargs[n].translate(self.uescs_g)
        sc = 'SELECT {1}, {2} FROM {0} WHERE {1} LIKE ?'.format(
            self.table_a,
            self.col,
            self.col_q
        )
        cs = self._slr_get_cursor()
        term = reltxt(kwargs['name'], kwargs['a_from'], kwargs['a_to'])
        return cs.execute(sc, (term,))

