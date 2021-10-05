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
from html import unescape

# Reserved symbols that cannot be used in anchors
CHAR_REL = "\u21e8"     # Arrow Right
CHARS_R = {"REL_START": CHAR_REL}

# Reserved symbols that cannot be used as the first character
# of anchors
CHARS_R_PX = {
    "A_ID": "\u0040",   # At-Sign
    "TYPEOF": "\u220a", # Small Element-of Symbol
}
chars_r_px_list = [s for s in CHARS_R_PX.values()]

# Reserved symbols used as wildcards in queries
CHARS_R_WC = {
    "WILDCARD_ONE": "\u003f",   # Question mark
    "WILDCARD_ZEROPLUS": "\u002a",  # Asterisk
}

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


class SQLiteRepo:
    """
    Repository to manage a TAGS database in storage, using SQLite 3

    """
    CHARS_R_WC_SLR = {
        "WILDCARD_ONE": "\u005f",   # Question Mark
        "WILDCARD_ZEROPLUS": "\u0025", # Percent Sign
    }
    num_q_args = ('q', 'q_gt', 'q_gte', 'q_lt', 'q_lte')
    table_a = "a"
    trans_wc = {}  # see wildcard dict preparation code below
    escape_a = uesc_dict(CHARS_R)
    escape_px = uesc_dict(CHARS_R_PX)
    col = "item"
    col_q = "q"
    limit = 32
    escape = '\\'
    _test_sample = {
        "R": ["{0}X{0}".format(x) for x in CHARS_R.values()],
        "R_PX": ["{0}X{0}".format(x) for x in CHARS_R_PX.values()],
        "WC": ["{0}X{0}".format(x) for x in CHARS_R_WC.values()],
        "WC_SLR": ["{0}X{0}".format(x) for x in CHARS_R_WC_SLR.values()],
    } # strings containing reserved chars for unit test use

    # Prepare wildcard translation dict
    #  change TAGS wildcards to SQL wildcards
    for k in CHARS_R_WC.keys():
        trans_wc[ord(CHARS_R_WC[k])] = CHARS_R_WC_SLR[k]
    #  escape SQL wildcard characters
    for v in CHARS_R_WC_SLR.values():
        trans_wc[ord(v)] = r"{}{}".format(escape, v)

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

    def _ck_q_isnum(self, ck_args=('q','d'), **kwargs):
        for a in ck_args:
            try:
                if type(kwargs[a]) not in (int, float):
                    raise TypeError('argument {a} must be a number')
            except KeyError:
                pass

    def _prep_term(self, term):
        """
        Return an str of a ready-to-use form of a search term.
        TAGS wildcards are converted to SQL wildcards, HTML Entities
        used as escape codes are decoded, local aliases are converted
        to integer ROWIDs.

        """
        if term.startswith(CHARS_R_PX['A_ID']): return int(term[1:])
        else: return unescape(term.translate(self.trans_wc))

    def _prep_a(self, a):
        """
        Prepare an anchor for insertion into database, converting
        reserved characters into escape codes as necessary
        """
        out = ""
        if a[0] in chars_r_px_list:
            out = "".join((a[0].translate(self.escape_px), a[1:]))
        else:
            out = a
        return out.translate(self.escape_a)

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
            if a.startswith(CHARS_R_PX['A_ID']):
                sc = sc_ck_alias
                term = int(a[1:])
            else:
                sc = sc_ck
                term = a
            cs = self._slr_get_cursor()
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
        # PROTIP: also works with relations; relations are special anchors
        sc_up = "UPDATE {} SET {} = ? ".format(self.table_a, self.col_q)
        sc = "".join((sc_up, self._slr_a_where_clause(rels=True)))
        cus = self._slr_get_cursor()
        cus.execute(sc, (q, ae))
        self._db_conn.commit()

    def _slr_incr_q(self, ae, d):
        sc_incr = "UPDATE {0} SET {1}={1}+? ".format(self.table_a, self.col_q)
        sc = "".join((sc_incr, self._slr_a_where_clause(rels=True)))
        cus = self._slr_get_cursor()
        cus.execute(sc, (d, ae))
        self._db_conn.commit()

    def _slr_get_cursor(self):
        if not self._db_cus:
            self._db_cus = self._db_conn.cursor()
        return self._db_cus

    def _slr_get_rowids(self, a, **kwargs):
        """Returns SQLite ROWIDs for anchors matching a"""
        sc_rowid = "SELECT ROWID, {} from {} ".format(self.col, self.table_a)
        sc = "".join((sc_rowid, self._slr_a_where_clause(),))
        term = self._prep_term(a)
        cs = kwargs.get('cursor', self._slr_get_cursor())
        return cs.execute(sc, (term,))

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

    def _slr_a_where_clause(self, rels=False, alias=False):
        """
        Returns an SQL WHERE clause for SELECT, DELETE and UPDATE
        operations on anchors and relations.

        When rels is True, the clause includes relations.

        When alias is True, the clause selects anchors by SQLite
        ROWID

        """
        out = ""
        if alias:
            out = "WHERE ROWID = ? "
        else:
            out = "WHERE {} LIKE ? ESCAPE '{}' ".format(self.col, self.escape)
        if not rels:
            excl_rels = "AND {} NOT LIKE '%{}%' ".format(self.col, CHAR_REL)
            out = "".join((out, excl_rels))
        return out

    def _slr_q_clause(self, **kwargs):
        """
        Returns an expression string for searching anchors by
        q values, for use with SQL WHERE clauses.

        Arguments
        =========
        * q: Specify exact quantity. Cannot be used with any
          other argument.

        * q_gt, q_gte: Specify lower bound (greater than x);
          q_gt=x becomes q > x; q_gte=x becomes q >= x.
          Both arguments cannot be used together or with q.

        * q_lt, q_lte: Specify upper bound (less than x);
          q_lt=x becomes q < x; q_lte becomes q <= x.
          Both arguments cannot be used together or with q.

        * q_not: when set to True, prepare a clause for
          selecting anchors NOT within the range specified.

        Arguments will be ignored if mutually exclusive
        arguments are used together. The order of precedence
        is as follows: q, (q_gt or q_lt), (q_gte or q_lte)

        Note
        ====
        If the lower bound is higher than the upper bound,
        a range exclusion expression will be returned.

        e.g. q_gt=4, q_lt=3 returns "q > 4 OR q < 3",
        equivalent to "NOT (q > 3 AND q < 4)"

        """
        lbe = None  # lower bound
        ub = None  # upper bound
        lbe_expr = ""
        ub_expr = ""
        inter = ""
        self._ck_q_isnum(ck_args=self.num_q_args, **kwargs)
        if 'q' in kwargs:
            lbe = kwargs['q']
            lbe_expr = "{} = {}".format(self.col_q, lbe)
        else:
            # lower bound
            if 'q_gt' in kwargs:
                lbe = kwargs['q_gt']
                lbe_expr = "{} > {}".format(self.col_q, lbe)
            elif 'q_gte' in kwargs:
                lbe = kwargs['q_gte']
                lbe_expr = "{} >= {}".format(self.col_q, lbe)
            # upper bound
            if 'q_lt' in kwargs:
                ub = kwargs['q_lt']
                ub_expr = "{} < {}".format(self.col_q, ub)
            elif 'q_lte' in kwargs:
                ub = kwargs['q_lte']
                ub_expr = "{} <= {}".format(self.col_q, ub)
            if lbe is not None and ub is not None:
                if lbe < ub:
                    inter = " AND "
                else:
                    inter = " OR "
        if lbe is not None or ub is not None:
            q_not = kwargs.get('q_not', False)
            if q_not:
                return " AND NOT ({}{}{})".format(lbe_expr, inter, ub_expr)
            else:
                return " AND {}{}{}".format(lbe_expr, inter, ub_expr)
        else:
            return ''

    def reltxt(self, namee, a1e, a2e, alias=None):
        """
        Return a string representing a relation with name namee between
        anchor a1e and a2e. This function is also used for building
        search terms containing wildcards.

        Note that special symbols are not escaped: malformed relations
        are not suppressed. Please escape these symbols before using
        this function.

        When alias is set to 'local', local aliased relations are
        created instead. On SQLite repositories, this means anchors
        are referenced by a short code like @n, where n is the anchor's
        SQLite ROWID. Aliased relations are space-efficient, and are
        practically required for longer anchors, but not searchable.

        """
        # NOTE: The 'e' suffix in the argument names means that
        # the argument is 'expected to be already escaped'
        af = None
        at = None
        if alias == 'local':
            a1rid = next(self._slr_get_rowids(a1e))[0]
            a2rid = next(self._slr_get_rowids(a2e))[0]
            af = "{}{}".format(CHARS_R_PX['A_ID'], a1rid)
            at = "{}{}".format(CHARS_R_PX['A_ID'], a1rid)
        else:
            af = a1e
            at = a2e
        return '{}{}{}{}{}'.format(namee, CHAR_REL, af, CHAR_REL, at)

    def get_a(self, a, **kwargs):
        """
        Get an iterator containing anchors matching a. Use '*' as a
        wildcard for zero or more characters, or '.' as a wildcard
        for a single character.

        Alternate cursors may be specified using the cursor keyword
        argument. This is used internally by get_rels() in order
        to resolve relations to anchors.

        """
        sc_select = "SELECT {}, {} FROM {} ".format(
            self.col, self.col_q, self.table_a,
        )
        is_alias = a.startswith(CHARS_R_PX['A_ID'])
        sc = "".join((sc_select, self._slr_a_where_clause(alias=is_alias)))
        if kwargs:
            sc_q_range = self._slr_q_clause(**kwargs)
            sc = "".join((sc, sc_q_range))
        cs = kwargs.get('cursor', self._slr_get_cursor())
        # NOTE: all generated statements are expected to have just
        # a single parameter at this time.
        return cs.execute(sc, (self._prep_term(a),))

    def put_a(self, a, q=None):
        """
        Put an anchor a with optional numerical quantity value q.

        """
        self._slr_insert_into_a(self._prep_a(a), q)

    def set_a_q(self, a, q):
        """
        Assign a numerical quantity q to an anchor a.

        """
        self._ck_q_isnum(q=q)
        self._slr_set_q(self._prep_a(a), q)

    def incr_a_q(self, a, d):
        """
        Increment or decrement a quantity assigned to anchor a by
        d. When d<0, the quantity is decreased.

        If no value has been assigned to the anchor, this method has
        no effect.

        """
        self._ck_q_isnum(d=d)
        self._slr_incr_q(self._prep_a(a), d)

    def delete_a(self, a):
        """
        Delete anchors matching a. Specify the exact name of an
        anchor, or use the asterisk '*' as a wildcard (with caution).

        If the name of the anchor contains an asterisk, use the
        escape sequence &ast; instead.

        """
        sc_delete = "DELETE FROM {} ".format(self.table_a)
        sc = "".join((sc_delete, self._slr_a_where_clause()))
        cs = self._slr_get_cursor()
        cs.execute(sc, (self._prep_term(a),))
        self._db_conn.commit()

    def put_rel(self, name, a1, a2, q=None, **kwargs):
        """
        Create a relation between anchors a1 and a2, with an
        optional numerical quantity value q.

        Use the keyword argument prep_a=False to skip anchor
        name preprocessing, usually in order to create aliased
        relations.

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

    def set_rel_q(self, name, a_from, a_to, q):
        """
        Assign a numerical quantity q to a relation between anchors
        a_from and a_to.

        """
        ck_fns = (
            self._slr_ck_anchors_exist,
            self._ck_q_isnum,
        )
        ae_from = self._prep_a(a_from)
        ae_to = self._prep_a(a_to)
        namee = self._prep_a(name)
        for f in ck_fns:
            f(namee=namee, a1e=ae_from, a2e=ae_to, q=q)
        term = self.reltxt(name, ae_from, ae_to)
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
        ae_from = self._prep_a(a_from)
        ae_to = self._prep_a(a_to)
        namee = self._prep_a(name)
        for f in ck_fns:
            f(namee=namee, a1e=ae_from, a2e=ae_to, d=d)
        term = self.reltxt(name, ae_from, ae_to)
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

        * If any name or anchor contains an asterisk, or question mark,
          use the escape sequence &ast; and &quest; instead.

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
        sc = "".join((sc_delete, self._slr_a_where_clause(rels=True)))
        cs = self._slr_get_cursor()
        term = self.reltxt(namee, ae_from, ae_to)
        cs.execute(sc, (term,))
        self._db_conn.commit()

    def get_rels(self, **kwargs):
        """
        Return an iterator containing relations by name, anchor, or
        wildcard. Each relation is a list of four elements:

        [relation_name, from_anchor, to_anchor, quantity]

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
        * If any name or anchor contains an asterisk or question mark,
          use the HTML entities '&ast;' and '&quest;' instead.

        """
        # TODO: Also return aliased relations in results even when
        # anchor names are used
        argnames = ('name', 'a_from', 'a_to')
        for n in argnames:
            if n not in kwargs:
                kwargs[n] = '%'
            else:
                kwargs[n] = self._prep_term(kwargs[n])
        sc_select = "SELECT {}, {} FROM {} ".format(
            self.col, self.col_q, self.table_a
        )
        sc = "".join((sc_select, self._slr_a_where_clause(rels=True)))
        sc_q_range = self._slr_q_clause(**kwargs)
        if sc_q_range:
            sc = "".join((sc, sc_q_range))
        cs = self._slr_get_cursor()
        term = self.reltxt(kwargs['name'], kwargs['a_from'], kwargs['a_to'])
        rows = cs.execute(sc, (term,))
        ans = (r[0].split(CHAR_REL)+[r[1],] for r in rows)
        cs2 = self._db_conn.cursor() # needed to resolve anchor from name
        return(
            (
                k[0],
                next(self.get_a(k[1], cursor=cs2)),
                next(self.get_a(k[2], cursor=cs2)),
                k[3]
            ) for k in ans
        )

