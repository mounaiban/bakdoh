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

class Anchor:
    # Anchor (graph node) class. Includes navigation methods.
    def __init__(self, content, q=None, **kwargs):
        # kwargs accepted: get_q (bool), db
        self.db = kwargs.get('db')
        self.content = content
        self.q = q
        if kwargs.get('get_q', True) and self.q is None:
            self.get_q()

    def __eq__(self, other):
        """Anchor comparison: two Anchors are of equal value when
        both content and q are of equal value

        """
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

    def __init__(self, repo, **kwargs):
        # Prepare repository 'repo' beforehand, then set up a DB
        # like: DB(repo)
        #
        # Please see the documentation for the repository class
        # for details on how to create or load databases
        self.repo = repo

    def delete_a(self, s, **kwargs):
        """
        Delete anchors matching 's'.

        Accepts the same arguments and wildcard syntax as get_rels(),
        please see the documentation for that method for details.

        Accepted arguments: s, q, (q_gt or q_gte), (q_lt or q_lte).
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

        Accepted arguments: a_from, a_to, name, q, (q_gt or q_gte),
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

    def get_a(self, a, **kwargs):
        """
        Return an iterator containing anchors by content or
        wildcard.

        Arguments
        =========
        * name : return only relations matching 'name'

        * q : return only relations with a q-value equals to 'q'

        * q_gt : return only relations with a q-value greater than 'q_gt';
         cannot be used with 'q' or 'q_gte'

        * q_gte : return only relations with a q-value greater than or equal
          to 'q_gte'; cannot be used with 'q' or 'q_gt'

        * q_lt : return only relations with a q-value smaller than 'q_lt';
         cannot be used with 'q' or 'q_lte'

        * q_lte : return only relations with a q-value smaller than or
          equal to 'q_lte'; cannot be used with 'q' or 'q_lt'

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
        return (
            Anchor(a, q, db=self) for a, q in self.repo.get_a(a, **kwargs)
        )

    def get_rel_names(self, s, **kwargs):
        # Return an iterator of relation names in use between
        # two anchors, a_from and a_to, matching s
        return (r[0] for r in self.repo.get_rel_names(s, **kwargs))

    def get_rels(self, **kwargs):
        """
        Return an iterator containing relations by name, anchor, or
        wildcard. Each relation is presented as a list of four
        elements:

        [relation_name, from_anchor, to_anchor, quantity]

        Arguments
        =========
        * a_from : return only relations from anchors matching 'a_from'

        * a_to : return only relations towards anchors matching 'a_from'

        * name : return only relations matching 'name'

        * q : return only relations with a q-value equals to 'q'

        * q_gt : return only relations with a q-value greater than 'q_gt';
         cannot be used with 'q' or 'q_gte'

        * q_gte : return only relations with a q-value greater than or equal
          to 'q_gte'; cannot be used with 'q' or 'q_gt'

        * q_lt : return only relations with a q-value smaller than 'q_lt';
         cannot be used with 'q' or 'q_lte'

        * q_lte : return only relations with a q-value smaller than or
          equal to 'q_lte'; cannot be used with 'q' or 'q_lt'

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
        return (
            (n, next(self.get_a(f)), next(self.get_a(t)), q)
            for n, f, t, q in self.repo.get_rels(**kwargs)
        )

    def incr_a_q(self, a, d):
        """
        Increment or decrement a quantity assigned to anchor 'a'
        by d. When d<0, the quantity is decreased.

        If no value has been assigned to the anchor, this method
        has no effect.

        """
        self.repo.incr_a_q(a, d)

    def incr_rel_q(self, name, a_from, a_to, d):
        """
        Increment or decrement a quantity assigned to a relation between
        a_from and a_to by d. When d<0, the quantity is decreased.

        If no value has been assigned to the relation, this method has
        no effect.

        """
        self.repo.incr_rel_q(name, a_from, a_to, d)

    def put_a(self, a, q=None):
        """
        Create an anchor containing str 'a' and an optional
        numerical quantity value 'q'.

        Anchors of the same content can only be inserted once
        per database.

        """
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
        self.repo.put_rel(rel, a_from, a_to)

    def set_a_q(self, s, q):
        """
        Assign a numerical quantity q to an anchor 's'.

        """
        self.repo.set_a_q(s, q)

    def set_rel_q(self, name, a_from, a_to, q):
        """
        Assign a numerical quantity q to a relation between anchors
        a_from and a_to.

        """
        self.repo.set_rel_q(name, a_from, a_to, q)

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
        sc = "".join((sc_up, self._slr_a_where_clause(is_rel=True)))
        cus = self._slr_get_cursor()
        cus.execute(sc, (q, ae))
        self._db_conn.commit()

    def _slr_incr_q(self, ae, d):
        sc_incr = "UPDATE {0} SET {1}={1}+? ".format(self.table_a, self.col_q)
        sc = "".join((sc_incr, self._slr_a_where_clause(is_rel=True)))
        cus = self._slr_get_cursor()
        cus.execute(sc, (d, ae))
        self._db_conn.commit()

    def _slr_get_a(self, ae, **kwargs):
        """Helper method used by get_a() and get_rels() to fetch anchors
        and relations from the SQLite backing store.

        Please see DB.get_rels() for usage

        Supported kwargs: cursor, is_rel, q, (q_gt or q_gte),
        (q_lt or q_lte), q_not

        """
        params = [ae,]
        sc_select = "SELECT {}, {} FROM {} ".format(
            self.col, self.col_q, self.table_a,
        )
        sc_where = self._slr_a_where_clause(
            is_rel=kwargs.get('is_rel', False),
            is_alias=kwargs.get('is_alias', False)
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
        return cs.execute(sc, params)

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

    def _slr_a_where_clause(self, is_rel=False, is_alias=False):
        """
        Returns an SQL WHERE clause for SELECT, DELETE and UPDATE
        operations on anchors and relations.

        When is_rel is True, the clause includes relations.

        When is_alias is True, the clause selects anchors by SQLite
        ROWID

        """
        out = "WHERE "
        if is_alias:
            out = "".join((out, "ROWID = ? "))
        else:
            out = "".join(
                (out, "{} LIKE ? ESCAPE '{}' ".format(self.col, self.escape))
            )
        if not is_rel:
            out = "".join(
                (out, "AND {} NOT LIKE '%{}%' ".format(self.col, CHAR_REL))
            )
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
        self._ck_q_isnum(ck_args=self.num_q_args, **kwargs)
        if 'q' in kwargs:
            # exact
            lbe = kwargs['q']
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
                    at = "{}{}".format(CHARS_R_PX['A_ID'], rid2)
                if alias_fmt & 2:
                    af = "{}{}".format(CHARS_R_PX['A_ID'], rid1)
        return '{}{}{}{}{}'.format(namee, CHAR_REL, af, CHAR_REL, at)

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
        is_alias = a.startswith(CHARS_R_PX['A_ID'])
        return self._slr_get_a(
            self._prep_term(a), is_alias=is_alias, is_rel=False, **kwargs
        )

    def put_a(self, a, q=None):
        """
        Put an anchor a with optional numerical quantity value q.

        """
        self._slr_insert_into_a(self._prep_a(a), q)

    def set_a_q(self, a, q):
        """Handle DB request to assign a numerical quantity to an
        anchor. Called from DB.set_a_q()

        """
        self._ck_q_isnum(q=q)
        self._slr_set_q(self._prep_a(a), q)

    def incr_a_q(self, a, d):
        """Handle DB request to increment/decrement a numerical
        quantity of an anchor. Called from DB.incr_a_q()

        """
        self._ck_q_isnum(d=d)
        self._slr_incr_q(self._prep_a(a), d)

    def delete_a(self, a):
        """Handle DB request to delete anchors. Accepts the same arguments
        as DB.delete_a(). Please see the documentation of that method
        for usage.

        """
        sc_delete = "DELETE FROM {} ".format(self.table_a)
        sc = "".join((sc_delete, self._slr_a_where_clause()))
        cs = self._slr_get_cursor()
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

    def set_rel_q(self, name, a_from, a_to, q):
        """Handle DB request to set the numerical quantity assigned
        to the relationship named 'name' from anchor 'a_from' to
        'a_to'. Called from DB.set_rel_q()

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
        """Handle DB request to increment/decrement the numerical
        quantity assigned to the relationship named 'name' from anchor
        'a_from' to 'a_to'. Called from DB.incr_rel_q()

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
        """Handle DB request to delete relations. Accepts the same arguments
        as DB.delete_rels(). Please see the documentation of that method
        for usage.

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
        sc = "".join((sc_delete, self._slr_a_where_clause(is_rel=True)))
        cs = self._slr_get_cursor()
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
            """.format(self.col, CHAR_REL, self.table_a)
        sc_where = self._slr_a_where_clause(is_rel=True)
        sc = "".join((sc_relnames, sc_where))
        wczp = CHARS_R_WC['WILDCARD_ZEROPLUS']
        term = self._prep_term(self.reltxt(
            s,
            kwargs.get('a_from', wczp),
            kwargs.get('a_to', wczp)
        ))
        cs = kwargs.get('cursor', self._slr_get_cursor())
        return cs.execute(sc, (term,))

    def get_rels(self, **kwargs):
        """Handle DB request to return an iterator of relations.
        Accepts the same arguments as DB.get_rels(). Please see the
        documentation of that method for usage.

        """
        nameargs = ('name', 'a_from', 'a_to')
        for n in nameargs:
            if n not in kwargs:
                kwargs[n] = '%'
            else:
                kwargs[n] = self._prep_term(kwargs[n])
        term = self.reltxt(kwargs['name'], kwargs['a_from'], kwargs['a_to'])
        rels = self._slr_get_a(term, is_rel=True, **kwargs)
        return (r[0].split(CHAR_REL)+[r[1],] for r in rels)

