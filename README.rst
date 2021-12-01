==================
The Bakdoh Project
==================

-----
About
-----
Welcome! This is just a little experimental project on deeply-embedded
graph databases.

The main goal here is to attempt to prototype a graph database system
with three main goals:

1. Be as size-efficient and fast as possible, and

2. Be as easy to use as possible,

3. Depend on non-built-in libraries as little as possible, for ease
   of deployment.

Bakdoh is just a hobby project (well, for now); it *won't be big and
professional*, unlike `Neo4j`_ or `OrientDB`_.

.. _Neo4j: https://github.com/neo4j
.. _OrientDB: https://github.com/orientdb

--------
Contents
--------
At the moment there are only two parts to this project:

Totally Approachable Graph System (TAGS)
========================================
The TAGS is an API and format for graph databases.  Contents are
stored as individually-accessible values called **Anchors**
interconnected by **Relations**.  No complete objects are stored in
the database; Relations serve to reconstruct the objects, as well as
maintain links between objects.

This allows sharing (normalisation) of values between objects.

The API is found in the ``tags.py`` module.

TAGS SQLite Repository (SQLiteRepo)
===================================
SQLiteRepo stores TAGS databases inside a two-table SQLite database:
one for the graph and the other for configuration.  The repository
code is kept in the same module as the TAGS API, ``tags.py``.
