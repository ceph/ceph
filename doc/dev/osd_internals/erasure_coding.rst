==============================
Erasure Coded Placement Groups
==============================

The documentation of the erasure coding implementation in Ceph was
created in July 2013. It is included in Ceph even before erasure
coding is available because it drives a number of architectural
changes. It is meant to be updated to reflect the `progress of these
architectural changes <http://tracker.ceph.com/issues/4929>`_, up to
the point where it becomes a reference of the erasure coding
implementation itself.

Glossary
--------

*chunk* 
   when the encoding function is called, it returns chunks of the
   same size.

*stripe* 
   when an object is too large to be encoded with a single call,
   each set of chunks created by a call to the encoding function is
   called a stripe.

*shard|strip*
   the file that holds all chunks of a same rank for a given object.

Example:
::
 
                 OSD 40                       OSD 33
       +-------------------------+ +-------------------------+
       |      shard 0 - PG 10    | |      shard 1 - PG 10    |
       |+------ object O -------+| |+------ object O -------+|
       ||+---------------------+|| ||+---------------------+||
 stripe|||    chunk  0         ||| |||    chunk  1         ||| ...
   0   |||    [0,+N)           ||| |||    [0,+N)           ||| 
       ||+---------------------+|| ||+---------------------+||
       ||+---------------------+|| ||+---------------------+||
 stripe|||    chunk  0         ||| |||    chunk  1         ||| ...
   1   |||    [N,+N)           ||| |||    [N,+N)           |||
       ||+---------------------+|| ||+---------------------+||
       ||+---------------------+|| ||+---------------------+||
 stripe||| chunk  0 [N*2,+len) ||| ||| chunk  1 [N*2,+len) ||| ...
   2   ||+---------------------+|| ||+---------------------+||
       |+-----------------------+| |+-----------------------+|
       |         ...             | |         ...             |
       +-------------------------+ +-------------------------+

Table of content
----------------

.. toctree::
   :maxdepth: 1

   High level design document <erasure_coding/pgbackend>
   Developer notes <erasure_coding/developer_notes>
   Draft PGBackend.h header <erasure_coding/PGBackend-h>
