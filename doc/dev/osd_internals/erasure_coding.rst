==============================
Erasure Coded Placement Groups
==============================

Glossary
--------

*chunk* 
   when the encoding function is called, it returns chunks of the same
   size. Data chunks which can be concatenated to reconstruct the original
   object and coding chunks which can be used to rebuild a lost chunk.

*chunk rank*
   the index of a chunk when returned by the encoding function. The
   rank of the first chunk is 0, the rank of the second chunk is 1
   etc.

*stripe* 
   when an object is too large to be encoded with a single call,
   each set of chunks created by a call to the encoding function is
   called a stripe.

*shard|strip*
   an ordered sequence of chunks of the same rank from the same
   object.  For a given placement group, each OSD contains shards of
   the same rank. When dealing with objects that are encoded with a
   single operation, *chunk* is sometime used instead of *shard*
   because the shard is made of a single chunk. The *chunks* in a
   *shard* are ordered according to the rank of the stripe they belong
   to.

*K*
   the number of data *chunks*, i.e. the number of *chunks* in which the
   original object is divided. For instance if *K* = 2 a 10KB object
   will be divided into *K* objects of 5KB each.

*M* 
   the number of coding *chunks*, i.e. the number of additional *chunks*
   computed by the encoding functions. If there are 2 coding *chunks*, 
   it means 2 OSDs can be out without losing data.

*N*
   the number of data *chunks* plus the number of coding *chunks*, 
   i.e. *K+M*.

*rate*
   the proportion of the *chunks* that contains useful information, i.e. *K/N*.
   For instance, for *K* = 9 and *M* = 3 (i.e. *K+M* = *N* = 12) the rate is 
   *K* = 9 / *N* = 12 = 0.75, i.e. 75% of the chunks contain useful information.

The definitions are illustrated as follows (PG stands for placement group):
::
 
                 OSD 40                       OSD 33
       +-------------------------+ +-------------------------+
       |      shard 0 - PG 10    | |      shard 1 - PG 10    |
       |+------ object O -------+| |+------ object O -------+|
       ||+---------------------+|| ||+---------------------+||
 stripe|||    chunk  0         ||| |||    chunk  1         ||| ...
   0   |||    stripe 0         ||| |||    stripe 0         ||| 
       ||+---------------------+|| ||+---------------------+||
       ||+---------------------+|| ||+---------------------+||
 stripe|||    chunk  0         ||| |||    chunk  1         ||| ...
   1   |||    stripe 1         ||| |||    stripe 1         |||
       ||+---------------------+|| ||+---------------------+||
       ||+---------------------+|| ||+---------------------+||
 stripe|||    chunk  0         ||| |||    chunk  1         ||| ...
   2   |||    stripe 2         ||| |||    stripe 2         |||
       ||+---------------------+|| ||+---------------------+||
       |+-----------------------+| |+-----------------------+|
       |         ...             | |         ...             |
       +-------------------------+ +-------------------------+

Table of content
----------------

.. toctree::
   :maxdepth: 1

   Developer notes <erasure_coding/developer_notes>
   Jerasure plugin <erasure_coding/jerasure>
   High level design document <erasure_coding/pgbackend>
