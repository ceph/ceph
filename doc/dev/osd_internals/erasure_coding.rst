==============================
Erasure Coded Placement Groups
==============================

Glossary
--------

*chunk* 
   when the encoding function is called, it returns chunks of the same size.
   There are two kinds of chunks: (1) data chunks, which can be concatenated to
   reconstruct the original object, and (2) coding chunks, which can be used to
   rebuild a lost chunk.

*chunk rank*
   the index of a chunk, as determined by the encoding function. The
   rank of the first chunk is 0, the rank of the second chunk is 1,
   and so on.

*K*
   the number of "data *chunks*" into which an object is divided. For example, 
   if *K* = 2, then a 10KB object is divided into two objects of 5KB each.

*M* 
   the number of coding *chunks* (the number of chunks in addition to the "data
   chunks") computed by the encoding functions. *M* is equal to the number of
   OSDs that can be lost from the cluster without the cluster suffering data
   loss. For example, if there are two coding *chunks*, then two OSDs can be
   down without data loss.

*N*
   the number of data *chunks* plus the number of coding *chunks*. *K* + *M*.

*rate*
   the proportion of the *chunks* containing useful information: that is, *K*
   divided by *N*. For example, suppose that *K* = 9 and *M* = 3. This would
   mean that *N* = 12 (because *K* + *M* = 9 + 3). Therefore, the rate (*K* /
   *N*) is 9 / 12 = 0.75. In other words, 75% of the chunks contain useful
   information.

*shard* (also called *strip*)
   an ordered sequence of chunks of the same rank from the same
   object. For a given placement group, each OSD contains shards of
   the same rank. When dealing with objects that are encoded with a
   single operation, *chunk* is sometimes used instead of *shard*
   because the shard is made of a single chunk. The *chunks* in a
   *shard* are ordered according to the rank of the stripe they belong
   to.

*stripe* 
   if an object is so large that encoding it requires more than one call to the
   encoding function, each of these calls will create a set of chunks called a
   *stripe*.

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

Table of contents
-----------------

.. toctree::
   :maxdepth: 1

   Developer notes <erasure_coding/developer_notes>
   Jerasure plugin <erasure_coding/jerasure>
   High level design document <erasure_coding/ecbackend>
