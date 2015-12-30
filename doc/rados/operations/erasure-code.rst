=============
 Erasure code
=============

A Ceph pool is associated to a type to sustain the loss of an OSD
(i.e. a disk since most of the time there is one OSD per disk). The
default choice when `creating a pool <../pools>`_ is *replicated*,
meaning every object is copied on multiple disks. The `Erasure Code
<https://en.wikipedia.org/wiki/Erasure_code>`_ pool type can be used
instead to save space.

Creating a sample erasure coded pool
------------------------------------

The simplest erasure coded pool is equivalent to `RAID5
<https://en.wikipedia.org/wiki/Standard_RAID_levels#RAID_5>`_ and
requires at least three hosts::

    $ ceph osd pool create ecpool 12 12 erasure
    pool 'ecpool' created
    $ echo ABCDEFGHI | rados --pool ecpool put NYAN -
    $ rados --pool ecpool get NYAN -
    ABCDEFGHI

.. note:: the 12 in *pool create* stands for 
          `the number of placement groups <../pools>`_.

Erasure code profiles
---------------------

The default erasure code profile sustains the loss of a single OSD. It
is equivalent to a replicated pool of size two but requires 1.5TB
instead of 2TB to store 1TB of data. The default profile can be
displayed with::

    $ ceph osd erasure-code-profile get default
    directory=.libs
    k=2
    m=1
    plugin=jerasure
    ruleset-failure-domain=host
    technique=reed_sol_van

Choosing the right profile is important because it cannot be modified
after the pool is created: a new pool with a different profile needs
to be created and all objects from the previous pool moved to the new.

The most important parameters of the profile are *K*, *M* and
*ruleset-failure-domain* because they define the storage overhead and
the data durability. For instance, if the desired architecture must
sustain the loss of two racks with a storage overhead of 40% overhead,
the following profile can be defined::

    $ ceph osd erasure-code-profile set myprofile \
       k=3 \
       m=2 \
       ruleset-failure-domain=rack
    $ ceph osd pool create ecpool 12 12 erasure myprofile
    $ echo ABCDEFGHI | rados --pool ecpool put NYAN -
    $ rados --pool ecpool get NYAN -
    ABCDEFGHI

The *NYAN* object will be divided in three (*K=3*) and two additional
*chunks* will be created (*M=2*). The value of *M* defines how many
OSD can be lost simultaneously without losing any data. The
*ruleset-failure-domain=rack* will create a CRUSH ruleset that ensures
no two *chunks* are stored in the same rack.

.. ditaa::
                            +-------------------+
                       name |       NYAN        |
                            +-------------------+
                    content |     ABCDEFGHI     |
                            +--------+----------+
                                     |
                                     |
                                     v
                              +------+------+
              +---------------+ encode(3,2) +-----------+
              |               +--+--+---+---+           |
              |                  |  |   |               |
              |          +-------+  |   +-----+         |
              |          |          |         |         |
           +--v---+   +--v---+   +--v---+  +--v---+  +--v---+
     name  | NYAN |   | NYAN |   | NYAN |  | NYAN |  | NYAN |
           +------+   +------+   +------+  +------+  +------+
    shard  |  1   |   |  2   |   |  3   |  |  4   |  |  5   |
           +------+   +------+   +------+  +------+  +------+
  content  | ABC  |   | DEF  |   | GHI  |  | YXY  |  | QGC  |
           +--+---+   +--+---+   +--+---+  +--+---+  +--+---+
              |          |          |         |         |
              |          |          v         |         |
              |          |       +--+---+     |         |
              |          |       | OSD1 |     |         |
              |          |       +------+     |         |
              |          |                    |         |
              |          |       +------+     |         |
              |          +------>| OSD2 |     |         |
              |                  +------+     |         |
              |                               |         |
              |                  +------+     |         |
              |                  | OSD3 |<----+         |
              |                  +------+               |
              |                                         |
              |                  +------+               |
              |                  | OSD4 |<--------------+
              |                  +------+
              |
              |                  +------+
              +----------------->| OSD5 |
                                 +------+

 
More information can be found in the `erasure code profiles
<../erasure-code-profile>`_ documentation.

Erasure coded pool and cache tiering
------------------------------------

Erasure coded pools require more resources than replicated pools and
lack some functionalities such as partial writes. To overcome these
limitations, it is recommended to set a `cache tier <../cache-tiering>`_
before the erasure coded pool.

For instance, if the pool *hot-storage* is made of fast storage::

    $ ceph osd tier add ecpool hot-storage
    $ ceph osd tier cache-mode hot-storage writeback
    $ ceph osd tier set-overlay ecpool hot-storage

will place the *hot-storage* pool as tier of *ecpool* in *writeback*
mode so that every write and read to the *ecpool* are actually using
the *hot-storage* and benefit from its flexibility and speed.

It is not possible to create an RBD image on an erasure coded pool
because it requires partial writes. It is however possible to create
an RBD image on an erasure coded pools when a replicated pool tier set
a cache tier::

    $ rbd create --size 10G ecpool/myvolume

More information can be found in the `cache tiering
<../cache-tiering>`_ documentation.

Glossary
--------

*chunk*
   when the encoding function is called, it returns chunks of the same
   size. Data chunks which can be concatenated to reconstruct the original
   object and coding chunks which can be used to rebuild a lost chunk.

*K*
   the number of data *chunks*, i.e. the number of *chunks* in which the
   original object is divided. For instance if *K* = 2 a 10KB object
   will be divided into *K* objects of 5KB each.

*M*
   the number of coding *chunks*, i.e. the number of additional *chunks*
   computed by the encoding functions. If there are 2 coding *chunks*,
   it means 2 OSDs can be out without losing data.


Table of content
----------------

.. toctree::
	:maxdepth: 1

	erasure-code-profile
	erasure-code-jerasure
	erasure-code-isa
	erasure-code-lrc
	erasure-code-shec
