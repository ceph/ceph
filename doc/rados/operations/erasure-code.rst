.. _ecpool:

=============
 Erasure code
=============

By default, Ceph `pools <../pools>`_ are created with the type "replicated". In
replicated-type pools, every object is copied to multiple disks (this
multiple copying is the "replication").

In contrast, `erasure-coded <https://en.wikipedia.org/wiki/Erasure_code>`_
pools use a method of data protection that is different from replication. In
erasure coding, data is broken into fragments of two kinds: data blocks and
parity blocks. If a drive fails or becomes corrupted, the parity blocks are
used to rebuild the data. At scale, erasure coding saves space relative to
replication.

In this documentation, data blocks are referred to as "data chunks"
and parity blocks are referred to as "encoding chunks".

Erasure codes are also called "forward error correction codes". The
first forward error correction code was developed in 1950 by Richard
Hamming at Bell Laboratories.


Creating a sample erasure coded pool
------------------------------------

The simplest erasure coded pool is equivalent to `RAID5
<https://en.wikipedia.org/wiki/Standard_RAID_levels#RAID_5>`_ and
requires at least three hosts:

.. prompt:: bash $

   ceph osd pool create ecpool erasure

::

   pool 'ecpool' created

.. prompt:: bash $

   echo ABCDEFGHI | rados --pool ecpool put NYAN -
   rados --pool ecpool get NYAN -
   
::

   ABCDEFGHI

Erasure code profiles
---------------------

The default erasure code profile can sustain the loss of two OSDs. This erasure
code profile is equivalent to a replicated pool of size three, but requires
2TB to store 1TB of data instead of 3TB to store 1TB of data. The default
profile can be displayed with this command:

.. prompt:: bash $

   ceph osd erasure-code-profile get default
   
::

   k=2
   m=2
   plugin=jerasure
   crush-failure-domain=host
   technique=reed_sol_van

.. note::
   The default erasure-coded pool, the profile of which is displayed here, is
   not the same as the simplest erasure-coded pool. 
   
   The default erasure-coded pool has two data chunks (k) and two coding chunks
   (m). The profile of the default erasure-coded pool is "k=2 m=2".

   The simplest erasure-coded pool has two data chunks (k) and one coding chunk
   (m). The profile of the simplest erasure-coded pool is "k=2 m=1".

Choosing the right profile is important because the profile cannot be modified
after the pool is created. If you find that you need an erasure-coded pool with
a profile different than the one you have created, you must create a new pool
with a different (and presumably more carefully-considered) profile. When the
new pool is created, all objects from the wrongly-configured pool must be moved
to the newly-created pool. There is no way to alter the profile of a pool after its creation.

The most important parameters of the profile are *K*, *M* and
*crush-failure-domain* because they define the storage overhead and
the data durability. For example, if the desired architecture must
sustain the loss of two racks with a storage overhead of 67% overhead,
the following profile can be defined:

.. prompt:: bash $

   ceph osd erasure-code-profile set myprofile \
       k=3 \
       m=2 \
       crush-failure-domain=rack
   ceph osd pool create ecpool erasure myprofile
   echo ABCDEFGHI | rados --pool ecpool put NYAN -
   rados --pool ecpool get NYAN -

::

    ABCDEFGHI

The *NYAN* object will be divided in three (*K=3*) and two additional
*chunks* will be created (*M=2*). The value of *M* defines how many
OSD can be lost simultaneously without losing any data. The
*crush-failure-domain=rack* will create a CRUSH rule that ensures
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


Erasure Coding with Overwrites
------------------------------

By default, erasure coded pools only work with uses like RGW that
perform full object writes and appends.

Since Luminous, partial writes for an erasure coded pool may be
enabled with a per-pool setting. This lets RBD and CephFS store their
data in an erasure coded pool:

.. prompt:: bash $

    ceph osd pool set ec_pool allow_ec_overwrites true

This can be enabled only on a pool residing on BlueStore OSDs, since
BlueStore's checksumming is used during deep scrubs to detect bitrot
or other corruption. In addition to being unsafe, using Filestore with
EC overwrites results in lower performance compared to BlueStore.

Erasure coded pools do not support omap, so to use them with RBD and
CephFS you must instruct them to store their data in an EC pool, and
their metadata in a replicated pool. For RBD, this means using the
erasure coded pool as the ``--data-pool`` during image creation:

.. prompt:: bash $

    rbd create --size 1G --data-pool ec_pool replicated_pool/image_name

For CephFS, an erasure coded pool can be set as the default data pool during
file system creation or via `file layouts <../../../cephfs/file-layouts>`_.


Erasure coded pool and cache tiering
------------------------------------

Erasure coded pools require more resources than replicated pools and
lack some functionality such as omap. To overcome these
limitations, one can set up a `cache tier <../cache-tiering>`_
before the erasure coded pool.

For instance, if the pool *hot-storage* is made of fast storage:

.. prompt:: bash $

   ceph osd tier add ecpool hot-storage
   ceph osd tier cache-mode hot-storage writeback
   ceph osd tier set-overlay ecpool hot-storage

will place the *hot-storage* pool as tier of *ecpool* in *writeback*
mode so that every write and read to the *ecpool* are actually using
the *hot-storage* and benefit from its flexibility and speed.

More information can be found in the `cache tiering
<../cache-tiering>`_ documentation.  Note however that cache tiering
is deprecated and may be removed completely in a future release.

Erasure coded pool recovery
---------------------------
If an erasure coded pool loses some data shards, it must recover them from others.
This involves reading from the remaining shards, reconstructing the data, and
writing new shards.
In Octopus and later releases, erasure-coded pools can recover as long as there are at least *K* shards
available. (With fewer than *K* shards, you have actually lost data!)

Prior to Octopus, erasure coded pools required at least ``min_size`` shards to be
available, even if ``min_size`` is greater than ``K``. We recommend ``min_size``
be ``K+2`` or more to prevent loss of writes and data.
This conservative decision was made out of an abundance of caution when
designing the new pool mode.  As a result pools with lost OSDs but without
complete loss of any data were unable to recover and go active
without manual intervention to temporarily change the ``min_size`` setting.

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
	erasure-code-clay
