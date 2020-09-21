===============
 Deduplication
===============


Introduction
============

Applying data deduplication on an existing software stack is not easy 
due to additional metadata management and original data processing 
procedure. 

In a typical deduplication system, the input source as a data
object is split into multiple chunks by a chunking algorithm.
The deduplication system then compares each chunk with
the existing data chunks, stored in the storage previously.
To this end, a fingerprint index that stores the hash value
of each chunk is employed by the deduplication system
in order to easily find the existing chunks by comparing
hash value rather than searching all contents that reside in
the underlying storage.

There are many challenges in order to implement deduplication on top
of Ceph. Among them, two issues are essential for deduplication.
First is managing scalability of fingerprint index; Second is
it is complex to ensure compatibility between newly applied
deduplication metadata and existing metadata.

Key Idea
========
1. Content hashing (Double hashing): Each client can find an object data 
for an object ID using CRUSH. With CRUSH, a client knows object's location
in Base tier. 
By hashing object's content at Base tier, a new OID (chunk ID) is generated.
Chunk tier stores in the new OID that has a partial content of original object.

 Client 1 -> OID=1 -> HASH(1's content)=K -> OID=K -> 
 CRUSH(K) -> chunk's location


2. Self-contained object: The external metadata design
makes difficult for integration with storage feature support
since existing storage features cannot recognize the
additional external data structures. If we can design data
deduplication system without any external component, the
original storage features can be reused.

More details in https://ieeexplore.ieee.org/document/8416369

Design
======

.. ditaa::

           +-------------+
           | Ceph Client |
           +------+------+
                  ^
     Tiering is   |  
    Transparent   |               Metadata
        to Ceph   |           +---------------+
     Client Ops   |           |               |   
                  |    +----->+   Base Pool   |
                  |    |      |               |
                  |    |      +-----+---+-----+
                  |    |            |   ^ 
                  v    v            |   |   Dedup metadata in Base Pool
           +------+----+--+         |   |   (Dedup metadata contains chunk offsets
           |   Objecter   |         |   |    and fingerprints)
           +-----------+--+         |   |
                       ^            |   |   Data in Chunk Pool
                       |            v   |
                       |      +-----+---+-----+
                       |      |               |
                       +----->|  Chunk Pool   |
                              |               |
                              +---------------+
                                    Data


Pool-based object management:
We define two pools.
The metadata pool stores metadata objects and the chunk pool stores
chunk objects. Since these two pools are divided based on
the purpose and usage, each pool can be managed more
efficiently according to its different characteristics. Base
pool and the chunk pool can separately select a redundancy
scheme between replication and erasure coding depending on
its usage and each pool can be placed in a different storage
location depending on the required performance.

Regarding how to use, please see osd_internals/manifest.rst

Usage Patterns
==============

The different ceph interface layers present potentially different oportunities
and costs for deduplication and tiering in general.

RadosGW
-------

S3 big data workloads seem like a good opportunity for deduplication.  These
objects tend to be write once, read mostly objects which don't see partial
overwrites.  As such, it makes sense to fingerprint and dedup up front.

Unlike cephfs and rbd, radosgw has a system for storing
explicit metadata in the head object of a logical s3 object for
locating the remaining pieces.  As such, radosgw could use the
refcounting machinery (osd_internals/refcount.rst) directly without
needing direct support from rados for manifests.

RBD/Cephfs
----------

RBD and CephFS both use deterministic naming schemes to partition
block devices/file data over rados objects.  As such, the redirection
metadata would need to be included as part of rados, presumably
transparently.

Moreover, unlike radosgw, rbd/cephfs rados objects can see overwrites.
For those objects, we don't really want to perform dedup, and we don't
want to pay a write latency penalty in the hot path to do so anyway.
As such, performing tiering and dedup on cold objects in the background
is likely to be preferred.
   
One important wrinkle, however, is that both rbd and cephfs workloads
often feature usage of snapshots.  This means that the rados manifest
support needs robust support for snapshots.

RADOS Machinery
===============

For more information on rados redirect/chunk/dedup support, see osd_internals/manifest.rst.
For more information on rados refcount support, see osd_internals/refcount.rst.

Status and Future Work
======================

At the moment, there exists some preliminary support for manifest
objects within the osd as well as a dedup tool.

RadosGW data warehouse workloads probably represent the largest
opportunity for this feature, so the first priority is probably to add
direct support for fingerprinting and redirects into the refcount pool
to radosgw.

Aside from radosgw, completing work on manifest object support in the
osd particularly as it relates to snapshots would be the next step for
rbd and cephfs workloads.

