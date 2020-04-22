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

Manifest Object: 
Metadata objects are stored in the
base pool, which contains metadata for data deduplication.

::
  
        struct object_manifest_t {
                enum {
                        TYPE_NONE = 0,
                        TYPE_REDIRECT = 1,
                        TYPE_CHUNKED = 2,
                };
                uint8_t type;  // redirect, chunked, ...
                hobject_t redirect_target;
                std::map<uint64_t, chunk_info_t> chunk_map;
        }


A chunk Object: 
Chunk objects are stored in the chunk pool. Chunk object contains chunk data 
and its reference count information.


Although chunk objects and manifest objects have a different purpose 
from existing objects, they can be handled the same way as 
original objects. Therefore, to support existing features such as replication,
no additional operations for dedup are needed.


Regarding how to use, please see :doc:`doc/dev/osd_internals/manifest.rst`

