============
Erasure Code
============

Introduction
------------

An erasure coded pool only supports full writes, appends and read. It
does not support snapshots or clone. An ErasureCodedPGBackend is derived
from PGBackend.


Glossary
--------

* Stripe

* Data chunk and parity chunk

* Shard


Reading and writing encoded chunks from and to OSDs
---------------------------------------------------
An erasure coded pool stores each object as M+K chunks. It is divided
into M data chunks and K parity chunks. The pool is configured to have
a size of M+K so that each chunk is stored in an OSD in the acting
set. The rank of the chunks is stored as an attribute of the object.

An erasure coded pool is created to use five OSDs ( M+K = 5 ) and
sustain the loss of two of them ( K = 2 ).

When the object *NYAN* containing *ABCDEFGHI* is written to it, the
erasure encoding function splits the content in three data chunks,
simply by dividing the content in three : the first contains *ABC*,
the second *DEF* and the last *GHI*. The function also creates two
parity chunks : the fourth with *YXY* and the fifth with *GQC*. Each
chunk is stored in an OSD in the acting set. The chunks are stored in
objects that have the same name ( *NYAN* ) but reside on different
OSDs. The order in which the chunks were created must be preserved and
is stored as an attribute of the object. The chunk *1* contains *ABC*
and is stored on *OSD5*, the chunk *4* contains *XYY* and is stored on
*OSD3*.
::
                             +-------------------+
                        name |        NYAN       |
                             +-------------------+
                     content |      ABCDEFGHI    |
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
 attribute  |  1   |   |  2   |   |  3   |  |  4   |  |  5   |
            +------+   +------+   +------+  +------+  +------+
   content  | ABC  |   | DEF  |   | GHI  |  | YXY  |  | QGC  |
            +--+---+   +--+---+   +--+---+  +--+---+  +--+---+
               |          |          |         |         |
               |          |          |         |         |
               |          |       +--+---+     |         |
               |          |       | OSD1 |     |         |
               |          |       +------+     |         |
               |          |       +------+     |         |
               |          +------>| OSD2 |     |         |
               |                  +------+     |         |
               |                  +------+     |         |
               |                  | OSD3 |<----+         |
               |                  +------+               |
               |                  +------+               |
               |                  | OSD4 |<--------------+
               |                  +------+
               |                  +------+
               +----------------->| OSD5 |
                                  +------+




When the object *NYAN* is read from the erasure coded pool, the
decoding function reads three chunks : chunk *1* containing *ABC*,
chunk *3* containing *GHI* and chunk *4* containing *YXY* and rebuild
the original content of the object *ABCDEFGHI*. The decoding function
is informed that the chunks *2* and *5* are missing. The chunk *5*
could not be read because the *OSD4* is *out*. The decoding function
is called as soon as three chunks are read : *OSD2* was the slowest
and its chunk was not taken into account.
::
                             +-------------------+
                        name |        NYAN       |
                             +-------------------+
                     content |      ABCDEFGHI    |
                             +--------+----------+
                                      ^
                                      |
                                      |
                               +------+------+
                               | decode(3,2) |
                               | erased 2,5  |
               +-------------->|             |
               |               +-------------+
               |                     ^   ^
               |                     |   +-----+
               |                     |         |
            +--+---+   +------+   +--+---+  +--+---+
      name  | NYAN |   | NYAN |   | NYAN |  | NYAN |
            +------+   +------+   +------+  +------+
 attribute  |  1   |   |  2   |   |  3   |  |  4   |
            +------+   +------+   +------+  +------+
   content  | ABC  |   | DEF  |   | GHI  |  | YXY  |
            +--+---+   +--+---+   +--+---+  +--+---+
               ^          ^          ^         ^
               |          |          |         |
               |          |       +--+---+     |
               |          |       | OSD1 |     |
               |          |       +------+     |
               |          |       +------+     |
               |     SLOW +-------| OSD2 |     |
               |                  +------+     |
               |                  +------+     |
               |                  | OSD3 |-----+
               |                  +------+
               |                  +------+
               |                  | OSD4 | OUT
               |                  +------+
               |                  +------+
               +------------------| OSD5 |
                                  +------+

Interrupted full writes
-----------------------

In an erasure coded pool the primary OSD is the first of the acting
set and receives all write operations. It is responsible for encoding
the payload into M+K chunks and send them to the OSDs in the acting
set. It is also responsible for maintaining an authoritative version
of the placement group logs.
::
     primary
   +---OSD 1---+
   |       log |
   |           |
   |+----+     |
   ||D1v1| 1,1 |
   |+----+     |
   +-----------+
               +---OSD 2---+
               |+----+ log |
               ||D2v1| 1,1 |
               |+----+     |
               +-----------+
               +---OSD 3---+
               |       log |
               |           |
               |+----+     |
               ||P1v1| 1,1 |
               |+----+     |
               +-----------+

An erasure coded placement group has been created with M = 2 + K = 1 and is supported by three OSDs, two for M and one for K. The acting set of the placement group is made of *OSD 1* *OSD 2* and *OSD 3*. An object has been encoded and stored in the OSDs : the chunk D1v1 (i.e. Data chunk number 1 version 1) is on *OSD 1*, D2v1 on *OSD 2* and P1v1 (i.e. Parity chunk number 1 version 1) on *OSD 3*. The placement group logs on each OSD are in synch at epoch 1 version 1 (i.e. 1,1).
::
     primary
   +---OSD 1---+
   |+----+ log |
   ||D1v2| 1,2 |<----------------- WRITE FULL
   |+----+     |
   |+----+     |
   ||D1v1| 1,1 |
   |+----+     |
   +++---------+
    ||         +---OSD 2---+
    ||  +----+ |+----+ log |
    |+-->D2v2| ||D2v1| 1,1 |
    |   +----+ |+----+     |
    |          +-----------+
    |          +---OSD 3---+
    |          |+----+ log |
    +---------->|P1v2| 1,2 |
               |+----+     |
               |+----+     |
               ||P1v1| 1,1 |
               |+----+     |
               +-----------+

*OSD 1* is the primary and receives a WRITE FULL from a client, meaning the payload is to replace the content of the object entirely, it is not a partial write that would only overwrite part of it. The version two of the object is created to override the version one. *OSD 1* encodes the payload into three chunks : D1v2 (i.e. Data chunk number 1 version 2) will be on *OSD 1*, D2v2 on *OSD 2* and P1v2 (i.e. Parity chunk number 1 version 2) on *OSD 3*. Each chunk is sent to the target OSD, including the primary OSD which is responsible for storing chunks in addition to handling write operations and maintaining an authoritative version of the placement group logs. When an OSD receives the message instructing it to write the chunk, it also creates a new entry in the placement group logs to reflect the change. For instance, as soon as *OSD 3* stores *P1v2*, it adds the entry 1,2 ( i.e. epoch 1, version 2 ) to its logs. Because the OSDs work asynchronously, some chunks may still be in flight ( such as *D2v2* ) while others are acknowledged and on disk ( such as *P1v1* and *D1v1* ). 
::
     primary
   +---OSD 1---+
   |+----+ log |
   ||D1v2| 1,2 |<----------------- WRITE FULL
   |+----+     |
   |+----+     |
   ||D1v1| 1,1 |
   |+----+     |
   +++---------+
    ||         +---OSD 2---+
    ||         |+----+ log |
    |+--------->|D2v2| 1,2 |
    |          |+----+     |
    |          |+----+     |
    |          ||D2v1| 1,1 |
    |          |+----+     |
    |          +-----------+
    |          +---OSD 3---+
    |          |+----+ log |
    +---------->|P1v2| 1,2 |
               |+----+     |
               |+----+     |
               ||P1v1| 1,1 |
               |+----+     |
               +-----------+

If all goes well, the chunks are acknowledged on each OSD in the acting set and the *last_complete* pointer of the logs can move from *1,1* to *1,2* and the files used to store the chunks of the previous version of the object can be removed : *D1v1* on *OSD 1*, *D2v1* on *OSD 2* and *P1v1* on *OSD 3*.
::
               +---OSD 1---+
               |           |
               |   DOWN    |
               |           |
               +-----------+
               +---OSD 2---+
               |+----+ log |
               ||D2v1| 1,1 |
               |+----+     |
               +-----------+
               +---OSD 3---+
               |+----+ log |
               ||P1v2| 1,2 |
               |+----+     |
               |+----+     |
               ||P1V1| 1,1 |
               |+----+     |
    primary    +-----------+
  +---OSD 4---+
  |       log |
  |       1,1 |
  |           |
  +-----------+

But accidents happen. If *OSD 1* goes down while *D2v2* is still in flight, the version 2 of the object is partially written : *OSD 3* has one chunk but does not have enough to recover. It lost two chunks : *D1v2* and *D2v2* but the erasure coding parameters M = 2 + K = 1 requires that at least two chunks are available to rebuild the third. *OSD 4* becomes the new primary and finds that the *last_complete* log entry ( i.e. all objects before this entry were known to be available on all OSDs in the previous acting set ) is *1,1* and will be the head of the new authoritative log. 
::
               +---OSD 2---+
               |+----+ log |
               ||D2v1| 1,1 |
               |+----+     |
               +-----------+
               +---OSD 3---+
               |+----+ log |
               ||P1V1| 1,1 |
               |+----+     |
    primary    +-----------+
  +---OSD 4---+
  |       log |
  |       1,1 |
  |           |
  +-----------+

The log entry *1,2* found on *OSD 3* is divergent from the new authoritative log provided by *OSD 4* : it is discarded and the file containing the *P1v2* chunk is removed.
::
               +---OSD 2---+
               |+----+ log |
               ||D2v1| 1,1 |
               |+----+     |
               +-----------+
               +---OSD 3---+
               |+----+ log |
               ||P1V1| 1,1 |
               |+----+     |
    primary    +-----------+
  +---OSD 4---+
  |+----+ log |
  ||D1v1| 1,1 |
  |+----+     |
  +-----------+

The *D1v1* chunk is rebuilt with the *repair* function of the erasure coding library during scrubbing and stored on the new primary *OSD 4*. 

Interrupted append
------------------

An object is coded in stripes as described above. In the case of a full write, and assuming the object size is not too large to encode it in memory, there is a single stripe. When appending to an existing object, the stripe size is retrieved from the attributes of the object and if the total size of the object is a multiple of the stripe size and the payload of the append message is lower or equal to the strip size, the following applies. It applies, for instance, when *rgw* writes an object with sequence of append instead of a single write.
::
     primary
   +---OSD 1---+
   |+-s1-+ log |
   ||S1D1| 1,2 |<----------------- APPEND
   ||----|     |
   ||S2D1| 1,1 |
   |+----+     |
   +++---------+
    ||         +---OSD 2---+
    ||  +-s2-+ |+-s2-+ log |
    |+-->S2D2| ||S1D2| 1,1 |
    |   +----+ |+----+     |
    |          +-----------+
    |          +---OSD 3---+
    |          |+-s3-+ log |
    +---------->|S1P1| 1,2 |
               ||----|     |
               ||S2P1| 1,1 |
               |+----+     |
               +-----------+

*OSD 1* is the primary and receives an APPEND from a client, meaning the payload is to be appended at the end of the object. *OSD 1* encodes the payload into three chunks : S2D1 (i.e. Stripe two data chunk number 1 ) will be in s1 ( shard 1 ) on *OSD 1*, S2D2 in s2 on *OSD 2* and S2P1 (i.e. Stripe two parity chunk number 1 ) in s3 on *OSD 3*. Each chunk is sent to the target OSD, including the primary OSD which is responsible for storing chunks in addition to handling write operations and maintaining an authoritative version of the placement group logs. When an OSD receives the message instructing it to write the chunk, it also creates a new entry in the placement group logs to reflect the change. For instance, as soon as *OSD 3* stores *S2P1*, it adds the entry 1,2 ( i.e. epoch 1, version 2 ) to its logs. The log entry also carries the nature of the operation: in this case 1,2 is an APPEND where 1,1 was a CREATE. Because the OSDs work asynchronously, some chunks may still be in flight ( such as *S2D2* ) while others are acknowledged and on disk ( such as *S2D1* and *S2P1* ). 
::
               +---OSD 1---+
               |           |
               |   DOWN    |
               |           |
               +-----------+
               +---OSD 2---+
               |+-s2-+ log |
               ||S1D2| 1,1 |
               |+----+     |
               +-----------+
               +---OSD 3---+
               |+-s3-+ log |
               ||S1P1| 1,2 |
               ||----|     |
               ||S2P1| 1,1 |
               |+----+     |
    primary    +-----------+
  +---OSD 4---+
  |       log |
  |       1,1 |
  |           |
  +-----------+

If *OSD 1* goes down while *S2D2* is still in flight, the payload is partially appended : s3 ( shard 3) in *OSD 3* has one chunk but does not have enough to recover because s1 and s2 don't have it. It lost two chunks : *S2D1* and *S2D2* but the erasure coding parameters M = 2 + K = 1 requires that at least two chunks are available to rebuild the third. *OSD 4* becomes the new primary and finds that the *last_complete* log entry ( i.e. all objects before this entry were known to be available on all OSDs in the previous acting set ) is *1,1* and will be the head of the new authoritative log. 
::
               +---OSD 2---+
               |+-s2-+ log |
               ||S1D2| 1,1 |
               |+----+     |
               +-----------+
               +---OSD 3---+
               |+-s3-+ log |
               ||S1P1| 1,1 |
               |+----+     |
    primary    +-----------+
  +---OSD 4---+
  |       log |
  |       1,1 |
  |           |
  +-----------+

The log entry *1,2* found on *OSD 3* is divergent from the new authoritative log provided by *OSD 4* : it is discarded and the file containing the *S2P1* chunk is truncated to the nearest multiple of the stripe size.

Erasure code library
--------------------

Using `Reed-Solomon <https://en.wikipedia.org/wiki/Reed_Solomon>`_,
with parameters M+K object O is encoded by dividing it into chunks O1,
O2, ...  OM and computing parity chunks P1, P2, ... PK. Any M chunks
out of the available M+K chunks can be used to obtain the original
object.  If data chunk O2 or parity chunk P2 are lost, they can be
repaired using any M chunks out of the M+K chunks. If more than K
chunks are lost, it is not possible to recover the object.

Reading the original content of object O could be a simple
concatenation of O1, O2, ... OM, if using `systematic codes
<http://en.wikipedia.org/wiki/Systematic_code>`_. Otherwise the
chunks must be given to the erasure code library to retrieve the
content of the object.

Reed-Solomon is significantly more expensive to encode than fountain
codes with the current `jerasure implementation
<http://web.eecs.utk.edu/~plank/plank/papers/CS-08-627.html>`_. However
`gf-complete
<http://web.eecs.utk.edu/~plank/plank/papers/CS-13-703.html>`_ that
will be used in the upcoming version of jerasure is twice faster and
the difference becomes negligible. The difference is even more
important when an object is divided in hundreds or more chunks, but
Ceph will typically be used with less than 32 chunks.

Performances depend on the parameters to the Reed-Solomon functions
but they are also influenced by the buffer sizes used when calling
the encoding functions: smaller buffers will mean more calls and more
overhead.

Although Reed-Solomon is provided as a default, Ceph uses it via an
abastract API designed to allow each pool to chose the plugin that
implements it.
::
  ceph osd pool set-erasure-code <pool> plugin-dir <dir>
  ceph osd pool set-erasure-code <pool> plugin <plugin>

The *<plugin>* is dynamically loaded from *<dir>* (defaults to
*/usr/lib/ceph/erasure-code-plugins* ) and expected to implement the
*create_erasure_code_context* function

* erasure_coding_t \*create_erasure_code_context(g_conf)

  return an object configured to encode and decode according to a
  given algorithm and a given set of parameters as specified in
  g_conf. Parameters must be prefixed with erasure-code to avoid name
  collisions
  ::
   ceph osd pool set-erasure-code <pool> m 10
   ceph osd pool set-erasure-code <pool> k 3
   ceph osd pool set-erasure-code <pool> algorithm Reed-Solomon

Erasure code library abstract API
---------------------------------

The following are methods of the abstract class erasure_coding_t.

* set<int> minimum_to_decode(const set<int> &want_to_read, const set<int> &available_chunks);

  returns the smallest subset of *available_chunks* that needs to be retrieved in order
  to successfully decode *want_to_read* chunks.

* set<int> minimum_to_decode_with_cost(const set<int> &want_to_read, const map<int, int> &available)

  returns the minimum cost set required to read the specified
  chunks given a mapping of available chunks to costs.  The costs might
  allow to consider the difference between reading local chunks vs
  remote chunks.  

* map<int, buffer> encode(const set<int> &want_to_encode, const buffer &in)

  encode the content of *in* and return a map associating the chunk
  number with its encoded content. The map only contains the chunks
  contained in the *want_to_encode* set. For instance, in the simplest
  case M=2,K=1 for a buffer containing AB, calling
  ::
    encode([1,2,3], 'AB')
    => { 1 => 'A', 2 => 'B', 3 => 'Z' }
  
  If only the parity chunk is of interest, calling
  ::
    encode([3], 'AB')    
    => { 3 => 'Z' }
  

* map<int, buffer> decode(const set<int> &want_to_read, const map<int, buffer> &chunks)

  decode *chunks* to read the content of the *want_to_read* chunks and
  return a map associating the chunk number with its decoded
  content. For instance, in the simplest case M=2,K=1 for an
  encoded payload of data A and B with parity Z, calling
  ::
    decode([1,2], { 1 => 'A', 2 => 'B', 3 => 'Z' })
    => { 1 => 'A', 2 => 'B' }

  If however, the chunk B is to be read but is missing it will be:
  ::
    decode([2], { 1 => 'A', 3 => 'Z' })
    => { 2 => 'B' }

Erasure code jerasure plugin
----------------------------

The parameters interpreted by the jerasure plugin are:
::
   ceph osd pool set-erasure-code <pool> m <unsigned int> (defaults 10)
   ceph osd pool set-erasure-code <pool> k <unsigned int> (default 3)
   ceph osd pool set-erasure-code <pool> algorithm <string> (default Reed-Solomon)


Scrubbing
---------

The simplest form of scrubbing is to check with each OSDs holding a
chunk if it exists locally. If more thank K chunks are missing the
object is marked as lost. If up to K chunks are missing they are
repaired and written to the relevant OSDs.

From time to time it may make sense to attempt to read and object,
using all of its chunks. If the decode function fails, the object is
lost.

Bit flips happen. Not often, but it is possible. Here is `an article
from 2011 <http://www.linux-mag.com/id/8794/>`_ also search for "bit
rot" and "bit error rate". To detect corrupted chunks, a checksum
(CRC23C for instance) should be added as an attribute of the file
containing the chunk so that deep scrubbing can check that the chunk
is valid by recomputing the content of the chunk and compare it with
the signature. BTRFS and ZFS have a CRC32C check built-in on a per
block basis.

Notes
-----

This document is a description of how erasure coding could be
implemented, it does not reflect the current state of the code
base. Possible optimizations are mentionned where relevant but the
first implementation should not include any of them: they are
presented to show that there is a path toward optimization starting
from simple minded implementation.

If the objects are large, it may be impractical to encode and decode
them in memory. However, when using *RBD* a 1TB device is divided in
many individual 4MB objects and *RGW* does the same.

Encoding and decoding is implemented in the OSD. Although it could be
implemented client side for read write, the OSD must be able to encode
and decode on its own when scrubbing.

If a partial read is required, an optimization could be to only fetch
the chunk that contains the data instead of always fetching all
chunks. For instance if *H* is required in the example above, chunk 3
is read if available. Reading 3 chunks is a fallback in case chunk 3 is
not available.

Partial reads and writes
------------------------

If an object is large, reading or writing all of it when changing only
a few bytes is expensive. It is more efficient to only read or write a
subset of the object. When a client writes on an existing object, it
can provide the offset and the length of the write as well as the payload with the `CEPH_OSD_OP_WRITE <https://github.com/ceph/ceph/blob/962b64a83037ff79855c5261325de0cd1541f582/src/osd/ReplicatedPG.cc#L2542>`_ operation. It is refered to as *partial write* and is different from the `CEPH_OSD_OP_WRITEFULL operation <https://github.com/ceph/ceph/blob/962b64a83037ff79855c5261325de0cd1541f582/src/osd/ReplicatedPG.cc#L2552>`_ which writes the entire object at once.

When using replicas for partial writes or reads, the primary OSD
translates them into read(2) and write(2) POSIX system calls. When
writing, it then forwards the CEPH_OSD_OP_WRITE message to the
replicas and waits for them to acknowledge they are done.

When reading erasure coded objects, at least K chunks must be read and
decoded to extract the desired bytes. If a `systematic code
<https://en.wikipedia.org/wiki/Systematic_code>`_ is used ( i.e. the
data chunks are readable by simple concatenation ) read can be
optimized to use the chunk containing the desired bytes and rely on
the erasure decoding function only if a chunk is missing.

When writing an erasure coded object, changing even one byte requires
that it is encoded again in full.

If Ceph is only used thru the radosgw or librbd, objects will mostly
have the same size. The radosgw user may upload a 1GB object, it will
be divided into smaller 4MB objects behind the scene ( or whatever is
set with rgw obj stripe size ). If a KVM is attached a 10GB RBD block
device, it will also be divided into smaller 4BM objects ( or whatever
size is given to the --stripe-unit argument when creating the RBD
block ). In both cases, writing one byte at the beginning will only
require to encode the first object and not all of them.

Objects can be further divided into stripes to reduce the overhead of
partial writes. For instance:
::
           +-----------------------+
           |+---------------------+|
           ||    stripe 0         ||
           ||    [0,N)            ||
           |+---------------------+|
           |+---------------------+|
           ||    stripe 1         ||
           ||    [N,N*2)          ||
           |+---------------------+|
           |+---------------------+|
           || stripe 3 [N*2,len)  ||
           |+---------------------+|
           +-----------------------+
               object of size len

Each stripe is encoded independantly and the same OSDs are used for
all of them. For instance, if stripe 0 is encoded into 3 chunks on
OSDs 5, 8 and 9, stripe 1 is also encoded into 3 chunks on the same
OSDs. The size of a stripe is stored as an attribute of the object.
When writing one byte at offset N, instead of re-encoding the whole
object it is enough to re-encode the stripe that contains it.

