========
Manifest
========


============
Introduction
============

As described in ../deduplication.rst, adding transparent redirect
machinery to RADOS would enable a more capable tiering solution
than RADOS currently has with "cache/tiering".

See ../deduplication.rst

At a high level, each object has a piece of metadata embedded in
the object_info_t which can map subsets of the object data payload
to (refcounted) objects in other pools.

This document exists to detail:

1. Manifest data structures
2. Rados operations for manipulating manifests.
3. How those operations interact with other features like snapshots.

Data Structures
===============

Each object contains an object_manifest_t embedded within the
object_info_t (see osd_types.h):

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

TODO: check the following

The type enum reflects three possible states an object can be in:

1. TYPE_NONE: normal rados object
2. TYPE_REDIRECT: object payload is backed by a single object
   specified by redirect_target
3. TYPE_CHUNKED: object payload is distributed among objects with
   size and offset specified by the chunk_map. chunk_map maps
   the offset of the chunk to a chunk_info_t shown below further
   specifying the length, target oid, and flags.

::

        struct chunk_info_t {
          typedef enum {
            FLAG_DIRTY = 1, 
            FLAG_MISSING = 2,
            FLAG_HAS_REFERENCE = 4,
            FLAG_HAS_FINGERPRINT = 8,
          } cflag_t;
          uint32_t offset;
          uint32_t length;
          hobject_t oid;
          cflag_t flags;   // FLAG_*

TODO: apparently we specify the offset twice, with different widths

Request Handling
================

Similarly to cache/tiering, the initial touchpoint is
maybe_handle_manifest_detail.

For manifest operations listed below, we return NOOP and continue onto
dedicated handling within do_osd_ops.

For redirect objects which haven't been promoted (apparently oi.size >
0 indicates that it's present?) we proxy reads and writes.

For reads on TYPE_CHUNKED, if can_proxy_chunked_read (basically, all
of the ops are reads of extents in the object_manifest_t chunk_map),
we proxy requests to those objects.



RADOS Interface
================

To set up deduplication pools, you must have two pools. One will act as the 
base pool and the other will act as the chunk pool. The base pool need to be
configured with fingerprint_algorithm option as follows.

::

  ceph osd pool set $BASE_POOL fingerprint_algorithm sha1|sha256|sha512 
  --yes-i-really-mean-it

1. Create objects ::

        - rados -p base_pool put foo ./foo

        - rados -p chunk_pool put foo-chunk ./foo-chunk

2. Make a manifest object ::

        - rados -p base_pool set-chunk foo $START_OFFSET $END_OFFSET --target-pool 
        chunk_pool foo-chunk $START_OFFSET --with-reference

Operations:

* set-redirect 

  set a redirection between a base_object in the base_pool and a target_object 
  in the target_pool.
  A redirected object will forward all operations from the client to the 
  target_object. ::

        void set_redirect(const std::string& tgt_obj, const IoCtx& tgt_ioctx,
		      uint64_t tgt_version, int flag = 0);
  
        rados -p base_pool set-redirect <base_object> --target-pool <target_pool> 
         <target_object>

* set-chunk 

  set the chunk-offset in a source_object to make a link between it and a 
  target_object. ::

        void set_chunk(uint64_t src_offset, uint64_t src_length, const IoCtx& tgt_ioctx,
                   std::string tgt_oid, uint64_t tgt_offset, int flag = 0);
  
        rados -p base_pool set-chunk <source_object> <offset> <length> --target-pool 
         <caspool> <target_object> <taget-offset> 

* tier-promote 

  promote the object (including chunks). ::

        void tier_promote();

        rados -p base_pool tier-promote <obj-name> 

* unset-manifest

  unset the manifest info in the object that has manifest. ::

        void unset_manifest();

        rados -p base_pool unset-manifest <obj-name>

* tier-flush

  flush the object which has chunks to the chunk pool. ::

        void tier_flush();

        rados -p base_pool tier-flush <obj-name>


Dedup tool
==========

Dedup tool has two features: finding an optimal chunk offset for dedup chunking 
and fixing the reference count (see ./refcount.rst).

* find an optimal chunk offset

  a. fixed chunk  

    To find out a fixed chunk length, you need to run the following command many 
    times while changing the chunk_size. ::

            ceph-dedup-tool --op estimate --pool $POOL --chunk-size chunk_size  
              --chunk-algorithm fixed --fingerprint-algorithm sha1|sha256|sha512

  b. rabin chunk(Rabin-karp algorithm) 

    As you know, Rabin-karp algorithm is string-searching algorithm based
    on a rolling-hash. But rolling-hash is not enough to do deduplication because 
    we don't know the chunk boundary. So, we need content-based slicing using 
    a rolling hash for content-defined chunking.
    The current implementation uses the simplest approach: look for chunk boundaries 
    by inspecting the rolling hash for pattern(like the
    lower N bits are all zeroes). 
      
    - Usage

      Users who want to use deduplication need to find an ideal chunk offset.
      To find out ideal chunk offset, Users should discover
      the optimal configuration for their data workload via ceph-dedup-tool.
      And then, this chunking information will be used for object chunking through
      set-chunk api. ::

              ceph-dedup-tool --op estimate --pool $POOL --min-chunk min_size  
                --chunk-algorithm rabin --fingerprint-algorithm rabin

      ceph-dedup-tool has many options to utilize rabin chunk.
      These are options for rabin chunk. ::

              --mod-prime <uint64_t>
              --rabin-prime <uint64_t>
              --pow <uint64_t>
              --chunk-mask-bit <uint32_t>
              --window-size <uint32_t>
              --min-chunk <uint32_t>
              --max-chunk <uint64_t>

      Users need to refer following equation to use above options for rabin chunk. ::

              rabin_hash = 
                (rabin_hash * rabin_prime + new_byte - old_byte * pow) % (mod_prime)

  c. Fixed chunk vs content-defined chunk

    Content-defined chunking may or not be optimal solution.
    For example,

    Data chunk A : abcdefgabcdefgabcdefg

    Let's think about Data chunk A's deduplication. Ideal chunk offset is
    from 1 to 7 (abcdefg). So, if we use fixed chunk, 7 is optimal chunk length.
    But, in the case of content-based slicing, the optimal chunk length
    could not be found (dedup ratio will not be 100%).
    Because we need to find optimal parameter such
    as boundary bit, window size and prime value. This is as easy as fixed chunk.
    But, content defined chunking is very effective in the following case.

    Data chunk B : abcdefgabcdefgabcdefg

    Data chunk C : Tabcdefgabcdefgabcdefg
      

* fix reference count
  
  The key idea behind of reference counting for dedup is false-positive, which means 
  (manifest object (no ref), chunk object(has ref)) happen instead of 
  (manifest object (has ref), chunk 1(no ref)).
  To fix such inconsistency, ceph-dedup-tool supports chunk_scrub. ::

          ceph-dedup-tool --op chunk_scrub --chunk_pool $CHUNK_POOL


=====================
Status and Future Work
======================

At the moment, the above interfaces exist in rados, but have unclear
interactions with snapshots.

Snapshots
---------

Here are some design questions we'll need to tackle:

1. set-redirect

   * What happens if set on a clone?
   * 
