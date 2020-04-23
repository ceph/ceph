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


Status and Future Work
======================

At the moment, initial versions of a manifest data structure along
with IO path support and rados control operations exist.  This section
is meant to outline next steps.
Future work will be proceeded as cleanups -> snapshot -> cache/tiering -> rbd support
in order.

Cleanups
--------

There are some rough edges we may want to improve:

* object_manifest_t: appears to duplicate offset for chunks between the
  map and the chunk_info_t with different widths (uint64_t in map vs
  uint32_t in chunk_info_t).
* SET_REDIRECT: Should perhaps create the object if it doesn't exist.
* SET_CHUNK:

  * Appears to trigger a new clone as user_modify gets set in
    do_osd_ops.  This probably isn't desirable, see Snapshots section
    below for some options on how generally to mix these operations
    with snapshots.
    TODO: Modify SET_CHUNK to set user_modify = false not to trigger a new clone.
  * Appears to assume that the corresponding section of the object
    does not exist (sets FLAG_MISSING) but does not check whether the
    corresponding extent exists already in the object.
    TODO: Set FLAG_DIRTY if the corresponding section of the object exists.

  * Appears to clear the manifest unconditionally if not chunked,
    that's probably wrong.  We should return an error if it's a
    REDIRECT.
    TODO: add the following lines ::

	case CEPH_OSD_OP_SET_CHUNK:
	  if (oi.manifest.is_redirect()) {
	    result = -EINVAL;
	    goto fail;
	  }


* TIER_PROMOTE:

  * Document control flow in this file.  The CHUNKED case is
    particularly unintuitive and could use some explanation.
    - Control flow
    Based on the chunk_map in a head object (each chunk's state can be 
    dirty or missing), chunked objects for the head object 
    can be retrieved when tier_promote is invoked.
    After promotion is completed, all chunks in the object should be clean state.

    One thing to note is snapshotted manifest object.
    Suppose that there are 10(9), 5(4). If we want to read clone id 4,
    need to read chunks according to the chunk_map for clone id 4.
    But, because the base tier keeps object_info_t in the case of manifest object,
    the read with snapshot can work.
    If clone id 4 is dirty or clean, the chunks will be read directly from the base tier.
    If clone id 4 is missing, the chunks will be copied from the lower tier 

  * SET_REDIRECT clears the contents of the object.  PROMOTE appears
    to copy them back in, but does not unset the redirect or clear the
    reference. Does this not violate the invariant?  In particular, as
    long as the redirect is set, it appears that all operations
    will be proxied even after the promote defeating the purpose.
    TODO: To prevent it, clear redirect as part of promote.

  * For a chunked manifest, we appear to flush prior to promoting,
    it that actually desirable?
    TODO: The initial thought behind of this is that we don't know fingerprint 
    oid before flushing the chunk. So, before promoting the chunk, 
    we need to figure out the fingerprint oid.
    But, to avoid unnecessary dedup work if the user specifically ask for 
    the data to be resident in the base pool, TIER_DO_NOT_DEDUP_PIN can be used.

Plans
  1. https://github.com/ceph/ceph/pull/29283 
  (This PR adds basic snapshotted manifest object by using per clone's chunk_info_t)
  2. Modify SET_CHUNK not to trigger a clone
  3. Cleanups regarding existing manifest object as discussed here
   

Cache/Tiering
-------------

There already exists a cache/tiering mechanism based on whiteouts.
One goal here should ultimately be for this manifest machinery to
provide a complete replacement.

See cache-pool.rst

The manifest machinery already shares some code paths with the
existing cache/tiering code, mainly stat_flush.

In no particular order, here's in incomplete list of things that need
to be wired up to provide feature parity:

* Online object access information: The osd already has pool configs
  for maintaining bloom filters which provide estimates of access
  recency for objects.  We probably need to modify this to permit
  hitset maintenance for a normal pool -- there are already
  CEPH_OSD_OP_PG_HITSET* interfaces for querying them.
* Tiering agent: The osd already has a background tiering agent which
  would need to be modified to instead flush and evict using
  manifests.

Plans
 
* Rework the tiering agent for manifest objects
  - flush method (passive vs active)

    Current flush operation for manifest objects uses a active flush model like
    read the cotent - generating fingerprint - write and set.
    But, exising tiering method uses copy-get and copy-from to flush the object.
    I think the active flush model has two advantages compared to the existing method.
    First, it can reduce the number of operations for a communication. Such as
    Active: read -> generating fingerprint -> write and set
    Passive: request copy_from -> copy_get -> read -> getnerating fingerprint -> write and set
    Second, By managing I/O related to flush on the base tier, 
    we can control overall I/Os on the cluster appropriately.

  - tier agent

    Because the active flush model is used, the tier agent does more jobs than before
    (read - generating fingerprint - write and set vs sending copy_from). 
    So, my concern is a parallism and performance.
    since a single agent thread is probably hard to achieve high throughput.
    To this end, the idea is to use N threads working on N pgs.
    Existing agent threads (agent_work) can manages the list of dirty objects (pgbackend->list_partial)
    , and flush the dirty objects in parallel. 

* Use exiting existing features regarding the cache flush policy such as histset, age, ratio
  - hitset
  - age, ratio, bytes

* Add tiering-mode to manifest-tiering
  - Writeback
  - Read-only



Snapshots
---------

Fundamentally, I think we need to be able to manipulate the manifest
status of clones because we want to be able to dynamically promote,
flush (if the state was dirty when the clone was created), and evict
clones.

As such, I think we want to make a new op type which permits writes on
clones.

See snaps.rst for a rundown of the librados snapshot system and osd
support details.  I'd like to call out one particular data structure
we may want to exploit.

We maintain a clone_overlap mapping which gives between two adjacent
clones byte ranges which are identical.  It's used during recovery and
cache/tiering promotion to ensure that the ObjectStore ends up with
the right byte range sharing via clone.  We probably want to ensure
that adjacent clones agree on chunk mappings for shared regions.

* Implementation

  My thought is that set_chunk and set_reredirect shouldn't set
  user_modify to indicate not to trigger a new clone (probably also need a flag like cache_evict).
  Also, to use manifest with snapshot, set_chunk should be set when the manifest object is created.
  As a result, the overall procedure is:

    1. Write the object A
    2. SET_CHUNK (offset: 0 ~ 4)
    3. SET_CHUNK (offset: 8 ~ 12)
    4. SET_CHUNK (offset: 16 ~ 20)
    5. Create a snapshot 
    6. Write the object A
    7. Create a snapshot

  When a snapshot is created between 5 and 6, clones prior to head are dirty
  if the head object is dirty (and the flush should start from old clones).
  Also, there are two use cases.

    Use case 1

      1. Create object A
      2. Write Full
      3. SET_CHUNK
      4. SET_CHUNK
      5. Write the object A
      6. Create a snapshot
      7. Write the object A
      8. Create a snapshot

    Use case 2

      1. Create object A
      2. Write Full
      3. SET_CHUNK
      4. SET_CHUNK
      5. Write the object A
      6. Create a snapshot ABC
      7. Write the object A
      8. SET_CHUNK to the snapshot ABC

  The state of the chunk_map in clone should be MISSING after eviction is done.
  If we want to read clones, we have to look at the chunk_info_t according to given snap_id 
  to find out which chunk is needed.
  Therefore, clone needs to be mutable. To do that, we probably need a new op type.
  If snap read occurs, the chunks can be read from snapshots without doing a rollback.
  Such reads must return the contents of the object at the time the snapshot was taken.

  - Clone_overlap

  clone_overlap is an optimization that ensures that recovery preserves the underlying 
  ObjectStore level byte range sharing inherent in clone.
  Therefore, to support snapshot with manifest object, we should ensure interaction 
  between clone_overlap and manifest object. To do so, here is the basic example describing how 
  it works.
  HEAD: [1 ~ 10 nochunk] [10 ~ 10 nochunk] [20 ~ 10 nochunk]
  clone 10(9, 7): [1 ~ 10 nochunk] [10 ~ 10 nochunk] [20 ~ 10 nochunk]
  clone 6(6, 5): [1 ~ 10 nochunk] [10 ~ 10 nochunk] [20 ~ 10 nochunk]
  clone 4(2, 1): [1 ~ 10 nochunk] [10 ~ 10 nochunk] [20 ~ 10 nochunk]
  clone_overlap: {10: [1 ~ 10, 20 ~ 10], 6: [10 ~ 10], 4: [1 ~ 10, 10 ~ 20]}

  At this point, if we SET_CHUNK clone 6 20 ~ 10 aaa, the object will become as below.

  HEAD: [1 ~ 10 nochunk] [10 ~ 10 nochunk] [20 ~ 10 nochunk]
  clone 10(9, 7): [1 ~ 10 nochunk] [10 ~ 10 nochunk] [20 ~ 10 nochunk]
  clone 6(6, 5): [1 ~ 10 nochunk] [10 ~ 10 nochunk] [20 ~ 10 aaa]
  clone 4(2, 1): [1 ~ 10 nochunk] [10 ~ 10 nochunk] [20 ~ 10 aaa]
  clone_overlap: {10: [1 ~ 10, 20 ~ 10], 6: [10 ~ 10], 4:[1 ~ 10]}
  

  - Leak management

  Fixing a reference leak by dedup-tool also need to be reconsidered because
  dedup-tool finds out the leaks by using the head object. If the head object 
  has snapshots, it should search chunk_info_t in clones as well.
  

* Plans

  1. separate evict from flush (manifest tier uses flush+evict)
  2. promote, flush and evict clones for snapshotted manifest object (with a new op)
  3. scrub test for manifest object and dedup tool


Interaction between RBD
-----------------------

ToDo


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

  Returns ENOENT if the object does not exist (TODO: why?)
  Returns EINVAL if the object already is a redirect.

  Takes a reference to target as part of operation, can possibly leak a ref
  if the acting set resets and the client dies between taking the ref and
  recording the redirect.

  Truncates object, clears omap, and clears xattrs as a side effect.

  At the top of do_osd_ops, does not set user_modify.

  This operation is not a user mutation and does not trigger a clone to be created.

  The purpose of set_redirect is two.

  1. Redirect all operation to the target object (like proxy)
  2. Cache when tier_promote is called (rediect will be cleared at this time).

* set-chunk 

  set the chunk-offset in a source_object to make a link between it and a 
  target_object. ::

        void set_chunk(uint64_t src_offset, uint64_t src_length, const IoCtx& tgt_ioctx,
                   std::string tgt_oid, uint64_t tgt_offset, int flag = 0);
  
        rados -p base_pool set-chunk <source_object> <offset> <length> --target-pool 
         <caspool> <target_object> <taget-offset> 

  Returns ENOENT if the object does not exist (TODO: why?)
  Returns EINVAL if the object already is a redirect.
  Returns EINVAL if on ill-formed parameter buffer.
  Returns ENOTSUPP if existing mapped chunks overlap with new chunk mapping.

  Takes references to targets as part of operation, can possibly leak refs
  if the acting set resets and the client dies between taking the ref and
  recording the redirect.

  Truncates object, clears omap, and clears xattrs as a side effect.

  This operation is not a user mutation and does not trigger a clone to be created.

  TODO: SET_CHUNK appears to clear the manifest unconditionally if it's not chunked.
  That seems wrong. ::

       if (!oi.manifest.is_chunked()) {
         oi.manifest.clear();
       }

* tier-promote 

  promote the object (including chunks). ::

        void tier_promote();

        rados -p base_pool tier-promote <obj-name> 

  Returns ENOENT if the object does not exist
  Returns EINVAL if the object already is a redirect.

  For a chunked manifest, copies all chunks to head.

  For a redirect manifest, copies data to head.

  TODO: To atomically replace a redirect or dedup'd chunk with a local copy atomically,
  redirect will be clear after the promote.

  Does not clear the manifest.

  Note: For a chunked manifest, calls start_copy on itself and uses the
  existing read proxy machinery to proxy the reads.

  TODO: Use TIER_DO_NOT_DEDUP_PIN to avoid unnecessary dedup work.
  - Two use cases.

    Case a: 

      1. Create Object A and B
      2. Setchunk A to B
      3. Write A
      4. TIER_DO_NOT_DEDUP_PIN
      5. Flush does not occur

    Case b:

      1. Create Object A and B
      2. Setchunk A to B
      3. TIER_DO_NOT_DEDUP_PIN
      4. Promote A
      5. Write A
      6. Flush does not occur

  TODO: Free old fingerprint oid earlier.
  There is a HEAD: [1-10 manifest: aaa, clean, size is 20]
  Then, we write the region of 5 ~ 15.
  HEAD:[size is 20] (1-10 is not in the manifest)
  Then, we write the region of 6 ~ 15.
  HEAD:[size is 20] (1-10 is not in the manifest)
  Then, we write the region of 7 ~ 15.
  If the tiering agent wants to dedup 1-10 because it is now cold, it can use a read and set-chunk to:
  Read 1-10 and computation
  HEAD:[1-10 manifest:ddd, clean, size is 20]
  This way, we shorten the lifetime of the aaa dedup target object freeing space earlier.

  At the top of do_osd_ops, does not set user_modify.


* unset-manifest

  unset the manifest info in the object that has manifest. ::

        void unset_manifest();

        rados -p base_pool unset-manifest <obj-name>

  Clears manifest chunks or redirect.  Lazily releases references, may
  leak.

  do_osd_ops seems not to include it in the user_modify=false whitelist,
  and so will trigger a snapshot.  Note, this will be true even for a
  redirect though SET_REDIRECT does not flip user_modify.

* tier-flush

  flush the object which has chunks to the chunk pool. ::

        void tier_flush();

        rados -p base_pool tier-flush <obj-name>

  Included in the user_modify=false whitelist, does not trigger a clone.


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

