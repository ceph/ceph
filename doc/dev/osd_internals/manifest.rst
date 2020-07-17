========
Manifest
========


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
3. Status and Plans


Intended Usage Model
====================

RBD
---

For RBD, the primary goal is for either an osd-internal agent or a
cluster-external agent to be able to transparently shift portions
of the consituent 4MB extents between a dedup pool and a hot base
pool.

As such, rbd operations (including class operations and snapshots)
must have the same observable results regardless of the current
status of the object.

Moreover, tiering/dedup operations must interleave with rbd operations
without changing the result.

Thus, here is a sketch of how I'd expect a tiering agent to perform
basic operations:

* Demote cold rbd chunk to slow pool:

  1. Read object, noting current user_version.
  2. In memory, run CDC implementation to fingerprint object.
  3. Write out each resulting extent to an object in the cold pool
     using the CAS class.
  4. Submit operation to base pool:

     * ASSERT_VER with the user version from the read to fail if the
       object has been mutated since the read.
     * SET_CHUNK for each of the extents to the corresponding object
       in the base pool.
     * EVICT_CHUNK for each extent to free up space in the base pool.
       Results in each chunk being marked MISSING.

  RBD users should then either see the state prior to the demotion or
  subsequent to it.

  Note that between 3 and 4, we potentially leak references, so a
  periodic scrub would be needed to validate refcounts.

* Promote cold rbd chunk to fast pool.

  1. Submit TIER_PROMOTE

For clones, all of the above would be identical except that the
initial read would need a LIST_SNAPS to determine which clones exist
and the PROMOTE or SET_CHUNK/EVICT operations would need to include
the cloneid.

RadosGW
-------

For reads, RadosGW could operate as RBD above relying on the manifest
machinery in the OSD to hide the distinction between the object being
dedup'd or present in the base pool

For writes, RadosGW could operate as RBD does above, but it could
optionally have the freedom to fingerprint prior to doing the write.
In that case, it could immediately write out the target objects to the
CAS pool and then atomically write an object with the corresponding
chunks set.

Status and Future Work
======================

At the moment, initial versions of a manifest data structure along
with IO path support and rados control operations exist.  This section
is meant to outline next steps.

At a high level, our future work plan is:

- Cleanups: Address immediate inconsistencies and shortcomings outlined
  in the next section.
- Testing: Rados relies heavily on teuthology failure testing to validate
  features like cache/tiering.  We'll need corresponding tests for
  manifest operations.
- Snapshots: We want to be able to deduplicate portions of clones
  below the level of the rados snapshot system.  As such, the
  rados operations below need to be extended to work correctly on
  clones (e.g.: we should be able to call SET_CHUNK on a clone, clear the
  corresponding extent in the base pool, and correctly maintain osd metadata).
- Cache/tiering: Ultimately, we'd like to be able to deprecate the existing
  cache/tiering implementation, but to do that we need to ensure that we
  can address the same use cases.


Cleanups
--------

The existing implementation has some things that need to be cleaned up:

* SET_REDIRECT: Should create the object if it doesn't exist, otherwise
  one couldn't create an object atomically as a redirect.
* SET_CHUNK:

  * Appears to trigger a new clone as user_modify gets set in
    do_osd_ops.  This probably isn't desirable, see Snapshots section
    below for some options on how generally to mix these operations
    with snapshots.  At a minimum, SET_CHUNK probably shouldn't set
    user_modify.
  * Appears to assume that the corresponding section of the object
    does not exist (sets FLAG_MISSING) but does not check whether the
    corresponding extent exists already in the object.  Should always
    leave the extent clean.
  * Appears to clear the manifest unconditionally if not chunked,
    that's probably wrong.  We should return an error if it's a
    REDIRECT ::

	case CEPH_OSD_OP_SET_CHUNK:
	  if (oi.manifest.is_redirect()) {
	    result = -EINVAL;
	    goto fail;
	  }


* TIER_PROMOTE:

  * SET_REDIRECT clears the contents of the object.  PROMOTE appears
    to copy them back in, but does not unset the redirect or clear the
    reference. This violates the invariant that a redirect object
    should be empty in the base pool.  In particular, as long as the
    redirect is set, it appears that all operations will be proxied
    even after the promote defeating the purpose.  We do want PROMOTE
    to be able to atomically replace a redirect with the actual
    object, so the solution is to clear the redirect at the end of the
    promote.
  * For a chunked manifest, we appear to flush prior to promoting.
    Promotion will often be used to prepare an object for low latency
    reads and writes, accordingly, the only effect should be to read
    any MISSING extents into the base pool.  No flushing should be done.

* High Level:

  * It appears that FLAG_DIRTY should never be used for an extent pointing
    at a dedup extent.  Writing the mutated extent back to the dedup pool
    requires writing a new object since the previous one cannot be mutated,
    just as it would if it hadn't been dedup'd yet.  Thus, we should always
    drop the reference and remove the manifest pointer.

  * There isn't currently a way to "evict" an object region.  With the above
    change to SET_CHUNK to always retain the existing object region, we
    need an EVICT_CHUNK operation to then remove the extent.


Testing
-------

We rely really heavily on randomized failure testing.  As such, we need
to extend that testing to include dedup/manifest support as well.  Here's
a short list of the touchpoints:

* Thrasher tests like qa/suites/rados/thrash/workloads/cache-snaps.yaml

  That test, of course, tests the existing cache/tiering machinery.  Add
  additional files to that directory that instead setup a dedup pool.  Add
  support to ceph_test_rados (src/test/osd/TestRados*).

* RBD tests

  Add a test that runs an rbd workload concurrently with blind
  promote/evict operations.

* RadosGW

  Add a test that runs a rgw workload concurrently with blind
  promote/evict operations.


Snapshots
---------

Fundamentally, I think we need to be able to manipulate the manifest
status of clones because we want to be able to dynamically promote,
flush (if the state was dirty when the clone was created), and evict
extents from clones.

As such, the plan is to allow the object_manifest_t for each clone
to be independent.  Here's an incomplete list of the high level
tasks:

* Modify the op processing pipeline to permit SET_CHUNK, EVICT_CHUNK
  to operation directly on clones.
* Ensure that recovery checks the object_manifest prior to trying to
  use the overlaps in clone_range.  ReplicatedBackend::calc_*_subsets
  are the two methods that would likely need to be modified.

See snaps.rst for a rundown of the librados snapshot system and osd
support details.  I'd like to call out one particular data structure
we may want to exploit.

The dedup-tool needs to be updated to use LIST_SNAPS to discover
clones as part of leak detection.

An important question is how we deal with the fact that many clones
will frequently have references to the same backing chunks at the same
offset.  In particular, make_writeable will generally create a clone
that shares the same object_manifest_t references with the exception
of any extents modified in that transaction.  The metadata that
commits as part of that transaction must therefore map onto the same
refcount as before because otherwise we'd have to first increment
refcounts on backing objects (or risk a reference to a dead object)
Thus, we introduce a simple convention: consecutive clones which
share a reference at the same offset share the same refcount.  This
means that a write that invokes make_writeable may decrease refcounts,
but not increase them.  This has some conquences for removing clones.
Consider the following sequence ::

  write foo [0, 1024)
  flush foo ->
    head: [0, 512) aaa, [512, 1024) bbb
    refcount(aaa)=1, refcount(bbb)=1
  snapshot 10
  write foo [0, 512) ->
    head:               [512, 1024) bbb
    10  : [0, 512) aaa, [512, 1024) bbb
    refcount(aaa)=1, refcount(bbb)=1
  flush foo ->
    head: [0, 512) ccc, [512, 1024) bbb
    10  : [0, 512) aaa, [512, 1024) bbb
    refcount(aaa)=1, refcount(bbb)=1, refcount(ccc)=1
  snapshot 20
  write foo [0, 512) (same contents as the original write)
    head:               [512, 1024) bbb
    20  : [0, 512) ccc, [512, 1024) bbb
    10  : [0, 512) aaa, [512, 1024) bbb
    refcount(aaa)=?, refcount(bbb)=1
  flush foo
    head: [0, 512) aaa, [512, 1024) bbb
    20  : [0, 512) ccc, [512, 1024) bbb
    10  : [0, 512) aaa, [512, 1024) bbb
    refcount(aaa)=?, refcount(bbb)=1, refcount(ccc)=1

What should be the refcount for aaa be at the end?  By our
above rule, it should be two since the two aaa refs are not
contiguous.  However, consider removing clone 20 ::

  initial:
    head: [0, 512) aaa, [512, 1024) bbb
    20  : [0, 512) ccc, [512, 1024) bbb
    10  : [0, 512) aaa, [512, 1024) bbb
    refcount(aaa)=2, refcount(bbb)=1, refcount(ccc)=1
  trim 20
    head: [0, 512) aaa, [512, 1024) bbb
    10  : [0, 512) aaa, [512, 1024) bbb
    refcount(aaa)=?, refcount(bbb)=1, refcount(ccc)=0

At this point, our rule dictates that refcount(aaa) is 1.
This means that removing 20 needs to check for refs held by
the clones on either side which will then match.

See osd_types.h:object_manifest_t::calc_refs_to_drop_on_removal
for the logic implementing this rule.

This seems complicated, but it gets us two valuable properties:

1) The refcount change from make_writeable will not block on
   incrementing a ref
2) We don't need to load the object_manifest_t for every clone
   to determine how to handle removing one -- just the ones
   immediately preceeding and suceeding it.

All clone operations will need to consider adjacent chunk_maps
when adding or removing references.

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

* Use exiting existing features regarding the cache flush policy such as
  histset, age, ratio.
  - hitset
  - age, ratio, bytes

* Add tiering-mode to manifest-tiering.
  - Writeback
  - Read-only


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


FLAG_DIRTY at this time can happen if an extent with a fingerprint
is written.  This should be changed to drop the fingerprint instead.


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
  2. Cache when tier_promote is called (redirect will be cleared at this time).

* set-chunk 

  set the chunk-offset in a source_object to make a link between it and a 
  target_object. ::

        void set_chunk(uint64_t src_offset, uint64_t src_length, const IoCtx& tgt_ioctx,
                   std::string tgt_oid, uint64_t tgt_offset, int flag = 0);
  
        rados -p base_pool set-chunk <source_object> <offset> <length> --target-pool 
         <caspool> <target_object> <target-offset> 

  Returns ENOENT if the object does not exist (TODO: why?)
  Returns EINVAL if the object already is a redirect.
  Returns EINVAL if on ill-formed parameter buffer.
  Returns ENOTSUPP if existing mapped chunks overlap with new chunk mapping.

  Takes references to targets as part of operation, can possibly leak refs
  if the acting set resets and the client dies between taking the ref and
  recording the redirect.

  Truncates object, clears omap, and clears xattrs as a side effect.

  This operation is not a user mutation and does not trigger a clone to be created.

  TODO: SET_CHUNK appears to clear the manifest unconditionally if it's not chunked. ::

       if (!oi.manifest.is_chunked()) {
         oi.manifest.clear();
       }

* evict-chunk

  Clears an extent from an object leaving only the manifest link between
  it and the target_object. ::

        void evict_chunk(
	  uint64_t offset, uint64_t length, int flag = 0);
  
        rados -p base_pool evict-chunk <offset> <length> <object>

  Returns EINVAL if the extent is not present in the manifest.

  Note: this does not exist yet.


* tier-promote 

  promotes the object ensuring that subsequent reads and writes will be local ::

        void tier_promote();

        rados -p base_pool tier-promote <obj-name> 

  Returns ENOENT if the object does not exist

  For a redirect manifest, copies data to head.

  TODO: Promote on a redirect object needs to clear the redirect.

  For a chunked manifest, reads all MISSING extents into the base pool,
  subsequent reads and writes will be served from the base pool.

  Implementation Note: For a chunked manifest, calls start_copy on itself.  The
  resulting copy_get operation will issue reads which will then be redirected by
  the normal manifest read machinery.

  Does not set the user_modify flag.

  Future work will involve adding support for specifying a clone_id.

* unset-manifest

  unset the manifest info in the object that has manifest. ::

        void unset_manifest();

        rados -p base_pool unset-manifest <obj-name>

  Clears manifest chunks or redirect.  Lazily releases references, may
  leak.

  do_osd_ops seems not to include it in the user_modify=false whitelist,
  and so will trigger a snapshot.  Note, this will be true even for a
  redirect though SET_REDIRECT does not flip user_modify.  This should
  be fixed -- unset-manifest should not be a user_modify.

* tier-flush

  flush the object which has chunks to the chunk pool. ::

        void tier_flush();

        rados -p base_pool tier-flush <obj-name>

  Included in the user_modify=false whitelist, does not trigger a clone.

  Does not evict the extents.


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

