=================================
 The Logical Address in SeaStore
=================================

The laddr (LBA btree) is the central part of the extent locating
process. The legacy implementation of laddr allocation, however, has
some defects, such as address exhaustion, inefficient cloning process,
and so on. This document proposes a new design to address the above
issues.

Defects of Legacy Laddr Hint
============================

One logical extent is normally derived from an onode, so to allocate a
laddr, we need to use the information of the corresponding onode to
construct a laddr hint and use it to search the LBA btree to find a
suitable laddr. (Not all logical extents are related to the onode, but
these are only a small part of the total logical extents, so we could
ignore them for now).

The legacy laddr hint is constructed as (note the each laddr represents
one 4KiB physical blocks on the disk):

::

   [shard:8][pool:8][crush:32][zero:16]

Each pair of square brackets represents a property stored within the
laddr integer. Each property contains a name and the number of bits it
uses.

When the laddr allocation conflicts, the strategy is to search the
free space backwards from hint linearly.

This laddr layout and conflict policy has some problems:

#. The pool field is short; pools with id above 127 are used for
   temporary recovery data by convention, which causes normal object
   data to be mixed with temporary recovery data, and when the pool id
   is larger than 255, data from different pools are mixed together.
#. The shard field is long; we are unlikely to use more than 128
   shards for EC.
#. The snap info is missing; when taking a snapshot for an onode,
   their hint will be the same, which causes the inefficient cloning
   process.
#. A hint could represent a 256MiB logical address space, which
   indicates that when its snapshots' count is larger than 16 (each
   onode reserves 16MiB space, without considering metadata extents
   under the same onode), it will conflict with other onodes' laddr
   hints.
#. The most significant bits in the crush field are pg id, which makes
   the distribution of laddr uniform at the pool level, but on each
   OSD, the laddr's distribution is sparse. The remaining bits will
   make conflict more frequent when pg id grows.

Considering these issues, we can conclude that 64-bit length is
insufficient if we want to maintain semantic information within laddr
(i.e., objects under the same pg share the same laddr prefix).

128-bit static layout laddr design
==================================

laddr uses a 128-bit integer to represent the address value
internally.  Besides the basic properties inherited from integers,
such as strong ordering and arithmetic operations, certain types of
laddr also include properties derived from user data (RADOS objects).
The static layout design ensures these properties are deterministic
and predictable, allowing further optimizations based on laddr.

Overview
--------

The layout of laddr consists of three parts:

::

   [upgrade:1][object_info:76][object_content:51]

When the object info for different objects does not conflict within an
OSD, we can obtain a useful property: each RADOS object and its
head/clone can have a unique laddr prefix within an OSD.

Based on this property, we can:

#. Group different snapshots of a RADOS object under the same laddr
   prefix, which could speed up the cloning process.
#. If the RADOS object data/metadata of a head/clone share the same
   prefix, removing a head/clone will also be possible via range
   deletion.
#. Track frequently accessed objects using laddr without keeping the
   full object name.

Object Info
-----------

The definition of this property is:

::

   [shard:6][pool:12][reverse_hash:32][local_object_id:26]

The shard, pool, and reverse hash come from the information of the
RADOS object. The local object id is a random number to identify a
unique object within seastore. Two different RADOS objects should
never share the same reverse hash and local object id within a pool,
which is handled by the allocation process.

All bits set to 1 in the object info represent the invalid prefix, and
RADOS logical extents must not use it to avoid sharing the same prefix
with ``L_ADDR_NULL``.

There are two special rules for global metadata logical extents:

#. RootMetaBlock and CollectionNode: When a laddr is used to represent
   these two types of extents, all object info bits should be zero,
   and the RADOS object data should never use this prefix.
#. SeastoreNodeExtent: Since this type of extent is used to store the
   ghobject, its hint is derived from the first slot of the ghobject
   it stores, then the local object id and local clone id (see below)
   should be zero. The RADOS object data should not use this prefix
   either. NOTE: It's possible that shard, pool, and hash are zero; we
   allow them to mix with RootBlock and CollectionNode.

This layout allows:

-  2\ :sup:`12`\ =4096 pools per cluster
-  2\ :sup:`6`-1=63 shards per pool for EC
-  2\ :sup:`16`\ =65536 pgs per pool and OSD (the most significant 16
   bits in reversed hash are represented as pg id internally)
-  2\ :sup:`42`\ =4T objects per pg (the remaining 16 bits of hash + 26
   bits object id)

Object Content
--------------

Global metadata logical extents use these bits as block address
directly, while the RADOS objects further divide these bits into:

::

   [local_clone_id:23][is_metadata:1][blocks:27]

Like the local object id, each clone/snapshot of a RADOS object has a
unique local clone id under the same object laddr prefix. When
creating a new snapshot, take a random local clone id as the new base
address for the snap object. As mentioned above, the local clone id
bits must not be all 1.

The indirect mapping of clone objects can only store the local clone
id of its intermediate key; see ``pladdr_t::build_laddr()``.

The remaining 28 bits are used to represent the address of concrete
data extents. Each address represents one 4KiB block on disk.

``is_metadata`` is true indicates the remaining bits represent the
address of an Omap*Node. Take a random value as the address when
allocating a new omap extent.

It's possible for OMapInnerNode to store only the block offset field
in each extent rather than the full 128-bit laddr to locate its
children, which would increase the fan-out of the OMap inner node.

When ``is_metadata`` is false, the remaining bits represent the
address of ObjectDataBlock.

This layout allows:

#. 2\ :sup:`23`\ =8M clones per object
#. 2\ :sup:`27`\ =128M blocks per clone of an object (128M \* 4KiB =
   512GiB)

Conflict ratio
--------------

The allocation of local object id, local clone id, and metadata blocks
requires random selection for now. We expect the success ratio to be
~90% so that address allocation won't cause performance issues; a 90%
success ratio means:

-  objects per pg < 400G
-  clones per object < 800K
-  metadata of object < 50GiB

Upgrade
-------

This bit is reserved for layout updates.

If the layout of laddr changes in the future, this bit will be used to
transition addresses from the old layout to the new layout.

TODO: Implement fsck process to support layout upgrades.

Summary
-------

-  For RootMetaBlock and CollectionNode:

   ::

      [upgrade:1][all_zero:76][offset:51]

-  For SeastoreNodeExtent:

   ::

      [upgrade:1][shard:6][pool:12][reverse_hash:32][zero:26][offset:51]

-  For RADOS extents (OMapInnerNode, OmapLeafNode, and ObjectDataBlock):

   ::

      [upgrade:1][shard:6][pool:12][reverse_hash:32][local_object_id:26][local_clone_id:23][is_metadata:1][blocks:27]

   local object id is non-zero.
