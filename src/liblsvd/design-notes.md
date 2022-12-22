# new-lsvd

### 3/23/22

- the whole idea of putting checkpoints inline with the rest of the write cache may be flawed. Maybe we should reserve a section - worst case is 20B for 8KB. (well, worst case is a bit more than that, since in theory we could have a bunch of single-sector writes. Worst is actually 12*8+8 = 104B per 8k, but that's still less than 2%)

### 3/21/22

- fixed the bug that was overwriting the read cache map
- added clean shutdown stuff to `rbd_close`, but now fio read seems to hang when it's done
- some cleanup of cache.py
- added some synchronization so that translate::flush actually waits until stuff has been written. (more importantly, until it's been updated in the map so we can do a checkpoint)

### 3/20/22

weird things to fix and add to tests:
- write cache is overwriting the read cache map, I think, with PAD record.
- read cache only registers 200 misses, but replacing the cache line fetch logic with straight read doubles the throughput
- not getting misses despite random read of 1GB volume with 100MB cache

## stuff
thread pool:

took a stab at using https://github.com/DeveloperPaul123/thread-pool,
which seems to be a nice header-only thread pool, but it's a pain in
the ass to compile.

Instead think I'll use:
https://github.com/fbastos1/thread_pool_cpp17

from
https://codereview.stackexchange.com/questions/221626/c17-thread-pool

# basic design

Need to implement lots of the functions from [librbd.h](https://github.com/ceph/ceph/blob/master/src/include/rbd/librbd.h)

This seems to be a list of most of the methods that we should implement - note that in a lot of cases we probably have to figure out which version (e.g. create/create2/create3/...) the tools are using

- `rbd_list2` - list images
- `rbd_create`, `rbd_create2`, `rbd_create3`, `rbd_create4` - various ways of creating image, we should pick one?
- `rbd_clone`, `rbd_clone2`, `rbd_clone3` - create a clone; again, pick 1?
- `rbd_remove`
- `rbd_open`, `rbd_open_by_id`, `rbd_aio_open`, `rbd_aio_open_by_id`
- `rbd_close`, `rbd_aio_close`
- `rbd_get_size` - [maybe]
- `rbd_copy`, `rbd_copy2` .. 4 - [maybe]
- `rbd_snap_list`, `rbd_snap_list_end`, `rbd_snap_exists`
- `rbd_snap_create`, `rbd_snap_create2`
- `rbd_snap_remove`, `rbd_snap_remove2`, `rbd_snap_remove_by_id`
- `rbd_snap_rollback`
- `rbd_list_children` - not sure we can do this (list children of clones)
- `rbd_read`, `rbd_read2`, `rbd_read_iterate`, `rbd_read_iterate2`
- `rbd_write`, `rbd_write2`
- `rbd_discard`, `rbd_aio_discard`
- `rbd_aio_write`, `rbd_aio_write2`, `rbd_aio_writev`
- `rbd_aio_read`, `rbd_aio_read2`, `rbd_aio_readv`
- `rbd_aio_create_completion`, `rbd_aio_is_complete`, `rbd_aio_wait_for_complete`, `rbd_aio_get_return_value`, `rbd_aio_release`
- `rbd_flush`, `rbd_aio_flush`


### Phases of development

1. basic read/write functionality, with working garbage collection. Image creation via external utility

2. logging of journal map, local journal recovery

3. incremental checkpointing of backend map, failure recovery.

4. clone - read/write support in library, creation via external utility

5. snapshot - creation by external utility while volume is quiesced, add deferred GC delete to library

6. fancy GC / defragmentation

7. roll snap/clone external support into the library??? not sure how important this is

### Journal and local cache format

Need to specify detailed format of the journal:

- individual records
- allocation (bitmap?)
- how do incoming writes and cached reads go together?
- eviction algorithm
- map persistence
- crash recovery process

Note that we can do 1-ahead allocation so that we can link journal records both forwards and backwards.

Hmm, we can allocate in fairly big chunks (makes the journal header a bit more complex) which means that the bitmap can be smaller. E.g. 4KB = 32MB bitmap for 1TB, but 64KB = 2MB bitmap.

But that's also problematic, since it means we may need to do GC to free blocks, so maybe we're stuck with 32MB/TB.

### Object format / header

This needs to handle a bunch of stuff for recovery, snap&clone, incremental map persistence, etc.

I've got some notes somewhere about it.

### Software structure

Each virtual disk instance has:

- LBA -> (obj,offset) map, for all complete write batches
- LBA -> ptr map for any writes currently being batched
- (obj,offset) -> LBA map to find cached data
- current write batch (process when we hit size or timeout limit)
- outstanding writes to backend
- outstanding reads to backend

I've been thinking that there's a single write batch buffered in memory. That means when it fills we need to atomically:
- sort and merge it
- assign an object number, put extents into LBA->(obj,offset) map
- wait for NVMe writes to complete
- set new empty buffer, map

We need NVMe writes to complete, since the only way here to access new data after it's no longer in the newest buffer is to get it from the NVMe.

Actually I think it would be better to have a list of buffer/map pairs, and we can remove a pair from the list and recycle them when all their NVMe writes complete.

We don't need to worry about new writes after that point, since they should last in the journaled cache until after they've been successfully written to back-end S3.

So the order of checking things for a read is:
- incoming buffer+map (LBA->ptr)
- list of buffer+map with NVMe writes pending (LBA->ptr)
- backend map (LBA->(obj,offset))
    - now check cache map (obj,offset)->LBA
        if found, read from cache; otherwise fetch remote

Incoming buffer+map needs a reader/writer lock, as it's shared between reads and writes. The buffer+map pairs waiting on NVMe write are read-only, but we need a reader/writer lock on the list so that they don't get deleted while we're looking at them.

The backend map and cache maps can both have single reader/writer locks, writes should update cache map before backend map, one at a time, for consistency.

### "Instantaneous garbage collection"

1. calculate all the data to garbage collection
2. read it in, save a copy in NVMe
3. atomically update it in the backend map and the cache map
4. write it to the back end

Step 3 will lock the backend map for a while, since we have to re-check all the calculations from steps 1 and 2, but it's still in-memory. If it's all written to NVMe cache, then step 4 can happen **after** the map update and we're still OK.

Note that the data written to NVMe might be quite cold, so once the writeback in step 4 is finished we may want to prioritize it for eviction.

### Cache replacement

The extent map implements A and D bits; D is set implicitly by update operations, while A is set explicitly by the application.

This allows us to do rolling checkpoints for the object map, as long as we don't support discard. Discard is tricker - we could probably implement it by mapping to a sentinel value, e.g. object = MAX, offset = LBA. Not sure if we should ever remove these from the map - with 24 bits for the length field, max extent size = 2GB so it won't add many extra extents. 

A single A  bit restricts us in the replacement algorithms we can use. One possibility is 2Q/CLOCK, maintaining 2 maps:

- CLOCK on first map moves cold extents to 2nd map
- CLOCK on 2nd map chooses extents for eviction
- update trims from 2nd map and inserts into first map
- lookup has to check both maps and merge the results. 

### lazy GC writes

Data objects need to have pointer to last "real" data object, so we can ignore missing GC writes

### object headers

Standard header
```
magic
version
volume UUID
object type: superblock / data / checkpoint
header len (sectors?)
data len (sectors?)
```

Superblock
```
<standard header>
volume size
number of objects?
total storage?
checkpoints: list of obj#
clone info - list of {name, UUID, seq#}
snapshots - list of {UUID, seq#}
pad to 4KB ???
```
Do we want a list of all the outstanding checkpoints? or just the last one?

Data object
```
<standard header>
active checkpoints: list of seq#
last real data object (see GC comment)
objects cleaned: list of {seq#, deleted?)
extents: list of {LBA, len, ?offset?}
pad to sector or 4KB
<data>
```

Checkpoint
```
<standard header>
active checkpoints: list of seq#
map: list of {LBA, len, seq#, offset}
objects: list of {seq#, data_sectors, live_sectors}
deferred deletes: list of {seq#, delete time}
```

### random thoughts

GC - fetch live data object-at-a-time and store it in local SSD. Note that we have to flag those locations as being temporary allocations, so they don't get marked as "allocated" in a checkpoint.

If we allocate blocks of size X (8KB, 16KB etc) then we free blocks of that size. With X>=8KB we don't need to worry about bookkeeping for headers. Freeing is complicated, because we have to check that the adjacent map entry isn't still pointing to part of the block, but at least we don't need to keep counters for each block. For now stick with 4KB, as it's probably small compared to the extent map.

(hmm, if we allocated 64KB blocks and kept 4-bit counters of the number of live 4K blocks in each 64KB, we could reduce the bitmap size by 4. Then again, just going to 16KB would do that, and we could still use bitmap ops and other good stuff)

1TB / 16KB mean extent size would be 128M extents, or 2GB RAM. Note that we can probably structure the code to merge a lot of writes going to NVMe - post the write to a list, each worker thread grabs all the writes currently on the list.

objects.cc - need an object that takes a memory buffer and can return pointers to header, other parts, and iterators for the various structures.
It should also be able to serialize a set of vectors etc. Probably need std::string versions of the data structures that have names in them.

Need a convention for how to handle buckets. I think including them in the name - "bucket/key" - is fine. Remember that this is going to also have to work for files and RADOS.

How do we structure the local storage?

superblock has
- timestamp
- remote volume UUID
- pointer to last chunk written
- <bitmap info>
- <map info>

note that temporary allocations are going to need some sort of journal header. Actually, can just flag them as such, and they get freed on replay.

Bitmap and map are stored in the same way - a count (of bits or map entries) and a list of block numbers containing the data. (if we use 4KB block numbers we can use 32 bits for everything and still handle 16TB local SSDs)

actual format in the superblock is probably
- int64 count
- int32 block count
- int32 data block 
- int32 indirect block 
- int32 double indirect block
- int32 triple indirect

Use convention that only one of data / indirect / double indirect are used. 

- data block: 4KB of data
- indirect: 1K 4KB blocks
- double: 1M 4KB blocks = 4GB
- triple: 4TB, i.e. more than enough

So maybe:

- int32 count
- int32 tree level (1, 2, 3 or 4 for direct / single..triple indirect)
- int32 block count
- int32 block

Allocate storage and write all the data for bitmap and map before writing the superblock. Keep 2 copies of the superblock, write the first synchronously before writing the 2nd. On restart take the one with the most recent timestamp.

Keep superblocks in blocks 0 and 1?

Block allocator needs to return the block number for the next one that will be allocated, so we can do forward linking for log replay.

SEQUENCE NUMBER. That's what the timestamp is - it's a counter that's incremented with every write. We need that in the journal header.

Keep the CRC in the journal header, note that we only have to check it on log replay.

Should there be a "clean" flag, or just recover every time we remount? Also should we store pointer to last chunk written, or pointer to next chunk? (which initializes the allocator if we're in the clean state)

Need a reader for the tree-structured data, so that we can iterate through it.

### actual journal format

old format, from `dm-disbd.c`
```
struct _disheader {
        uint32_t magic;
        uint32_t seq;
        uint32_t crc32;
        uint16_t n_extents;
        uint16_t n_sectors;
} __attribute__((packed));

struct _ext_local {
        uint64_t lba : 47;
        uint64_t len : 16;
        uint64_t dirty : 1;
} __attribute__((packed));

#define HDR_EXTS ((DIS_HDR_SIZE - sizeof(struct _disheader)) / sizeof(struct _ext_local))

struct dis_header {
        struct _disheader h;
        struct _ext_local extents[HDR_EXTS];
};
```

NOTE - how much performance does the CRC cost? Would a checksum be better?

Header needs to have:
- magic number
- sequence number
- (block number? verify written in the right place)
- list of blocks - need to mark as allocated on replay
- pointer to next block
- checksum / CRC. probably.
- flag for whether it contains data or not.  [more complicated...]
- list of extents

On replay we put the data into the map and flag the header blocks as free. If it's a non-data entry, it's one of several things:

- superblock-related data. if the sequence number
    - precedes the current superblock, it stays allocated and we free the header
    - is after the current superblock, we free the whole thing
- temporary storage for GC. free unconditionally
- checkpoint-related data. we wouldn't be reading it if the checkpoint were complete - free it

Need to keep track of the header blocks for data writes and checkpoint data (bitmap, map) - when the checkpoint is complete we free them. Or rather, free them in the in-memory bitmap before writing it to disk.

wait a minute, the superblock *is* the checkpoint. So any committed checkpoint will have a write frontier that is after any of its data. 

progress:
- wrote simple bitmap allocator
maybe we need a sequence number class? for journal and backend

parts of the whole thing:
- (1) queue Q1 for writes
- (2) thread pool reading from Q1 and writing to NVMe
- (3) NVMe complete posts to queue Q2
- (4) thread pool takes batch

Easiest way to do batching is after (3), NVMe completion. We haven't replied to the write yet, so it's OK for reads to return older information. This means the NVMe writer:
1. writes to NVMe
2. on write completion, copy to batch buffer and map
3. send write completion

Probably better to have write threads to NVMe rather than async I/O, as it's more likely to use multiple CPUs? It's easier, anyway.

items at the first queue:
- LBA, len
- iovec, count
- async callback info

thread takes a list of these, crafts a single NVMe write, sends it out. Maybe max I/O size? Use blktrace to see where they get split, probably 256K is OK.

block-to-LBA and block-to-offset macros.

## Possible problems

`int64_t` vs `uint64_t` - we're losing a bit of precision in a bunch of places because of this. I think we need to move to unsigned integers for the bitfields, but I'm worried about signed/unsigned problems.

- **DONE** changed to unsigned. In particular, signed bitfields of length 1 don't work very well...

extent length - what happens if we try to merge two adjacent extents that overflow the length field?
TODO - need to check for this; maybe modify the `adjacent` test so it fails on overflow.

extent length -  what happens if we try to insert a range that's too large? This should only happen if we're implementing discard with a tombstone sentinel, as we're not going to create objects large enough or allocate memory or NVMe space large enough. In that case we have to loop and insert multiple.

## Schedule

1. finish extent map, unit tests
1. basic non-GC device, using librbd? BDUS?. Need simple "mkdisk" utility. File backend?
1. superblock 
1. startup / recovery code
1. simple (full) checkpoints
1. simple GC
1. incremental checkpoints
1. clone
1. snapshots
1. backends: S3 / file / RADOS
1. better GC

How to set up unit/integration tests?
- read tests with prefab data
- write tests
- python tools for generating / parsing object data
- python driver script? Or use libcheck?

### notes on recovery algorithm

- after a crash we have to (a) recover the cache, (b) recover the back end, and then (c) find any writes in the cache which aren't present in the back end and replay them. To deal with that, the object header should include the sequence number of the newest write reflected in it, so we can rewind the cache log to that point and replay it.
- after cache recovery, there might be writes that completed somewhere beyond the end of the log. (e.g. writes to locations 1, 2, and 4 completed, we detect end of log after 2) There's a tiny chance that we could write to location 3, then crash, and then come back up and think that 4 was part of the log. There are a few ways to deal with this, but it's not very likely so think about it later. (and maybe look up what ext4/jbd does?)

### notes on GC

We need to be able to perform GC without blocking incoming reads.
- strategy 1: incoming writes check the list of LBA ranges being copied by a running GC cycle, and block if they interfere
- strategy 2: keep a list of writes during the GC copy process. When it's done, subtract those ranges from the GC map updates.

### Cache sharing

In theory multiple virtual disks based on the same clone image ought to be able to share locally cached blocks from the base image. In practice it's a bit tricky because each virtual disk is in a separate QEMU process.

Proposal - separate cache daemon for caching clone base images. Use shared memory to communicate with individual virtual disks:
- communicate via unix socket
- pass read-only file descriptor for cache partition
- cache map in shared memory - see shm\_overview(7)
- sequence number in shared memory
- clients send usage information over socket

Sequence number is even when map is valid, odd when it's being updated. To use the cache:
- check that map is valid, sequence number S
- translate with local copy of map
- fetch from cache
- check that map sequence is still S. If not, bail and fetch directly

The shared cache should update the map infrequently, and take a decent amount of time in doing so. (1 second?)

### Write and read cache

If the read cache uses 64K chunks, then 1TB/64k = 16M entries, or 64MB at 4 bytes each.
32+16 = 48 = 256TB max volume size for 4 byte entries. Note that current extent.cc has 128TB limit, although it could get bigger if I reduced the max extent length, which is currently 24 bits (= 2^33 bytes).

It wouldn't be a big deal to serialize 5-byte entries, which would give a max size of 64PB or something. If we drop the max extent length to 1GB (21 bit sector number) then we would be able to get 1PB into the object map, or 8PB if we move to 4KB block addressing. (with 8GB max chunk again - 64PB if we drop extent length to 18 bits) Of course at that point the memory usage for the object map is kind of crazy - 1PB with 1M extents is a billion entries. Actually, 24GB for the map isn't horribly excessive - that's about $100, and 1PB of bare disks costs maybe $15K.

How do we do LRU ordering or whatever? Assign order to each entry, truncate it to 8 bits, store it. Or if we're using saturating LFU or something like that, just store those values.

**Important:** need version number for map, replacement state info because it's going to change. It's probably useful to provide a small region for version-specific metadata, too. E.g.:
- length in blocks
- type
- type-specific data (32B? 64B?)
- start block <- assumes sequential allocation

For the simple map, type-specific data would only be the count of entries. (or equivalently the total length in bytes)

For write cache, journal needs entry types for checkpoint and pad

Note that we can add the write cache first and test it, then add the read cache.

**Direct I/O** - I really think we should be using direct i/o to the cache device, as otherwise we don't have any guarantees about when data goes to the NVMe drive. This means we're going to have to copy everything into aligned buffers, probably. Which in turn means that we probably don't have to copy in the translation layer, but we need some way of notifying the cache that we're done with a specific buffer.

### notes on migrating data from write cache to read cache

1. same logic as garbage collection - read journal headers and double-check against map to filter out stale data
2. how to handle sending random chunks of data to read cache indexed by object ID / offset?

Tag each block in read cache with a bitmap indicating which blocks are valid. Assume 4KB blocks, don't store any finer granularity in read cache -> 16 bits per 64KB block. Send LBA/data to read cache, which looks up obj/offset mapping and either allocates new 64K block or overwrites contents of an existing one.

Start out by using random replacement in the read cache, fix it later.

How do we tie together read cache and backend? There's something of a locking and mapping problem if we encapsulate the write cache, read cache, and translation layer separately.
- write cache - this can have its own lock, maybe a reader/write lock
- translation layer lock - again maybe a reader/writer lock?
- read cache - reader/writer?

The read cache needs read-only access to the translation layer object map. Or do we connect the read cache to the translation layer? Write cache has interface for garbage collection, returns list of LBA extents and block pointers, plus a token to pass back when the data has been copied. write cache GC sends this info to read cache, 

Solution:
- read cache and write cache each have their own mutex and map
- object map + shared\_mutex are separate and shared by read cache and translation layer.
so 4 mutexes in total.

### Read cache

The read cache keeps a bitmap of the valid pages in each block. This is basically only used when we recycle data from the write cache to the read cache - this way we can just accept the data without worrying if it makes up a full block. If we try to read from the cache block and come across an invalid page, we just fetch the whole damn block.

Note that we just check the pages that are being referenced, so it's ok if the surrounding pages in the object are garbage - we'll never check the corresponding bits, so we'll never fetch the garbage

### valgrind
To run Gdb with `valgrind`:
```
valgrind --vgdb-error=1 --vgdb=full --vgdb-stop-at=all ./unit-test
```

Example of running Python-based tests under `valgrind` with Gdb:
```
valgrind --vgdb-error=1 --vgdb=full --vgdb-stop-at=all --suppressions=valgrind-python.supp python3 test1.py
```

### more gc, as I'm writing the read cache

need a flag to rcache->add to indicate that it's low priority, also another call to check whether something's already in cache. (and to maybe lock it?) Use that for GC - after getting the list of candidate objects, go through the list (not holding map lock) and fetch any data not already in cache. 

### left to do (Mon Mar 7)

- fix header length issues **DONE**
- tests for the read cache
    - test basic add and read **DONE**
    - test eviction
    - refactor eviction for easier testing?
- log cleaner for the write cache
- garbage collector
    - some sort of priority/depriority for GC blocks?
- librbd interface

- `io_uring` implementation (wait until everything else works)
- proper eviction algorithm for read cache (maybe d-choices w/ sequence #s)
- progressive checkpoints
- snapshot
- clone

Note that we can keep a read sequence number, save its value for each cache block, and do pseudo-LRU by choosing d random blocks and taking the oldest sequence number. For now 32 bits should work - 4G / 10K IOPS = 400K seconds, close to a week. 
Later we may want to use 64 bits per block, which lets us do either a sequence number or LFUDA. (possibly a d-choices version)

TODO: add sequencing, d-choices LRU, persist the eviction status. **add fields to superblock [DONE]**

### Oh No, object offsets are broken [nvm, fixed]

If the read cache is going to use the object map directly, it either needs access to header lengths, or offsets in the map entries need to include the object headers. The problem is that I'd like to keep the in-memory access to batches in the translation layer, so that current unit tests continue to work. Maybe it can be optional based on a runtime parameter?

I think the best approach is:
- if in-memory is enabled, keep `in_mem_objects` and update the objmap immediately on write; offsets will not include header length, as the header doesn't exist yet
- otherwise just leave the extents in the batch. (write cache will have them anyway)
- when a batch is complete, (a) coalesce writes, (b) write the object, and (c) update the map, with offsets inclusive of header length

specific changes:

`translate.writev`
- add cache/nocache mode on init
- in nocache mode, add batches to `in_mem_objects`, add extents to objmap immediately. (with 0 hdrlen)

`translate.worker_thread`
- coalesce writes in batch
- update objmap, including hdrlen

`translate.read`
- don't add in hdrlen

### status

**Fri Mar 11 22:00:00 2022**

- read on a null disk seems to work on fio
- write crashes immediately with iovec problems

test setup:
```
rm /mnt/nvme/lsvd/*
dd if=/dev/zero bs=1024k count=100 of=/mnt/nvme/lsvd/SSD status=none
python3 mkdisk.py --uuid 7cf1fca0-a182-11ec-8abf-37d9345adf42 --size 1g /mnt/nvme/lsvd/obj
python3 mkcache.py --uuid 7cf1fca0-a182-11ec-8abf-37d9345adf42 /mnt/nvme/lsvd/SSD
```

**Mon Mar 7 13:50:00 2022**
- write cache write logic is done
- TODO: tail cleaning for write cache
- read cache is started
- TODO: fix nomenclature - sector=512 / page=4K / unit=64K (or cache "line"?)

**Sun Feb 27 23:55:50 2022**
- init from superblock (sort of)
- started some unit tests using python unittest framework and ctypes

**Sun Feb 27 10:03:23 2022** Basic functionality seems to be working under BDUS.
- file only, no objects yet
- starts up with empty disk - no map recovery
- no cache

Things to do now:
1. basic python unit tests
2. write coalescing, in-memory map
3. map recovery on startup
4. cache writes, change in-memory map to cache map
5. object backend

**Mon Jan 31 23:48:00 2022** object headers seem to be mostly done. Journal headers for writes are done, but need to figure out the superblock. Steps from here:
1. get read/write working with librbd interface and null operations
2. add journal writes to NVMe, with allocator and map
3. add checkpoint code
4. test recovery

After that I can start writing backend objects

```
Local Variables:
mode: Markdown
eval: (auto-fill-mode -1)
eval: (visual-line-mode 1)
End:
```
