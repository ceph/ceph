# LSVD - Log-Structured Virtual Disk

Original paper [here](https://dl.acm.org/doi/10.1145/3492321.3524271)

This is a re-write with a number of differences:
- supported backends are RADOS and local files (for debugging), no S3 support yet
- user-space implementation, with no kernel component. Among other things, no need for individual partitions for the cache, since you can use a regular file
- reasonable metadata with versions, IDs, extensibility - the previous version was grad student code...

It's not quite as fast as the the version in the paper, but on my old machines (IvyBridge) and Ceph 17.2.0 I'm getting about 100K write IOPS against an erasure-coded pool on SATA SSDs with moderate CPU load on the backend (~30% CPU * 32 OSDs) vs maybe 5x that for half as many IOPS with straight RBD and a replicated pool.

Note that although individual disk performance is important, the main goal is to be able to support higher aggregate client IOPS against a given backend OSD pool.

## what's here

This builds `liblsvd.so`, which provides most of the basic RBD API; you can use `LD_PRELOAD` to use this in place of RBD with `fio`, KVM/QEMU, and a few other tools. It also includes some tests and tools described below.

## stability

As of Dec 22 it still suffers from occasional lost completions - I did a download and full build of Ceph on a 16-VCPU VM, and I had to restart QEMU half a dozen times when dropped I/Os caused hung processes.

## Configuration

LSVD is not yet merged into the Ceph configuration framework, and uses its own system. 
It reads from a configuration file (`lsvd.conf` or `/usr/local/etc/lsvd.conf`) or from environment variables of the form `LSVD_<NAME>`, where NAME is the upper-case version of the config file variable.
Default values can be found in `config.h`

Parameters are:

- `batch_size`, `LSVD_BATCH_SIZE`: size of objects written to the backend, in bytes (K/M recognized as 1024, 1024\*1024). Default: 8MiB
- `wcache_batch`: write cache batching (see below)
- `wcache_chunk': maximum size of atomic write, in bytes - larger writes will be split and may be non-atomic.
- `cache_dir` - directory used for cache file and GC temporary files. Note that `lsvd_imgtool` can format a partition for cache and symlink it into this directory, although the performance improvement seems limited.
- `xlate_window`: max writes (i.e. objects) in flight to the backend. Note that this value is coupled to the size of the write cache, which must be big enough to hold all outstanding writes in case of a crash.
- `hard_sync` (untested): "flush" forces all batched writes to the backend.
- `backend`: "file" or "rados" (default rados). The "file" backend is for testing only
- `cache_size` (bytes, K/M/G): total size of the cache file. Currently split 1/3 write, 2/3 read. Ignored if the cache file already exists.
- `ckpt_interval` N: limits the number of objects to be examined during crash recovery by flushing metadata every N objects.
- `flush_msec`: timeout for flushing batched writes
- `gc_threshold` (percent): described below

Typically the only parameters that need to be set are `cache_dir` and `cache_size`.
Parameters may be added or removed as we tune things and/or figure out how to optimize at runtime instead of bothering the user for a value.

## Using LSVD with fio and QEMU

First create a volume:
```
build$ sudo bin/lsvd_imgtool --create --rados \
	--cache-dir=/mnt/nvme/lsvd --size=20g ec83b_pool/ubuntu_3
```

Then you can start QEMU:
```
build$ sudo env LSVD_CACHE_DIR=/mnt/nvme/lsvd \
         LSVD_CACHE_SIZE=1000m \
         LD_PRELOAD=$PWD/lib/liblsvd.so \
		 LD_LIBRARY_PATH=$PWD/lib \
         XAUTHORITY=$HOME/.Xauthority \
   qemu-system-x86_64 -m 1024 -cdrom whatever.iso \
    -blockdev '{"driver":"rbd","pool":"ec83b_pool",
	           "image":"ec83b_pool/ubuntu_3",
               "server":[{"host":"10.1.0.4","port":"6789"}],
               "node-name":"libvirt-2-storage","auto-read-only":true,
                "discard":"unmap"}' \
	-device virtio-blk,drive=libvirt-2-storage \
    -device e1000,netdev=net0 \
	-netdev user,id=net0,hostfwd=tcp::5555-:22 \
    -k en-us -machine accel=kvm -smp 2 -m 1024
```

Or you can fun `fio` like this:
```
build$ cat > rbd-w.fio <<EOF
[global]
ioengine=rbd
clientname=admin
pool=ec83b_pool
rbdname=ec83b_pool/fio-target
busy_poll=0

rw=randwrite
#rw=randread
bs=4k
runtime=100
time_based
direct=1

[rbd_lsvd]
iodepth=128
EOF

build$ sudo bin/lsvd_imgtool --create --rados \
	--cache-dir=/mnt/nvme/lsvd --size=10g ec83b_pool/fio-target

build$ sudo env LD_PRELOAD=$PWD/lib/liblsvd.so \
    LSVD_CACHE_DIR=/mnt/nvme/test fio rbd-w.fio
```

Note that FIO random write throughput varies widely depending on which version you use.
The 2.38-1 package in Ubuntu 22.04 performs fairly poorly; when compiled with default options it does a lot better, and better still if you build it without `rbd_poll` support. 
(it's not a config option, so you have to hack things)

Do not use multiple fio jobs on the same image - currently there's no protection and they'll stomp all over each other. 
RBD performs horribly in that case, but AFAIK it doesn't compromise correctness.

[TODO - use RADOS locking to prevent multiple opens, maybe use refcounts to allow multiple threads to "open" and access the same image]

## Image and object names

Currently the name of an image is *pool/name*; LSVD will ignore the ioctx passed in the API and open the pool named in the image name. [TODO]

Given an image name of `mypool/disk_x`, LSVD stores a *superblock* in the object `disk_x`, and a series of *data objects* and *checkpoint objects* in `disk_x.00000001`, etc. using hexadecimal 32-bit sequence numbers. 

- superblock - holds volume size, clone linkage, [TBD snapshot info], and clean shutdown info / pointer to roll-forward point.
- data objects - data blocks and metadata (i.e. list of LBAs) for log recovery
- checkpoints - full map, persisted at shutdown and periodically to limit the amount of log recovery needed after a crash

[TODO - use a larger sequence number - at 100K IOPS, 2^32 gives us a bit less than 3 years]

[TODO - checkpoints can be large for large volumes, although we don't have good data points for realistic workloads. (The GC does defragmentation, and the map uses length encoded extents, so unless you run pure random writes it's hard to calculate the map size) 
We probably need to split the checkpoint into multiple objects and write it incrementally to handle multi-TB volumes]

## How it works

There's a more detailed (although possibly out-of-date) description of the internals in [review-notes.md](review-notes.md)

Writes: Incoming writes are optionally batched, if there's a queue for the NVMe device, and written in journal records to NVMe from within the calling thread.
A libaio completion thread passes the data to the translation layer, where it gets copied into a batch buffer, and then notifies callers.
Full batches are tossed on a queue for another thread, as lock delays in the translation layer tend to be a lot longer than in the write cache. 
They get written out and the new LBA to object/offset mapping is recorded in the object map.

Reads: this is more complicated, and described in some detail in [review-notes.md](review-notes.md).
We iterate over the requested address range, finding extents which are resident in the write cache, passing any other mapped extents to the read cache, and zeroing any unmapped extents, aggregating all the individual requests we've broken it up into.
Then we launch all the requests and wait for them to complete.

Completions come from the write cache libaio callback thread, the read cache callback thread, or RADOS aio notification.

(note that heavily fragmented reads are atypical, but of course have to be handled properly for correctness)

## Garbage collection

LSVD currently uses a fairly straightforward greedy garbage collector, selecting objects with utilization (i.e. fraction of live sectors) below a certain threshold and cleaning them.
This threshold can be set (as a percent, out of 100) with the `gc_threshold` parameter.
Higher values will reduce wasted space, while lower values will decrease 
As a rough guide, overall volume utilization will be halfway between this threshold and 100% - e.g. a value of 60 should give an overall utilization of about 80%, or a space expansion of 25% (i.e. 100/80). 

Raising this value above about 70 is not recommended, and we would suggest instead using lower-rate erasure codes - e.g. an 8,2 code plus 25% overhead (i.e. gc=60) gives an overall space expansion of 1.56x, vs 3x for triple replication.

The GC algorithm runs in fairly large batches, and stores data in a temporary file in the cache directory rather than buffering it in memory.
At the moment we don't have any controls on the size of this file or mechanism for limiting it, although in practice it's been reasonably small compared to the cache itself. [TODO]

## Cache consistency

The write cache can't be decoupled from the virtual disk, but there are a number of mechanisms to ensure consistency and some parameters to tweak them.

1. Write ordering - write order is maintained through to the backend, which is important for some of the recovery mechanisms below
1. Post-crash cache recovery - the write cache is a journal, and after recovery we replay any writes which didn't make it to the backend.
1. Cache loss - *prefix consistency* is maintained - if any write is visible in the image, all writes preceding that are visible. This ensures file system consistency, but may lose data.
1. Write atomicity - writes up to 2MB are guaranteed to be atomic. Larger ones are split into 2MB chunks, and it's possible that the tail of such a sequence could be lost while the first chunks are recorded. 

The `hard_flush` configuration option will cause flush operations to push all data to the backend, at a substantial loss in performance for sync-heavy workloads. Alternately the `flush_msec` option (default 2s) can be used to bound the duration of possible data loss.

Note that `lsvd_crash_test` simulates crashes by killing a subprocess while it's writing, and optionally deletes the cache before recovery.
To verify consistency writes are stamped with sequence numbers, and the test finds the last sequence number present in the image and verifies that all preceding writes are fully present as well.

## Erasure code notes

The write logic creates large objects (default 8MB), so it works pretty well on pretty much any pool.
The read cache typically fetches 64K blocks, so there may be a bit of extra load on the backend if you use the default 4KB stripe size instead of an  erasure code with a stripe size of 64K; however I haven't really tested how much of a difference this makes.
Most of the testing to date has been with an 8,3 code with 64K stripe size.

## Tools
`lsvd_imgtool` mostly just calls the LSVD versions of `rbd_create` and `rbd_remove`, although it can also format a cache file (e.g. if you're using a raw partition) 
```
build$ bin/lsvd_imgtool --help
Usage: lsvd_imgtool [OPTION...]
IMAGE

  -C, --create               create image
  -d, --cache-dir=DIR        cache directory
  -D, --delete               delete image
  -I, --info                 show image information
  -k, --mkcache=DEV          use DEV as cache
  -O, --rados                use RADOS
  -z, --size=SIZE            size in bytes (M/G=2^20,2^30)
  -?, --help                 Give this help list
      --usage                Give a short usage message
```

Other tools live in the `tools` subdirectory - see the README there for more details.

## Tests

There are two tests included: `lsvd_rnd_test` and `lsvd_crash_test`. 
They do random writes of various sizes, with random data, and each 512-byte sector is "stamped" with its LBA and a sequence number for the write.
CRCs are saved for each sector, and after a bunch of writes we read everything back and verify that the CRCs match.

## `lsvd_rnd_test`

```
build$ bin/lsvd_rnd_test --help
Usage: lsvd_rnd_test [OPTION...] RUNS

  -c, --close                close and re-open
  -d, --cache-dir=DIR        cache directory
  -D, --delay                add random backend delays
  -k, --keep                 keep data between tests
  -l, --len=N                run length
  -O, --rados                use RADOS
  -p, --prefix=PREFIX        object prefix
  -r, --reads=FRAC           fraction reads (0.0-1.0)
  -R, --reverse              reverse NVMe completion order
  -s, --seed=S               use this seed (one run)
  -v, --verbose              print LBAs and CRCs
  -w, --window=W             write window
  -x, --existing             don't delete existing cache
  -z, --size=S               volume size (e.g. 1G, 100M)
  -Z, --cache-size=N         cache size (K/M/G)
  -?, --help                 Give this help list
      --usage                Give a short usage message
```

Unlike the normal library, it defaults to storing objects on the filesystem; the image name is just the path to the superblock object (the --prefix argument), and other objects live in the same directory.
If you use this, you probably want to use the `--delay` flag, to have object read/write requests subject to random delays.
It creates a volume of --size bytes, does --len random writes of random lengths, and then reads it all back and checks CRCs. 
It can do multiple runs; if you don't specify --keep it will delete and recreate the volume between runs. 
The --close flag causes it to close and re-open the image between runs; otherwise it stays open.

### `lsvd_rnd_test`

This is pretty similar, except that does the writes in a subprocess which kills itself with `_exit` rather than finishing gracefully, and it has an option to delete the cache before restarting.

This one needs to be run with the file backend, because some of the test options crash the writer, recover the image to read and verify it, then restore it back to its crashed state before starting the writer up again.

It uses the write sequence numbers to figure out which writes made it to disk before the crash, scanning all the sectors to find the highest sequence number stamp, then it veries that the image matches what you would get if you apply all writes up to and including that sequence number.

```
build$ bin/lsvd_crash_test --help
Usage: lsvd_crash_test [OPTION...] RUNS

  -2, --seed2                seed-generating seed
  -d, --cache-dir=DIR        cache directory
  -D, --delay                add random backend delays
  -k, --keep                 keep data between tests
  -l, --len=N                run length
  -L, --lose-writes=N        delete some of last N cache writes
  -n, --no-wipe              don't clear image between runs
  -o, --lose-objs=N          delete some of last N objects
  -p, --prefix=PREFIX        object prefix
  -r, --reads=FRAC           fraction reads (0.0-1.0)
  -R, --reverse              reverse NVMe completion order
  -s, --seed=S               use this seed (one run)
  -S, --sleep                child sleeps for debug attach
  -v, --verbose              print LBAs and CRCs
  -w, --window=W             write window
  -W, --wipe-cache           delete cache on restart
  -x, --existing             don't delete existing cache
  -z, --size=S               volume size (e.g. 1G, 100M)
  -Z, --cache-size=N         cache size (K/M/G)
  -?, --help                 Give this help list
      --usage                Give a short usage message
```
