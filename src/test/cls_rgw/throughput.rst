Direct RGW CLS throughput
=========================

``ceph_perf_cls_rgw`` is an optional live throughput companion for comparing
the native and Datum CLS bindings through real librados requests and an OSD.
Build the target explicitly in each matching Ceph build::

    cmake --build BUILD --parallel 8 --target ceph_perf_cls_rgw

Use a prepared, dedicated replicated test pool and a uniquely owned, empty
namespace. The driver creates and removes only its own objects in that
namespace; it does not create pools, deploy daemons or switch modules::

    BENCH_OSD_PID=12345 BUILD/bin/ceph_perf_cls_rgw \
      /path/to/ceph.conf cls-throughput 9 8 cls-throughput-unique-token

Arguments are configuration, pool, seconds per case, concurrent requests and
namespace. The client archive and the loaded RGW CLS module must both come
from the selected source revision. Restart the test OSD when changing its
module; replacing a shared library while it is loaded does not select it.

The fixed seven cases are a native RADOS stat control, BI lists with 1 and
1,000 entries, OLH reads with 1 and 1,000 epoch groups, single BI puts and
64-entry BI batch puts. Both write cases include 4,096 additional opaque
bytes in eight fragments per entry. Batch writes overwrite existing keys
with ``check_existing=true`` and validate stable accounting. Each writer
has its own object, preventing object-level contention between writers.
Reads use the existing list/map client interfaces used by RGW.

Preparation, complete fixture checks, eight warmup calls per worker,
post-write verification and cleanup are outside each measured interval.
Measured requests include client construction, native compatibility results,
checks of status/cardinality, result destruction, network and server work.
Output is CSV with requests/s, entries/s, process CPU per request for the
client and the one explicitly identified local OSD, and p50/p99/max latency.
Latency uses one-microsecond bins through 200ms and retains longer samples
in a separate vector, sorted after timing. Percentiles include those samples
without clipping; ``histogram_slow_count`` reports their count. The control
counts one object per operation. OLH entries/s counts epoch groups, not
individual events.

A short comparison can use A-B-B-A order with nine seconds per case: 252
timed seconds across four runs, leaving roughly 48 seconds for sequential
OSD restarts, preparation and cleanup within a five-minute target. Build time
is separate. Record the actual total wall time; slow deployment or storage
may exceed that target. Keep source, toolchain, dependencies, allocator,
concurrency, OSD settings, case order and storage layout matched. Preserve
raw samples, fixture failures, source/binary hashes and cleanup results.
Two samples per build are a quick regression check, not evidence of small
differences or statistical equivalence. Reverse the outer order in a later
comparison when more confidence is needed.

The project-local deployment wrapper emits Chunderdome's ``metrics.tsv``,
``protocol.txt``, ``runner-sha256.stdout`` and ``status`` archive format, so
the existing ``chunderdome summarize`` and ``chunderdome compare`` commands
can consume the results without changing Chunderdome's runner.

This targets the four migrated CLS methods directly. Plain hsbench S3 LIST
uses another listing method. Versioned S3 traffic is a separate end-to-end
OLH workload; it does not establish BI write or full reshard coverage. These
direct measurements do not include HTTP, authentication, RGW frontend work,
reshard scheduling or a multi-OSD/EC deployment.
