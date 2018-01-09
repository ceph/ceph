============================
 New Cache Mode - Swap
============================

Tiering devotes to improving the I/O performance by migrating the hot objects
to cache tier. The most commonly used cache mode is Write Back Mode. Objects
would be promoted if they appeared in more than, for example, two hit sets.
At the same time, Tier Agent would flush most of them periodically and evict
the cold objects.

In the time-skewed workloads, it works well. But in some workloads, such as
RBD with 4k rand writes, we do not expect frequent promote, flush, evict.
Because migration between pools is expensive, we want to promote only if the
object is hot enough and flushes only if the object could be evicted.

The new Cache Mode Swap is designed to fill this requirement. In order to
achieve these goals, two things should be done:

# record every single object's temperature and get a swap line.
# promote objects above swap line, meanwhile, evict the objects below swap
line if the usage rate exceeds the expected value.

We could record the temperatures in bloom hit sets and get ranking of
objects with Pow2Histogram. But when we trying to judge whether an object was
hot enough to promote, it doesn't work as accurate as expected.

class TempHitSet is introduced to do what we expect. It would store all local
objects' temperature in RAM. Every single temperature info would cost about 12
bytes, including 4 bytes hash, 4 bytes temperature, and 4 bytes last decay time.
Each hit to the local object will increase the temperature. With that, we could
figure out quickly whether a local object is hot or not. Also, we could limit
the cost of RAM by setting the target_max_objects to the specific value.


Procedures for op process in swap cache mode
=============================================

Firstly, if the object exists in local disk, hit the HitSet with object's hash.
If not, ignore.

Secondly, choose the agent mode. If the usage rate exceeds the
target_promote_ratio, set promote_mode to PROMOTE_FULL, else set it to
PROMOTE_SOME. Calculate the swap line.

Thirdly, handle cache with Cache Mode Swap.
 - If the object exists, handle the op locally.
 - If the object shows in promote queue, proxy op and promote the object.
 - If the object doesn't exist nor shows in promote queue, proxy op.

Finally, get remote object's temperature from MOSDReply of proxy read/write. If
hot enough, the object would be put into promote queue, waiting to be
promoted.


Procedures for agent flush and evict
=====================================

Firstly, the flush mode is only related to the usage of pool. If the usage rate
exceeds the expected, set flush mode to FLUSH_SOME to flush and evict some cold
objects.

Secondly, skip all objects whose temperature is higher than swap line.

Finally, evict action would be triggered after flushing objects.


Procedures for using the new cache mode
=======================================

Firstly, create cache pool and storage pool. Then, set overlay to storage pool
as other cachemode.

Secondly, set the cache mode to swap for cache pool. The
target_max_objects/size is also required for this new mode.

.. code-block:: bash

    $ ceph osd tier cache-mode cachepool swap --yes-i-really-mean-it
    $ ceph osd pool set cachepool target_max_size 100G
    $ ceph osd pool set cachepool target_max_objects 1000000

Finally, you may test it by fio using the specific distribution.
