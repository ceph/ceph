=====================
Asynchronous Recovery
=====================

Ceph Placement Groups (PGs) maintain a log of write transactions to
facilitate speedy recovery of data. During recovery, each of these PG logs
is used to determine which content in each OSD is missing or outdated.
This obviates the need to scan all RADOS objects.
See :ref:`Log Based PG <log-based-pg>` for more details on this process.

Prior to the Nautilus release this recovery process was synchronous: it
blocked writes to a RADOS object until it was recovered. In contrast,
backfill could allow writes to proceed (assuming enough up-to-date replicas
were available) by temporarily assigning a different acting set, and
backfilling an OSD outside of the acting set. In some circumstances
this ends up being significantly better for availability, e.g. if the
PG log contains 3000 writes to disjoint objects.  When the PG log contains
thousands of entries, it could actually be faster (though not as safe) to
trade backfill for recovery by deleting and redeploying the containing
OSD than to iterate through the PG log.  Recovering several megabytes
of RADOS object data (or even worse, several megabytes of omap keys,
notably RGW bucket indexes) can drastically increase latency for a small
update, and combined with requests spread across many degraded objects
it is a recipe for slow requests.

To avoid this we can perform recovery in the background on an OSD
out-of-band of the live acting set, similar to backfill, but still using
the PG log to determine what needs to be done. This is known as *asynchronous
recovery*.

The threshold for performing asynchronous recovery instead of synchronous
recovery is not a clear-cut. There are a few criteria which
need to be met for asynchronous recovery:

* Try to keep ``min_size`` replicas available
* Use the approximate magnitude of the difference in length of
  logs combined with historical missing objects to estimate the cost of
  recovery
* Use the parameter ``osd_async_recovery_min_cost`` to determine
  when asynchronous recovery is appropriate

With the existing peering process, when we choose the acting set we
have not fetched the PG log from each peer; we have only the bounds of
it and other metadata from their ``pg_info_t``. It would be more expensive
to fetch and examine every log at this point, so we only consider an
approximate check for log length for now. In Nautilus, we improved
the accounting of missing objects, so post-Nautilus this information
is also used to determine the cost of recovery.

While async recovery is occurring, writes to members of the acting set
may proceed, but we need to send their log entries to the async
recovery targets (just like we do for backfill OSDs) so that they
can completely catch up.
