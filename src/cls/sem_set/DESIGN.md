# Closing the RGWDataChangesLog write hole #

The Cause: The 'window' optimization creates a situation where some
changes exist only in RAM until a timer expires. If an RGW crashes
after a write, the data log entry might never be made.

## Requirements ##

* We need to handle syncing to multiple zones
* Some zones may not sync some buckets
* We need to maintain a strict ordering within the shard, with shards
  with the oldest un-swnc activity coming at the top of the listing
* We need to coalesce writes within a shard
* Spurious entries are permitted (but should be infrequent). They
  impact efficiency, not correctness.

## Definitions ##

* `RGWDataChangesLog`: The class implementing the datalog
  functionality.
* `cur_cycle`: The current working set of entries that will be written
  to FIFO at the close of the window.
* `add_entry`: The function in the datalog API to write an entry.
* `renew_entries`: Function run periodically to write an entry to FIFO
  for every bucketshard in `cur_cycle`.
* `recover`: Proposed function to complete initiated but incomplete datalog writes.
* semaphore object: Proposed object on the OSD holding a count for
  every bucketshard, sharded the same as datalog shards.
* bs1, ...: Arbitrary bucket shards

## Current Proposal ##

* Make `add_entry` a transaction.
* Reify `cur_cycle` on the OSD in the semaphore object.
* Add a `recover` function to be run at startup.
* Keep the window optimization.


### RGWDataChangesLog ###

* When `add_entry` is called with bs1:
    + If `cur_cycle` does not contain bs1, increment bs1's count in
      the semaphore object
	+ Otherwise, proceed as we do currently.
* In `renew_entries`, after the FIFO entry for a given shard has been
  written, decrement the count associated with that bucket shard on
  the semaphore object.
* In `recover`:
    1. Read the semaphore object, keeping bucketshards and counts in
       memory (`unordered_map`).
	2. For every bucketshard, write an entry into the FIFO.
    3. Send a notification on the semaphore object
    4. On receiving a notification, RGWDataChangesLog will send
       `cur_cycle` as the response.
    5. On successful notify, go through each response and decrement
       each bucketshard in the unordered map.  (e.g. if three RGWs
       respond with bs1 in their `cur_cycle`, bs1 will be decremented
       thrice)
	6. For each entry in the unordered map, decrement on the semaphore
       object only if the object's count is greater than 0. Send a
       grace period corresponding to the length of time since fetch
       times a fudge factor.
    7. If the `notify` operation errors, don't decrement anything.
* Have some task call `compress` on a regular basis (Daily? Hourly?),
  to keep seldom used or deleted bucket shards from slowing down
  listing


### CLS module requirements ###

* Our back-end data store will be omap. We will use exactly one
  key/value pair for every bucketshard in the system
* To avoid the performance problems of deletes and reinsertions, don't
  delete keys when they fall to zero, just set their value to 0.

#### Operations ####

* increment([bucketshard, ...]) -> {}:
    - For each bucketshard, look up the given key in omap. If it
      exists, set the value to one more than is currently
      there. Otherwise create it with a value of 1.
* decrement([bucketshard, ...], grace) -> {}:
    - For each bucketshard, look it up in omap. If it does not exist
      or the value is 0, error. If the last decrement was within the
	  'grace' timespan, skip it. Otherwise write decremented value.
    - Should it actually error or do we want saturating arithmetic at
      0? Given that the recovery design proposed below allows values
      to remain spuriously non-decremented but tries to rule out
      spurious decrements, we likely want to have the system scream
      bloody murder if we underflow.
* list(cursor?) -> ([(entry, count), ...], cursor):
    - Return entries starting from cursor. Skip any with a semaphore of 0.
* compress(cursor?) -> cursor?:
    - Go through and delete all entries with a 0 count.
    - Return cursor if we can't fit more calls the operation
	  and need another go-round

## Analysis ##

* Casey's semaphore idea solves the race between `add_entry` and
  `renew_entries` within a single process and between RGWs on
  different hosts
* The use of watch/notify solves the potential race between `recover`
  and other RGWs, ensuring that we won't delete someone else's
  transaction.
* As described, notify fails safe, as we don't decrement semaphores on
  error. This can lead to spurious recover in the future, but that's
  not harmful.
* In general, unpaired increments are considered acceptable, as
  they'll tend toward zero over time through recovery.
* Watch/Notify seems a better choice than locking, as it can't
  introduce a stall on `add_entry`.
* `increment`/`decrement` take a vector of bucketshards so they're
  ready to support batching.
* We will never have more omap entries than we do bucketshards in the
  system, and the operations are eventually all read-modify-write. Not
  deleting saves us from the worst pathologies of omap.

## Questions ##

* Do we want to shard the semaphore object?
    - [casey] yes, for write parallelism. we don't want all object
      writes to serialize on a single rados object

* What happens when we delete buckets? Would we need to clean up the
  asociated bucketshards and delete them?
    - [casey] same comes up for bucket reshards, where we add :gen to
     these bucket-shard-gen keys i do think we want to delete keys
     after they reach 0. ideally we'd do this in batches rocksdb
     deletes suck because they leave lots of tombstones behind, but
     that mainly effects listing performance. here we only list for
     recovery, and recovery only wants to see entries with count > 0

* Is a separate `compress` function really necessary? It might be
  worth doing test runs between a version that just deletes inline and
  one that uses a separate compress step.
