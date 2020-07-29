# Dynamic Resharding for Multisite

## Requirements

* Each zone manages bucket resharding decisions independently
    - With per-bucket replication policies, some zones may only replicate a subset of objects, so require fewer shards.
    - Avoids having to coordinate reshards across zones.
* Resharding a bucket does not require a full sync of its objects
    - Existing bilogs must be preserved and processed before new bilog shards.
* Backward compatibility
    - No zone can reshard until all peer zones upgrade to a supported release.
    - Requires a manual zonegroup change to enable resharding.

## Layout

A layout describes a set of rados objects, along with some strategy to distribute things across them. A bucket index layout distributes object names across some number of shards via `ceph_str_hash_linux()`. Resharding a bucket enacts a transition from one such layout to another. Each layout could represent data differently. For example, a bucket index layout would be used with cls_rgw to write/delete keys. Whereas a datalog layout may be used with cls_log to append and trim log entries, then later transition to a layout based on some other primitive like cls_queue or cls_fifo.

## Bucket Index Resharding

To reshard a bucket, we currently create a new bucket instance with the desired sharding layout, and switch to that instance when resharding completes. In multisite, though, the metadata master zone is authoritative for all bucket metadata, including the sharding layout and reshard status. Any changes to metadata must take place on the metadata master zone and replicate from there to other zones.

If we want to allow each zone to manage its bucket sharding independently, we can't allow them each to create a new bucket instance, because data sync relies on the consistency of instance ids between zones. We also can't allow metadata sync to overwrite our local sharding information with the metadata master's copy.

That means that the bucket's sharding information needs to be kept private to the local zone's bucket instance, and that information also needs to track all reshard status that's currently spread between the old and new bucket instance metadata: old shard layout, new shard layout, and current reshard progress. To make this information private, we can just prevent metadata sync from overwriting these fields.

This change also affects the rados object names of the bucket index shards, currently of the form `.dir.<instance-id>.<shard-id>`. Since we need to represent multiple sharding layouts for a single instance-id, we need to add some unique identifier to the object names. This comes in the form of a generation number, incremented with each reshard, like `.dir.<instance-id>.<generation>.<shard-id>`. The first generation number 0 would be omitted from the object names for backward compatibility.

## Bucket Index Log Resharding

The bucket replication logs for multisite are stored in the same bucket index shards as the keys that they modify. However, we can't reshard these log entries like we do with with normal keys, because other zones need to track their position in the logs. If we shuffle the log entries around between shards, other zones no longer have a way to associate their old shard marker positions with the new shards, and their only recourse would be to restart a full sync. So when resharding buckets, we need to preserve the old bucket index logs so that other zones can finish processing their log entries, while any new events are recorded in the new bucket index logs.

An additional goal is to move replication logs out of omap (so out of the bucket index) into separate rados objects. To enable this, the bucket instance metadata should be able to describe a bucket whose *index layout* is different from its *log layout*. For existing buckets, the two layouts would be identical and share the bucket index objects. Alternate log layouts are otherwise out of scope for this design.

To support peer zones that are still processing old logs, the local bucket instance metadata must track the history of all log layouts that haven't been fully trimmed yet. Once bilog trimming advances past an old generation, it can delete the associated rados objects and remove that layout from the bucket instance metadata. To prevent this history from growing too large, we can refuse to reshard bucket index logs until trimming catches up.

The distinction between *index layout* and *log layout* is important, because incremental sync only cares about changes to the *log layout*. Changes to the *index layout* would only affect full sync, which uses a custom RGWListBucket extension to list the objects of each index shard separately. But by changing the scope of full sync from per-bucket-shard to per-bucket and using a normal bucket listing to get all objects, we can make full sync independent of the *index layout*. And once the replication logs are moved out of the bucket index, dynamic resharding is free to change the *index layout* as much as it wants with no effect on multisite replication.

## Tasks

### Bucket Reshard

* Modify existing state machine for bucket reshard to mutate its existing bucket instance instead of creating a new one.

* Add fields for log layout. When resharding a bucket whose logs are in the index:
    - Add a new log layout generation to the bucket instance
    - Copy the bucket index entries into their new index layout
    - Commit the log generation change so new entries will be written there
    - Create a datalog entry with the new log generation

### Metadata Sync

* When sync fetches a bucket instance from the master zone, preserve any private fields in the local instance. Use cls_version to guarantee that we write back the most recent version of those private fields.

### Data Sync

* Datalog entries currently include a bucket shard number. We need to add the log generation number to these entries so we can tell which sharding layout it refers to. If we see a new generation number, that entry also implies an obligation to finish syncing all shards of prior generations.

### Bucket Sync Status

* Add a per-bucket sync status object that tracks:
    - full sync progress,
    - the current generation of incremental sync, and
    - the set of shards that have completed incremental sync of that generation
* Existing per-bucket-shard sync status objects continue to track incremental sync.
    - their object names should include the generation number, except for generation 0
* For backward compatibility, add special handling when we get ENOENT trying to read this per-bucket sync status:
    - If the remote's oldest log layout has generation=0, read any existing per-shard sync status objects. If any are found, resume incremental sync from there.
    - Otherwise, initialize for full sync.

### Bucket Sync

* Full sync uses a single bucket-wide listing to fetch all objects.
    - Use a cls_lock to prevent different shards from duplicating this work.
* When incremental sync gets to the end of a log shard (i.e. listing the log returns truncated=false):
    - If the remote has a newer log generation, flag that shard as 'resharded' in the bucket sync status.
    - Once all shards in the current generation reach that 'resharded' state, incremental bucket sync can advance to the next generation.
    - Use cls_version on the bucket sync status object to detect racing writes from other shards.

### Log Trimming

* Use generation number from sync status to trim the right logs
* Once all shards of a log generation are trimmed:
    - Remove their rados objects.
    - Remove the associated incremental sync status objects.
    - Remove the log generation from its bucket instance metadata.

### Admin APIs

* RGWOp_BILog_List response should include the bucket's highest log generation
    - Allows incremental sync to determine whether truncated=false means that it's caught up, or that it needs to transition to the next generation.
* RGWOp_BILog_Info response should include the bucket's lowest and highest log generations
    - Allows bucket sync status initialization to decide whether it needs to scan for existing shard status, and where it should resume incremental sync after full sync completes.
* RGWOp_BILog_Status response should include per-bucket status information
    - For log trimming of old generations
