# Non-block Resharding

## Requirements

* Non-block resharding a bucket require upgraded to a supported release
    - Existing bucket reshard should be completed firstly before upgrading.

* Backward compatibility
    - If the rgw or rgw-admin nodes that do resharding upgrades to supported release but parts of osd nodes not, the reshard will be blocked as before.
    - Only parts of osd nodes upgraded to a supported release and the rgw or rgw-admin nodes not, the reshard will execute blocked too.

## Designing

Split the bucket resharding into two phases: logrecord and progress. A duplicated copy of index entry will be written with index operation to src shards in first phase, and the client writes will not be blocked; we then block the client writes, go through the recording log and copy the changed index entries to dest shards in second phase. In this way, we can greatly reduce the time blocking client writes in resharding a bucket.

The record log key is like `0x802001_idx`, the `idx` uses the same key with original index entry but under this new 2001_ namespace, with versioned entries under 2001_1000_ or 2001_1001_.

## Tasks

### Record Duplicated Index Entries

* The policy adopted here is not only recording a copy for the entire write op, but for every change to the index entry. One op may correspond to multiple copys. In this way, the complexity of index synchronization can be reduced. You don't have to think about the details of operation. Especially with versioned objects, the same entry may involve multiple changes in a entire write or delete operation. Here, a copy is recorded for each change, repeated writes to the same index entry would just overwrite the same entry.

### Copy Index Entries

* In logrecord state, copy inventoried index entries to dest shards and record a duplicated copy for new writting entry.

* In progress state, block the writes, listing the copys written in logrecord state and copy then to dest shards. If the index key exists in dest shard but not in src shard, then delete it from dest shard too.

### Bucket Stats

* There is such a situation, the index entrie that have already been copyed to dest shards in logrecord state,  may be copyed again in the progress state. For this scenario, their stats in dest shards should be subtracted firstly:
    - Request corresponding index entry from dest shard too based on key recorded in 2001_ namespace
    - Get old stats of index entry if it exists in dest shard
    - Subtract the old entry stats of dest shard as adding stats of the new copyed one

### Reshard Logrecord Judge

When a bucket reshard faild in the logrecord phase, the duplicated copys should be stopped written within a short time. To achieve it, we judge whether the resharding is executing properly in recording log once in the while, and the time is `rgw_reshard_progress_judge_interval`. If it has already failed, we clear resharding status and stop recording copys.

### Backward Compatibility

* The privious release only has one reshard phase: the progress phase which will block client writes. Because our release contains this phase and the process is same too, that means it is superset of privious release. So when privious rgw initiates a reshard, it will execute as before.

* When a updated rgw initiates a reshard, it firstly enter the logrecord phase which privious releases do not realized. That means the nodes which do not upgraded will deal with client write operations without recording copys. It may leads to part of these index entries missed. So we forbit this scene by adding `trim_reshard_log_entries()` and `cls_rgw_bucket_init_index2()` control source and target versions, old osds would fail the request with -EOPNOTSUPP. so radosgw could start by trying that on all shards. if there are no errors, it can safely proceed with the new scheme. If any of the osds do return -EOPNOTSUPP there, then rgw fall back to the current resharding scheme where writes are blocked the whole time.

## Future Prospects

* If the block time is still too long, one more logrecord phase can be added. When dealing with initial record copys, do not blocking writes, instead, new duplicated index copys will be recorded with comming writes. And block writes in dealing with secord copys, the blocked time will be cutted down to negligible in this way.
