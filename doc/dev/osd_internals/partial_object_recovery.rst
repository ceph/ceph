=======================
Partial Object Recovery
=======================

Partial Object Recovery devotes to improving the efficiency of
log-based recovery rather than backfill. Original log-based recovery
calculates missing_set based on the difference between pg_log.

The whole object should be recovery from one OSD to another
if the object is indicated modified by pg_log regardless of how much
content in the object is really modified. That means a 4M object,
which is just modified 4k inside, should recovery the whole 4M object
rather than the modified 4k content. In addition, object map should be
also recovered even if it is not modified at all.

Partial Object Recovery is designed to solve the problem mentioned above.
In order to achieve the goals, two things should be done: 

1. logging where the object is modified is necessary
2. logging whether the object_map of an object is modified is also necessary

class ObjectCleanRegion is introduced to do what we want.
clean_offsets is a variable of interval_set<uint64_t>
and is used to indicate the unmodified content in an object.
clean_omap is a variable of bool indicating whether object_map is modified.
new_object means that osd does not exist for an object
max_num_intervals is an upbound of the number of intervals in clean_offsets
so that the memory cost of clean_offsets is always bounded.

The shortest clean interval will be trimmed if the number of intervals
in clean_offsets exceeds the boundary.

    etc. max_num_intervals=2, clean_offsets:{[5~10], [20~5]}

    then new interval [30~10] will evict out the shortest one [20~5]

    finally, clean_offsets becomes {[5~10], [30~10]}

Procedures for Partial Object Recovery
======================================

Firstly, OpContext and pg_log_entry_t should contain ObjectCleanRegion.
In do_osd_ops(), finish_copyfrom(), finish_promote(), corresponding content
in ObjectCleanRegion should mark dirty so that trace the modification of an object.
Also update ObjectCleanRegion in OpContext to its pg_log_entry_t.

Secondly, pg_missing_set can build and rebuild correctly.
when calculating pg_missing_set during peering process,
also merge ObjectCleanRegion in each pg_log_entry_t.

    etc. object aa has pg_log:
        26'101 {[0~4096, 8192~MAX], false}

        26'104 {0~8192, 12288~MAX, false}

        28'108 {[0~12288, 16384~MAX], true}

    missing_set for object aa: merge pg_log above --> {[0~4096, 16384~MAX], true}.
    which means 4096~16384 is modified and object_map is also modified on version 28'108

Also, OSD may be crash after merge log.
Therefore, we need to read_log and rebuild pg_missing_set. For example, pg_log is:

    object aa: 26'101 {[0~4096, 8192~MAX], false}

    object bb: 26'102 {[0~4096, 8192~MAX], false}

    object cc: 26'103 {[0~4096, 8192~MAX], false}

    object aa: 26'104 {0~8192, 12288~MAX, false}

    object dd: 26'105 {[0~4096, 8192~MAX], false}

    object aa: 28'108 {[0~12288, 16384~MAX], true}

Originally, if bb,cc,dd is recovered, and aa is not.
So we need to rebuild pg_missing_set for object aa,
and find aa is modified on version 28'108.
If version in object_info is 26'96 < 28'108,
we don't need to consider 26'104 and 26'101 because the whole object will be recovered.
However, Partial Object Recovery should also require us to rebuild ObjectCleanRegion.

Knowing whether the object is modified is not enough.

Therefore, we also need to traverse the pg_log before,
that says 26'104 and 26'101 also > object_info(26'96)
and rebuild pg_missing_set for object aa based on those three logs: 28'108, 26'104, 26'101.
The way how to merge logs is the same as mentioned above

Finally, finish the push and pull process based on pg_missing_set.
Updating copy_subset in recovery_info based on ObjectCleanRegion in pg_missing_set.
copy_subset indicates the intervals of content need to pull and push.

The complicated part here is submit_push_data
and serval cases should be considered separately.
what we need to consider is how to deal with the object data,
object data makes up of omap_header, xattrs, omap, data:
    
case 1: first && complete: since object recovering is finished in a single PushOp,
we would like to preserve the original object and overwrite on the object directly.
Object will not be removed and touch a new one.

    issue 1: As object is not removed, old xattrs remain in the old object
    but maybe updated in new object. Overwriting for the same key or adding new keys is correct,
    but removing keys will be wrong. 
    In order to solve this issue, We need to remove the all original xattrs in the object, and then update new xattrs.

    issue 2: As object is not removed,
    object_map may be recovered depending on the clean_omap.
    Therefore, if recovering clean_omap, we need to remove old omap of the object for the same reason
    since omap updating may also be a deletion.
    Thus, in this case, we should do:

        1) clear xattrs of the object
        2) clear omap of the object if omap recovery is needed
        3) truncate the object into recovery_info.size
        4) recovery omap_header
        5) recovery xattrs, and recover omap if needed
        6) punch zeros for original object if fiemap tells nothing there
        7) overwrite object content which is modified
        8) finish recovery

case 2:  first && !complete: object recovering should be done in multiple times.
Here, target_oid will indicate a new temp_object in pgid_TEMP,
so the issues are a bit difference.

    issue 1: As object is newly created, there is no need to deal with xattrs

    issue 2: As object is newly created,
    and object_map may not be transmitted depending on clean_omap.
    Therefore, if clean_omap is true, we need to clone object_map from original object.
    issue 3: As object is newly created, and unmodified data will not be transmitted.
    Therefore, we need to clone unmodified data from the original object.
    Thus, in this case, we should do:

        1) remove the temp object
        2) create a new temp object
        3) set alloc_hint for the new temp object
        4) truncate new temp object to recovery_info.size
        5) recovery omap_header
        6) clone object_map from original object if omap is clean
        7) clone unmodified object_data from original object
        8) punch zeros for the new temp object
        9) recovery xattrs, and recover omap if needed
        10) overwrite object content which is modified
        11) remove the original object
        12) move and rename the new temp object to replace the original object
        13) finish recovery
