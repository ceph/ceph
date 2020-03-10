# Compare status api 

Compare status api is used as the new admin api to compare metadata sync status.

## Design

Dashboard will send a request to the Status api of target zone, and get back a list of markers
and timestamps. Include that in its request to status api on source zone .Sync.read_sync_status in
get_md_sync_status gets sync status marker for each shard and it gets executed in
RGWOp_MDLog_Status.

CompareStatus takes as input the list of 64 status markers and the following steps are repeated
over the markers:

1: RGWOp_MDLog_CompareStatus calls meta_log.get_info() for getting the highest log marker
for each shard. This was a function of sync.read_master_log_shards_info() in
get_md_sync_status in rgw_admin.cc which is sending 64 HTTP requests to the master zone,
one for each shard.
sync.read_master_log_shards_info() was executed by RGWOp_MDLog_ShardInfo in
rgw_rest_log.cc.

2: CompareStatus output current and latest timestamp in json format.
[ {“Shard_id” : “..” , "current_timestamp": "...", "latest_timestamp": "..." },...] 


