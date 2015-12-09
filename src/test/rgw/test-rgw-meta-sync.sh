#!/bin/bash

. "`dirname $0`/test-rgw-common.sh"

set -e

meta_sync_status_json=`$(rgw_admin 2) metadata sync status`


global_sync_status=$(json_extract sync_status.info.status $meta_sync_status_json)
num_shards=$(json_extract sync_status.info.num_shards $meta_sync_status_json)

echo "sync_status: $global_sync_status"

sync_markers=$(json_extract sync_status.markers $meta_sync_status_json)

# num_shards=$(python_array_len $sync_markers)

# echo $num_shards

sync_states=$(project_python_array_field val.state $sync_markers)

# echo ${sync_states[*]}
