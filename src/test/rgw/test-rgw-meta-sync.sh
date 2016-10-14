#!/bin/bash

. "`dirname $0`/test-rgw-common.sh"

set -e

function get_metadata_sync_status {
  id=$1
  realm=$2

  meta_sync_status_json=`$(rgw_admin $id) --rgw-realm=$realm metadata sync status`

  global_sync_status=$(json_extract sync_status.info.status $meta_sync_status_json)
  num_shards=$(json_extract sync_status.info.num_shards $meta_sync_status_json)

  echo "sync_status: $global_sync_status"

  sync_markers=$(json_extract sync_status.markers $meta_sync_status_json)

  num_shards2=$(python_array_len $sync_markers)

  [ "$global_sync_status" == "sync" ] && $assert $num_shards2 -eq $num_shards

  sync_states=$(project_python_array_field val.state $sync_markers)
  eval secondary_status=$(project_python_array_field val.marker $sync_markers)
}

function get_metadata_log_status {
  master_id=$1
  realm=$2

  master_mdlog_status_json=`$(rgw_admin $master_id) --rgw_realm=$realm  mdlog status`
  master_meta_status=$(json_extract "" $master_mdlog_status_json)

  eval master_status=$(project_python_array_field marker $master_meta_status)
}

function wait_for_meta_sync {
  master_id=$1
  id=$2
  realm=$3

  get_metadata_log_status $master_id $realm
  echo "master_status=${master_status[*]}"

  while true; do
    get_metadata_sync_status $id $realm

    echo "secondary_status=${secondary_status[*]}"

    fail=0
    for i in `seq 0 $((num_shards-1))`; do
      if [ "${master_status[$i]}" \> "${secondary_status[$i]}" ]; then
        echo "shard $i not done syncing (${master_status[$i]} > ${secondary_status[$i]})"
        fail=1
        break
      fi
    done

    [ $fail -eq 0 ] && echo "Success" && return || echo "Sync not complete"

    sleep 5
  done
}

