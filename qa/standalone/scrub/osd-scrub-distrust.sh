#!/bin/bash -x
#
# Copyright (C) 2014 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

# Test development and debugging
# Set to "yes" in order to ignore diff errors and save results to update test
getjson="yes"

# Filter out mtime and local_mtime dates, version, prior_version and last_reqid (client) from any object_info.
jqfilter='def walk(f):
  . as $in
  | if type == "object" then
      reduce keys[] as $key
        ( {}; . + { ($key):  ($in[$key] | walk(f)) } ) | f
    elif type == "array" then map( walk(f) ) | f
    else f
    end;
walk(if type == "object" then del(.mtime) else . end)
| walk(if type == "object" then del(.local_mtime) else . end)
| walk(if type == "object" then del(.last_reqid) else . end)
| walk(if type == "object" then del(.version) else . end)
| walk(if type == "object" then del(.prior_version) else . end)
| walk(if type == "object" then del(.redirect_target) else . end)
| walk(if type == "object" then del(.legacy_snaps) else . end)'

sortkeys='import json; import sys ; JSON=sys.stdin.read() ; ud = json.loads(JSON) ; print json.dumps(ud, sort_keys=True, indent=2)'

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7107" # git grep '\<7107\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--osd-distrust-data-digest=true "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function add_something() {
    local dir=$1
    local poolname=$2
    local obj=${3:-SOMETHING}
    local scrub=${4:-noscrub}

    if [ "$scrub" = "noscrub" ];
    then
        ceph osd set noscrub || return 1
        ceph osd set nodeep-scrub || return 1
    else
        ceph osd unset noscrub || return 1
        ceph osd unset nodeep-scrub || return 1
    fi

    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put $obj $dir/ORIGINAL || return 1
}

#
# Test automatic repair with distrust set
#
function TEST_distrust_scrub_replicated() {
    local dir=$1
    local poolname=dsr_pool
    local total_objs=2

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1

    create_pool foo 1 || return 1
    create_pool $poolname 1 1 || return 1
    wait_for_clean || return 1

    for i in $(seq 1 $total_objs) ; do
        objname=ROBJ${i}
        add_something $dir $poolname $objname || return 1
    done

    local pg=$(get_pg $poolname ROBJ0)

    for i in $(seq 1 $total_objs) ; do
        objname=ROBJ${i}

        case $i in
	1)
	    # Deep-scrub only (all replicas are diffent than the object info
           local payload=XROBJ1
           echo $payload > $dir/new.ROBJ1
	   objectstore_tool $dir 0 $objname set-bytes $dir/new.ROBJ1 || return 1
	   objectstore_tool $dir 1 $objname set-bytes $dir/new.ROBJ1 || return 1
	   ;;

	2)
	    # Deep-scrub only (all replicas are diffent than the object info
           local payload=XROBJ2
           echo $payload > $dir/new.ROBJ2
	   objectstore_tool $dir 0 $objname set-bytes $dir/new.ROBJ2 || return 1
	   objectstore_tool $dir 1 $objname set-bytes $dir/new.ROBJ2 || return 1
	   # Make one replica have a different object info, so a full repair must happen too
	   objectstore_tool $dir 0 $objname corrupt-info || return 1
	   ;;
        esac
    done

    # This should fix the data_digest because osd-distrust-data-digest is true
    pg_deep_scrub $pg

    # This hangs if the scrub didn't repair the data_digest
    timeout 30 rados -p $poolname get ROBJ1 $dir/robj1.out || return 1
    diff -q $dir/new.ROBJ1 $dir/robj1.out || return 1
    rm -f $dir/new.ROBJ1 $dir/robj1.out || return 1

    rados list-inconsistent-pg $poolname > $dir/json || return 1
    # Check pg count
    test $(jq '. | length' $dir/json) = "1" || return 1
    # Check pgid
    test $(jq -r '.[0]' $dir/json) = $pg || return 1

    rados list-inconsistent-obj $pg > $dir/json || return 1
    # Get epoch for repair-get requests
    epoch=$(jq .epoch $dir/json)

    jq "$jqfilter" << EOF | jq '.inconsistents' | python -c "$sortkeys" > $dir/checkcsjson
{
  "inconsistents": [
    {
      "shards": [
        {
          "object_info": {
            "watchers": {},
            "manifest": {
              "redirect_target": {
                "namespace": "",
                "pool": -9223372036854776000,
                "max": 0,
                "hash": 0,
                "snapid": 0,
                "key": "",
                "oid": ""
              },
              "type": 0
            },
            "alloc_hint_flags": 255,
            "expected_write_size": 0,
            "local_mtime": "2018-07-24 15:05:56.027234",
            "mtime": "2018-07-24 15:05:56.021775",
            "size": 7,
            "user_version": 2,
            "last_reqid": "client.4137.0:1",
            "prior_version": "0'0",
            "version": "23'2",
            "oid": {
              "namespace": "",
              "pool": 3,
              "max": 0,
              "hash": 2026323607,
              "snapid": -2,
              "key": "",
              "oid": "ROBJ2"
            },
            "lost": 0,
            "flags": [
              "dirty",
              "data_digest"
            ],
            "legacy_snaps": [],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xffffffff",
            "expected_object_size": 0
          },
          "data_digest": "0x0bb7ab52",
          "omap_digest": "0xffffffff",
          "size": 7,
          "errors": [],
          "primary": false,
          "osd": 0
        },
        {
          "object_info": {
            "watchers": {},
            "manifest": {
              "redirect_target": {
                "namespace": "",
                "pool": -9223372036854776000,
                "max": 0,
                "hash": 0,
                "snapid": 0,
                "key": "",
                "oid": ""
              },
              "type": 0
            },
            "alloc_hint_flags": 0,
            "expected_write_size": 0,
            "local_mtime": "2018-07-24 15:05:56.027234",
            "mtime": "2018-07-24 15:05:56.021775",
            "size": 7,
            "user_version": 2,
            "last_reqid": "client.4137.0:1",
            "prior_version": "0'0",
            "version": "23'2",
            "oid": {
              "namespace": "",
              "pool": 3,
              "max": 0,
              "hash": 2026323607,
              "snapid": -2,
              "key": "",
              "oid": "ROBJ2"
            },
            "lost": 0,
            "flags": [
              "dirty",
              "data_digest"
            ],
            "legacy_snaps": [],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xffffffff",
            "expected_object_size": 0
          },
          "data_digest": "0x0bb7ab52",
          "omap_digest": "0xffffffff",
          "size": 7,
          "errors": [],
          "primary": true,
          "osd": 1
        }
      ],
      "selected_object_info": {
        "watchers": {},
        "manifest": {
          "redirect_target": {
            "namespace": "",
            "pool": -9223372036854776000,
            "max": 0,
            "hash": 0,
            "snapid": 0,
            "key": "",
            "oid": ""
          },
          "type": 0
        },
        "alloc_hint_flags": 0,
        "expected_write_size": 0,
        "local_mtime": "2018-07-24 15:05:56.027234",
        "mtime": "2018-07-24 15:05:56.021775",
        "size": 7,
        "user_version": 2,
        "last_reqid": "client.4137.0:1",
        "prior_version": "0'0",
        "version": "23'2",
        "oid": {
          "namespace": "",
          "pool": 3,
          "max": 0,
          "hash": 2026323607,
          "snapid": -2,
          "key": "",
          "oid": "ROBJ2"
        },
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "legacy_snaps": [],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0
      },
      "union_shard_errors": [],
      "errors": [
        "object_info_inconsistency"
      ],
      "object": {
        "version": 2,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ2"
      }
    }
  ],
  "epoch": 42
}
EOF

    jq "$jqfilter" $dir/json | jq '.inconsistents' | python -c "$sortkeys" > $dir/csjson
    diff ${DIFFCOLOPTS} $dir/checkcsjson $dir/csjson || test $getjson = "yes" || return 1
    if test $getjson = "yes"
    then
        jq '.' $dir/json > save1.json
    fi

    if test "$LOCALRUN" = "yes" && which jsonschema > /dev/null;
    then
      jsonschema -i $dir/json $CEPH_ROOT/doc/rados/command/list-inconsistent-obj.json || return 1
    fi

    repair $pg
    wait_for_clean

    timeout 30 rados -p $poolname get ROBJ2 $dir/robj2.out || return 1
    diff -q $dir/new.ROBJ2 $dir/robj2.out || return 1
    rm -f $dir/new.ROBJ2 $dir/robj2.out || return 1

    rados rmpool $poolname $poolname --yes-i-really-really-mean-it
    teardown $dir || return 1
}

main osd-scrub-distrust "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-scrub-distrust"
# End:
