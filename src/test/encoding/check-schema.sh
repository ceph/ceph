#!/usr/bin/env bash
set -e

source $(dirname $0)/../detect-build-env-vars.sh
schema_dir=${CEPH_ROOT}/doc/api/schema

tmpdir=$(mktemp -d)
trap "rm -rf ${tmpdir}" EXIT

function check_schema {
  local type=${1}
  local schema=${schema_dir}/${2}
  ntests=$(ceph-dencoder type ${type} count_tests)
  for testn in `seq 1 1 ${ntests}`; do
    tmp=$(mktemp --tmpdir ${tdir})
    ceph-dencoder type ${type} select_test ${testn} dump_json > ${tmp}
    ${CEPH_BUILD_VIRTUALENV}/schema-virtualenv/bin/python \
      ${CEPH_ROOT}/src/test/encoding/validate-schema.py \
      ${CEPH_ROOT}/doc/api/schema ${tmp} ${schema}
    rm ${tmp}
  done
}

check_schema MonMap mon_map.json
check_schema OSDMap osd_map.json
check_schema FSMap fs_map.json
check_schema MDSMap mds_map.json
check_schema MgrMap mgr_map.json
check_schema ServiceMap service_map.json
check_schema CrushWrapper crush_map.json
check_schema object_stat_sum_t object_stat_sum.json
check_schema object_stat_collection_t object_stat_collection.json
check_schema pool_stat_t pool_stat.json
