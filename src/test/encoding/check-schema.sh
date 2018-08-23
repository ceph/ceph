#!/usr/bin/env bash
set -e

source $(dirname $0)/../detect-build-env-vars.sh
schema_dir=${CEPH_ROOT}/doc/api/schema

# for jsonschema
PATH=${CEPH_BUILD_VIRTUALENV}/schema-virtualenv/bin:$PATH

tmpdir=$(mktemp -d)
trap "rm -rf ${tmpdir}" EXIT

function check_schema {
  local type=${1}
  local schema=${schema_dir}/${2}
  ntests=$(ceph-dencoder type ${type} count_tests)
  for testn in `seq 1 1 ${ntests}`; do
    tmp=$(mktemp --tmpdir ${tdir})
    ceph-dencoder type ${type} select_test ${testn} dump_json > ${tmp}
    jsonschema -i ${tmp} ${schema}
    rm ${tmp}
  done
}

check_schema MonMap mon_map.json
