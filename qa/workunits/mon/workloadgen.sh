#!/bin/bash -x
# vim: ts=8 sw=2 smarttab
#
# $0.sh - run mon workload generator

d() {
  [[ "$VERBOSE" != "" && $VERBOSE -eq 1 ]] && echo "## DEBUG ## $*"
}

d "check for required binaries"

required_bins="ceph crushtool ceph_test_mon_workloadgen"
for b in $required_bins; do
  which $b >& /dev/null
  if [[ $? -ne 0 ]]; then
    echo "Unable to find '$b' in PATH"
    exit 1
  fi
done

d "Start workunit"

crush_map_fn=test.crush.map
create_crush=0
clobber_crush=0
new_cluster=0
do_run=0
num_osds=0

# Assume the test is in PATH
bin_test=ceph_test_mon_workloadgen

num_osds=10
if [[ "$LOADGEN_NUM_OSDS" != "" ]]; then
  num_osds=$LOADGEN_NUM_OSDS
fi

duration=300
[ ! -z $DURATION ] && duration=$DURATION

d "checking osd tree"

crush_testing_root="`ceph osd tree | grep 'root[ \t]\+testing'`"

d "$crush_testing_root"

if [[ "$crush_testing_root" == "" ]]; then
  d "set create_crush"
  create_crush=1
fi

d "generate run_id (create_crush = $create_crush)"

run_id=`uuidgen`

d "run_id = $run_id ; create_crush = $create_crush"

if [[ $create_crush -eq 1 ]]; then
  tmp_crush_fn="/tmp/ceph.$run_id.crush"
  ceph osd getcrushmap -o $tmp_crush_fn
  crushtool -d $tmp_crush_fn -o $tmp_crush_fn.plain

  highest_root_id=0
  root_ids_raw="`cat $tmp_crush_fn.plain | grep id`"
  ifs=$IFS
  IFS=$'\n'
  for l in $root_ids_raw; do
    root_id=`echo $l | sed 's/.*-\([[:digit:]]\+\).*/\1/'`
    d "root id = $root_id ; highest = $highest_root_id"
    if [[ $root_id -gt $highest_root_id ]]; then
      highest_root_id=$root_id
    fi
  done
  our_root_id=$(($highest_root_id+1))
  IFS=$ifs

  cat << EOF >> $tmp_crush_fn.plain
root testing {
  id -$our_root_id
  alg straw
  hash 0  # rjenkins1
}
rule testingdata {
  ruleset 0
  type replicated
  min_size 1
  max_size 10
  step take testing
  step choose firstn 0 type osd
  step emit
}
rule testingmetadata {
  ruleset 1
  type replicated
  min_size 1
  max_size 10
  step take testing
  step choose firstn 0 type osd
  step emit
}
rule testingrbd {
  ruleset 2
  type replicated
  min_size 1
  max_size 10
  step take testing
  step choose firstn 0 type osd
  step emit
}
EOF

  if [[ $VERBOSE -eq 1 ]]; then
    cat $tmp_crush_fn.plain
  fi

  crushtool -c $tmp_crush_fn.plain -o $tmp_crush_fn
  if [[ $? -eq 1 ]]; then
    echo "Error compiling test crush map; probably need newer crushtool"
    echo "NOK"
    exit 1
  fi

  d "created crush"

  ceph osd setcrushmap -i $tmp_crush_fn
fi

keyring="/tmp/ceph.$run_id.keyring"

ceph auth get-or-create-key osd.admin mon 'allow rwx' osd 'allow *'
ceph auth export | grep -v "export" > $keyring

osd_ids=""

for osd in `seq 1 $num_osds`; do
  id=`ceph osd create`
  osd_ids="$osd_ids $id"
  d "osd.$id"
  ceph osd crush set $id osd.$id 1.0 host=testhost rack=testrack root=testing
done

d "osds: $osd_ids"

stub_id_args=""
f=
l=
for i in $osd_ids; do
  d "i: $i"
  if [[ $stub_id_args == "" ]]; then
    stub_id_args="--stub-id $i"
    f=$i
  fi
  if [[ $l != "" ]]; then
    if [[ $i -gt $(($l+1)) ]]; then
      stub_id_args="$stub_id_args..$l --stub-id $i"
      f=$i
    fi
  fi
  l=$i
done
if [[ $l -gt $f ]]; then
  stub_id_args="$stub_id_args..$l"
fi

args="$EXTRA_ARGS --duration $duration $stub_id_args"
  
d "running: $args"

$bin_test --keyring $keyring $args
