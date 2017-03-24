#!/bin/bash
# vim: ts=8 sw=2 smarttab
#
# $0.sh - run mon workload generator

if [[ ! -e "./ceph_ver.h" ]]; then
  echo "This script must be run from the repository's src/ directory"
  exit 1
fi

usage() {
  echo "usage: $1 [options..] <num-osds>"
  echo 
  echo "options:"
  echo "  -v, --verbose        Be more verbose"
  echo "  -c, --conf FILE      ceph.conf location"
  echo "  -d, --duration SECS  Run test for SECS seconds (default: 300)"
  echo "      --debug LEVEL    Set the test's debug level (default: 0)"
  echo "  -n, --new            Make a fresh run by creating a new cluster"
  echo
  echo "environment variables:"
  echo "  EXTRA_ARGS            Pass additional ceph arguments to the test"
  echo
}

stop_ceph() {
  if [[ ! -e "init-ceph" ]]; then
    echo "could not find 'init-ceph'; killing only by hand and may bob have"
    echo "mercy on our souls"
  else
    ./init-ceph stop
  fi

  for i in mon osd mds; do
    killall -9 ceph-$i
  done
}

start_mon() {
  if [[ ! -e "init-ceph" ]]; then
    echo "could not find 'init-ceph'; attempting to start monitors by hand"

    for i in a b c; do
      ./ceph-mon -i $i -c ceph.conf -k keyring -d
    done
  else
    ./init-ceph start mon
  fi
}

make_fresh() {
  rm -fr dev/ out/ keyring
  mkdir dev

  if [[ ! -e "vstart.sh" ]]; then
    echo "could not find 'vstart.sh', which is weird; what have you done?"
    exit 1
  fi

  env MON=3 OSD=0 MDS=0 MGR=0 \
    ./vstart.sh -n -l -d mon
}

DEBUG=0
TEST_CEPH_CONF=
DURATION=
LOADGEN_NUM_OSDS=
ARGS=

[[ ! -z $EXTRA_ARGS ]] && ARGS="$EXTRA_ARGS"

fresh_run=0

while [[ $# -gt 0 ]];
do
  case "$1" in
    -v | --verbose)
      VERBOSE=1
      shift
      ;;
    -c | --conf)
      if [[ "$2" == "" ]]; then
	echo "'$1' expects an argument; none was given"
	exit 1
      fi
      TEST_CEPH_CONF="$2"
      shift 2
      ;;
    --debug)
      if [[ -z $2 ]]; then
	echo "'$1' expects an argument; none was given"
	exit 1
      fi
      ARGS="$ARGS --debug-none $2"
      shift 2
      ;;
    -d | --duration)
      if [[ -z $2 ]]; then
	echo "'$1' expects an argument; none was given"
	exit 1
      fi
      DURATION=$2
      shift 2
      ;;
    -n | --new)
      fresh_run=1
      shift
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "$1: unknown option" >&2
      usage $0
      exit 1
      ;;
    *)
      LOADGEN_NUM_OSDS=$1
      shift
      break
      ;;
  esac
done

if [[ -z $LOADGEN_NUM_OSDS ]]; then
  echo "must specify the number of osds"
  usage $0
  exit 1
fi

stop_ceph ;
[[ $fresh_run -eq 1 ]] && make_fresh ;
start_mon ;

env VERBOSE=$VERBOSE TEST_CEPH_CONF="$TEST_CEPH_CONF" \
    DURATION=$DURATION EXTRA_ARGS="$ARGS" \
    LOADGEN_NUM_OSDS=$LOADGEN_NUM_OSDS \
    PATH="$PATH:`pwd`" ../qa/workunits/mon/workloadgen.sh
