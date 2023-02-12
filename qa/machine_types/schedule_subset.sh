#!/bin/bash -e

#command line => CEPH_BRANCH=<branch>; MACHINE_NAME=<machine_type>; SUITE_NAME=<suite>; ../schedule_subset.sh <day_of_week> $CEPH_BRANCH $MACHINE_NAME $SUITE_NAME $CEPH_QA_EMAIL $KERNEL <$FILTER>

partitions="$1"
shift
branch="$1"
shift
machine="$1"
shift
suite="$1"
shift
email="$1"
shift
kernel="$1"
shift
# rest of arguments passed directly to teuthology-suite

echo "Scheduling $branch branch"
teuthology-suite -v -c "$branch" -m "$machine" -k "$kernel" -s "$suite" --ceph-repo https://git.ceph.com/ceph.git --suite-repo https://git.ceph.com/ceph.git --subset "$((RANDOM % partitions))/$partitions" --newest 100 -e "$email" "$@"
