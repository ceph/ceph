#!/bin/bash -e

#command line => CEPH_BRANCH=<branch>; MACHINE_NAME=<machine_type>; SUITE_NAME=<suite>; ../schedule_subset.sh <day_of_week> $CEPH_BRANCH $MACHINE_NAME $SUITE_NAME $CEPH_QA_EMAIL $KERNEL <$FILTER>

partition="$1"
shift
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
teuthology-suite -v -c "$branch" -m "$machine" -k "$kernel" -s "$suite" --subset $(echo "$(date +%j) + $partition" | bc)/"$partitions" --newest 100 -e "$email" "$@"
