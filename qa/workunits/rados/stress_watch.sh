#!/bin/sh -e

ceph_test_stress_watch
ceph_multi_stress_watch rep reppool repobj
ceph_multi_stress_watch ec ecpool ecobj

exit 0
