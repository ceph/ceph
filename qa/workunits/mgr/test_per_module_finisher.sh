#!/usr/bin/env bash
set -ex

# This testcase tests the per module finisher stats for enabled modules
# using check counter (qa/tasks/check_counter.py).

# 'balancer' commands
ceph balancer pool ls

# 'crash' commands
ceph crash ls
ceph crash ls-new

# 'device' commands
ceph device query-daemon-health-metrics mon.a

# 'iostat' command
ceph iostat &
pid=$!
sleep 3
kill -SIGTERM $pid

# 'pg_autoscaler' command
ceph osd pool autoscale-status

# 'progress' command
ceph progress
ceph progress json

# 'status' commands
ceph fs status
ceph osd status

# 'telemetry' commands
ceph telemetry status
ceph telemetry diff

echo OK
