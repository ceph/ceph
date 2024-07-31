#!/usr/bin/env bash
#
# rbd_mirror_fsx_prepare.sh - test rbd-mirror daemon under FSX workload
#
# The script is used to compare FSX-generated images between two clusters.
#

set -ex

. $(dirname $0)/rbd_mirror_helpers.sh

setup
