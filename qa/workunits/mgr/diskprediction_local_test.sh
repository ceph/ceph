#! /usr/bin/env bash
# -*- mode:shell-script; tab-width:8; sh-basic-offset:2; indent-tabs-mode:t -*-
# vim: ts=8 sw=8 ft=bash smarttab

set -ex

# run directly diskprediction_local unit tests
python3 -m /usr/share/ceph/mgr/diskprediction_local/tests.test_predictors
