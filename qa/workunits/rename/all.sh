#!/bin/bash -ex

dir=`dirname $0`

CEPH_TOOL='./ceph'
$CEPH_TOOL || CEPH_TOOL='ceph'

CEPH_ARGS=$CEPH_ARGS CEPH_TOOL=$CEPH_TOOL $dir/prepare.sh

CEPH_ARGS=$CEPH_ARGS CEPH_TOOL=$CEPH_TOOL $dir/pri_nul.sh
rm ./?/* || true

CEPH_ARGS=$CEPH_ARGS CEPH_TOOL=$CEPH_TOOL $dir/rem_nul.sh
rm ./?/* || true

CEPH_ARGS=$CEPH_ARGS CEPH_TOOL=$CEPH_TOOL $dir/pri_pri.sh
rm ./?/* || true

CEPH_ARGS=$CEPH_ARGS CEPH_TOOL=$CEPH_TOOL $dir/rem_pri.sh
rm ./?/* || true

CEPH_ARGS=$CEPH_ARGS CEPH_TOOL=$CEPH_TOOL $dir/rem_rem.sh
rm ./?/* || true

CEPH_ARGS=$CEPH_ARGS CEPH_TOOL=$CEPH_TOOL $dir/pri_nul.sh
rm -r ./?/* || true

CEPH_ARGS=$CEPH_ARGS CEPH_TOOL=$CEPH_TOOL $dir/pri_pri.sh
rm -r ./?/* || true

CEPH_ARGS=$CEPH_ARGS CEPH_TOOL=$CEPH_TOOL $dir/dir_pri_pri.sh
rm -r ./?/* || true

CEPH_ARGS=$CEPH_ARGS CEPH_TOOL=$CEPH_TOOL $dir/dir_pri_nul.sh
rm -r ./?/* || true

