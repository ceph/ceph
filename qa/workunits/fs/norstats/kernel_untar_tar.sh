#!/usr/bin/env bash
# check if there is file changed while being archived

set -ex

KERNEL=linux-4.0.5

wget -q http://download.ceph.com/qa/$KERNEL.tar.xz

mkdir untar_tar
cd untar_tar

tar Jxvf ../$KERNEL.tar.xz $KERNEL/Documentation/
tar cf doc.tar $KERNEL

tar xf doc.tar
sync
tar c $KERNEL >/dev/null

rm -rf $KERNEL

tar xf doc.tar
sync
tar c $KERNEL >/dev/null

echo Ok
