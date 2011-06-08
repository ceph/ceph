#!/bin/bash -ex

dir=`dirname $0`

$dir/prepare.sh

$dir/pri_nul.sh
rm mnt/?/* || true

$dir/rem_nul.sh
rm mnt/?/* || true

$dir/pri_pri.sh
rm mnt/?/* || true

$dir/rem_pri.sh
rm mnt/?/* || true

$dir/rem_rem.sh
rm mnt/?/* || true

$dir/pri_nul.sh
rm -r mnt/?/* || true

$dir/pri_pri.sh
rm -r mnt/?/* || true

