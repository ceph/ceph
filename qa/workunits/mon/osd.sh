#!/bin/sh -x

set -e

ua=`uuidgen`
ub=`uuidgen`

# shoudl get same id with same uuid
na=`ceph osd create $ua`
test $na -eq `ceph osd create $ua`

nb=`ceph osd create $ub`
test $nb -eq `ceph osd create $ub`
test $nb -ne $na

ceph osd rm $na
ceph osd rm $na
ceph osd rm $nb
ceph osd rm 1000

na2=`ceph osd create $ua`

echo OK

