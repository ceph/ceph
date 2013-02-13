#!/bin/sh -e

suite=$1
ceph=$2
kernel=$3
email=$4
flavor=$5
teuthology_branch=$6
template=$7

if [ -z "$email" ]; then
    echo "usage: $0 <suite> <ceph branch> <kernel branch> <email> [flavor] [teuthology-branch] [template]"
    echo "  flavor can be 'basic', 'gcov', 'notcmalloc'."
    exit 1
fi

[ -z "$flavor" ] && flavor='basic'


##
test ! -d ~/src/ceph-qa-suite && echo "error: expects to find ~/src/ceph-qa-suite" && exit 1
test ! -d ~/src/teuthology/virtualenv/bin && echo "error: expects to find ~/src/teuthology/virtualenv/bin" && exit 1

## get sha1
KERNEL_SHA1=`wget http://gitbuilder.ceph.com/kernel-deb-precise-x86_64-basic/ref/$kernel/sha1 -O- 2>/dev/null`
CEPH_SHA1=`wget http://gitbuilder.ceph.com/ceph-tarball-precise-x86_64-$flavor/ref/$ceph/sha1 -O- 2>/dev/null`

[ -z "$KERNEL_SHA1" ] && echo "kernel branch $kernel dne" && exit 1
[ -z "$CEPH_SHA1" ] && echo "ceph branch $ceph dne" && exit 1

if wget http://github.com/ceph/s3-tests/tree/$ceph -O- 2>/dev/null >/dev/null ; then
    s3branch=$ceph
else
    echo "branch $ceph not in s3-tests.git; will use master for s3tests"
    s3branch='master'
fi


## always include this
fn="/tmp/schedule.suite.$$"
trap "rm $fn" EXIT
cat <<EOF > $fn
kernel:
  kdb: true
  sha1: $KERNEL_SHA1
nuke-on-error: true
tasks:
- chef:
- clock:
overrides:
  workunit:
    sha1: $CEPH_SHA1
  s3tests:
    branch: $s3branch
  ceph:
    sha1: $CEPH_SHA1
    log-whitelist:
    - slow request
EOF

if [ "$flavor" = "gcov" ]; then
    cat <<EOF >> $fn
    coverage: yes
EOF
fi

## template, too?
if [ -n "$template" ]; then
    sed s/CEPH_SHA1/$CEPH_SHA1/ $template | sed s/KERNEL_SHA1/$KERNEL_SHA1/ >> $fn
fi

##
stamp=`date +%Y-%m-%d_%H:%M:%S`
name=`whoami`"-$stamp-$suite-$ceph-$kernel-$flavor"

if [ -z "$teuthology_branch" ]; then
    if wget http://github.com/ceph/teuthology/tree/$ceph -O- 2>/dev/null >/dev/null ; then
        teuthology_branch=$ceph
    else
        echo "branch $ceph not in teuthology.git; will use master for teuthology"
        teuthology_branch='master'
    fi
fi

~/src/teuthology/virtualenv/bin/teuthology-suite -v $fn \
    --collections ~/src/ceph-qa-suite/suites/$suite/* \
    --email $email \
    --timeout 21600 \
    --name $name \
    --branch $teuthology_branch
