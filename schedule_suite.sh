#!/bin/sh -e

suite=$1
ceph=$2
kernel=$3
email=$4
flavor=$5
teuthology_branch=$6
mtype=$7
template=$8
distro=$9

if [ -z "$email" ]; then
    echo "usage: $0 <suite> <ceph branch> <kernel branch> <email> [flavor] [teuthology-branch] [machinetype] [template] [distro]"
    echo "  flavor can be 'basic', 'gcov', 'notcmalloc'."
    exit 1
fi

[ -z "$flavor" ] && flavor='basic'

[ -z "$distro" ] && distro='ubuntu'

if [ "$kernel" = "-" ]
then
    kernelvalue=""
else
    KERNEL_SHA1=`wget http://gitbuilder.ceph.com/kernel-deb-precise-x86_64-basic/ref/$kernel/sha1 -O- 2>/dev/null`
    [ -z "$KERNEL_SHA1" ] && echo "kernel branch $kernel dne" && exit 1
    kernelvalue="kernel:
  kdb: true
  sha1: $KERNEL_SHA1"
fi
##
test ! -d ~/src/ceph-qa-suite && echo "error: expects to find ~/src/ceph-qa-suite" && exit 1
test ! -d ~/src/teuthology/virtualenv/bin && echo "error: expects to find ~/src/teuthology/virtualenv/bin" && exit 1

## get sha1
CEPH_SHA1=`wget http://gitbuilder.ceph.com/ceph-deb-precise-x86_64-$flavor/ref/$ceph/sha1 -O- 2>/dev/null`

[ -z "$CEPH_SHA1" ] && echo "ceph branch $ceph dne" && exit 1


if [ -n "$teuthology_branch" ] && wget http://github.com/ceph/s3-tests/tree/$teuthology_branch -O- 2>/dev/null >/dev/null ; then
    s3branch=$teuthology_branch
elif wget http://github.com/ceph/s3-tests/tree/$ceph -O- 2>/dev/null >/dev/null ; then
    s3branch=$ceph
else
    echo "branch $ceph not in s3-tests.git; will use master for s3tests"
    s3branch='master'
fi
echo "s3branch $s3branch"

if [ -z "$teuthology_branch" ]; then
    if wget http://github.com/ceph/teuthology/tree/$ceph -O- 2>/dev/null >/dev/null ; then
        teuthology_branch=$ceph
    else
        echo "branch $ceph not in teuthology.git; will use master for teuthology"
        teuthology_branch='master'
    fi
fi
echo "teuthology branch $teuthology_branch"

[ -z "$mtype" ] && mtype="plana"

## always include this
fn="/tmp/schedule.suite.$$"
trap "rm $fn" EXIT
cat <<EOF > $fn
teuthology_branch: $teuthology_branch
$kernelvalue
nuke-on-error: true
machine_type: $mtype
os_type: $distro
tasks:
- chef:
- clock.check:
overrides:
  workunit:
    sha1: $CEPH_SHA1
  s3tests:
    branch: $s3branch
  install:
    ceph:
      sha1: $CEPH_SHA1
  ceph:
    sha1: $CEPH_SHA1
    conf:
      mon:
        debug ms: 1
        debug mon: 20
        debug paxos: 20
    log-whitelist:
    - slow request
  ceph-deploy:
    branch:
      dev: $ceph
    conf:
      mon:
        debug mon: 1
        debug paxos: 20
        debug ms: 20
      client:
        log file: /var/log/ceph/ceph-\$name.\$pid.log
  admin_socket:
    branch: $ceph
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
name=`whoami`"-$stamp-$suite-$ceph-$kernel-$flavor-$mtype"

echo "name $name"

./virtualenv/bin/teuthology-suite -v $fn \
    --collections ~/src/ceph-qa-suite/suites/$suite/* \
    --email $email \
    --timeout 36000 \
    --name $name \
    --worker $mtype
