#!/bin/bash

suite=$1
ceph=$2
kernel=$3
email=$4
flavor=$5
teuthology_branch=$6
mtype=$7
template=$8
distro=$9

if [ -z "$kernel" ]; then
    echo "usage: $0 <suite> <ceph branch> <kernel branch> [email] [flavor] [teuthology-branch] [machinetype] [template] [distro]"
    echo "  flavor can be 'basic', 'gcov', 'notcmalloc'."
    exit 1
fi

if [ -z "$email" ]
then
    email='ceph-qa@ceph.com'
    email_specified=0
else
    email_specified=1
fi
[ -z "$flavor" ] && flavor='basic'
[ -z "$distro" ] && distro='ubuntu'
[ -z "$mtype" ] && mtype='plana'

multi=`echo $mtype | awk -F' |,|-|\t' '{print NF}'`
if [ $multi -gt 1 ]
then
    tube=multi
else
    tube=$mtype
fi

stamp=`date +%Y-%m-%d_%H:%M:%S`
nicesuite=`echo $suite | sed 's/\//:/g'`
name=`whoami`"-$stamp-$nicesuite-$ceph-$kernel-$flavor-$tube"

function schedule_fail {
    SUBJECT="Failed to schedule $name"
    MESSAGE="$@"
    echo $SUBJECT:
    echo $MESSAGE
    if [ ! -z "$email" ] && [ "$email_specified" -eq 1 ]
    then
        echo "$MESSAGE" | mail -s "$SUBJECT" $email
    fi
    exit 1
}

if [ "$kernel" = "-" ]
then
    kernelvalue=""
else
    if [ "$kernel" = "distro" ]
    then
        KERNEL_SHA1=distro
    else
        KERNEL_SHA1=`wget http://gitbuilder.ceph.com/kernel-deb-precise-x86_64-basic/ref/$kernel/sha1 -O- 2>/dev/null`
    fi
        [ -z "$KERNEL_SHA1" ] && schedule_fail "Couldn't find kernel branch $kernel"
        kernelvalue="kernel:
  kdb: true
  sha1: $KERNEL_SHA1"
fi
##
[ ! -d ~/src/ceph-qa-suite ] && schedule_fail "error: expects to find ~/src/ceph-qa-suite"
[ ! -d ~/src/teuthology/virtualenv/bin ] && schedule_fail "error: expects to find ~/src/teuthology/virtualenv/bin"

## get sha1
if [ "$distro" = "ubuntu" ]
then
    if [ "$mtype" = "saya" ]
    then
        CEPH_SHA1=`wget http://gitbuilder.ceph.com/ceph-deb-saucy-armv7l-$flavor/ref/$ceph/sha1 -O- 2>/dev/null`
    else
        CEPH_SHA1=`wget http://gitbuilder.ceph.com/ceph-deb-precise-x86_64-$flavor/ref/$ceph/sha1 -O- 2>/dev/null`
    fi
else
    CEPH_SHA1=`wget http://gitbuilder.ceph.com/ceph-rpm-fc18-x86_64-$flavor/ref/$ceph/sha1 -O- 2>/dev/null`
fi

[ -z "$CEPH_SHA1" ] && schedule_fail "Can't find ceph branch $ceph"

# Are there packages for this sha1?
if [ "$distro" = "ubuntu" ]
then
    if [ "$mtype" = "saya" ]
    then
        CEPH_VER=`wget http://gitbuilder.ceph.com/ceph-deb-saucy-armv7l-$flavor/sha1/$CEPH_SHA1/version -O- 2>/dev/null`
    else
        CEPH_VER=`wget http://gitbuilder.ceph.com/ceph-deb-precise-x86_64-$flavor/sha1/$CEPH_SHA1/version -O- 2>/dev/null`
    fi
else
    CEPH_VER=`wget http://gitbuilder.ceph.com/ceph-rpm-fc18-x86_64-$flavor/sha1/$CEPH_SHA1/version -O- 2>/dev/null`
fi

[ -z "$CEPH_VER" ] && schedule_fail "Can't find packages for ceph branch $ceph sha1 $CEPH_SHA1"

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
      osd:
        debug ms: 1
        debug osd: 5
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

echo "name $name"

./virtualenv/bin/teuthology-suite -v $fn \
    --base ~/src/ceph-qa-suite/suites \
    --collections $suite \
    --email $email \
    --timeout 36000 \
    --name $name \
    --worker $tube
