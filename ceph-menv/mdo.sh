#!/bin/bash
cmd=`basename $0`
MENV_ROOT=`dirname $0`/..

if [ -f $MENV_ROOT/.menvroot ]; then
    . $MENV_ROOT/.menvroot
fi

[ "$MRUN_CEPH_ROOT" == "" ] && MRUN_CEPH_ROOT=$HOME/ceph

if [ "$MRUN_CLUSTER" == "" ]; then
    ${MRUN_CEPH_ROOT}/build/bin/$cmd "$@"
    exit $?
fi

${MRUN_CEPH_ROOT}/src/mrun $MRUN_CLUSTER $cmd "$@"
