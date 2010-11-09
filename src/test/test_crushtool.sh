#!/bin/bash -x

#
# Test crushtool to make sure that it can handle various crush maps
#

# Includes
dir=`dirname $0`
dir=`readlink -f $dir`
source $dir/test_common.sh
setup_tempdir
cd $dir/..

run_crushtool() {
        ./crushtool $@ || die "crushtool failed!"
}

run() {
        run_crushtool -c $dir/test_crushtool/need_tree_order.crush \
                -o $TEMPDIR/nto.compiled
        run_crushtool -d $TEMPDIR/nto.compiled -o $TEMPDIR/nto.conf
        run_crushtool -c $TEMPDIR/nto.conf -o $TEMPDIR/nto.recompiled
}

$@
