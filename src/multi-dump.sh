#!/bin/bash

#
# multi-dump.sh
#
# Dumps interesting information about the Ceph cluster at a series of epochs.
#

### Functions
usage() {
        cat <<EOF
multi-dump.sh: dumps out ceph maps

-D                         Enable diff-mode
-e <start-epoch>           What epoch to end with.
-h                         This help message
-s <start-epoch>           What epoch to start with. Defaults to 1.
-t <map-type>              What type of map to dump. Defaults to osdmap.
                           Valid map types are: osdmap, 
EOF
}

cleanup() {
        [ -n ${TEMPDIR} ] && rm -rf "${TEMPDIR}"
}

die() {
        echo $@
        exit 1
}

dump_osdmap() {
        for v in `seq $START_EPOCH $END_EPOCH`; do
                ./ceph osd getmap $v -o $TEMPDIR/$v >> $TEMPDIR/cephtool-out \
                        || die "cephtool failed to dump epoch $v"
        done
        if [ $DIFFMODE -eq 1 ]; then
                for v in `seq $START_EPOCH $END_EPOCH`; do
                        ./osdmaptool --print $TEMPDIR/$v > $TEMPDIR/$v.out
                done
                cat $TEMPDIR/$START_EPOCH.out
                E=$((END_EPOCH-1))
                for v in `seq $START_EPOCH $E`; do
                        S=$((v+1))
                        echo "************** $S **************"
                        diff  $TEMPDIR/$v.out $TEMPDIR/$S.out
                done
        else
                for v in `seq $START_EPOCH $END_EPOCH`; do
                        echo "************** $v **************"
                        ./osdmaptool --print $TEMPDIR/$v \
                                || die "osdmaptool failed to print epoch $v"
                done
        fi
}

### Setup
trap cleanup INT TERM EXIT
TEMPDIR=`mktemp -d`
MYDIR=`dirname $0`
MYDIR=`readlink -f $MYDIR`
MAP_TYPE=osdmap
cd $MYDIR

### Parse arguments
DIFFMODE=0
START_EPOCH=1
END_EPOCH=0

while getopts  "De:hs:t:" flag; do
case $flag in
        D) DIFFMODE=1;;

        e) END_EPOCH=$OPTARG;;

        h)  usage
            exit 0
            ;;

        s) START_EPOCH=$OPTARG;;

        t) MAP_TYPE=$OPTARG;;

        *) usage
           exit 1;;
esac
done
[ $END_EPOCH -eq 0 ] && die "You must supply an end epoch with -e"

### Dump maps
case $MAP_TYPE in
        "osdmap") dump_osdmap;;

        *) die "sorry, don't know how to handle MAP_TYPE '$MAP_TYPE'"
esac

exit 0
