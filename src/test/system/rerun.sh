#!/usr/bin/env bash

[ -z $ITERATIONS ] && ITERATIONS=10

TMPDIR=`mktemp -d -t rerun_logs.XXXXXXXXXX` || exit 1

rm -rf $TMPDIR/logs
mkdir $TMPDIR/logs

for i in `seq 1 $ITERATIONS`; do
    echo "********************* iteration $i *********************"
    LOG_FILE_BASE=$TMPDIR/logs $EXE "$@"
    if [ $? -ne 0 ]; then
        die "failed! logs are in $TMPDIR/logs"
    fi
done

echo "********************* success *********************"
rm -rf $TMPDIR
exit 0
