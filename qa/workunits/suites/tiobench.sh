#!/bin/bash -x

die() {
    echo $@
    exit 1
}

timer_fail() {
    echo "timer expired: tiobench has timed out."
    exit 1
}

which tiotest || die "you must install the tiobench package"

trap timer_fail ALRM

pid=$$
( sleep 420 ; kill -14 $pid ) &

for i in `seq 1 10`; do
    tiotest -f 20 -t 10 -d . -T -c -D 20 -r 1000 || die "tiotest failed"
done

echo OK.
