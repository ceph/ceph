#! /usr/bin/env bash
cmd_wait=$1
shift
sites=$*
donebit=0
while [ $donebit -ne 1 ]; do
    sleep 10
    donebit=1
    for rem in $sites; do
        rval=`ssh $rem ps aux | grep $cmd_wait | wc -l` 
        if [ $rval -gt 0 ]; then
            donebit=0
        fi
    done
done
