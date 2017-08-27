#! /usr/bin/env bash
source copy_func.sh
allparms=$*
cmdv=$1
shift
sites=$*
for mac in $sites; do
    echo $cmdv $mac
    if [ -f ~/secrets ]; then
        copy_file ~/secrets $mac . 0777 ubuntu:ubuntu
    fi
    copy_file execs/${cmdv} $mac . 0777 ubuntu:ubuntu
    ssh $mac ./${cmdv} &
done
./staller.sh $allparms
for mac in $sites; do
    ssh $mac sudo rm -rf secrets
done
echo "DONE"
