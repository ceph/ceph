#!/bin/bash

set -e

script_root=`dirname $0`

if [ -e CMakeCache.txt ]; then
    script_root=$PWD
fi

[ "$#" -lt 1 ] && echo "usage: $0 <name> [entity [id]]" && exit 1

name=$1
entity=$2
id=$3

run_root=$script_root/run/$name
pidpath=$run_root/out

if [ "$entity" == "" ]; then
  pfiles=`ls $pidpath/*.pid` || true
elif [ "$id" == "" ]; then
  pfiles=`ls $pidpath/$entity.*.pid` || true
else
  pfiles=`ls $pidpath/$entity.$id.pid` || true
fi

for pidfile in $pfiles; do
  pid=`cat $pidfile`
  fname=`echo $pidfile | sed 's/.*\///g'`
  echo $pid
  [ "$pid" == "" ] && exit
  [ $pid -eq 0 ] && exit
  echo pid=$pid
  extra_check=""
  entity=`echo $fname | sed 's/\..*//g'`
  [ "$entity" == "radosgw" ] && extra_check="-e lt-radosgw"
  echo entity=$entity pid=$pid
  while ps -p $pid -o args= | grep -q -e $entity $extracheck ; do
    cmd="kill $signal $pid"
    printf "$cmd..."
    $cmd
    sleep 1
    continue
  done
done

