#!/usr/bin/env bash

set -e

script_root=`dirname $0`

if [ -e CMakeCache.txt ]; then
    script_root=$PWD
elif [ -e $script_root/../build/CMakeCache.txt ]; then
    script_root=`(cd $script_root/../build; pwd)`
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

MAX_RETRIES=20

for pidfile in $pfiles; do
  pid=`cat $pidfile`
  fname=`echo $pidfile | sed 's/.*\///g'`
  [ "$pid" == "" ] && exit
  [ $pid -eq 0 ] && exit
  echo pid=$pid
  extra_check=""
  entity=`echo $fname | sed 's/\..*//g'`
  name=`echo $fname | sed 's/\.pid$//g'`
  [ "$entity" == "radosgw" ] && extra_check="-e lt-radosgw"
  echo entity=$entity pid=$pid name=$name
  counter=0
  signal=""
  while ps -p $pid -o args= | grep -q -e $entity $extracheck ; do
    if [[ "$counter" -gt MAX_RETRIES ]]; then
        signal="-9"
    fi
    cmd="kill $signal $pid"
    printf "$cmd...\n"
    $cmd
    sleep 1
    counter=$((counter+1))
    continue
  done
done

