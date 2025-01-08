#!/usr/bin/env bash

set -ex

mkdir -p ./dir0/new1
mkdir -p ./dir0/new2
touch ./dir0/new1/file{0..9}

count=0

while true ;
do
  for i in file0 file1 file2 file3 file4 file5 file6 file7 file8 file9
  do
    echo _____ $i ;
    date -u;
    ls -l ./dir0/new1/ ./dir0/new2/
    /usr/bin/mv -f ./dir0/new1/$i ./dir0/new2/
  done

  for i in file0 file1 file2 file3 file4 file5 file6 file7 file8 file9
  do
    date -u;
    echo _____ $i ;
    ls -l ./dir0/new1/ ./dir0/new2/
    /usr/bin/mv -f ./dir0/new2/$i ./dir0/new1/
  done

  count=$((count+1))
  if [ "$count" -gt 10000 ]; then
      break
  fi
done
