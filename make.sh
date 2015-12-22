#!/bin/bash

source="dm_clock_srv.cc test_server.cc test.cc"

fail() {
    echo "Failed!"
    exit 1
}

obj() {
    echo $1 | sed 's/\.[^.]*$/.o/'
}

cflags="-g -O0"

obj=""
for s in $source ; do
    g++ -std=c++11 $cflags -I /usr/local/include -c -o obj/`obj $s` src/$s || fail
    obj="$obj `obj $s`"
done
g++ $cflags -pthread -o test $obj       || fail

echo "Succeeded!"
