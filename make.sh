#!/bin/bash

source="dm_clock_srv.cc test_server.cc test_client.cc test.cc"

fail() {
    echo "Failed."
    exit 1
}

obj() {
    echo $1 | sed 's/\.[^.]*$/.o/'
}

cflags="-std=c++11 -g -O0 -pthread"
lflags="-g -O0"

# clang does not want -pthread at linking stage, gcc does
if g++ --version 2>&1 | grep clang > /dev/null; then
    :
else
    lflags="$lflags -pthread"
fi
    
obj=""
for s in $source ; do
    echo "Compiling ${s}"
    g++ $cflags -I /usr/local/include -c -o obj/`obj $s` src/$s || fail
    obj="$obj obj/`obj $s`"
done

echo "linking ${obj}"
g++ $lflags -o test $obj || fail

echo "Succeeded!"
