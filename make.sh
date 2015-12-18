#!/bin/bash

# server_src="src/dm_clock_srv.cc src/test.cc"

fail() {
    echo "Failed!"
    exit 1
}

cflags="-g -O0"

g++ -std=c++11 $cflags -I /usr/local/include -c src/dm_clock_srv.cc || fail
g++ -std=c++11 $cflags -I /usr/local/include -c src/test.cc         || fail
g++ $cflags -o test dm_clock_srv.o test.o                           || fail

echo "Succeeded!"
