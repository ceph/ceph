#!/bin/bash

# server_src="src/dm_clock_srv.cc src/test.cc"

fail() {
    echo "Failed!"
    exit 1
}

g++ -std=c++11 -I /usr/local/include -c src/dm_clock_srv.cc || fail
g++ -std=c++11 -I /usr/local/include -c src/test.cc         || fail
g++ -o test dm_clock_srv.o test.o                           || fail

echo "Succeeded!"
