#!/bin/bash

server_src="src/dm_clock_srv.cc src/test.cc"

g++ -std=c++11 -I /usr/local/include -o test $server_src
