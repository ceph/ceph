#!/bin/bash

server_src="src/test2.cc"
out=test2

g++ -std=c++11 -I /usr/local/include -g -o $out $server_src
# g++ -std=c++11 -I /usr/local/include -o $out $server_src
