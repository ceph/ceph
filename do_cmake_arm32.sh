#!/bin/bash

# Assemble the ARGS
ARGS="${ARGS} -DCMAKE_BUILD_TYPE=Release"
ARGS="${ARGS} -DCMAKE_C_FLAGS_RELEASE=\"-Os\""
ARGS="${ARGS} -DCMAKE_CXX_FLAGS_RELEASE=\"-Os\""
ARGS="${ARGS} -DCMAKE_C_COMPILER=/usr/local/gcc-8.2.0/bin/gcc-8.2.0"
ARGS="${ARGS} -DCMAKE_CXX_COMPILER=/usr/local/gcc-8.2.0/bin/g++-8.2.0" 

# Now execute cmake script
ARGS="${ARGS}" ./do_cmake.sh
