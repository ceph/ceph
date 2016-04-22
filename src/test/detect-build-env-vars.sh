#!/bin/bash

if [ -n "$CEPH_BUILD_DIR" ] && [ -n "$CEPH_ROOT" ] && [ -n "$CEPH_BIN" ] && [ -n "$CEPH_LIB" ]; then
  echo "Enivronment Variables Already Set"
elif [ -e CMakeCache.txt ]; then
  echo "Environment Variables Not All Set, Detected Build System CMake"
  echo "Setting Environment Variables"
  export CEPH_ROOT=`grep Ceph_SOURCE_DIR CMakeCache.txt | cut -d "=" -f 2`
  export CEPH_BUILD_DIR=`pwd`
  export CEPH_BIN=$CEPH_BUILD_DIR/bin
  export CEPH_LIB=$CEPH_BUILD_DIR/lib
  export PATH=$CEPH_BIN:$PATH
  export LD_LIBRARY_PATH=$CEPH_LIB
elif [ -e .libs ]; then
  echo "Environment Variables Not All Set, Detected Build System Autotools"
  echo "Setting Environment Variables"
  export CEPH_ROOT=".."
  export CEPH_BUILD_DIR="."
  export CEPH_BIN="."
  export CEPH_LIB=".libs"
  export PATH=.:$PATH
  export LD_LIBRARY_PATH=".libs"
else
  echo "Please execute this command out of the proper directory"
  exit 1
fi


