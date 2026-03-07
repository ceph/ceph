#!/usr/bin/env bash

if [ -n "$CEPH_BUILD_DIR" ] && [ -n "$CEPH_ROOT" ] && [ -n "$CEPH_BIN" ] && [ -n "$CEPH_LIB" ]; then
  echo "Enivronment Variables Already Set"
elif [ -e CMakeCache.txt ]; then
  echo "Environment Variables Not All Set, Detected Build System CMake"
  echo "Setting Environment Variables"
  export CEPH_ROOT=`grep ceph_SOURCE_DIR CMakeCache.txt | cut -d "=" -f 2`
  export CEPH_BUILD_DIR=`pwd`
  export CEPH_BIN=$CEPH_BUILD_DIR/bin
  export CEPH_LIB=$CEPH_BUILD_DIR/lib
  export PATH=$CEPH_BIN:$PATH
  export LD_LIBRARY_PATH=$CEPH_LIB
else
  echo "Please execute this command out of the proper directory"
  exit 1
fi

# Export sanitizer options if they're set by the test framework.
# These are set via CMake's set_property(TEST ... PROPERTY ENVIRONMENT ...)
# but need to be explicitly exported to be inherited by child processes
# (e.g., ceph CLI tools, ceph-osd, ceph-mon, etc.)
export ASAN_OPTIONS LSAN_OPTIONS UBSAN_OPTIONS TSAN_OPTIONS
