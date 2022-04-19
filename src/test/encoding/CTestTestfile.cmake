# CMake generated Testfile for 
# Source directory: /home/sh3ll/ceph/ceph/src/test/encoding
# Build directory: /home/sh3ll/ceph/ceph/src/test/encoding
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(check-generated.sh "/home/sh3ll/ceph/ceph/src/test/encoding/check-generated.sh")
set_tests_properties(check-generated.sh PROPERTIES  ENVIRONMENT "CEPH_ROOT=/home/sh3ll/ceph/ceph;CEPH_BIN=/home/sh3ll/ceph/ceph/bin;CEPH_LIB=/home/sh3ll/ceph/ceph/lib;CEPH_BUILD_DIR=/home/sh3ll/ceph/ceph;LD_LIBRARY_PATH=/home/sh3ll/ceph/ceph/lib;PATH=/home/sh3ll/ceph/ceph/bin:/home/sh3ll/ceph/ceph/src:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin;PYTHONPATH=/home/sh3ll/ceph/ceph/lib/cython_modules/lib.3:/home/sh3ll/ceph/ceph/src/pybind;CEPH_BUILD_VIRTUALENV=/home/sh3ll/ceph/ceph" TIMEOUT "3600" _BACKTRACE_TRIPLES "/home/sh3ll/ceph/ceph/cmake/modules/AddCephTest.cmake;5;add_test;/home/sh3ll/ceph/ceph/src/test/encoding/CMakeLists.txt;2;add_ceph_test;/home/sh3ll/ceph/ceph/src/test/encoding/CMakeLists.txt;0;")
add_test(readable.sh "/home/sh3ll/ceph/ceph/src/test/encoding/readable.sh")
set_tests_properties(readable.sh PROPERTIES  ENVIRONMENT "CEPH_ROOT=/home/sh3ll/ceph/ceph;CEPH_BIN=/home/sh3ll/ceph/ceph/bin;CEPH_LIB=/home/sh3ll/ceph/ceph/lib;CEPH_BUILD_DIR=/home/sh3ll/ceph/ceph;LD_LIBRARY_PATH=/home/sh3ll/ceph/ceph/lib;PATH=/home/sh3ll/ceph/ceph/bin:/home/sh3ll/ceph/ceph/src:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin;PYTHONPATH=/home/sh3ll/ceph/ceph/lib/cython_modules/lib.3:/home/sh3ll/ceph/ceph/src/pybind;CEPH_BUILD_VIRTUALENV=/home/sh3ll/ceph/ceph" TIMEOUT "3600" _BACKTRACE_TRIPLES "/home/sh3ll/ceph/ceph/cmake/modules/AddCephTest.cmake;5;add_test;/home/sh3ll/ceph/ceph/src/test/encoding/CMakeLists.txt;3;add_ceph_test;/home/sh3ll/ceph/ceph/src/test/encoding/CMakeLists.txt;0;")
