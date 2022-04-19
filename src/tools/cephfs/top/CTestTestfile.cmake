# CMake generated Testfile for 
# Source directory: /home/sh3ll/ceph/ceph/src/tools/cephfs/top
# Build directory: /home/sh3ll/ceph/ceph/src/tools/cephfs/top
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(setup-venv-for-cephfs-top "/home/sh3ll/ceph/ceph/src/tools/setup-virtualenv.sh" "--python=/usr/bin/python3.8" "/home/sh3ll/ceph/ceph/cephfs-top-virtualenv")
set_tests_properties(setup-venv-for-cephfs-top PROPERTIES  FIXTURES_SETUP "venv-for-cephfs-top" WORKING_DIRECTORY "/home/sh3ll/ceph/ceph/src/tools/cephfs/top" _BACKTRACE_TRIPLES "/home/sh3ll/ceph/ceph/cmake/modules/AddCephTest.cmake;74;add_test;/home/sh3ll/ceph/ceph/src/tools/cephfs/top/CMakeLists.txt;6;add_tox_test;/home/sh3ll/ceph/ceph/src/tools/cephfs/top/CMakeLists.txt;0;")
add_test(teardown-venv-for-cephfs-top "/usr/bin/cmake" "-E" "remove_directory" "/home/sh3ll/ceph/ceph/cephfs-top-virtualenv")
set_tests_properties(teardown-venv-for-cephfs-top PROPERTIES  FIXTURES_CLEANUP "venv-for-cephfs-top" _BACKTRACE_TRIPLES "/home/sh3ll/ceph/ceph/cmake/modules/AddCephTest.cmake;80;add_test;/home/sh3ll/ceph/ceph/src/tools/cephfs/top/CMakeLists.txt;6;add_tox_test;/home/sh3ll/ceph/ceph/src/tools/cephfs/top/CMakeLists.txt;0;")
add_test(run-tox-cephfs-top "/home/sh3ll/ceph/ceph/src/script/run_tox.sh" "--source-dir" "/home/sh3ll/ceph/ceph" "--build-dir" "/home/sh3ll/ceph/ceph" "--tox-path" "/home/sh3ll/ceph/ceph/src/tools/cephfs/top" "--tox-envs" "py3" "--venv-path" "/home/sh3ll/ceph/ceph/cephfs-top-virtualenv")
set_tests_properties(run-tox-cephfs-top PROPERTIES  ENVIRONMENT "CEPH_ROOT=/home/sh3ll/ceph/ceph;CEPH_BIN=/home/sh3ll/ceph/ceph/bin;CEPH_LIB=/home/sh3ll/ceph/ceph/lib;CEPH_BUILD_VIRTUALENV=/home/sh3ll/ceph/ceph;LD_LIBRARY_PATH=/home/sh3ll/ceph/ceph/lib;PATH=/home/sh3ll/ceph/ceph/bin:/home/sh3ll/ceph/ceph/src:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin;PYTHONPATH=/home/sh3ll/ceph/ceph/src/pybind" FIXTURES_REQUIRED "venv-for-cephfs-top" _BACKTRACE_TRIPLES "/home/sh3ll/ceph/ceph/cmake/modules/AddCephTest.cmake;85;add_test;/home/sh3ll/ceph/ceph/src/tools/cephfs/top/CMakeLists.txt;6;add_tox_test;/home/sh3ll/ceph/ceph/src/tools/cephfs/top/CMakeLists.txt;0;")
