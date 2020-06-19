set(CEPH_BUILD_VIRTUALENV $ENV{TMPDIR})
if(NOT CEPH_BUILD_VIRTUALENV)
  set(CEPH_BUILD_VIRTUALENV ${CMAKE_BINARY_DIR})
endif()

if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(qa TOX_ENVS flake8-py3 import-tasks)
endif()
