configure_file(${CMAKE_CURRENT_SOURCE_DIR}/ceph/version.py.in
  ${CMAKE_CURRENT_SOURCE_DIR}/ceph/version.py @ONLY)

include(Distutils)
distutils_install_module(ceph)

if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(python-common TOX_ENVS py3 lint)
endif()
