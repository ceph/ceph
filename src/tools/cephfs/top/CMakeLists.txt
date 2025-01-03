include(Distutils)
distutils_install_module(cephfs-top)

if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(cephfs-top)
endif()

set(MINIMUM_COMPATIBLE_VERSION 3.6.0)
find_package(Python3 ${MINIMUM_COMPATIBLE_VERSION} REQUIRED
  COMPONENTS Interpreter)
