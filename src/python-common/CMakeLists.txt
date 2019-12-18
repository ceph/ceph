include(Distutils)
distutils_install_module(ceph)

if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(python-common TOX_ENVS lint)
endif()
