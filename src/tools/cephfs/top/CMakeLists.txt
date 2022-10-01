include(Distutils)
distutils_install_module(cephfs-top)

if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(cephfs-top)
endif()
