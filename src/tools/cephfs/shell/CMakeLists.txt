include(Distutils)
distutils_install_module(cephfs-shell)

if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(cephfs-shell)
endif()
