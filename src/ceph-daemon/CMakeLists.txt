if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(ceph-daemon TOX_ENVS)
endif()
