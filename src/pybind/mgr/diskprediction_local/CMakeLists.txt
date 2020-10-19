if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(mgr-diskprediction_local)
endif()
