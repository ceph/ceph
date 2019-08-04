if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(mgr-insights ${CMAKE_CURRENT_SOURCE_DIR})
endif()
