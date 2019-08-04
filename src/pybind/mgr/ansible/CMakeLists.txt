if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(mgr-ansible ${CMAKE_CURRENT_SOURCE_DIR})
endif()
