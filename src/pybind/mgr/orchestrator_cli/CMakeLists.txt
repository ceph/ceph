if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(mgr-orchestrator_cli ${CMAKE_CURRENT_SOURCE_DIR})
endif()
