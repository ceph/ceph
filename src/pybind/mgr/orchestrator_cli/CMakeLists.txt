if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(mgr-orchestrator_cli)
endif()
