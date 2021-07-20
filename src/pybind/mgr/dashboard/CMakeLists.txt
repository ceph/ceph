add_subdirectory(frontend)

if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(mgr-dashboard TOX_ENVS py3 lint check openapi-check)
endif()
