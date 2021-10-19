if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(prometheus-alerts ${CMAKE_CURRENT_SOURCE_DIR} TOX_ENVS py3)
endif()
