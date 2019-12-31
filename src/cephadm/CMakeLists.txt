if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(cephadm TOX_ENVS mypy)
endif()
