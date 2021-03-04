if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(cephadm TOX_ENVS py3 mypy flake8)
endif()
