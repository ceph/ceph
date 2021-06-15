if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(cephadm TOX_ENVS py3 mypy flake8)
endif()

add_custom_command(
    OUTPUT "${CMAKE_SOURCE_DIR}/src/cephadm/cephadm"
    DEPENDS "${CMAKE_SOURCE_DIR}/src/cephadm/cephadm.py"
    COMMAND bash build.sh
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/cephadm
    COMMENT "compiling cephadm"
)

add_custom_target(cephadm-cephadm
  ALL
  DEPENDS ${CMAKE_SOURCE_DIR}/src/cephadm/cephadm
)
