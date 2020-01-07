if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(cephadm TOX_ENVS mypy)
endif()

add_custom_command(
    OUTPUT "${CMAKE_SOURCE_DIR}/src/cephadm/cephadm"
    DEPENDS "${CMAKE_SOURCE_DIR}/src/cephadm/cephadm.py"
    COMMAND rm -rf build
    COMMAND mkdir -p build/cephadm
    COMMAND cp cephadm.py build/cephadm
    COMMAND cd build && python3 -m zipapp cephadm -p /usr/bin/python3
    COMMAND mv build/cephadm.pyz cephadm
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/cephadm
    COMMENT "zipping cephadm"
)

add_custom_target(cephadm-cephadm
  ALL
  DEPENDS ${CMAKE_SOURCE_DIR}/src/cephadm/cephadm
)
