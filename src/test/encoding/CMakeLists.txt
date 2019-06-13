# scripts
add_ceph_test(check-generated.sh ${CMAKE_CURRENT_SOURCE_DIR}/check-generated.sh)
add_ceph_test(readable.sh ${CMAKE_CURRENT_SOURCE_DIR}/readable.sh)

set(SCHEMA_VENV ${CEPH_BUILD_VIRTUALENV}/schema-virtualenv)
add_custom_target(check-schema-test-venv
  COMMAND ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh ${SCHEMA_VENV}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/test/encoding
  COMMENT "schema tests virtualenv is being created")

add_ceph_test(check-schema.sh ${CMAKE_CURRENT_SOURCE_DIR}/check-schema.sh)
add_dependencies(tests check-schema-test-venv)
