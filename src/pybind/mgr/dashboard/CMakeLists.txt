install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DESTINATION ${CEPH_INSTALL_DATADIR}/mgr
  ${mgr_module_install_excludes}
  PATTERN "frontend/*" EXCLUDE
  PATTERN ".*" EXCLUDE)
 
if(WITH_MGR_DASHBOARD_FRONTEND)
  # build from source
  add_subdirectory(frontend)
  if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(mgr-dashboard
    TOX_ENVS py3 lint check openapi-check grafonnet-check)
  set(ver 0.1.0)
  set(name grafonnet-lib)
  set(tar ${CMAKE_CURRENT_BINARY_DIR}/v${ver}.tar.gz)
  file(DOWNLOAD 
    https://github.com/grafana/grafonnet-lib/archive/refs/tags/v{ver}.tar.gz
    ${tar})
  add_test(
    NAME extract-${name}
    COMMAND ${CMAKE_COMMAND} -E tar xfz ${tar}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
  set_tests_properties(extract-${name} PROPERTIES
    FIXTURES_SETUP ${name})
  add_test(
    NAME remove-${name}
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${name}-${ver})
  set_tests_properties(remove-${name} PROPERTIES
    FIXTURES_CLEANUP ${name})
  set_property(
    TEST ${test_name}
    APPEND_STRING
    PROPERTY FIXTURES_REQUIRED ${name})
  endif()
else()
  # prebuilt
  install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/frontend/dist
    DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/frontend)
  install(FILES frontend/package.json
    DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/frontend)
endif()
