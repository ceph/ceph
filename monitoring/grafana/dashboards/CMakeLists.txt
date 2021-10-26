set(CEPH_GRAFANA_DASHBOARDS_DIR "${CMAKE_INSTALL_SYSCONFDIR}/grafana/dashboards/ceph-dashboard"
  CACHE PATH "Location for grafana dashboards")
file(GLOB CEPH_GRAFANA_DASHBOARDS "*.json")
install(FILES
  ${CEPH_GRAFANA_DASHBOARDS}
  DESTINATION ${CEPH_GRAFANA_DASHBOARDS_DIR})

set(CEPH_BUILD_VIRTUALENV $ENV{TMPDIR})
if(NOT CEPH_BUILD_VIRTUALENV)
  set(CEPH_BUILD_VIRTUALENV ${CMAKE_BINARY_DIR})
endif()

if(WITH_GRAFANA)
  include(AddCephTest)
  add_tox_test(grafana-check TOX_ENVS grafonnet-check)
  add_tox_test(grafana-query-test TOX_ENVS promql-query-test)
  add_tox_test(grafana-lint TOX_ENVS lint)
  set(ver 0.1.0)
  set(name grafonnet-lib)
  include(ExternalProject)
  ExternalProject_Add(${name}
    URL https://github.com/grafana/${name}/archive/v${ver}/${name}-${ver}.tar.gz
    URL_MD5 0798752ed40864fa8b3db40a3c970642
    BUILD_COMMAND ""
    CONFIGURE_COMMAND ""
    INSTALL_COMMAND ""
    EXCLUDE_FROM_ALL ON)
  add_dependencies(tests
    ${name})
  ExternalProject_Get_Property(${name} SOURCE_DIR)
  set_property(
    TEST run-tox-grafana-check run-tox-grafana-query-test run-tox-grafana-lint
    APPEND
    PROPERTY ENVIRONMENT
    GRAFONNET_PATH=${SOURCE_DIR}/grafonnet)
endif()
