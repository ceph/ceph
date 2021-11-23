if(WITH_GRAFANA)
  set(CEPH_GRAFANA_DASHBOARDS_DIR "${CMAKE_INSTALL_SYSCONFDIR}/grafana/dashboards/ceph-dashboard"
    CACHE PATH "Location for grafana dashboards")
  file(GLOB CEPH_GRAFANA_DASHBOARDS "dashboards_out/*.json")
  install(FILES
    ${CEPH_GRAFANA_DASHBOARDS}
    DESTINATION ${CEPH_GRAFANA_DASHBOARDS_DIR})
  if(WITH_TESTS)
    ExternalProject_Add(jsonnet-bundler
      GIT_REPOSITORY "https://github.com/jsonnet-bundler/jsonnet-bundler.git"
      GIT_TAG "v0.4.0"
      GIT_SHALLOW TRUE
      SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/src/jsonnet-bundler
      CONFIGURE_COMMAND ""
      DOWNLOAD_DIR ${CMAKE_CURRENT_BINARY_DIR}/src
      BUILD_COMMAND make build
      BUILD_IN_SOURCE 1
      INSTALL_COMMAND cp <SOURCE_DIR>/_output/jb <INSTALL_DIR>)

    set(CEPH_BUILD_VIRTUALENV $ENV{TMPDIR})
    if(NOT CEPH_BUILD_VIRTUALENV)
      include(AddCephTest)
      set(CEPH_BUILD_VIRTUALENV ${CMAKE_BINARY_DIR})
      add_tox_test(grafana-lint TOX_ENVS lint)
      add_tox_test(jsonnet-lint TOX_ENVS jsonnet-lint)
      add_tox_test(jsonnet-check TOX_ENVS jsonnet-check)
      add_tox_test(alerts-check TOX_ENVS alerts-check)
      add_tox_test(alerts-lint TOX_ENVS alerts-lint)
      add_tox_test(promql-query-test TOX_ENVS promql-query-test)
    endif()

    if(DEFINED PROMTOOL_EXECUTABLE)
      set(promtool_executable_checked TRUE)
    endif()

    find_program(PROMTOOL_EXECUTABLE promtool)
    if(PROMTOOL_EXECUTABLE)
      execute_process(
        COMMAND ${PROMTOOL_EXECUTABLE} test rules /dev/null
        RESULT_VARIABLE rc
        OUTPUT_QUIET)
      if(NOT rc)
        add_ceph_test(run-promtool-unittests
          ${PROMTOOL_EXECUTABLE} test rules ${CMAKE_SOURCE_DIR}/monitoring/ceph-mixin/tests_alerts/test_alerts.yml)
      elseif(NOT promtool_executable_checked)
        message(WARNING "'${PROMTOOL_EXECUTABLE} test rules' does not work, "
          "please use a newer prometheus")
      endif()
    elseif(NOT promtool_executable_checked)
      message(WARNING "run-promtool-unittests is skipped due to missing promtool")
    endif()
  endif()
endif()
