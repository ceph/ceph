if(WITH_GRAFANA)
  set(CEPH_GRAFANA_DASHBOARDS_DIR "${CMAKE_INSTALL_SYSCONFDIR}/grafana/dashboards/ceph-dashboard"
    CACHE PATH "Location for grafana dashboards")
  file(GLOB CEPH_GRAFANA_DASHBOARDS "dashboards_out/*.json")
  install(FILES
    ${CEPH_GRAFANA_DASHBOARDS}
    DESTINATION ${CEPH_GRAFANA_DASHBOARDS_DIR})
  if(WITH_TESTS)
    set(CEPH_BUILD_VIRTUALENV $ENV{TMPDIR})
    if(NOT CEPH_BUILD_VIRTUALENV)
      include(AddCephTest)
      set(CEPH_BUILD_VIRTUALENV ${CMAKE_BINARY_DIR})

      add_test(NAME jsonnet-build
          COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/jsonnet-build.sh ${CMAKE_CURRENT_BINARY_DIR})
      set_property(TEST jsonnet-build PROPERTY
          FIXTURES_SETUP jsonnet)
      add_test(NAME jsonnet-cleanup
          COMMAND rm -rf go-jsonnet
                         ${CMAKE_CURRENT_BINARY_DIR}/jsonnet
                         ${CMAKE_CURRENT_BINARY_DIR}/jsonnetfmt)
      set_property(TEST jsonnet-cleanup PROPERTY
          FIXTURES_CLEANUP jsonnet)

      add_test(NAME jsonnet-bundler-build
          COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/jsonnet-bundler-build.sh ${CMAKE_CURRENT_BINARY_DIR})
      set_property(TEST jsonnet-bundler-build PROPERTY
          FIXTURES_SETUP jsonnet-bundler)
      add_test(NAME jsonnet-bundler-cleanup
          COMMAND rm -rf jsonnet-bundler ${CMAKE_CURRENT_BINARY_DIR}/jb)
      set_property(TEST jsonnet-bundler-cleanup PROPERTY
          FIXTURES_CLEANUP jsonnet-bundler)

      add_tox_test(grafana-lint TOX_ENVS lint)
      add_tox_test(jsonnet-lint TOX_ENVS jsonnet-lint)
      set_property(TEST run-tox-jsonnet-lint PROPERTY
          FIXTURES_REQUIRED venv-for-jsonnet-lint jsonnet
          ENVIRONMENT "PATH=${CMAKE_CURRENT_BINARY_DIR}:$ENV{PATH}")
      add_tox_test(jsonnet-check TOX_ENVS jsonnet-check)
      set_property(TEST run-tox-jsonnet-check PROPERTY
          FIXTURES_REQUIRED venv-for-jsonnet-check jsonnet jsonnet-bundler
          ENVIRONMENT "PATH=${CMAKE_CURRENT_BINARY_DIR}:$ENV{PATH}")

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
