set(MGR_INSIGHTS_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/mgr-insights-virtualenv)

add_custom_target(mgr-insights-test-venv
  COMMAND ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh --python=${MGR_PYTHON_EXECUTABLE} ${MGR_INSIGHTS_VIRTUALENV}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/insights
  COMMENT "insights tests virtualenv is being created")
add_dependencies(tests mgr-insights-test-venv)
