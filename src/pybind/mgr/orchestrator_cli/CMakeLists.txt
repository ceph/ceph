set(MGR_ORCHESTRATOR_CLI_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/mgr-orchestrator_cli-virtualenv)

add_custom_target(mgr-orchestrator_cli-test-venv
  COMMAND ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh --python=${MGR_PYTHON_EXECUTABLE} ${MGR_ORCHESTRATOR_CLI_VIRTUALENV}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/orchestrator_cli
  COMMENT "orchestrator_cli tests virtualenv is being created")
add_dependencies(tests mgr-orchestrator_cli-test-venv)
