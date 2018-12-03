set(MGR_ANSIBLE_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/mgr-ansible-virtualenv)

add_custom_target(mgr-ansible-test-venv
  COMMAND ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh --python=${MGR_PYTHON_EXECUTABLE} ${MGR_ANSIBLE_VIRTUALENV}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/ansible
  COMMENT "ansible tests virtualenv is being created")
add_dependencies(tests mgr-ansible-test-venv)
