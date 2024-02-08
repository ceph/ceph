
include(Distutils)

distutils_install_module(ceph_node_proxy
  INSTALL_SCRIPT ${CMAKE_INSTALL_FULL_SBINDIR})

# Required for running ceph-node-proxy in a vstart environment
set(CEPH_NODE_PROXY_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/ceph-node-proxy-virtualenv)

add_custom_command(
  OUTPUT ${CEPH_NODE_PROXY_VIRTUALENV}/bin/python
  COMMAND ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh --python=${Python3_EXECUTABLE} ${CEPH_NODE_PROXY_VIRTUALENV}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/ceph-node-proxy
  COMMENT "ceph-node-proxy venv is being created")

add_custom_command(
  OUTPUT ${CEPH_NODE_PROXY_VIRTUALENV}/bin/ceph-node-proxy
  DEPENDS ${CEPH_NODE_PROXY_VIRTUALENV}/bin/python
  COMMAND . ${CEPH_NODE_PROXY_VIRTUALENV}/bin/activate && ${CEPH_NODE_PROXY_VIRTUALENV}/bin/python setup.py develop && deactivate
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/ceph-node-proxy
  COMMENT "${CMAKE_SOURCE_DIR}/src/ceph-node-proxy")

add_custom_target(ceph-node-proxy-venv-setup
  DEPENDS ${CEPH_NODE_PROXY_VIRTUALENV}/bin/ceph-node-proxy)

