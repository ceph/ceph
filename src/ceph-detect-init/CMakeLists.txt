set(CEPH_DETECT_INIT_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/ceph-detect-init-virtualenv)

add_custom_target(ceph-detect-init
  COMMAND
  ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh --python=python2.7 ${CEPH_DETECT_INIT_VIRTUALENV} &&
  ${CEPH_DETECT_INIT_VIRTUALENV}/bin/pip install --no-index --find-links=file:${CMAKE_SOURCE_DIR}/src/ceph-detect-init/wheelhouse -e .
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/ceph-detect-init
  COMMENT "ceph-detect-init is being created")
add_dependencies(tests ceph-detect-init)

include(Distutils)
distutils_install_module(ceph_detect_init)
