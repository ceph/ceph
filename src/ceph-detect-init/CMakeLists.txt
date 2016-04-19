set(CEPH_DETECT_INIT_VIRTUALENV ${CMAKE_BINARY_DIR}/ceph-detect-init-virtualenv)

add_custom_target(ceph-detect-init
  COMMAND
  ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh ${CEPH_DETECT_INIT_VIRTUALENV} &&
  ${CEPH_DETECT_INIT_VIRTUALENV}/bin/pip install --no-index --use-wheel --find-links=file:${CMAKE_SOURCE_DIR}/src/ceph-detect-init/wheelhouse -e .
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/ceph-detect-init
  COMMENT "ceph-detect-init is being created")
