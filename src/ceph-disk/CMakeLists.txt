add_custom_target(ceph-disk
  COMMAND
  ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh &&
  virtualenv/bin/pip install --no-index --use-wheel --find-links=file:${CMAKE_SOURCE_DIR}/src/ceph-disk/wheelhouse -e .
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/ceph-disk
  COMMENT "ceph-disk is being created")
