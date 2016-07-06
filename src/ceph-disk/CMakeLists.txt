set(CEPH_DISK_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/ceph-disk-virtualenv)

add_custom_target(ceph-disk ALL
  COMMAND
  ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh ${CEPH_DISK_VIRTUALENV} &&
  ${CEPH_DISK_VIRTUALENV}/bin/pip install --disable-pip-version-check --no-index --use-wheel --find-links=file:${CMAKE_SOURCE_DIR}/src/ceph-disk/wheelhouse -e .
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/ceph-disk
  COMMENT "ceph-disk is being created")

include(Distutils)
distutils_install_module(ceph_disk
  INSTALL_SCRIPT ${CMAKE_INSTALL_FULL_SBINDIR})
