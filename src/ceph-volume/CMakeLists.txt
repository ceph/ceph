
include(Distutils)
distutils_install_module(ceph_volume
  INSTALL_SCRIPT ${CMAKE_INSTALL_FULL_SBINDIR})

if(FREEBSD)
  add_subdirectory(plugin/zfs)
endif()

# Required for running ceph-volume inventory in a vstart environment
set(CEPH_VOLUME_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/ceph-volume-virtualenv)

add_custom_command(
  OUTPUT ${CEPH_VOLUME_VIRTUALENV}/bin/python
  COMMAND ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh --python=${PYTHON_EXECUTABLE} ${CEPH_VOLUME_VIRTUALENV}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/ceph-volume
  COMMENT "ceph-volume venv is being created")

add_custom_command(
  OUTPUT ${CEPH_VOLUME_VIRTUALENV}/bin/ceph-volume
  DEPENDS ${CEPH_VOLUME_VIRTUALENV}/bin/python
  COMMAND . ${CEPH_VOLUME_VIRTUALENV}/bin/activate && ${CEPH_VOLUME_VIRTUALENV}/bin/python setup.py develop && deactivate
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/ceph-volume
  COMMENT "${CMAKE_SOURCE_DIR}/src/ceph-volume")

add_custom_target(ceph-volume-venv-setup
  DEPENDS ${CEPH_VOLUME_VIRTUALENV}/bin/ceph-volume)

