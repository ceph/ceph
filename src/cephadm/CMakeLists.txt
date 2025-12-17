if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(cephadm TOX_ENVS __tox_defaults__)
endif()

set(bin_target_file ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/cephadm)

if(NOT DEFINED CEPHADM_BUNDLED_DEPENDENCIES)
  set(CEPHADM_BUNDLED_DEPENDENCIES "pip")
endif()

add_custom_command(
  OUTPUT "${bin_target_file}"
  DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/cephadm.py
    ${CMAKE_CURRENT_SOURCE_DIR}/build.py
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${Python3_EXECUTABLE} build.py
    --set-version-var=CEPH_GIT_VER=${CEPH_GIT_VER}
    --set-version-var=CEPH_GIT_NICE_VER=${CEPH_GIT_NICE_VER}
    --set-version-var=CEPH_RELEASE=${CEPH_RELEASE}
    --set-version-var=CEPH_RELEASE_NAME=${CEPH_RELEASE_NAME}
    --set-version-var=CEPH_RELEASE_TYPE=${CEPH_RELEASE_TYPE}
    --bundled-dependencies=${CEPHADM_BUNDLED_DEPENDENCIES}
    ${bin_target_file}
)

add_custom_target(cephadm ALL
  DEPENDS "${bin_target_file}")

install(PROGRAMS
  ${bin_target_file}
  DESTINATION ${CMAKE_INSTALL_SBINDIR})


if(WITH_UCA)
  set(uca_target_file ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/uca)
  add_custom_command(
    OUTPUT "${uca_target_file}"
    DEPENDS
      ${CMAKE_CURRENT_SOURCE_DIR}/uca.py
      ${CMAKE_CURRENT_SOURCE_DIR}/build.py
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND ${Python3_EXECUTABLE} build.py
      --main-script=uca.py
      --set-version-var=CEPH_GIT_VER=${CEPH_GIT_VER}
      --set-version-var=CEPH_GIT_NICE_VER=${CEPH_GIT_NICE_VER}
      --set-version-var=CEPH_RELEASE=${CEPH_RELEASE}
      --set-version-var=CEPH_RELEASE_NAME=${CEPH_RELEASE_NAME}
      --set-version-var=CEPH_RELEASE_TYPE=${CEPH_RELEASE_TYPE}
      --bundled-dependencies=${CEPHADM_BUNDLED_DEPENDENCIES}
      ${uca_target_file}
  )
  add_custom_target(uca ALL
    DEPENDS "${uca_target_file}")
  # UCA is meant to be run by 'admins' but on their workstations as typical
  # users, not root. thus uca should be installed in bin (not sbin)
  install(PROGRAMS
    ${uca_target_file}
    DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()
