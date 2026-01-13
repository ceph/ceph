# Add CPM packages in a consistent, Ceph-friendly way:

set(CEPH_ORIG_CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH}")
include("${CMAKE_CURRENT_LIST_DIR}/CPM.cmake")
set(CMAKE_MODULE_PATH "${CEPH_ORIG_CMAKE_MODULE_PATH}")

# Two settings govern CPM's behavior with respect to local packages:
# CPM_USE_LOCAL_PACKAGES and CPM_LOCAL_PACKAGES_ONLY. The first prefers local
# packages; the second requires them, e.g. for an offline-only build.
#
# This macro saves and restores both settings. If CPM_LOCAL_PACKAGES_ONLY is
# already set, it is honored.

macro(add_cpm PACKAGE_NAME USE_LOCAL_FLAG)
  set(_ORIG_CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH}")
  set(_ORIG_CPM_USE_LOCAL_PACKAGES "${CPM_USE_LOCAL_PACKAGES}")
  set(_ORIG_CPM_LOCAL_PACKAGES_ONLY "${CPM_LOCAL_PACKAGES_ONLY}")

  if(${USE_LOCAL_FLAG})
    set(CPM_USE_LOCAL_PACKAGES ON)
  endif()

  CPMAddPackage(${ARGN})

  set(CPM_USE_LOCAL_PACKAGES "${_ORIG_CPM_USE_LOCAL_PACKAGES}")
  set(CPM_LOCAL_PACKAGES_ONLY "${_ORIG_CPM_LOCAL_PACKAGES_ONLY}")
  set(CMAKE_MODULE_PATH "${_ORIG_CMAKE_MODULE_PATH}")

  message(STATUS "-- Enabled ${PACKAGE_NAME} support.")
endmacro()
