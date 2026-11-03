# The buffer lesson uses the common vocabulary header and Ceph's existing library.
set(CEPH_SOURCE_DIR "" CACHE PATH "Ceph source tree with initialized fmt headers")
set(CEPH_BUILD_DIR "" CACHE PATH "Compatible Ceph build with generated headers and libraries")
if(NOT EXISTS "${CEPH_SOURCE_DIR}/src/include/buffer.h")
  message(FATAL_ERROR "Ceph vocabulary examples require CEPH_SOURCE_DIR")
endif()
find_path(DATUM_TUTORIAL_CONFIG_INCLUDE_DIR acconfig.h
  HINTS "${CEPH_BUILD_DIR}/src/include" "${CEPH_BUILD_DIR}/include" NO_DEFAULT_PATH REQUIRED)
find_library(DATUM_TUTORIAL_CEPH_COMMON NAMES ceph-common libceph-common.so.2
  HINTS "${CEPH_BUILD_DIR}/lib" NO_DEFAULT_PATH REQUIRED)
find_path(DATUM_TUTORIAL_FMT_INCLUDE_DIR fmt/format.h
  HINTS "${CEPH_SOURCE_DIR}/src/fmt/include" NO_DEFAULT_PATH REQUIRED)

add_library(datum_tutorial_ceph INTERFACE)
target_link_libraries(datum_tutorial_ceph INTERFACE datum::core "${DATUM_TUTORIAL_CEPH_COMMON}")
target_include_directories(datum_tutorial_ceph INTERFACE "${DATUM_SOURCE_DIR}/adapters")
target_include_directories(datum_tutorial_ceph SYSTEM INTERFACE
  "${CEPH_SOURCE_DIR}/src"
  "${DATUM_TUTORIAL_CONFIG_INCLUDE_DIR}"
  "${DATUM_TUTORIAL_FMT_INCLUDE_DIR}")
