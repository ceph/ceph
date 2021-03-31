# Try to find liblz4
#
# Once done, this will define
#
# Zstd_FOUND
# Zstd_INCLUDE_DIRS
# Zstd_LIBRARIES
# Zstd_VERSION_STRING
# Zstd_VERSION_MAJOR
# Zstd_VERSION_MINOR
# Zstd_VERSION_RELEASE

find_path(Zstd_INCLUDE_DIR
  NAMES zstd.h
  HINTS ${Zstd_ROOT_DIR}/include)

if(Zstd_INCLUDE_DIR AND EXISTS "${Zstd_INCLUDE_DIR}/zstd.h")
  foreach(ver "MAJOR" "MINOR" "RELEASE")
    file(STRINGS "${Zstd_INCLUDE_DIR}/zstd.h" Zstd_VER_${ver}_LINE
      REGEX "^#define[ \t]+ZSTD_VERSION_${ver}[ \t]+[0-9]+$")
    string(REGEX REPLACE "^#define[ \t]+ZSTD_VERSION_${ver}[ \t]+([0-9]+)$"
      "\\1" Zstd_VERSION_${ver} "${Zstd_VER_${ver}_LINE}")
    unset(${Zstd_VER_${ver}_LINE})
  endforeach()
  set(Zstd_VERSION_STRING
    "${Zstd_VERSION_MAJOR}.${Zstd_VERSION_MINOR}.${Zstd_VERSION_RELEASE}")
endif()

find_library(Zstd_LIBRARY
  NAMES "${CMAKE_STATIC_LIBRARY_PREFIX}zstd.${CMAKE_STATIC_LIBRARY_SUFFIX}" zstd
  HINTS ${Zstd_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Zstd
  REQUIRED_VARS Zstd_LIBRARY Zstd_INCLUDE_DIR
  VERSION_VAR Zstd_VERSION_STRING)

mark_as_advanced(
  Zstd_LIBRARY
  Zstd_INCLUDE_DIR)

if(Zstd_FOUND AND NOT (TARGET Zstd::Zstd))
  set(Zstd_INCLUDE_DIRS ${Zstd_INCLUDE_DIR})
  set(Zstd_LIBRARIES ${Zstd_LIBRARY})
  add_library (Zstd::Zstd UNKNOWN IMPORTED)
  set_target_properties(Zstd::Zstd PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${Zstd_INCLUDE_DIR}
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION ${Zstd_LIBRARY}
    VERSION "${Zstd_VERSION_STRING}")
endif()
