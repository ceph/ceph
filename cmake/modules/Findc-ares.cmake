find_package(PkgConfig QUIET)

pkg_search_module(PC_cares
  libcares)

find_path(c-ares_INCLUDE_DIR
  NAMES ares_dns.h
  PATHS ${PC_cares_INCLUDE_DIRS})

find_library(c-ares_LIBRARY
  NAMES cares
  PATHS ${PC_cares_LIBRARY_DIRS})

set(c-ares_VERSION ${PC_cares_VERSION})

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(c-ares
  REQUIRED_VARS
    c-ares_INCLUDE_DIR
    c-ares_LIBRARY
  VERSION_VAR c-ares_VERSION)

if(c-ares_FOUND)
  if(NOT TARGET c-ares::cares)
    add_library(c-ares::cares UNKNOWN IMPORTED GLOBAL)
    set_target_properties(c-ares::cares PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${c-ares_INCLUDE_DIR}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${c-ares_LIBRARY}")
  endif()

  # to be compatible with old Seastar
  add_library(c-ares::c-ares ALIAS c-ares::cares)

  if(NOT TARGET c-ares::c-ares)
    add_library(c-ares::c-ares ALIAS c-ares::cares)
  endif()
endif()
