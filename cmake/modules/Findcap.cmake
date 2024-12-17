# Try to find libcap
#
find_package(PkgConfig QUIET REQUIRED)

pkg_check_modules(PC_cap QUIET cap)

find_library(cap_LIBRARY
  NAMES cap
  HINTS
    ${PC_cap_LIBDIR}
    ${PC_cap_LIBRARY_DIRS})

find_path(cap_INCLUDE_DIR
  NAMES sys/capability.h
  HINTS
    ${PC_cap_INCLUDEDIR}
    ${PC_cap_INCLUDE_DIRS})

mark_as_advanced(
  cap_LIBRARY
  cap_INCLUDE_DIR)

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (cap
  REQUIRED_VARS
    cap_LIBRARY
    cap_INCLUDE_DIR)

if(cap_FOUND AND NOT TARGET cap::cap)
  add_library(cap::cap UNKNOWN IMPORTED)
  set_target_properties(cap::cap
    PROPERTIES
      IMPORTED_LOCATION ${cap_LIBRARY}
      INTERFACE_INCLUDE_DIRECTORIES ${cap_INCLUDE_DIR})
endif()
