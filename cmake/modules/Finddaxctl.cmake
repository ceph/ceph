# - Find libdaxctl
# Find the daxctl libraries and includes
#
# daxctl_INCLUDE_DIR - where to find libdaxctl.h etc.
# daxctl_LIBRARIES - List of libraries when using daxctl.
# daxctl_FOUND - True if daxctl found.

find_path(daxctl_INCLUDE_DIR daxctl/libdaxctl.h)

if(daxctl_INCLUDE_DIR AND EXISTS "${daxctl_INCLUDE_DIR}/libdaxctl.h")
  foreach(ver "MAJOR" "MINOR" "RELEASE")
    file(STRINGS "${daxctl_INCLUDE_DIR}/libdaxctl.h" daxctl_VER_${ver}_LINE
      REGEX "^#define[ \t]+daxctl_VERSION_${ver}[ \t]+[0-9]+[ \t]+.*$")
    string(REGEX REPLACE "^#define[ \t]+daxctl_VERSION_${ver}[ \t]+([0-9]+)[ \t]+.*$"
      "\\1" daxctl_VERSION_${ver} "${daxctl_VER_${ver}_LINE}")
    unset(${daxctl_VER_${ver}_LINE})
  endforeach()
  set(daxctl_VERSION_STRING
    "${daxctl_VERSION_MAJOR}.${daxctl_VERSION_MINOR}.${daxctl_VERSION_RELEASE}")
endif()

find_library(daxctl_LIBRARY daxctl)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(daxctl
  REQUIRED_VARS daxctl_LIBRARY daxctl_INCLUDE_DIR
  VERSION_VAR daxctl_VERSION_STRING)

mark_as_advanced(daxctl_INCLUDE_DIR daxctl_LIBRARY)

if(daxctl_FOUND)
  set(daxctl_INCLUDE_DIRS ${daxctl_INCLUDE_DIR})
  set(daxctl_LIBRARIES ${daxctl_LIBRARY})
  if(NOT (TARGET daxctl::daxctl))
    add_library(daxctl::daxctl UNKNOWN IMPORTED)
    set_target_properties(daxctl::daxctl PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${daxctl_INCLUDE_DIRS}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${daxctl_LIBRARIES}"
      VERSION "${daxctl_VERSION_STRING}")
  endif()
endif()
