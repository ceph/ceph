# - Find libndctl
# Find the ndctl libraries and includes
#
# ndctl_INCLUDE_DIR - where to find libndctl.h etc.
# ndctl_LIBRARIES - List of libraries when using ndctl.
# ndctl_FOUND - True if ndctl found.

find_path(ndctl_INCLUDE_DIR ndctl/libndctl.h)

if(ndctl_INCLUDE_DIR AND EXISTS "${ndctl_INCLUDE_DIR}/libndctl.h")
  foreach(ver "MAJOR" "MINOR" "RELEASE")
    file(STRINGS "${ndctl_INCLUDE_DIR}/libndctl.h" ndctl_VER_${ver}_LINE
      REGEX "^#define[ \t]+ndctl_VERSION_${ver}[ \t]+[0-9]+[ \t]+.*$")
    string(REGEX REPLACE "^#define[ \t]+ndctl_VERSION_${ver}[ \t]+([0-9]+)[ \t]+.*$"
      "\\1" ndctl_VERSION_${ver} "${ndctl_VER_${ver}_LINE}")
    unset(${ndctl_VER_${ver}_LINE})
  endforeach()
  set(ndctl_VERSION_STRING
    "${ndctl_VERSION_MAJOR}.${ndctl_VERSION_MINOR}.${ndctl_VERSION_RELEASE}")
endif()

find_library(ndctl_LIBRARY ndctl)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ndctl
  REQUIRED_VARS ndctl_LIBRARY ndctl_INCLUDE_DIR
  VERSION_VAR ndctl_VERSION_STRING)

mark_as_advanced(ndctl_INCLUDE_DIR ndctl_LIBRARY)

if(ndctl_FOUND)
  set(ndctl_INCLUDE_DIRS ${ndctl_INCLUDE_DIR})
  set(ndctl_LIBRARIES ${ndctl_LIBRARY})
  if(NOT (TARGET ndctl::ndctl))
    add_library(ndctl::ndctl UNKNOWN IMPORTED)
    set_target_properties(ndctl::ndctl PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${ndctl_INCLUDE_DIRS}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${ndctl_LIBRARIES}"
      VERSION "${ndctl_VERSION_STRING}")
  endif()
endif()
