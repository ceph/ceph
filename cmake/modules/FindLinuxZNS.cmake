# Try to find linux/blkzoned.h

find_path(LinuxZNS_INCLUDE_DIR NAMES
  "linux/blkzoned.h")

find_package_handle_standard_args(LinuxZNS
  REQUIRED_VARS
  LinuxZNS_INCLUDE_DIR)

mark_as_advanced(
  LinuxZNS_INCLUDE_DIR)

if(LinuxZNS_FOUND AND NOT (TARGET Linux::ZNS))
  add_library(Linux::ZNS INTERFACE IMPORTED)
  set_target_properties(Linux::ZNS PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${LinuxZNS_INCLUDE_DIR}"
    INTERFACE_COMPILE_DEFINITIONS HAVE_ZNS=1)
endif()
