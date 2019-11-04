# - Find libnl-genl3
# Find the genl library and includes
#
# GENL_INCLUDE_DIR - where to find netlink.h, etc.
# GENL_LIBRARIES - List of libraries when using genl.
# GENL_FOUND - True if genl found.

find_path(GENL_INCLUDE_DIR NAMES netlink/netlink.h PATH_SUFFIXES libnl3)

find_library(LIBNL_LIB nl-3)
find_library(LIBNL_GENL_LIB nl-genl-3)
set(GENL_LIBRARIES
  ${LIBNL_LIB}
  ${LIBNL_GENL_LIB}
  )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(nl-genl-3
  DEFAULT_MSG GENL_LIBRARIES GENL_INCLUDE_DIR)

mark_as_advanced(
  GENL_LIBRARIES
  GENL_INCLUDE_DIR)
