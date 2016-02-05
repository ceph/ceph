# Try to find pciaccess
#
# Once done, this will define
#
# PCIACCESS_FOUND
# PCIACCESS_INCLUDE_DIR
# PCIACCESS_LIBRARIES

find_path(PCIACCESS_INCLUDE_DIR pciaccess.h)
find_library(PCIACCESS_LIBRARIES pciaccess)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PCIACCESS DEFAULT_MSG PCIACCESS_LIBRARIES PCIACCESS_INCLUDE_DIR)

mark_as_advanced(PCIACCESS_INCLUDE_DIR PCIACCESS_LIBRARIES)
