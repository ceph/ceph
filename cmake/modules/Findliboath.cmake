# CMake module to search for liboath headers
#
# If it's found it sets LIBOATH_FOUND to TRUE
# and following variables are set:
# LIBOATH_INCLUDE_DIR
# LIBOATH_LIBRARY
find_path(LIBOATH_INCLUDE_DIR
  oath.h
  PATHS
  /usr/include
  /usr/local/include
  /usr/include/liboath)
find_library(LIBOATH_LIBRARY NAMES oath liboath PATHS
  /usr/local/lib
  /usr/lib)

# handle the QUIETLY and REQUIRED arguments and set UUID_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(oath DEFAULT_MSG LIBOATH_LIBRARY LIBOATH_INCLUDE_DIR)

mark_as_advanced(LIBOATH_LIBRARY LIBOATH_INCLUDE_DIR)
