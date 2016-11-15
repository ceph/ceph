# CMake module to search for FastCGI headers
#
# If it's found it sets FCGI_FOUND to TRUE
# and following variables are set:
# FCGI_INCLUDE_DIR
# FCGI_LIBRARY
find_path(FCGI_INCLUDE_DIR
  fcgio.h
  PATHS
  /usr/include
  /usr/local/include
  /usr/include/fastcgi)
find_library(FCGI_LIBRARY NAMES fcgi libfcgi PATHS
  /usr/local/lib
  /usr/lib)

# handle the QUIETLY and REQUIRED arguments and set UUID_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(fcgi DEFAULT_MSG FCGI_LIBRARY FCGI_INCLUDE_DIR)

mark_as_advanced(FCGI_LIBRARY FCGI_INCLUDE_DIR)
