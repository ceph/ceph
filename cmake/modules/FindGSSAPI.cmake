# - Find Gssapi C Libraries
#
# GSSAPI_FOUND - True if found.
# GSSAPI_INCLUDE_DIR - Path to the gssapi include directory
# GSSAPI_LIBRARIES - Paths to the gssapi libraries

find_path(GSSAPI_INCLUDE_DIR gssapi.h PATHS
  /usr/include)

find_library(GSSAPI_LIBRARY gssapi_krb5)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GSSAPI DEFAULT_MSG
  GSSAPI_INCLUDE_DIR GSSAPI_LIBRARY)

set(GSSAPI_LIBRARIES ${GSSAPI_LIBRARY})

mark_as_advanced(
  GSSAPI_INCLUDE_DIR GSSAPI_LIBRARY)
