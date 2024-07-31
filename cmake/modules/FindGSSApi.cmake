# - Find KRB5/GSSAPI C Libraries
#
# GSSAPI_FOUND - True if found.
# GSSAPI_INCLUDE_DIR - Path to the KRB5/gssapi include directory
# GSSAPI_LIBRARIES - Paths to the KRB5/gssapi libraries

find_path(GSSAPI_INCLUDE_DIR gssapi.h PATHS
  /usr/include
  /opt/local/include
  /usr/local/include)

find_library(GSSAPI_KRB5_LIBRARY gssapi_krb5)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GSSApi DEFAULT_MSG
	GSSAPI_INCLUDE_DIR GSSAPI_KRB5_LIBRARY)

set(GSSAPI_LIBRARIES ${GSSAPI_KRB5_LIBRARY})

mark_as_advanced(
	GSSAPI_INCLUDE_DIR GSSAPI_KRB5_LIBRARY)

