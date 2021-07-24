# - Find OpenLDAP C Libraries
#
# OpenLDAP_FOUND - True if found.
# OpenLDAP_INCLUDE_DIR - Path to the openldap include directory
# OpenLDAP_LIBRARIES - Paths to the ldap and lber libraries

find_path(OpenLDAP_INCLUDE_DIR ldap.h PATHS
  /usr/include
  /opt/local/include
  /usr/local/include)

find_library(LDAP_LIBRARY ldap)
find_library(LBER_LIBRARY lber)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(OpenLDAP DEFAULT_MSG
  OpenLDAP_INCLUDE_DIR LDAP_LIBRARY LBER_LIBRARY)

set(OpenLDAP_LIBRARIES ${LDAP_LIBRARY} ${LBER_LIBRARY})

mark_as_advanced(
  OpenLDAP_INCLUDE_DIR LDAP_LIBRARY LBER_LIBRARY)
