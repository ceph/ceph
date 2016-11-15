# - Find OpenLDAP C Libraries
#
# OPENLDAP_FOUND - True if found.
# OPENLDAP_INCLUDE_DIR - Path to the openldap include directory
# OPENLDAP_LIBRARIES - Paths to the ldap and lber libraries

find_path(OPENLDAP_INCLUDE_DIR ldap.h PATHS
  /usr/include
  /opt/local/include
  /usr/local/include)

find_library(LDAP_LIBRARY ldap)
find_library(LBER_LIBRARY lber)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(OpenLdap DEFAULT_MSG
  OPENLDAP_INCLUDE_DIR LDAP_LIBRARY LBER_LIBRARY)

set(OPENLDAP_LIBRARIES ${LDAP_LIBRARY} ${LBER_LIBRARY})

mark_as_advanced(
  OPENLDAP_INCLUDE_DIR LDAP_LIBRARY LBER_LIBRARY)
