# - Find OpenLDAP C Libraries
#
# OPENLDAP_PREFIX - where to find ldap.h and libraries
# OPENLDAP_FOUND - True if found.

set(OPENLDAP_LIB_DIR "${OPENLDAP_PREFIX}/lib")

find_path(OPENLDAP_INCLUDE_DIR ldap.h NO_DEFAULT_PATH PATHS
  /usr/include
  /opt/local/include
  /usr/local/include
  "${OPENLDAP_PREFIX}/include"
  )

find_library(LIBLDAP NAMES ldap)
find_library(LIBLBER NAMES lber)

if (OPENLDAP_INCLUDE_DIR AND LIBLDAP AND LIBLBER)
  set(OPENLDAP_FOUND TRUE)
else (OPENLDAP_INCLUDE_DIR AND LIBLDAP AND LIBLBER)
  set(OPENLDAP_FOUND FALSE)
endif (OPENLDAP_INCLUDE_DIR AND LIBLDAP AND LIBLBER)

if (OPENLDAP_FOUND)
  message(STATUS "Found ldap: ${OPENLDAP_INCLUDE_DIR}")
else (OPENLDAP_FOUND)
  if (NOT OPENLDAP_INCLUDE_DIR)
    message(FATAL_ERROR "Missing required ldap.h (openldap-devel)")
  else (NOT OPENLDAP_INCLUDE_DIR)
    message (FATAL_ERROR "Missing required LDAP libraries (openldap)")
  endif (NOT OPENLDAP_INCLUDE_DIR)
endif (OPENLDAP_FOUND)

set(OPENLDAP_LIBS ${LIBLDAP} ${LIBLBER})

mark_as_advanced(
  OPENLDAP_INCLUDE_DIR OPENLDAP_LIB_DIR OPENLDAP_LIBRARIES
)
