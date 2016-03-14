# - Find OpenSSL Header and Libraries
#
# OPENSSL_PREFIX - where to find ssl.h and libraries
# OPENSSL_FOUND - True if found.

set(OPENSSL_LIB_DIR "${OPENSSL_PREFIX}/lib")

find_path(OPENSSL_INCLUDE_DIR ssl/ssl.h NO_DEFAULT_PATH PATHS
  /usr/include
  /opt/local/include
  /usr/local/include
  "${OPENSSL_PREFIX}/include"
  )

find_library(LIBSSL NAMES ssl)

if (OPENSSL_INCLUDE_DIR AND LIBSSL)
  set(OPENSSL_FOUND TRUE)
else (OPENSSL_INCLUDE_DIR AND LIBSSL)
  set(OPENSSL_FOUND FALSE)
endif (OPENSSL_INCLUDE_DIR AND LIBSSL)

if (OPENSSL_FOUND)
  message(STATUS "Found ldap: ${OPENSSL_INCLUDE_DIR}")
else (OPENSSL_FOUND)
  if (NOT OPENSSL_INCLUDE_DIR)
    message(FATAL_ERROR "Missing required ssl/ssl.h (openssl-devel)")
  else (NOT OPENSSL_INCLUDE_DIR)
    message (FATAL_ERROR "Missing required OpenSSL libraries")
  endif (NOT OPENSSL_INCLUDE_DIR)
endif (OPENSSL_FOUND)

set(OPENSSL_LIBS ${LIBSSL})

mark_as_advanced(
  OPENSSL_INCLUDE_DIR OPENSSL_LIB_DIR OPENSSL_LIBRARIES
)
