# - Try to find NSS
# Once done this will define
#
#  NSS_FOUND - system has NSS
#  NSS_INCLUDE_DIRS - the NSS include directory
#  NSS_LIBRARIES - Link these to use NSS
#  NSS_DEFINITIONS - Compiler switches required for using NSS
#
#  Copyright (c) 2010 Andreas Schneider <asn@redhat.com>
#
#  Redistribution and use is allowed according to the terms of the New
#  BSD license.
#  For details see the accompanying COPYING-CMAKE-SCRIPTS file.
#


if (NSS_LIBRARIES AND NSS_INCLUDE_DIRS)
  # in cache already
  set(NSS_FOUND TRUE)
else (NSS_LIBRARIES AND NSS_INCLUDE_DIRS)
  find_package(PkgConfig)
  if (PKG_CONFIG_FOUND)
    pkg_check_modules(_NSS nss)
  endif (PKG_CONFIG_FOUND)

  find_path(NSS_INCLUDE_DIR
    NAMES
      pk11pub.h
    PATHS
      ${_NSS_INCLUDEDIR}
      /usr/include
      /usr/local/include
      /opt/local/include
      /sw/include
    PATH_SUFFIXES
      nss3
      nss
  )

  find_library(SSL3_LIBRARY
    NAMES
      ssl3
    PATHS
      ${_NSS_LIBDIR}
      /usr/lib
      /usr/local/lib
      /opt/local/lib
      /sw/lib
  )

  find_library(SMIME3_LIBRARY
    NAMES
      smime3
    PATHS
      ${_NSS_LIBDIR}
      /usr/lib
      /usr/local/lib
      /opt/local/lib
      /sw/lib
  )

  find_library(NSS3_LIBRARY
    NAMES
      nss3
    PATHS
      ${_NSS_LIBDIR}
      /usr/lib
      /usr/local/lib
      /opt/local/lib
      /sw/lib
      /usr/lib/x86_64-linux-gnu
  )

  find_library(NSSUTIL3_LIBRARY
    NAMES
      nssutil3
    PATHS
      ${_NSS_LIBDIR}
      /usr/lib
      /usr/local/lib
      /opt/local/lib
      /sw/lib
  )

  set(NSS_INCLUDE_DIRS
    ${NSS_INCLUDE_DIR}
  )

  if (SSL3_LIBRARY)
    set(NSS_LIBRARIES
        ${NSS_LIBRARIES}
        ${SSL3_LIBRARY}
    )
  endif (SSL3_LIBRARY)

  if (SMIME3_LIBRARY)
    set(NSS_LIBRARIES
        ${NSS_LIBRARIES}
        ${SMIME3_LIBRARY}
    )
  endif (SMIME3_LIBRARY)

  if (NSS3_LIBRARY)
    set(NSS_LIBRARIES
        ${NSS_LIBRARIES}
        ${NSS3_LIBRARY}
    )
  endif (NSS3_LIBRARY)

  if (NSSUTIL3_LIBRARY)
    set(NSS_LIBRARIES
        ${NSS_LIBRARIES}
        ${NSSUTIL3_LIBRARY}
    )
  endif (NSSUTIL3_LIBRARY)

  include(FindPackageHandleStandardArgs)
  message(STATUS "NSS_LIBRARIES: ${NSS_LIBRARIES}")
  message(STATUS "NSS_INCLUDE_DIRS: ${NSS_INCLUDE_DIRS}")
  find_package_handle_standard_args(NSS DEFAULT_MSG NSS_LIBRARIES NSS_INCLUDE_DIRS)

  # show the NSS_INCLUDE_DIRS and NSS_LIBRARIES variables only in the advanced view
  mark_as_advanced(NSS_INCLUDE_DIRS NSS_LIBRARIES)

endif (NSS_LIBRARIES AND NSS_INCLUDE_DIRS)
