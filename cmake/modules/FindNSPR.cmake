# - Try to find NSPR
# Once done this will define
#
#  NSPR_FOUND - system has NSPR
#  NSPR_INCLUDE_DIRS - the NSPR include directory
#  NSPR_LIBRARIES - Link these to use NSPR
#  NSPR_DEFINITIONS - Compiler switches required for using NSPR
#
#  Copyright (c) 2010 Andreas Schneider <asn@redhat.com>
#
#  Redistribution and use is allowed according to the terms of the New
#  BSD license.
#  For details see the accompanying COPYING-CMAKE-SCRIPTS file.
#


if (NSPR_LIBRARIES AND NSPR_INCLUDE_DIRS)
  # in cache already
  set(NSPR_FOUND TRUE)
else (NSPR_LIBRARIES AND NSPR_INCLUDE_DIRS)
  find_package(PkgConfig)
  if (PKG_CONFIG_FOUND)
    pkg_check_modules(_NSPR nspr)
  endif (PKG_CONFIG_FOUND)

  find_path(NSPR_INCLUDE_DIR
    NAMES
      nspr.h
    PATHS
      ${_NSPR_INCLUDEDIR}
      /usr/include
      /usr/local/include
      /opt/local/include
      /sw/include
    PATH_SUFFIXES
      nspr4
      nspr
  )

  find_library(PLDS4_LIBRARY
    NAMES
      plds4
    PATHS
      ${_NSPR_LIBDIR}
      /usr/lib
      /usr/local/lib
      /opt/local/lib
      /sw/lib
  )

  find_library(PLC4_LIBRARY
    NAMES
      plc4
    PATHS
      ${_NSPR_LIBDIR}
      /usr/lib
      /usr/local/lib
      /opt/local/lib
      /sw/lib
  )

  find_library(NSPR4_LIBRARY
    NAMES
      nspr4
    PATHS
      ${_NSPR_LIBDIR}
      /usr/lib
      /usr/local/lib
      /opt/local/lib
      /sw/lib
  )

  set(NSPR_INCLUDE_DIRS
    ${NSPR_INCLUDE_DIR}
  )

  if (PLDS4_LIBRARY)
    set(NSPR_LIBRARIES
        ${NSPR_LIBRARIES}
        ${PLDS4_LIBRARY}
    )
  endif (PLDS4_LIBRARY)

  if (PLC4_LIBRARY)
    set(NSPR_LIBRARIES
        ${NSPR_LIBRARIES}
        ${PLC4_LIBRARY}
    )
  endif (PLC4_LIBRARY)

  if (NSPR4_LIBRARY)
    set(NSPR_LIBRARIES
        ${NSPR_LIBRARIES}
        ${NSPR4_LIBRARY}
    )
  endif (NSPR4_LIBRARY)

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(NSPR DEFAULT_MSG NSPR_LIBRARIES NSPR_INCLUDE_DIRS)

  # show the NSPR_INCLUDE_DIRS and NSPR_LIBRARIES variables only in the advanced view
  mark_as_advanced(NSPR_INCLUDE_DIRS NSPR_LIBRARIES)

endif (NSPR_LIBRARIES AND NSPR_INCLUDE_DIRS)
