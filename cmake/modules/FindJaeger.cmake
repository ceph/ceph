#.rst:
# FindJaeger
# ------------
#
# This module finds the `Jaeger` library.
#
# Imported target
# ^^^^^^^^^^^^^^^
#
# This module defines the following :prop_tgt:`IMPORTED` target:
#
# ``Jaeger``
#   The Jaeger library, if found
#
# Result variables
# ^^^^^^^^^^^^^^^^
#
# This module sets the following
#
# ``Jaeger_FOUND``
#   ``TRUE`` if system has Jaeger
# ``Jaeger_INCLUDE_DIRS``
#   The Jaeger include directories
# ``Jaeger_LIBRARIES``
#   The libraries needed to use Jaeger
# ``Jaeger_VERSION_STRING``
#   The Jaeger version
# ``Jaeger_HAS_TRACEF``
#   ``TRUE`` if the ``tracef()`` API is available in the system's LTTng-UST
# ``Jaeger_HAS_TRACELOG``
#   ``TRUE`` if the ``tracelog()`` API is available in the system's LTTng-UST

#=============================================================================
# Copyright 2018 Mania Abdi, Inc.
# Copyright 2018 Mania Abdi
#
# Distributed under the OSI-approved BSD License (the "License");
# see accompanying file Copyright.txt for details.
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the License for more information.
#=============================================================================
# (To distribute this file outside of CMake, substitute the full
#  License text for the above reference.)

find_path(Jaeger_INCLUDE_DIRS NAMES  jaegertracing/Tracer.h)
find_library(Jaeger_LIBRARIES NAMES jaegertracing)

if(Jaeger_INCLUDE_DIRS AND Jaeger_LIBRARIES)
  # find tracef() and tracelog() support
  set(Jaeger_HAS_TRACEF 0)
  set(Jaeger_HAS_TRACELOG 0)

  set(Jaeger_VERSION_STRING "0.5.0")

  if(NOT TARGET Jaeger)
    add_library(Jaeger UNKNOWN IMPORTED)
    set_target_properties(Jaeger PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${Jaeger_INCLUDE_DIRS}"
      INTERFACE_LINK_LIBRARIES ${CMAKE_DL_LIBS}
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${Jaeger_LIBRARIES}")
  endif()

  # add libdl to required libraries
  set(Jaeger_LIBRARIES ${Jaeger_LIBRARIES} ${CMAKE_DL_LIBS})
endif()

# handle the QUIETLY and REQUIRED arguments and set LTTNGUST_FOUND to
# TRUE if all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Jaeger FOUND_VAR Jaeger_FOUND
                                  REQUIRED_VARS Jaeger_LIBRARIES
                                                Jaeger_INCLUDE_DIRS
                                  VERSION_VAR Jaeger_VERSION_STRING)
mark_as_advanced(Jaeger_LIBRARIES Jaeger_INCLUDE_DIRS)

