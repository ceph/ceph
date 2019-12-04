#.rst:
# FindOpenTracing
# ------------
#
# This module finds the `OpenTracing` library.
#
# Imported target
# ^^^^^^^^^^^^^^^
#
# This module defines the following :prop_tgt:`IMPORTED` target:
#
# ``OpenTracing``
#   The Opentracing library, if found
#
# Result variables
# ^^^^^^^^^^^^^^^^
#
# This module sets the following
#
# ``OpenTracing_FOUND``
#   ``TRUE`` if system has OpenTracing
# ``OpenTracing_INCLUDE_DIRS``
#   The OpenTracing include directories
# ``OpenTracing_LIBRARIES``
#   The libraries needed to use OpenTracing
# ``OpenTracing_VERSION_STRING``
#   The OpenTracing version
# ``OpenTracing_HAS_TRACEF``
#   ``TRUE`` if the ``tracef()`` API is available in the system's LTTng-UST
# ``OpenTracing_HAS_TRACELOG``
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

find_path(OpenTracing_INCLUDE_DIRS NAMES  opentracing/tracer.h)
find_library(OpenTracing_LIBRARIES NAMES opentracing)

if(OpenTracing_INCLUDE_DIRS AND OpenTracing_LIBRARIES)
  # find tracef() and tracelog() support
  set(OpenTracing_HAS_TRACEF 0)
  set(OpenTracing_HAS_TRACELOG 0)

  # will need specifically 1.5.x for successful working with Jaeger
  set(OpenTracing_VERSION_STRING "1.5.x")

  if(NOT TARGET OpenTracing)
    add_library(OpenTracing UNKNOWN IMPORTED)
    set_target_properties(OpenTracing PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${OpenTracing_INCLUDE_DIRS}"
      INTERFACE_LINK_LIBRARIES ${CMAKE_DL_LIBS}
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${OpenTracing_LIBRARIES}")
  endif()

  # add libdl to required libraries
  set(OpenTracing_LIBRARIES ${OpenTracing_LIBRARIES} ${CMAKE_DL_LIBS})
endif()

# handle the QUIETLY and REQUIRED arguments and set LTTNGUST_FOUND to
# TRUE if all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(OpenTracing FOUND_VAR OpenTracing_FOUND
                                  REQUIRED_VARS OpenTracing_LIBRARIES
                                                OpenTracing_INCLUDE_DIRS
                                  VERSION_VAR OpenTracing_VERSION_STRING)
mark_as_advanced(OpenTracing_LIBRARIES OpenTracing_INCLUDE_DIRS)
