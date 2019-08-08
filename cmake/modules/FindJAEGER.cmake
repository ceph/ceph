#.rst:
# FindJaeger
# ------------
#
# This module finds the `Jaeger`__ library.
#
# Imported target
# ^^^^^^^^^^^^^^^
#
# This module defines the following :prop_tgt:`IMPORTED` target:
#
# ``Jaeger``
#   The jaeger library, if found
#
# Result variables
# ^^^^^^^^^^^^^^^^
#
# This module sets the following
#
# ``JAEGER_FOUND``
#   ``TRUE`` if system has Jaeger
# ``JAEGER_INCLUDE_DIRS``
#   The JAEGER include directories
# ``JAEGER_LIBRARIES``
#   The libraries needed to use JAEGER
# ``JAEGER_VERSION_STRING``
#   The JAEGER version
# ``JAEGER_HAS_TRACEF``
#   ``TRUE`` if the ``tracef()`` API is available in the system's LTTng-UST
# ``JAEGER_HAS_TRACELOG``
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

find_path(JAEGER_INCLUDE_DIRS NAMES  jaegertracing/Tracer.h)
find_library(JAEGER_LIBRARIES NAMES jaegertracing)

if(JAEGER_INCLUDE_DIRS AND JAEGER_LIBRARIES)
  # find tracef() and tracelog() support
  set(JAEGER_HAS_TRACEF 0)
  set(JAEGER_HAS_TRACELOG 0)

#  mania: I think I don't need this
#  if(EXISTS "${JAEGER_INCLUDE_DIRS}/lttng/tracef.h")
#    set(JAEGER_HAS_TRACEF TRUE)
#  endif()

#  mania: I think I don't need this
#  if(EXISTS "${LTTNGUST_INCLUDE_DIRS}/lttng/tracelog.h")
#    set(LTTNGUST_HAS_TRACELOG TRUE)
#  endif()

  # get version mania: we may need the version control later but for now we are OK.
#  set(lttngust_version_file "${LTTNGUST_INCLUDE_DIRS}/lttng/ust-version.h")
#  if(EXISTS "${lttngust_version_file}")
#    file(STRINGS "${lttngust_version_file}" lttngust_version_major_string
#         REGEX "^[\t ]*#define[\t ]+LTTNG_UST_MAJOR_VERSION[\t ]+[0-9]+[\t ]*$")
#    file(STRINGS "${lttngust_version_file}" lttngust_version_minor_string
#         REGEX "^[\t ]*#define[\t ]+LTTNG_UST_MINOR_VERSION[\t ]+[0-9]+[\t ]*$")
#    file(STRINGS "${lttngust_version_file}" lttngust_version_patch_string
#         REGEX "^[\t ]*#define[\t ]+LTTNG_UST_PATCHLEVEL_VERSION[\t ]+[0-9]+[\t ]*$")
#    string(REGEX REPLACE ".*([0-9]+).*" "\\1"
#           lttngust_v_major "${lttngust_version_major_string}")
#    string(REGEX REPLACE ".*([0-9]+).*" "\\1"
#           lttngust_v_minor "${lttngust_version_minor_string}")
#    string(REGEX REPLACE ".*([0-9]+).*" "\\1"
#           lttngust_v_patch "${lttngust_version_patch_string}")
    set(JAEGER_VERSION_STRING
        "0.0.0")
#    unset(lttngust_version_major_string)
#    unset(lttngust_version_minor_string)
#    unset(lttngust_version_patch_string)
#    unset(lttngust_v_major)
#    unset(lttngust_v_minor)
#    unset(lttngust_v_patch)
#  endif()
#  unset(lttngust_version_file)

  if(NOT TARGET JAEGER)
    add_library(JAEGER UNKNOWN IMPORTED)
    set_target_properties(JAEGER PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${JAEGER_INCLUDE_DIRS}"
      INTERFACE_LINK_LIBRARIES ${CMAKE_DL_LIBS}
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${JAEGER_LIBRARIES}")
  endif()

  # add libdl to required libraries
  set(JAEGER_LIBRARIES ${JAEGER_LIBRARIES} ${CMAKE_DL_LIBS})
endif()

# handle the QUIETLY and REQUIRED arguments and set LTTNGUST_FOUND to
# TRUE if all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(JAEGER FOUND_VAR JAEGER_FOUND
                                  REQUIRED_VARS JAEGER_LIBRARIES
                                                JAEGER_INCLUDE_DIRS
                                  VERSION_VAR JAEGER_VERSION_STRING)
mark_as_advanced(JAEGER_LIBRARIES JAEGER_INCLUDE_DIRS)

