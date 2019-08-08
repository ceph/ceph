#.rst:
# FindYAMLCPP
# ------------
#
# This module finds the `yaml cpp`__ library.
#
# Imported target
# ^^^^^^^^^^^^^^^
#
# This module defines the following :prop_tgt:`IMPORTED` target:
#
# ``Yaml``
#   The yaml library, if found
#
# Result variables
# ^^^^^^^^^^^^^^^^
#
# This module sets the following
#
# ``YAML_FOUND``
#   ``TRUE`` if system has yamlcpp
# ``YAML_INCLUDE_DIRS``
#   The YAML include directories
# ``YAML_LIBRARIES``
#   The libraries needed to use YAML
# ``YAML_VERSION_STRING``
#   The YAML version

#=============================================================================
# Copyright 2018 Mania Abdi.
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

find_path(YAMLCPP_INCLUDE_DIRS NAMES  yaml-cpp/yaml.h)
find_library(YAMLCPP_LIBRARIES NAMES yaml-cpp)

if(YAMLCPP_INCLUDE_DIRS AND YAMLCPP_LIBRARIES)
  # find tracef() and tracelog() support
  set(YAMLCPP_HAS_TRACEF 0)
  set(YAMLCPP_HAS_TRACELOG 0)

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
    set(YAMLCPP_VERSION_STRING
        "0.0.0")
#    unset(lttngust_version_major_string)
#    unset(lttngust_version_minor_string)
#    unset(lttngust_version_patch_string)
#    unset(lttngust_v_major)
#    unset(lttngust_v_minor)
#    unset(lttngust_v_patch)
#  endif()
#  unset(lttngust_version_file)

  if(NOT TARGET YAMLCPP)
    add_library(YAMLCPP UNKNOWN IMPORTED)
    set_target_properties(YAMLCPP PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${YAMLCPP_INCLUDE_DIRS}"
      INTERFACE_LINK_LIBRARIES ${CMAKE_DL_LIBS}
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${YAMLCPP_LIBRARIES}")
  endif()

  # add libdl to required libraries
  set(YAMLCPP_LIBRARIES ${YAMLCPP_LIBRARIES} ${CMAKE_DL_LIBS})
endif()

# handle the QUIETLY and REQUIRED arguments and set YAMLCPP_FOUND to
# TRUE if all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(YAMLCPP FOUND_VAR YAMLCPP_FOUND
                                  REQUIRED_VARS YAMLCPP_LIBRARIES
                                                YAMLCPP_INCLUDE_DIRS
                                  VERSION_VAR YAMLCPP_VERSION_STRING)
mark_as_advanced(YAMLCPP_LIBRARIES YAMLCPP_INCLUDE_DIRS)

