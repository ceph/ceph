#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Copyright (C) 2018 Scylladb, Ltd.
#

find_package (PkgConfig)

pkg_search_module (PC_yaml-cpp
  REQUIRED
  QUIET
  yaml-cpp)

find_path (yaml-cpp_INCLUDE_DIR
  NAMES yaml-cpp/yaml.h
  PATHS ${PC_yaml-cpp_INCLUDE_DIRS})

find_library (yaml-cpp_LIBRARY
  NAMES yaml-cpp
  PATHS ${PC_yaml-cpp_LIBRARY_DIRS})

set (yaml-cpp_VERSION ${PC_yaml-cpp_VERSION})

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args (yaml-cpp
  FOUND_VAR yaml-cpp_FOUND
  REQUIRED_VARS
    yaml-cpp_INCLUDE_DIR
    yaml-cpp_LIBRARY
  VERSION_VAR yaml-cpp_VERSION)

if (yaml-cpp_FOUND)
  set (yaml-cpp_INCLUDE_DIRS ${yaml-cpp_INCLUDE_DIR})
endif ()

if (yaml-cpp_FOUND AND NOT (TARGET yaml-cpp::yaml-cpp))
  add_library (yaml-cpp::yaml-cpp UNKNOWN IMPORTED)

  set_target_properties (yaml-cpp::yaml-cpp
    PROPERTIES
      IMPORTED_LOCATION ${yaml-cpp_LIBRARY}
      INTERFACE_COMPILE_OPTIONS "${PC_yaml-cpp_CFLAGS_OTHER}"
      INTERFACE_INCLUDE_DIRECTORIES ${yaml-cpp_INCLUDE_DIR})
endif ()

mark_as_advanced (
  yaml-cpp_INCLUDE_DIR
  yaml-cpp_LIBRARY)

