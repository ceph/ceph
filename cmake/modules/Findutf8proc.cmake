# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# note: cbodley copied this from the Arrow repo and removed ARROW variables

function(extract_utf8proc_version)
  if(utf8proc_INCLUDE_DIR)
    file(READ "${utf8proc_INCLUDE_DIR}/utf8proc.h" UTF8PROC_H_CONTENT)

    string(REGEX MATCH "#define UTF8PROC_VERSION_MAJOR [0-9]+"
                 UTF8PROC_MAJOR_VERSION_DEFINITION "${UTF8PROC_H_CONTENT}")
    string(REGEX MATCH "#define UTF8PROC_VERSION_MINOR [0-9]+"
                 UTF8PROC_MINOR_VERSION_DEFINITION "${UTF8PROC_H_CONTENT}")
    string(REGEX MATCH "#define UTF8PROC_VERSION_PATCH [0-9]+"
                 UTF8PROC_PATCH_VERSION_DEFINITION "${UTF8PROC_H_CONTENT}")

    string(REGEX MATCH "[0-9]+$" UTF8PROC_MAJOR_VERSION
                 "${UTF8PROC_MAJOR_VERSION_DEFINITION}")
    string(REGEX MATCH "[0-9]+$" UTF8PROC_MINOR_VERSION
                 "${UTF8PROC_MINOR_VERSION_DEFINITION}")
    string(REGEX MATCH "[0-9]+$" UTF8PROC_PATCH_VERSION
                 "${UTF8PROC_PATCH_VERSION_DEFINITION}")
    set(utf8proc_VERSION
        "${UTF8PROC_MAJOR_VERSION}.${UTF8PROC_MINOR_VERSION}.${UTF8PROC_PATCH_VERSION}"
        PARENT_SCOPE)
  else()
    set(utf8proc_VERSION
        ""
        PARENT_SCOPE)
  endif()
endfunction(extract_utf8proc_version)

if(NOT utf8proc_USE_STATIC_LIB)
  set(utf8proc_LIB_NAMES)
  if(CMAKE_IMPORT_LIBRARY_SUFFIX)
    list(APPEND utf8proc_LIB_NAMES
         "${CMAKE_IMPORT_LIBRARY_PREFIX}utf8proc${CMAKE_IMPORT_LIBRARY_SUFFIX}")
  endif()
  list(APPEND utf8proc_LIB_NAMES
       "${CMAKE_SHARED_LIBRARY_PREFIX}utf8proc${CMAKE_SHARED_LIBRARY_SUFFIX}")
else()
  if(MSVC AND NOT DEFINED utf8proc_MSVC_STATIC_LIB_SUFFIX)
    set(utf8proc_MSVC_STATIC_LIB_SUFFIX "_static")
  endif()
  set(utf8proc_STATIC_LIB_SUFFIX
      "${utf8proc_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(utf8proc_LIB_NAMES
      "${CMAKE_STATIC_LIBRARY_PREFIX}utf8proc${utf8proc_STATIC_LIB_SUFFIX}")
endif()

if(utf8proc_ROOT)
  find_library(utf8proc_LIB
               NAMES ${utf8proc_LIB_NAMES}
               PATHS ${utf8proc_ROOT}
               PATH_SUFFIXES lib lib64
               NO_DEFAULT_PATH)
  find_path(utf8proc_INCLUDE_DIR
            NAMES utf8proc.h
            PATHS ${utf8proc_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES include)
  extract_utf8proc_version()
else()
  find_library(utf8proc_LIB
               NAMES ${utf8proc_LIB_NAMES}
               PATH_SUFFIXES lib lib64)
  find_path(utf8proc_INCLUDE_DIR
            NAMES utf8proc.h
            PATH_SUFFIXES include)
  extract_utf8proc_version()
endif()

find_package_handle_standard_args(
  utf8proc
  REQUIRED_VARS utf8proc_LIB utf8proc_INCLUDE_DIR
  VERSION_VAR utf8proc_VERSION)

if(utf8proc_FOUND)
  set(utf8proc_FOUND TRUE)
  add_library(utf8proc::utf8proc UNKNOWN IMPORTED)
  set_target_properties(utf8proc::utf8proc
                        PROPERTIES IMPORTED_LOCATION "${utf8proc_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${utf8proc_INCLUDE_DIR}")
endif()
