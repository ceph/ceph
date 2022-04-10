#===============================================================================
# Copyright 2019-2021 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

#
# Intel® Integrated Performance Primitives Cryptography (Intel® IPP Cryptography)
# library detection routine.
#
# If found the following variables will be available:
#       IPPCRYPTO_FOUND
#       IPPCRYPTO_ROOT_DIR
#       IPPCRYPTO_INCLUDE_DIRS
#       IPPCRYPTO_LIBRARIES
#

include(FindPackageHandleStandardArgs)

macro(ippcp_not_found)
  set(IPPCRYPTO_FOUND OFF)
  set(IPPCRYPTO_ROOT_DIR "${IPPCRYPTO_ROOT_DIR}" CACHE PATH "Path to Intel IPP Cryptography root directory")
  return()
endmacro()

# Try to find Intel IPP Cryptography library on the system if root dir is not defined externally
if (NOT IPPCRYPTO_ROOT_DIR OR NOT EXISTS "${IPPCRYPTO_ROOT_DIR}/include/ippcp.h")
  set(ippcp_search_paths
    ${CMAKE_CURRENT_SOURCE_DIR}/../.build
    $ENV{IPPCRYPTOROOT})

  if(WIN32)
    list(APPEND ippcp_search_paths
      $ENV{ProgramFiles\(x86\)}/IntelSWTools/compilers_and_libraries/windows/ippcp
      $ENV{ProgramFiles\(x86\)}/Intel/oneAPI/ippcp/latest)
  endif()

  if(UNIX)
    list(APPEND ippcp_search_paths
      /opt/intel/ippcp
      $ENV{HOME}/intel/ippcp
      /opt/intel/oneapi/ippcp/latest
      $ENV{HOME}/intel/oneapi/ippcp/latest)
  endif()

  find_path(IPPCRYPTO_ROOT_DIR include/ippcp.h PATHS ${ippcp_search_paths})
endif()

set(IPPCRYPTO_INCLUDE_DIRS "${IPPCRYPTO_ROOT_DIR}/include" CACHE PATH "Path to Intel IPP Cryptography library include directory" FORCE)

# Check found directory
if(NOT IPPCRYPTO_ROOT_DIR
    OR NOT EXISTS "${IPPCRYPTO_ROOT_DIR}"
    OR NOT EXISTS "${IPPCRYPTO_INCLUDE_DIRS}"
    OR NOT EXISTS "${IPPCRYPTO_INCLUDE_DIRS}/ippcpdefs.h"
    )
  ippcp_not_found()
endif()

# Determine ARCH
set(IPPCRYPTO_ARCH "ia32")
if(CMAKE_CXX_SIZEOF_DATA_PTR EQUAL 8)
  set(IPPCRYPTO_ARCH "intel64")
endif()
if(CMAKE_SIZEOF_VOID_P)
  set(IPPCRYPTO_ARCH "intel64")
endif()

# Define list of libraries to search
set(IPPCP_SUFFIX "")
if(WIN32)
  set(IPPCP_SUFFIX "mt") # static lib on Windows
endif()
set(ippcp_search_libraries
  ippcp${IPPCP_SUFFIX})

# Define library search paths (TODO: to handle nonpic libraries)
set(ippcp_lib_search_paths "")
list(APPEND ippcp_lib_search_paths
  ${IPPCRYPTO_ROOT_DIR}/lib
  ${IPPCRYPTO_ROOT_DIR}/lib/${IPPCRYPTO_ARCH})

# Set preferences to look for static libraries only
set( _ippcrypto_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
if(WIN32)
  list(INSERT CMAKE_FIND_LIBRARY_SUFFIXES 0 .lib .a)
else()
  set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
endif()

foreach(lib ${ippcp_search_libraries})
  find_library(${lib} ${lib} ${ippcp_lib_search_paths})
  if(NOT ${lib})
    ippcp_not_found()
  endif()
  list(APPEND IPPCRYPTO_LIBRARIES ${${lib}})
endforeach()

list(REMOVE_DUPLICATES IPPCRYPTO_LIBRARIES)

message(STATUS "Found Intel IPP Cryptography at: ${IPPCRYPTO_ROOT_DIR}")

# Restore the original find library ordering
set(CMAKE_FIND_LIBRARY_SUFFIXES ${_ippcrypto_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})

set(IPPCRYPTO_FOUND ON)
set(IPPCRYPTO_ROOT_DIR "${IPPCRYPTO_ROOT_DIR}" CACHE PATH "Path to Intel IPP Cryptography root directory")
set(IPPCRYPTO_INCLUDE_DIRS "${IPPCRYPTO_INCLUDE_DIRS}" CACHE PATH "Path to Intel IPP Cryptography include directory")
set(IPPCRYPTO_LIBRARIES "${IPPCRYPTO_LIBRARIES}" CACHE STRING "Intel IPP Cryptography libraries")
