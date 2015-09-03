# This module can find FUSE Library
#
# Requirements:
# - CMake >= 2.8.3
#
# The following variables will be defined for your use:
# - FUSE_FOUND : was FUSE found?
# - FUSE_INCLUDE_DIRS : FUSE include directory
# - FUSE_LIBRARIES : FUSE library
# - FUSE_DEFINITIONS : FUSE cflags
# - FUSE_VERSION : complete version of FUSE (major.minor)
# - FUSE_MAJOR_VERSION : major version of FUSE
# - FUSE_MINOR_VERSION : minor version of FUSE
#
# Example Usage:
#
# 1. Copy this file in the root of your project source directory
# 2. Then, tell CMake to search this non-standard module in your project directory by adding to your CMakeLists.txt:
# set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})
# 3. Finally call find_package() once, here are some examples to pick from
#
# Require FUSE 2.6 or later
# find_package(FUSE 2.6 REQUIRED)
#
# if(FUSE_FOUND)
# add_definitions(${FUSE_DEFINITIONS})
# include_directories(${FUSE_INCLUDE_DIRS})
# add_executable(myapp myapp.c)
# target_link_libraries(myapp ${FUSE_LIBRARIES})
# endif()

#=============================================================================
# Copyright (c) 2012, julp
#
# Distributed under the OSI-approved BSD License
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#=============================================================================

cmake_minimum_required(VERSION 2.8.3)

########## Private ##########
function(fusedebug _varname)
    if(FUSE_DEBUG)
        message("${_varname} = ${${_varname}}")
    endif(FUSE_DEBUG)
endfunction(fusedebug)

########## Public ##########
set(FUSE_FOUND TRUE)
set(FUSE_LIBRARIES )
set(FUSE_DEFINITIONS )
set(FUSE_INCLUDE_DIRS )

find_package(PkgConfig)

set(PC_FUSE_INCLUDE_DIRS )
set(PC_FUSE_LIBRARY_DIRS )
if(PKG_CONFIG_FOUND)
    pkg_check_modules(PC_FUSE "fuse" QUIET)
    if(PC_FUSE_FOUND)
# fusedebug(PC_FUSE_LIBRARIES)
# fusedebug(PC_FUSE_LIBRARY_DIRS)
# fusedebug(PC_FUSE_LDFLAGS)
# fusedebug(PC_FUSE_LDFLAGS_OTHER)
# fusedebug(PC_FUSE_INCLUDE_DIRS)
# fusedebug(PC_FUSE_CFLAGS)
# fusedebug(PC_FUSE_CFLAGS_OTHER)
        set(FUSE_DEFINITIONS "${PC_FUSE_CFLAGS_OTHER}")
    endif(PC_FUSE_FOUND)
endif(PKG_CONFIG_FOUND)

find_path(
    FUSE_INCLUDE_DIRS
    NAMES fuse_common.h fuse_lowlevel.h fuse.h
    PATHS "${PC_FUSE_INCLUDE_DIRS}"
    DOC "Include directories for FUSE"
)

if(NOT FUSE_INCLUDE_DIRS)
    set(FUSE_FOUND FALSE)
endif(NOT FUSE_INCLUDE_DIRS)

find_library(
    FUSE_LIBRARIES
    NAMES "fuse"
    PATHS "${PC_FUSE_LIBRARY_DIRS}"
    DOC "Libraries for FUSE"
)

if(NOT FUSE_LIBRARIES)
    set(FUSE_FOUND FALSE)
endif(NOT FUSE_LIBRARIES)

if(FUSE_FOUND)
    if(EXISTS "${FUSE_INCLUDE_DIRS}/fuse_common.h")
        file(READ "${FUSE_INCLUDE_DIRS}/fuse_common.h" _contents)
        string(REGEX REPLACE ".*# *define *FUSE_MAJOR_VERSION *([0-9]+).*" "\\1" FUSE_MAJOR_VERSION "${_contents}")
        string(REGEX REPLACE ".*# *define *FUSE_MINOR_VERSION *([0-9]+).*" "\\1" FUSE_MINOR_VERSION "${_contents}")
        set(FUSE_VERSION "${FUSE_MAJOR_VERSION}.${FUSE_MINOR_VERSION}")
    endif()

    include(CheckCSourceCompiles)
    # Backup CMAKE_REQUIRED_*
    set(OLD_CMAKE_REQUIRED_INCLUDES "${CMAKE_REQUIRED_INCLUDES}")
    set(OLD_CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES}")
    set(OLD_CMAKE_REQUIRED_DEFINITIONS "${CMAKE_REQUIRED_DEFINITIONS}")
    # Add FUSE compilation flags
    set(CMAKE_REQUIRED_INCLUDES "${CMAKE_REQUIRED_INCLUDES}" "${FUSE_INCLUDE_DIRS}")
    set(CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES}" "${FUSE_LIBRARIES}")
    set(CMAKE_REQUIRED_DEFINITIONS "${CMAKE_REQUIRED_DEFINITIONS}" "${FUSE_DEFINITIONS}")
    check_c_source_compiles("#include <stdlib.h>
#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

int main(void) {
return 0;
}" FUSE_CFLAGS_CHECK)
    if(NOT FUSE_CFLAGS_CHECK)
        set(FUSE_DEFINITIONS "-D_FILE_OFFSET_BITS=64")
        # Should we run again previous test to assume the failure was due to missing definition -D_FILE_OFFSET_BITS=64?
    endif(NOT FUSE_CFLAGS_CHECK)
    # Restore CMAKE_REQUIRED_*
    set(CMAKE_REQUIRED_INCLUDES "${OLD_CMAKE_REQUIRED_INCLUDES}")
    set(CMAKE_REQUIRED_LIBRARIES "${OLD_CMAKE_REQUIRED_LIBRARIES}")
    set(CMAKE_REQUIRED_DEFINITIONS "${OLD_CMAKE_REQUIRED_DEFINITIONS}")
endif(FUSE_FOUND)

if(FUSE_INCLUDE_DIRS)
    include(FindPackageHandleStandardArgs)
    if(FUSE_FIND_REQUIRED AND NOT FUSE_FIND_QUIETLY)
        find_package_handle_standard_args(FUSE REQUIRED_VARS FUSE_LIBRARIES FUSE_INCLUDE_DIRS VERSION_VAR FUSE_VERSION)
    else()
        find_package_handle_standard_args(FUSE "FUSE not found" FUSE_LIBRARIES FUSE_INCLUDE_DIRS)
    endif()
else(FUSE_INCLUDE_DIRS)
    if(FUSE_FIND_REQUIRED AND NOT FUSE_FIND_QUIETLY)
        message(FATAL_ERROR "Could not find FUSE include directory")
    endif()
endif(FUSE_INCLUDE_DIRS)

mark_as_advanced(
    FUSE_INCLUDE_DIRS
    FUSE_LIBRARIES
)

# IN (args)
fusedebug("FUSE_FIND_COMPONENTS")
fusedebug("FUSE_FIND_REQUIRED")
fusedebug("FUSE_FIND_QUIETLY")
fusedebug("FUSE_FIND_VERSION")
# OUT
# Found
fusedebug("FUSE_FOUND")
# Definitions
fusedebug("FUSE_DEFINITIONS")
# Linking
fusedebug("FUSE_INCLUDE_DIRS")
fusedebug("FUSE_LIBRARIES")
# Version
fusedebug("FUSE_MAJOR_VERSION")
fusedebug("FUSE_MINOR_VERSION")
fusedebug("FUSE_VERSION")
