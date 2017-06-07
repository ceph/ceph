#.rst:
# FindPython3Libs
# --------------
#
# Find python libraries
#
# This module finds if Python is installed and determines where the
# include files and libraries are.  It also determines what the name of
# the library is.  This code sets the following variables:
#
# ::
#
#   PYTHON3LIBS_FOUND           - have the Python libs been found
#   PYTHON3_LIBRARIES           - path to the python library
#   PYTHON3_INCLUDE_PATH        - path to where Python.h is found (deprecated)
#   PYTHON3_INCLUDE_DIRS        - path to where Python.h is found
#   PYTHON3_DEBUG_LIBRARIES     - path to the debug library (deprecated)
#   PYTHON3LIBS_VERSION_STRING  - version of the Python libs found (since CMake 2.8.8)
#
#
#
# The Python3_ADDITIONAL_VERSIONS variable can be used to specify a list
# of version numbers that should be taken into account when searching
# for Python.  You need to set this variable before calling
# find_package(Python3Libs).
#
# If you'd like to specify the installation of Python to use, you should
# modify the following cache variables:
#
# ::
#
#   PYTHON3_LIBRARY             - path to the python library
#   PYTHON3_INCLUDE_DIR         - path to where Python.h is found
#
# If calling both ``find_package(PythonInterp)`` and
# ``find_package(Python3Libs)``, call ``find_package(PythonInterp)`` first to
# get the currently active Python version by default with a consistent version
# of PYTHON3_LIBRARIES.

#=============================================================================
# Copyright 2001-2009 Kitware, Inc.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# * Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimer in the
#   documentation and/or other materials provided with the distribution.
#
# * Neither the names of Kitware, Inc., the Insight Software Consortium,
#   nor the names of their contributors may be used to endorse or promote
#   products derived from this software without specific prior written
#   permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#=============================================================================

# Use the executable's path as a hint
set(_Python3_LIBRARY_PATH_HINT)
if(PYTHON3_EXECUTABLE)
  if(WIN32)
    get_filename_component(_Python3_PREFIX ${PYTHON3_EXECUTABLE} PATH)
    if(_Python3_PREFIX)
      set(_Python3_LIBRARY_PATH_HINT ${_Python3_PREFIX}/libs)
    endif()
    unset(_Python3_PREFIX)
  else()
    get_filename_component(_Python3_PREFIX ${PYTHON3_EXECUTABLE} PATH)
    get_filename_component(_Python3_PREFIX ${_Python3_PREFIX} PATH)
    if(_Python3_PREFIX)
      set(_Python3_LIBRARY_PATH_HINT ${_Python3_PREFIX}/lib)
    endif()
    unset(_Python3_PREFIX)
  endif()
endif()

include(CMakeFindFrameworks)
# Search for the python framework on Apple.
CMAKE_FIND_FRAMEWORKS(Python)

# Save CMAKE_FIND_FRAMEWORK
if(DEFINED CMAKE_FIND_FRAMEWORK)
  set(_Python3Libs_CMAKE_FIND_FRAMEWORK ${CMAKE_FIND_FRAMEWORK})
else()
  unset(_Python3Libs_CMAKE_FIND_FRAMEWORK)
endif()
# To avoid picking up the system Python.h pre-maturely.
set(CMAKE_FIND_FRAMEWORK LAST)

set(_PYTHON3_VERSIONS 3.6 3.5 3.4 3.3 3.2 3.1 3.0)

if(Python3Libs_FIND_VERSION)
    if(Python3Libs_FIND_VERSION_COUNT GREATER 1)
        set(_PYTHON3_FIND_MAJ_MIN "${Python3Libs_FIND_VERSION_MAJOR}.${Python3Libs_FIND_VERSION_MINOR}")
        unset(_PYTHON3_FIND_OTHER_VERSIONS)
        if(Python3Libs_FIND_VERSION_EXACT)
            if(_PYTHON3_FIND_MAJ_MIN STREQUAL Python3Libs_FIND_VERSION)
                set(_PYTHON3_FIND_OTHER_VERSIONS "${Python3Libs_FIND_VERSION}")
            else()
                set(_PYTHON3_FIND_OTHER_VERSIONS "${Python3Libs_FIND_VERSION}" "${_PYTHON3_FIND_MAJ_MIN}")
            endif()
        else()
            foreach(_PYTHON3_V ${_PYTHON${Python3Libs_FIND_VERSION_MAJOR}_VERSIONS})
                if(NOT _PYTHON3_V VERSION_LESS _PYTHON3_FIND_MAJ_MIN)
                    list(APPEND _PYTHON3_FIND_OTHER_VERSIONS ${_PYTHON3_V})
                endif()
             endforeach()
        endif()
        unset(_PYTHON3_FIND_MAJ_MIN)
    else()
        set(_PYTHON3_FIND_OTHER_VERSIONS ${_PYTHON${Python3Libs_FIND_VERSION_MAJOR}_VERSIONS})
    endif()
else()
    set(_PYTHON3_FIND_OTHER_VERSIONS ${_PYTHON3_VERSIONS})
endif()

# Set up the versions we know about, in the order we will search. Always add
# the user supplied additional versions to the front.
# If FindPythonInterp has already found the major and minor version,
# insert that version between the user supplied versions and the stock
# version list.
set(_Python3_VERSIONS ${Python3_ADDITIONAL_VERSIONS})
if(DEFINED PYTHON3_VERSION_MAJOR AND DEFINED PYTHON3_VERSION_MINOR)
  list(APPEND _Python3_VERSIONS ${PYTHON3_VERSION_MAJOR}.${PYTHON3_VERSION_MINOR})
endif()
list(APPEND _Python3_VERSIONS ${_PYTHON3_FIND_OTHER_VERSIONS})

unset(_PYTHON3_FIND_OTHER_VERSIONS)
unset(_PYTHON3_VERSIONS)

foreach(_CURRENT_VERSION ${_Python3_VERSIONS})
  string(REPLACE "." "" _CURRENT_VERSION_NO_DOTS ${_CURRENT_VERSION})
  if(WIN32)
    find_library(PYTHON3_DEBUG_LIBRARY
      NAMES python${_CURRENT_VERSION_NO_DOTS}_d python
      HINTS ${_Python3_LIBRARY_PATH_HINT}
      PATHS
      [HKEY_LOCAL_MACHINE\\SOFTWARE\\Python\\PythonCore\\${_CURRENT_VERSION}\\InstallPath]/libs/Debug
      [HKEY_CURRENT_USER\\SOFTWARE\\Python\\PythonCore\\${_CURRENT_VERSION}\\InstallPath]/libs/Debug
      [HKEY_LOCAL_MACHINE\\SOFTWARE\\Python\\PythonCore\\${_CURRENT_VERSION}\\InstallPath]/libs
      [HKEY_CURRENT_USER\\SOFTWARE\\Python\\PythonCore\\${_CURRENT_VERSION}\\InstallPath]/libs
      )
  endif()

  set(PYTHON3_FRAMEWORK_LIBRARIES)
  if(Python3_FRAMEWORKS AND NOT PYTHON3_LIBRARY)
    foreach(dir ${Python3_FRAMEWORKS})
      list(APPEND PYTHON3_FRAMEWORK_LIBRARIES
           ${dir}/Versions/${_CURRENT_VERSION}/lib)
    endforeach()
  endif()
  find_library(PYTHON3_LIBRARY
    NAMES
      python${_CURRENT_VERSION_NO_DOTS}
      python${_CURRENT_VERSION}mu
      python${_CURRENT_VERSION}m
      python${_CURRENT_VERSION}u
      python${_CURRENT_VERSION}
    HINTS
      ${_Python3_LIBRARY_PATH_HINT}
    PATHS
      ${PYTHON3_FRAMEWORK_LIBRARIES}
      [HKEY_LOCAL_MACHINE\\SOFTWARE\\Python\\PythonCore\\${_CURRENT_VERSION}\\InstallPath]/libs
      [HKEY_CURRENT_USER\\SOFTWARE\\Python\\PythonCore\\${_CURRENT_VERSION}\\InstallPath]/libs
    # Avoid finding the .dll in the PATH.  We want the .lib.
    NO_SYSTEM_ENVIRONMENT_PATH
  )
  # Look for the static library in the Python config directory
  find_library(PYTHON3_LIBRARY
    NAMES python${_CURRENT_VERSION_NO_DOTS} python${_CURRENT_VERSION}
    # Avoid finding the .dll in the PATH.  We want the .lib.
    NO_SYSTEM_ENVIRONMENT_PATH
    # This is where the static library is usually located
    PATH_SUFFIXES python${_CURRENT_VERSION}/config
  )

  # Don't search for include dir until library location is known
  if(PYTHON3_LIBRARY)

    # Use the library's install prefix as a hint
    set(_Python3_INCLUDE_PATH_HINT)
    get_filename_component(_Python3_PREFIX ${PYTHON3_LIBRARY} PATH)
    get_filename_component(_Python3_PREFIX ${_Python3_PREFIX} PATH)
    if(_Python3_PREFIX)
      set(_Python3_INCLUDE_PATH_HINT ${_Python3_PREFIX}/include)
    endif()
    unset(_Python3_PREFIX)

    # Add framework directories to the search paths
    set(PYTHON3_FRAMEWORK_INCLUDES)
    if(Python3_FRAMEWORKS AND NOT PYTHON3_INCLUDE_DIR)
      foreach(dir ${Python3_FRAMEWORKS})
        list(APPEND PYTHON3_FRAMEWORK_INCLUDES
          ${dir}/Versions/${_CURRENT_VERSION}/include)
      endforeach()
    endif()

    find_path(PYTHON3_INCLUDE_DIR
      NAMES Python.h
      HINTS
        ${_Python3_INCLUDE_PATH_HINT}
      PATHS
        ${PYTHON3_FRAMEWORK_INCLUDES}
        [HKEY_LOCAL_MACHINE\\SOFTWARE\\Python\\PythonCore\\${_CURRENT_VERSION}\\InstallPath]/include
        [HKEY_CURRENT_USER\\SOFTWARE\\Python\\PythonCore\\${_CURRENT_VERSION}\\InstallPath]/include
      PATH_SUFFIXES
        python${_CURRENT_VERSION}mu
        python${_CURRENT_VERSION}m
        python${_CURRENT_VERSION}u
        python${_CURRENT_VERSION}
    )
  endif()

  # For backward compatibility, set PYTHON3_INCLUDE_PATH.
  set(PYTHON3_INCLUDE_PATH "${PYTHON3_INCLUDE_DIR}")

  if(PYTHON3_INCLUDE_DIR AND EXISTS "${PYTHON3_INCLUDE_DIR}/patchlevel.h")
    file(STRINGS "${PYTHON3_INCLUDE_DIR}/patchlevel.h" python3_version_str
         REGEX "^#define[ \t]+PY_VERSION[ \t]+\"[^\"]+\"")
    string(REGEX REPLACE "^#define[ \t]+PY_VERSION[ \t]+\"([^\"]+)\".*" "\\1"
                         PYTHON3LIBS_VERSION_STRING "${python3_version_str}")
    unset(python3_version_str)
  endif()

  if(PYTHON3_LIBRARY AND PYTHON3_INCLUDE_DIR)
    break()
  endif()
endforeach()

unset(_Python3_INCLUDE_PATH_HINT)
unset(_Python3_LIBRARY_PATH_HINT)

mark_as_advanced(
  PYTHON3_DEBUG_LIBRARY
  PYTHON3_LIBRARY
  PYTHON3_INCLUDE_DIR
)

# We use PYTHON3_INCLUDE_DIR, PYTHON3_LIBRARY and PYTHON3_DEBUG_LIBRARY for the
# cache entries because they are meant to specify the location of a single
# library. We now set the variables listed by the documentation for this
# module.
set(PYTHON3_INCLUDE_DIRS "${PYTHON3_INCLUDE_DIR}")
set(PYTHON3_DEBUG_LIBRARIES "${PYTHON3_DEBUG_LIBRARY}")

# These variables have been historically named in this module different from
# what SELECT_LIBRARY_CONFIGURATIONS() expects.
set(PYTHON3_LIBRARY_DEBUG "${PYTHON3_DEBUG_LIBRARY}")
set(PYTHON3_LIBRARY_RELEASE "${PYTHON3_LIBRARY}")
include(SelectLibraryConfigurations)
SELECT_LIBRARY_CONFIGURATIONS(PYTHON3)
# SELECT_LIBRARY_CONFIGURATIONS() sets ${PREFIX}_FOUND if it has a library.
# Unset this, this prefix doesn't match the module prefix, they are different
# for historical reasons.
unset(PYTHON3_FOUND)

# Restore CMAKE_FIND_FRAMEWORK
if(DEFINED _Python3Libs_CMAKE_FIND_FRAMEWORK)
  set(CMAKE_FIND_FRAMEWORK ${_Python3Libs_CMAKE_FIND_FRAMEWORK})
  unset(_Python3Libs_CMAKE_FIND_FRAMEWORK)
else()
  unset(CMAKE_FIND_FRAMEWORK)
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Python3Libs
                                  REQUIRED_VARS PYTHON3_LIBRARIES PYTHON3_INCLUDE_DIRS
                                  VERSION_VAR PYTHON3LIBS_VERSION_STRING)

# PYTHON3_ADD_MODULE(<name> src1 src2 ... srcN) is used to build modules for python.
# PYTHON3_WRITE_MODULES_HEADER(<filename>) writes a header file you can include
# in your sources to initialize the static python modules
function(PYTHON3_ADD_MODULE _NAME )
  get_property(_TARGET_SUPPORTS_SHARED_LIBS
    GLOBAL PROPERTY TARGET_SUPPORTS_SHARED_LIBS)
  option(PYTHON3_ENABLE_MODULE_${_NAME} "Add module ${_NAME}" TRUE)
  option(PYTHON3_MODULE_${_NAME}_BUILD_SHARED
    "Add module ${_NAME} shared" ${_TARGET_SUPPORTS_SHARED_LIBS})

  # Mark these options as advanced
  mark_as_advanced(PYTHON3_ENABLE_MODULE_${_NAME}
    PYTHON3_MODULE_${_NAME}_BUILD_SHARED)

  if(PYTHON3_ENABLE_MODULE_${_NAME})
    if(PYTHON3_MODULE_${_NAME}_BUILD_SHARED)
      set(PY_MODULE_TYPE MODULE)
    else()
      set(PY_MODULE_TYPE STATIC)
      set_property(GLOBAL  APPEND  PROPERTY  PY_STATIC_MODULES_LIST ${_NAME})
    endif()

    set_property(GLOBAL  APPEND  PROPERTY  PY_MODULES_LIST ${_NAME})
    add_library(${_NAME} ${PY_MODULE_TYPE} ${ARGN})
#    target_link_libraries(${_NAME} ${PYTHON3_LIBRARIES})

    if(PYTHON3_MODULE_${_NAME}_BUILD_SHARED)
      set_target_properties(${_NAME} PROPERTIES PREFIX "${PYTHON3_MODULE_PREFIX}")
      if(WIN32 AND NOT CYGWIN)
        set_target_properties(${_NAME} PROPERTIES SUFFIX ".pyd")
      endif()
    endif()

  endif()
endfunction()

function(PYTHON3_WRITE_MODULES_HEADER _filename)

  get_property(PY_STATIC_MODULES_LIST  GLOBAL  PROPERTY PY_STATIC_MODULES_LIST)

  get_filename_component(_name "${_filename}" NAME)
  string(REPLACE "." "_" _name "${_name}")
  string(TOUPPER ${_name} _nameUpper)
  set(_filename ${CMAKE_CURRENT_BINARY_DIR}/${_filename})

  set(_filenameTmp "${_filename}.in")
  file(WRITE ${_filenameTmp} "/*Created by cmake, do not edit, changes will be lost*/\n")
  file(APPEND ${_filenameTmp}
"#ifndef ${_nameUpper}
#define ${_nameUpper}

#include <Python.h>

#ifdef __cplusplus
extern \"C\" {
#endif /* __cplusplus */

")

  foreach(_currentModule ${PY_STATIC_MODULES_LIST})
    file(APPEND ${_filenameTmp} "extern void init${PYTHON3_MODULE_PREFIX}${_currentModule}(void);\n\n")
  endforeach()

  file(APPEND ${_filenameTmp}
"#ifdef __cplusplus
}
#endif /* __cplusplus */

")


  foreach(_currentModule ${PY_STATIC_MODULES_LIST})
    file(APPEND ${_filenameTmp} "int ${_name}_${_currentModule}(void) \n{\n  static char name[]=\"${PYTHON3_MODULE_PREFIX}${_currentModule}\"; return PyImport_AppendInittab(name, init${PYTHON3_MODULE_PREFIX}${_currentModule});\n}\n\n")
  endforeach()

  file(APPEND ${_filenameTmp} "void ${_name}_LoadAllPythonModules(void)\n{\n")
  foreach(_currentModule ${PY_STATIC_MODULES_LIST})
    file(APPEND ${_filenameTmp} "  ${_name}_${_currentModule}();\n")
  endforeach()
  file(APPEND ${_filenameTmp} "}\n\n")
  file(APPEND ${_filenameTmp} "#ifndef EXCLUDE_LOAD_ALL_FUNCTION\nvoid CMakeLoadAllPythonModules(void)\n{\n  ${_name}_LoadAllPythonModules();\n}\n#endif\n\n#endif\n")

# with configure_file() cmake complains that you may not use a file created using file(WRITE) as input file for configure_file()
  execute_process(COMMAND ${CMAKE_COMMAND} -E copy_if_different "${_filenameTmp}" "${_filename}" OUTPUT_QUIET ERROR_QUIET)

endfunction()
