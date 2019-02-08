# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

#.rst:
# FindBoost
# ---------
#
# Find Boost include dirs and libraries
#
# Use this module by invoking find_package with the form::
#
#   find_package(Boost
#     [version] [EXACT]      # Minimum or EXACT version e.g. 1.67.0
#     [REQUIRED]             # Fail with error if Boost is not found
#     [COMPONENTS <libs>...] # Boost libraries by their canonical name
#                            # e.g. "date_time" for "libboost_date_time"
#     [OPTIONAL_COMPONENTS <libs>...]
#                            # Optional Boost libraries by their canonical name)
#     )                      # e.g. "date_time" for "libboost_date_time"
#
# This module finds headers and requested component libraries OR a CMake
# package configuration file provided by a "Boost CMake" build.  For the
# latter case skip to the "Boost CMake" section below.  For the former
# case results are reported in variables::
#
#   Boost_FOUND            - True if headers and requested libraries were found
#   Boost_INCLUDE_DIRS     - Boost include directories
#   Boost_LIBRARY_DIRS     - Link directories for Boost libraries
#   Boost_LIBRARIES        - Boost component libraries to be linked
#   Boost_<C>_FOUND        - True if component <C> was found (<C> is upper-case)
#   Boost_<C>_LIBRARY      - Libraries to link for component <C> (may include
#                            target_link_libraries debug/optimized keywords)
#   Boost_VERSION          - BOOST_VERSION value from boost/version.hpp
#   Boost_LIB_VERSION      - Version string appended to library filenames
#   Boost_MAJOR_VERSION    - Boost major version number (X in X.y.z)
#   Boost_MINOR_VERSION    - Boost minor version number (Y in x.Y.z)
#   Boost_SUBMINOR_VERSION - Boost subminor version number (Z in x.y.Z)
#   Boost_LIB_DIAGNOSTIC_DEFINITIONS (Windows)
#                          - Pass to add_definitions() to have diagnostic
#                            information about Boost's automatic linking
#                            displayed during compilation
#
# Note that Boost Python components require a Python version suffix
# (Boost 1.67 and later), e.g. ``python36`` or ``python27`` for the
# versions built against Python 3.6 and 2.7, respectively.  This also
# applies to additional components using Python including
# ``mpi_python`` and ``numpy``.  Earlier Boost releases may use
# distribution-specific suffixes such as ``2``, ``3`` or ``2.7``.
# These may also be used as suffixes, but note that they are not
# portable.
#
# This module reads hints about search locations from variables::
#
#   BOOST_ROOT             - Preferred installation prefix
#    (or BOOSTROOT)
#   BOOST_INCLUDEDIR       - Preferred include directory e.g. <prefix>/include
#   BOOST_LIBRARYDIR       - Preferred library directory e.g. <prefix>/lib
#   Boost_NO_SYSTEM_PATHS  - Set to ON to disable searching in locations not
#                            specified by these hint variables. Default is OFF.
#   Boost_ADDITIONAL_VERSIONS
#                          - List of Boost versions not known to this module
#                            (Boost install locations may contain the version)
#
# and saves search results persistently in CMake cache entries::
#
#   Boost_INCLUDE_DIR         - Directory containing Boost headers
#   Boost_LIBRARY_DIR_RELEASE - Directory containing release Boost libraries
#   Boost_LIBRARY_DIR_DEBUG   - Directory containing debug Boost libraries
#   Boost_<C>_LIBRARY_DEBUG   - Component <C> library debug variant
#   Boost_<C>_LIBRARY_RELEASE - Component <C> library release variant
#
# The following :prop_tgt:`IMPORTED` targets are also defined::
#
#   Boost::boost                  - Target for header-only dependencies
#                                   (Boost include directory)
#   Boost::<C>                    - Target for specific component dependency
#                                   (shared or static library); <C> is lower-
#                                   case
#   Boost::diagnostic_definitions - interface target to enable diagnostic
#                                   information about Boost's automatic linking
#                                   during compilation (adds BOOST_LIB_DIAGNOSTIC)
#   Boost::disable_autolinking    - interface target to disable automatic
#                                   linking with MSVC (adds BOOST_ALL_NO_LIB)
#   Boost::dynamic_linking        - interface target to enable dynamic linking
#                                   linking with MSVC (adds BOOST_ALL_DYN_LINK)
#
# Implicit dependencies such as Boost::filesystem requiring
# Boost::system will be automatically detected and satisfied, even
# if system is not specified when using find_package and if
# Boost::system is not added to target_link_libraries.  If using
# Boost::thread, then Threads::Threads will also be added automatically.
#
# It is important to note that the imported targets behave differently
# than variables created by this module: multiple calls to
# find_package(Boost) in the same directory or sub-directories with
# different options (e.g. static or shared) will not override the
# values of the targets created by the first call.
#
# Users may set these hints or results as cache entries.  Projects
# should not read these entries directly but instead use the above
# result variables.  Note that some hint names start in upper-case
# "BOOST".  One may specify these as environment variables if they are
# not specified as CMake variables or cache entries.
#
# This module first searches for the Boost header files using the above
# hint variables (excluding BOOST_LIBRARYDIR) and saves the result in
# Boost_INCLUDE_DIR.  Then it searches for requested component libraries
# using the above hints (excluding BOOST_INCLUDEDIR and
# Boost_ADDITIONAL_VERSIONS), "lib" directories near Boost_INCLUDE_DIR,
# and the library name configuration settings below.  It saves the
# library directories in Boost_LIBRARY_DIR_DEBUG and
# Boost_LIBRARY_DIR_RELEASE and individual library
# locations in Boost_<C>_LIBRARY_DEBUG and Boost_<C>_LIBRARY_RELEASE.
# When one changes settings used by previous searches in the same build
# tree (excluding environment variables) this module discards previous
# search results affected by the changes and searches again.
#
# Boost libraries come in many variants encoded in their file name.
# Users or projects may tell this module which variant to find by
# setting variables::
#
#   Boost_USE_DEBUG_LIBS     - Set to ON or OFF to specify whether to search
#                              and use the debug libraries.  Default is ON.
#   Boost_USE_RELEASE_LIBS   - Set to ON or OFF to specify whether to search
#                              and use the release libraries.  Default is ON.
#   Boost_USE_MULTITHREADED  - Set to OFF to use the non-multithreaded
#                              libraries ('mt' tag).  Default is ON.
#   Boost_USE_STATIC_LIBS    - Set to ON to force the use of the static
#                              libraries.  Default is OFF.
#   Boost_USE_STATIC_RUNTIME - Set to ON or OFF to specify whether to use
#                              libraries linked statically to the C++ runtime
#                              ('s' tag).  Default is platform dependent.
#   Boost_USE_DEBUG_RUNTIME  - Set to ON or OFF to specify whether to use
#                              libraries linked to the MS debug C++ runtime
#                              ('g' tag).  Default is ON.
#   Boost_USE_DEBUG_PYTHON   - Set to ON to use libraries compiled with a
#                              debug Python build ('y' tag). Default is OFF.
#   Boost_USE_STLPORT        - Set to ON to use libraries compiled with
#                              STLPort ('p' tag).  Default is OFF.
#   Boost_USE_STLPORT_DEPRECATED_NATIVE_IOSTREAMS
#                            - Set to ON to use libraries compiled with
#                              STLPort deprecated "native iostreams"
#                              ('n' tag).  Default is OFF.
#   Boost_COMPILER           - Set to the compiler-specific library suffix
#                              (e.g. "-gcc43").  Default is auto-computed
#                              for the C++ compiler in use.  A list may be
#                              used if multiple compatible suffixes should
#                              be tested for, in decreasing order of
#                              preference.
#   Boost_ARCHITECTURE       - Set to the architecture-specific library suffix
#                              (e.g. "-x64").  Default is auto-computed for the
#                              C++ compiler in use.
#   Boost_THREADAPI          - Suffix for "thread" component library name,
#                              such as "pthread" or "win32".  Names with
#                              and without this suffix will both be tried.
#   Boost_NAMESPACE          - Alternate namespace used to build boost with
#                              e.g. if set to "myboost", will search for
#                              myboost_thread instead of boost_thread.
#
# Other variables one may set to control this module are::
#
#   Boost_DEBUG              - Set to ON to enable debug output from FindBoost.
#                              Please enable this before filing any bug report.
#   Boost_DETAILED_FAILURE_MSG
#                            - Set to ON to add detailed information to the
#                              failure message even when the REQUIRED option
#                              is not given to the find_package call.
#   Boost_REALPATH           - Set to ON to resolve symlinks for discovered
#                              libraries to assist with packaging.  For example,
#                              the "system" component library may be resolved to
#                              "/usr/lib/libboost_system.so.1.67.0" instead of
#                              "/usr/lib/libboost_system.so".  This does not
#                              affect linking and should not be enabled unless
#                              the user needs this information.
#   Boost_LIBRARY_DIR        - Default value for Boost_LIBRARY_DIR_RELEASE and
#                              Boost_LIBRARY_DIR_DEBUG.
#
# On Visual Studio and Borland compilers Boost headers request automatic
# linking to corresponding libraries.  This requires matching libraries
# to be linked explicitly or available in the link library search path.
# In this case setting Boost_USE_STATIC_LIBS to OFF may not achieve
# dynamic linking.  Boost automatic linking typically requests static
# libraries with a few exceptions (such as Boost.Python).  Use::
#
#   add_definitions(${Boost_LIB_DIAGNOSTIC_DEFINITIONS})
#
# to ask Boost to report information about automatic linking requests.
#
# Example to find Boost headers only::
#
#   find_package(Boost 1.36.0)
#   if(Boost_FOUND)
#     include_directories(${Boost_INCLUDE_DIRS})
#     add_executable(foo foo.cc)
#   endif()
#
# Example to find Boost libraries and use imported targets::
#
#   find_package(Boost 1.56 REQUIRED COMPONENTS
#                date_time filesystem iostreams)
#   add_executable(foo foo.cc)
#   target_link_libraries(foo Boost::date_time Boost::filesystem
#                             Boost::iostreams)
#
# Example to find Boost Python 3.6 libraries and use imported targets::
#
#   find_package(Boost 1.67 REQUIRED COMPONENTS
#                python36 numpy36)
#   add_executable(foo foo.cc)
#   target_link_libraries(foo Boost::python36 Boost::numpy36)
#
# Example to find Boost headers and some *static* (release only) libraries::
#
#   set(Boost_USE_STATIC_LIBS        ON)  # only find static libs
#   set(Boost_USE_DEBUG_LIBS         OFF) # ignore debug libs and
#   set(Boost_USE_RELEASE_LIBS       ON)  # only find release libs
#   set(Boost_USE_MULTITHREADED      ON)
#   set(Boost_USE_STATIC_RUNTIME    OFF)
#   find_package(Boost 1.66.0 COMPONENTS date_time filesystem system ...)
#   if(Boost_FOUND)
#     include_directories(${Boost_INCLUDE_DIRS})
#     add_executable(foo foo.cc)
#     target_link_libraries(foo ${Boost_LIBRARIES})
#   endif()
#
# Boost CMake
# ^^^^^^^^^^^
#
# If Boost was built using the boost-cmake project it provides a package
# configuration file for use with find_package's Config mode.  This
# module looks for the package configuration file called
# BoostConfig.cmake or boost-config.cmake and stores the result in cache
# entry "Boost_DIR".  If found, the package configuration file is loaded
# and this module returns with no further action.  See documentation of
# the Boost CMake package configuration for details on what it provides.
#
# Set Boost_NO_BOOST_CMAKE to ON to disable the search for boost-cmake.

# Save project's policies
cmake_policy(PUSH)
cmake_policy(SET CMP0057 NEW) # if IN_LIST

#-------------------------------------------------------------------------------
# Before we go searching, check whether boost-cmake is available, unless the
# user specifically asked NOT to search for boost-cmake.
#
# If Boost_DIR is set, this behaves as any find_package call would. If not,
# it looks at BOOST_ROOT and BOOSTROOT to find Boost.
#
if (NOT Boost_NO_BOOST_CMAKE)
  # If Boost_DIR is not set, look for BOOSTROOT and BOOST_ROOT as alternatives,
  # since these are more conventional for Boost.
  if ("$ENV{Boost_DIR}" STREQUAL "")
    if (NOT "$ENV{BOOST_ROOT}" STREQUAL "")
      set(ENV{Boost_DIR} $ENV{BOOST_ROOT})
    elseif (NOT "$ENV{BOOSTROOT}" STREQUAL "")
      set(ENV{Boost_DIR} $ENV{BOOSTROOT})
    endif()
  endif()

  # Do the same find_package call but look specifically for the CMake version.
  # Note that args are passed in the Boost_FIND_xxxxx variables, so there is no
  # need to delegate them to this find_package call.
  find_package(Boost QUIET NO_MODULE)
  mark_as_advanced(Boost_DIR)

  # If we found boost-cmake, then we're done.  Print out what we found.
  # Otherwise let the rest of the module try to find it.
  if (Boost_FOUND)
    message(STATUS "Boost ${Boost_FIND_VERSION} found.")
    if (Boost_FIND_COMPONENTS)
      message(STATUS "Found Boost components:\n   ${Boost_FIND_COMPONENTS}")
    endif()
    # Restore project's policies
    cmake_policy(POP)
    return()
  endif()
endif()


#-------------------------------------------------------------------------------
#  FindBoost functions & macros
#

############################################
#
# Check the existence of the libraries.
#
############################################
# This macro was taken directly from the FindQt4.cmake file that is included
# with the CMake distribution. This is NOT my work. All work was done by the
# original authors of the FindQt4.cmake file. Only minor modifications were
# made to remove references to Qt and make this file more generally applicable
# And ELSE/ENDIF pairs were removed for readability.
#########################################################################

macro(_Boost_ADJUST_LIB_VARS basename)
  if(Boost_INCLUDE_DIR )
    if(Boost_${basename}_LIBRARY_DEBUG AND Boost_${basename}_LIBRARY_RELEASE)
      # if the generator is multi-config or if CMAKE_BUILD_TYPE is set for
      # single-config generators, set optimized and debug libraries
      get_property(_isMultiConfig GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
      if(_isMultiConfig OR CMAKE_BUILD_TYPE)
        set(Boost_${basename}_LIBRARY optimized ${Boost_${basename}_LIBRARY_RELEASE} debug ${Boost_${basename}_LIBRARY_DEBUG})
      else()
        # For single-config generators where CMAKE_BUILD_TYPE has no value,
        # just use the release libraries
        set(Boost_${basename}_LIBRARY ${Boost_${basename}_LIBRARY_RELEASE} )
      endif()
      # FIXME: This probably should be set for both cases
      set(Boost_${basename}_LIBRARIES optimized ${Boost_${basename}_LIBRARY_RELEASE} debug ${Boost_${basename}_LIBRARY_DEBUG})
    endif()

    # if only the release version was found, set the debug variable also to the release version
    if(Boost_${basename}_LIBRARY_RELEASE AND NOT Boost_${basename}_LIBRARY_DEBUG)
      set(Boost_${basename}_LIBRARY_DEBUG ${Boost_${basename}_LIBRARY_RELEASE})
      set(Boost_${basename}_LIBRARY       ${Boost_${basename}_LIBRARY_RELEASE})
      set(Boost_${basename}_LIBRARIES     ${Boost_${basename}_LIBRARY_RELEASE})
    endif()

    # if only the debug version was found, set the release variable also to the debug version
    if(Boost_${basename}_LIBRARY_DEBUG AND NOT Boost_${basename}_LIBRARY_RELEASE)
      set(Boost_${basename}_LIBRARY_RELEASE ${Boost_${basename}_LIBRARY_DEBUG})
      set(Boost_${basename}_LIBRARY         ${Boost_${basename}_LIBRARY_DEBUG})
      set(Boost_${basename}_LIBRARIES       ${Boost_${basename}_LIBRARY_DEBUG})
    endif()

    # If the debug & release library ends up being the same, omit the keywords
    if("${Boost_${basename}_LIBRARY_RELEASE}" STREQUAL "${Boost_${basename}_LIBRARY_DEBUG}")
      set(Boost_${basename}_LIBRARY   ${Boost_${basename}_LIBRARY_RELEASE} )
      set(Boost_${basename}_LIBRARIES ${Boost_${basename}_LIBRARY_RELEASE} )
    endif()

    if(Boost_${basename}_LIBRARY AND Boost_${basename}_HEADER)
      set(Boost_${basename}_FOUND ON)
      if("x${basename}" STREQUAL "xTHREAD" AND NOT TARGET Threads::Threads)
        string(APPEND Boost_ERROR_REASON_THREAD " (missing dependency: Threads)")
        set(Boost_THREAD_FOUND OFF)
      endif()
    endif()

  endif()
  # Make variables changeable to the advanced user
  mark_as_advanced(
      Boost_${basename}_LIBRARY_RELEASE
      Boost_${basename}_LIBRARY_DEBUG
  )
endmacro()

# Detect changes in used variables.
# Compares the current variable value with the last one.
# In short form:
# v != v_LAST                      -> CHANGED = 1
# v is defined, v_LAST not         -> CHANGED = 1
# v is not defined, but v_LAST is  -> CHANGED = 1
# otherwise                        -> CHANGED = 0
# CHANGED is returned in variable named ${changed_var}
macro(_Boost_CHANGE_DETECT changed_var)
  set(${changed_var} 0)
  foreach(v ${ARGN})
    if(DEFINED _Boost_COMPONENTS_SEARCHED)
      if(${v})
        if(_${v}_LAST)
          string(COMPARE NOTEQUAL "${${v}}" "${_${v}_LAST}" _${v}_CHANGED)
        else()
          set(_${v}_CHANGED 1)
        endif()
      elseif(_${v}_LAST)
        set(_${v}_CHANGED 1)
      endif()
      if(_${v}_CHANGED)
        set(${changed_var} 1)
      endif()
    else()
      set(_${v}_CHANGED 0)
    endif()
  endforeach()
endmacro()

#
# Find the given library (var).
# Use 'build_type' to support different lib paths for RELEASE or DEBUG builds
#
macro(_Boost_FIND_LIBRARY var build_type)

  find_library(${var} ${ARGN})

  if(${var})
    # If this is the first library found then save Boost_LIBRARY_DIR_[RELEASE,DEBUG].
    if(NOT Boost_LIBRARY_DIR_${build_type})
      get_filename_component(_dir "${${var}}" PATH)
      set(Boost_LIBRARY_DIR_${build_type} "${_dir}" CACHE PATH "Boost library directory ${build_type}" FORCE)
    endif()
  elseif(_Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT)
    # Try component-specific hints but do not save Boost_LIBRARY_DIR_[RELEASE,DEBUG].
    find_library(${var} HINTS ${_Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT} ${ARGN})
  endif()

  # If Boost_LIBRARY_DIR_[RELEASE,DEBUG] is known then search only there.
  if(Boost_LIBRARY_DIR_${build_type})
    set(_boost_LIBRARY_SEARCH_DIRS_${build_type} ${Boost_LIBRARY_DIR_${build_type}} NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
    if(Boost_DEBUG)
      message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
        " Boost_LIBRARY_DIR_${build_type} = ${Boost_LIBRARY_DIR_${build_type}}"
        " _boost_LIBRARY_SEARCH_DIRS_${build_type} = ${_boost_LIBRARY_SEARCH_DIRS_${build_type}}")
    endif()
  endif()
endmacro()

#-------------------------------------------------------------------------------

# Convert CMAKE_CXX_COMPILER_VERSION to boost compiler suffix version.
function(_Boost_COMPILER_DUMPVERSION _OUTPUT_VERSION _OUTPUT_VERSION_MAJOR _OUTPUT_VERSION_MINOR)
  string(REGEX REPLACE "([0-9]+)\\.([0-9]+)(\\.[0-9]+)?" "\\1"
    _boost_COMPILER_VERSION_MAJOR "${CMAKE_CXX_COMPILER_VERSION}")
  string(REGEX REPLACE "([0-9]+)\\.([0-9]+)(\\.[0-9]+)?" "\\2"
    _boost_COMPILER_VERSION_MINOR "${CMAKE_CXX_COMPILER_VERSION}")

  set(_boost_COMPILER_VERSION "${_boost_COMPILER_VERSION_MAJOR}${_boost_COMPILER_VERSION_MINOR}")

  set(${_OUTPUT_VERSION} ${_boost_COMPILER_VERSION} PARENT_SCOPE)
  set(${_OUTPUT_VERSION_MAJOR} ${_boost_COMPILER_VERSION_MAJOR} PARENT_SCOPE)
  set(${_OUTPUT_VERSION_MINOR} ${_boost_COMPILER_VERSION_MINOR} PARENT_SCOPE)
endfunction()

#
# Take a list of libraries with "thread" in it
# and prepend duplicates with "thread_${Boost_THREADAPI}"
# at the front of the list
#
function(_Boost_PREPEND_LIST_WITH_THREADAPI _output)
  set(_orig_libnames ${ARGN})
  string(REPLACE "thread" "thread_${Boost_THREADAPI}" _threadapi_libnames "${_orig_libnames}")
  set(${_output} ${_threadapi_libnames} ${_orig_libnames} PARENT_SCOPE)
endfunction()

#
# If a library is found, replace its cache entry with its REALPATH
#
function(_Boost_SWAP_WITH_REALPATH _library _docstring)
  if(${_library})
    get_filename_component(_boost_filepathreal ${${_library}} REALPATH)
    unset(${_library} CACHE)
    set(${_library} ${_boost_filepathreal} CACHE FILEPATH "${_docstring}")
  endif()
endfunction()

function(_Boost_CHECK_SPELLING _var)
  if(${_var})
    string(TOUPPER ${_var} _var_UC)
    message(FATAL_ERROR "ERROR: ${_var} is not the correct spelling.  The proper spelling is ${_var_UC}.")
  endif()
endfunction()

# Guesses Boost's compiler prefix used in built library names
# Returns the guess by setting the variable pointed to by _ret
function(_Boost_GUESS_COMPILER_PREFIX _ret)
  if("x${CMAKE_CXX_COMPILER_ID}" STREQUAL "xIntel")
    if(WIN32)
      set (_boost_COMPILER "-iw")
    else()
      set (_boost_COMPILER "-il")
    endif()
  elseif (GHSMULTI)
    set(_boost_COMPILER "-ghs")
  elseif("x${CMAKE_CXX_COMPILER_ID}" STREQUAL "xMSVC")
    if(MSVC_TOOLSET_VERSION GREATER_EQUAL 141)
      set(_boost_COMPILER "-vc141;-vc140")
    elseif(MSVC_TOOLSET_VERSION GREATER_EQUAL 80)
      set(_boost_COMPILER "-vc${MSVC_TOOLSET_VERSION}")
    elseif(NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 13.10)
      set(_boost_COMPILER "-vc71")
    elseif(NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 13) # Good luck!
      set(_boost_COMPILER "-vc7") # yes, this is correct
    else() # VS 6.0 Good luck!
      set(_boost_COMPILER "-vc6") # yes, this is correct
    endif()
  elseif (BORLAND)
    set(_boost_COMPILER "-bcb")
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "SunPro")
    set(_boost_COMPILER "-sw")
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "XL")
    set(_boost_COMPILER "-xlc")
  elseif (MINGW)
    if(${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION} VERSION_LESS 1.34)
        set(_boost_COMPILER "-mgw") # no GCC version encoding prior to 1.34
    else()
      _Boost_COMPILER_DUMPVERSION(_boost_COMPILER_VERSION _boost_COMPILER_VERSION_MAJOR _boost_COMPILER_VERSION_MINOR)
      set(_boost_COMPILER "-mgw${_boost_COMPILER_VERSION}")
    endif()
  elseif (UNIX)
    _Boost_COMPILER_DUMPVERSION(_boost_COMPILER_VERSION _boost_COMPILER_VERSION_MAJOR _boost_COMPILER_VERSION_MINOR)
    if(NOT Boost_VERSION VERSION_LESS 106900)
      # From GCC 5 and clang 4, versioning changes and minor becomes patch.
      # For those compilers, patch is exclude from compiler tag in Boost 1.69+ library naming.
      if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND _boost_COMPILER_VERSION_MAJOR VERSION_GREATER 4)
        set(_boost_COMPILER_VERSION "${_boost_COMPILER_VERSION_MAJOR}")
      elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND _boost_COMPILER_VERSION_MAJOR VERSION_GREATER 3)
        set(_boost_COMPILER_VERSION "${_boost_COMPILER_VERSION_MAJOR}")
      endif()
    endif()

    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      if(${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION} VERSION_LESS 1.34)
        set(_boost_COMPILER "-gcc") # no GCC version encoding prior to 1.34
      else()
        # Determine which version of GCC we have.
        if(APPLE)
          if(Boost_MINOR_VERSION)
            if(${Boost_MINOR_VERSION} GREATER 35)
              # In Boost 1.36.0 and newer, the mangled compiler name used
              # on macOS/Darwin is "xgcc".
              set(_boost_COMPILER "-xgcc${_boost_COMPILER_VERSION}")
            else()
              # In Boost <= 1.35.0, there is no mangled compiler name for
              # the macOS/Darwin version of GCC.
              set(_boost_COMPILER "")
            endif()
          else()
            # We don't know the Boost version, so assume it's
            # pre-1.36.0.
            set(_boost_COMPILER "")
          endif()
        else()
          set(_boost_COMPILER "-gcc${_boost_COMPILER_VERSION}")
        endif()
      endif()
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
      # TODO: Find out any Boost version constraints vs clang support.
      set(_boost_COMPILER "-clang${_boost_COMPILER_VERSION}")
    endif()
  else()
    # TODO at least Boost_DEBUG here?
    set(_boost_COMPILER "")
  endif()
  set(${_ret} ${_boost_COMPILER} PARENT_SCOPE)
endfunction()

#
# Get component dependencies.  Requires the dependencies to have been
# defined for the Boost release version.
#
# component - the component to check
# _ret - list of library dependencies
#
function(_Boost_COMPONENT_DEPENDENCIES component _ret)
  # Note: to add a new Boost release, run
  #
  #   % cmake -DBOOST_DIR=/path/to/boost/source -P Utilities/Scripts/BoostScanDeps.cmake
  #
  # The output may be added in a new block below.  If it's the same as
  # the previous release, simply update the version range of the block
  # for the previous release.  Also check if any new components have
  # been added, and add any new components to
  # _Boost_COMPONENT_HEADERS.
  #
  # This information was originally generated by running
  # BoostScanDeps.cmake against every boost release to date supported
  # by FindBoost:
  #
  #   % for version in /path/to/boost/sources/*
  #     do
  #       cmake -DBOOST_DIR=$version -P Utilities/Scripts/BoostScanDeps.cmake
  #     done
  #
  # The output was then updated by search and replace with these regexes:
  #
  # - Strip message(STATUS) prefix dashes
  #   s;^-- ;;
  # - Indent
  #   s;^set(;    set(;;
  # - Add conditionals
  #   s;Scanning /path/to/boost/sources/boost_\(.*\)_\(.*\)_\(.*);  elseif(NOT Boost_VERSION VERSION_LESS \10\20\3 AND Boost_VERSION VERSION_LESS xxxx);
  #
  # This results in the logic seen below, but will require the xxxx
  # replacing with the following Boost release version (or the next
  # minor version to be released, e.g. 1.59 was the latest at the time
  # of writing, making 1.60 the next, so 106000 is the needed version
  # number).  Identical consecutive releases were then merged together
  # by updating the end range of the first block and removing the
  # following redundant blocks.
  #
  # Running the script against all historical releases should be
  # required only if the BoostScanDeps.cmake script logic is changed.
  # The addition of a new release should only require it to be run
  # against the new release.

  # Handle Python version suffixes
  if(component MATCHES "^(python|mpi_python|numpy)([0-9][0-9]?|[0-9]\\.[0-9])\$")
    set(component "${CMAKE_MATCH_1}")
    set(component_python_version "${CMAKE_MATCH_2}")
  endif()

  set(_Boost_IMPORTED_TARGETS TRUE)
  if(Boost_VERSION AND Boost_VERSION VERSION_LESS 103300)
    message(WARNING "Imported targets and dependency information not available for Boost version ${Boost_VERSION} (all versions older than 1.33)")
    set(_Boost_IMPORTED_TARGETS FALSE)
  elseif(NOT Boost_VERSION VERSION_LESS 103300 AND Boost_VERSION VERSION_LESS 103500)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex thread)
    set(_Boost_REGEX_DEPENDENCIES thread)
    set(_Boost_WAVE_DEPENDENCIES filesystem thread)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 103500 AND Boost_VERSION VERSION_LESS 103600)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_WAVE_DEPENDENCIES filesystem system thread)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 103600 AND Boost_VERSION VERSION_LESS 103800)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_WAVE_DEPENDENCIES filesystem system thread)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 103800 AND Boost_VERSION VERSION_LESS 104300)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_THREAD_DEPENDENCIES date_time)
    set(_Boost_WAVE_DEPENDENCIES filesystem system thread date_time)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 104300 AND Boost_VERSION VERSION_LESS 104400)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l random)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_THREAD_DEPENDENCIES date_time)
    set(_Boost_WAVE_DEPENDENCIES filesystem system thread date_time)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 104400 AND Boost_VERSION VERSION_LESS 104500)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l random serialization)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_THREAD_DEPENDENCIES date_time)
    set(_Boost_WAVE_DEPENDENCIES serialization filesystem system thread date_time)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 104500 AND Boost_VERSION VERSION_LESS 104700)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l random)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_THREAD_DEPENDENCIES date_time)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread date_time)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 104700 AND Boost_VERSION VERSION_LESS 104800)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l random)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_THREAD_DEPENDENCIES date_time)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread date_time)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 104800 AND Boost_VERSION VERSION_LESS 105000)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l random)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_THREAD_DEPENDENCIES date_time)
    set(_Boost_TIMER_DEPENDENCIES chrono system)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread date_time)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 105000 AND Boost_VERSION VERSION_LESS 105300)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l regex random)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time)
    set(_Boost_TIMER_DEPENDENCIES chrono system)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 105300 AND Boost_VERSION VERSION_LESS 105400)
    set(_Boost_ATOMIC_DEPENDENCIES thread chrono system date_time)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l regex random)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time atomic)
    set(_Boost_TIMER_DEPENDENCIES chrono system)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 105400 AND Boost_VERSION VERSION_LESS 105500)
    set(_Boost_ATOMIC_DEPENDENCIES thread chrono system date_time)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_LOG_DEPENDENCIES log_setup date_time system filesystem thread regex chrono)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l regex random)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time atomic)
    set(_Boost_TIMER_DEPENDENCIES chrono system)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time atomic)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 105500 AND Boost_VERSION VERSION_LESS 105600)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_COROUTINE_DEPENDENCIES context system)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_LOG_DEPENDENCIES log_setup date_time system filesystem thread regex chrono)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l regex random)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time atomic)
    set(_Boost_TIMER_DEPENDENCIES chrono system)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time atomic)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 105600 AND Boost_VERSION VERSION_LESS 105900)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_COROUTINE_DEPENDENCIES context system)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_LOG_DEPENDENCIES log_setup date_time system filesystem thread regex chrono)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l atomic)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_RANDOM_DEPENDENCIES system)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time atomic)
    set(_Boost_TIMER_DEPENDENCIES chrono system)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time atomic)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 105900 AND Boost_VERSION VERSION_LESS 106000)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_COROUTINE_DEPENDENCIES context system)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_LOG_DEPENDENCIES log_setup date_time system filesystem thread regex chrono atomic)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l atomic)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_RANDOM_DEPENDENCIES system)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time atomic)
    set(_Boost_TIMER_DEPENDENCIES chrono system)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time atomic)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 106000 AND Boost_VERSION VERSION_LESS 106100)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_COROUTINE_DEPENDENCIES context system)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_LOG_DEPENDENCIES date_time log_setup system filesystem thread regex chrono atomic)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l atomic)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_RANDOM_DEPENDENCIES system)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time atomic)
    set(_Boost_TIMER_DEPENDENCIES chrono system)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time atomic)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 106100 AND Boost_VERSION VERSION_LESS 106200)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_CONTEXT_DEPENDENCIES thread chrono system date_time)
    set(_Boost_COROUTINE_DEPENDENCIES context system)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_LOG_DEPENDENCIES date_time log_setup system filesystem thread regex chrono atomic)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l atomic)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_RANDOM_DEPENDENCIES system)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time atomic)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time atomic)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 106200 AND Boost_VERSION VERSION_LESS 106300)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_CONTEXT_DEPENDENCIES thread chrono system date_time)
    set(_Boost_COROUTINE_DEPENDENCIES context system)
    set(_Boost_FIBER_DEPENDENCIES context thread chrono system date_time)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_LOG_DEPENDENCIES date_time log_setup system filesystem thread regex chrono atomic)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l atomic)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_RANDOM_DEPENDENCIES system)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time atomic)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time atomic)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 106300 AND Boost_VERSION VERSION_LESS 106500)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_CONTEXT_DEPENDENCIES thread chrono system date_time)
    set(_Boost_COROUTINE_DEPENDENCIES context system)
    set(_Boost_COROUTINE2_DEPENDENCIES context fiber thread chrono system date_time)
    set(_Boost_FIBER_DEPENDENCIES context thread chrono system date_time)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_LOG_DEPENDENCIES date_time log_setup system filesystem thread regex chrono atomic)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l atomic)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_RANDOM_DEPENDENCIES system)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time atomic)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time atomic)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 106500 AND Boost_VERSION VERSION_LESS 106700)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_CONTEXT_DEPENDENCIES thread chrono system date_time)
    set(_Boost_COROUTINE_DEPENDENCIES context system)
    set(_Boost_FIBER_DEPENDENCIES context thread chrono system date_time)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_LOG_DEPENDENCIES date_time log_setup system filesystem thread regex chrono atomic)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l atomic)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_NUMPY_DEPENDENCIES python${component_python_version})
    set(_Boost_RANDOM_DEPENDENCIES system)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time atomic)
    set(_Boost_TIMER_DEPENDENCIES chrono system)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time atomic)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 106700 AND Boost_VERSION VERSION_LESS 106800)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_CONTEXT_DEPENDENCIES thread chrono system date_time)
    set(_Boost_COROUTINE_DEPENDENCIES context system)
    set(_Boost_FIBER_DEPENDENCIES context thread chrono system date_time)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_LOG_DEPENDENCIES date_time log_setup system filesystem thread regex chrono atomic)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l atomic)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_NUMPY_DEPENDENCIES python${component_python_version})
    set(_Boost_RANDOM_DEPENDENCIES system)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time atomic)
    set(_Boost_TIMER_DEPENDENCIES chrono system)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time atomic)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  elseif(NOT Boost_VERSION VERSION_LESS 106800 AND Boost_VERSION VERSION_LESS 106900)
    set(_Boost_CHRONO_DEPENDENCIES system)
    set(_Boost_CONTEXT_DEPENDENCIES thread chrono system date_time)
    set(_Boost_CONTRACT_DEPENDENCIES thread chrono system date_time)
    set(_Boost_COROUTINE_DEPENDENCIES context system)
    set(_Boost_FIBER_DEPENDENCIES context thread chrono system date_time)
    set(_Boost_FILESYSTEM_DEPENDENCIES system)
    set(_Boost_IOSTREAMS_DEPENDENCIES regex)
    set(_Boost_LOG_DEPENDENCIES date_time log_setup system filesystem thread regex chrono atomic)
    set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l atomic)
    set(_Boost_MPI_DEPENDENCIES serialization)
    set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
    set(_Boost_NUMPY_DEPENDENCIES python${component_python_version})
    set(_Boost_RANDOM_DEPENDENCIES system)
    set(_Boost_THREAD_DEPENDENCIES chrono system date_time atomic)
    set(_Boost_TIMER_DEPENDENCIES chrono system)
    set(_Boost_WAVE_DEPENDENCIES filesystem system serialization thread chrono date_time atomic)
    set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
  else()
    if(NOT Boost_VERSION VERSION_LESS 106900)
      set(_Boost_CONTRACT_DEPENDENCIES thread chrono date_time)
      set(_Boost_COROUTINE_DEPENDENCIES context)
      set(_Boost_FIBER_DEPENDENCIES context)
      set(_Boost_IOSTREAMS_DEPENDENCIES regex)
      set(_Boost_LOG_DEPENDENCIES date_time log_setup filesystem thread regex chrono atomic)
      set(_Boost_MATH_DEPENDENCIES math_c99 math_c99f math_c99l math_tr1 math_tr1f math_tr1l atomic)
      set(_Boost_MPI_DEPENDENCIES serialization)
      set(_Boost_MPI_PYTHON_DEPENDENCIES python${component_python_version} mpi serialization)
      set(_Boost_NUMPY_DEPENDENCIES python${component_python_version})
      set(_Boost_THREAD_DEPENDENCIES chrono date_time atomic)
      set(_Boost_TIMER_DEPENDENCIES chrono system)
      set(_Boost_WAVE_DEPENDENCIES filesystem serialization thread chrono date_time atomic)
      set(_Boost_WSERIALIZATION_DEPENDENCIES serialization)
    endif()
    if(NOT Boost_VERSION VERSION_LESS 107000)
      message(WARNING "New Boost version may have incorrect or missing dependencies and imported targets")
    endif()
  endif()

  string(TOUPPER ${component} uppercomponent)
  set(${_ret} ${_Boost_${uppercomponent}_DEPENDENCIES} PARENT_SCOPE)
  set(_Boost_IMPORTED_TARGETS ${_Boost_IMPORTED_TARGETS} PARENT_SCOPE)

  string(REGEX REPLACE ";" " " _boost_DEPS_STRING "${_Boost_${uppercomponent}_DEPENDENCIES}")
  if (NOT _boost_DEPS_STRING)
    set(_boost_DEPS_STRING "(none)")
  endif()
  # message(STATUS "Dependencies for Boost::${component}: ${_boost_DEPS_STRING}")
endfunction()

#
# Get component headers.  This is the primary header (or headers) for
# a given component, and is used to check that the headers are present
# as well as the library itself as an extra sanity check of the build
# environment.
#
# component - the component to check
# _hdrs
#
function(_Boost_COMPONENT_HEADERS component _hdrs)
  # Handle Python version suffixes
  if(component MATCHES "^(python|mpi_python|numpy)([0-9][0-9]?|[0-9]\\.[0-9])\$")
    set(component "${CMAKE_MATCH_1}")
    set(component_python_version "${CMAKE_MATCH_2}")
  endif()

  # Note: new boost components will require adding here.  The header
  # must be present in all versions of Boost providing a library.
  set(_Boost_ATOMIC_HEADERS              "boost/atomic.hpp")
  set(_Boost_CHRONO_HEADERS              "boost/chrono.hpp")
  set(_Boost_CONTAINER_HEADERS           "boost/container/container_fwd.hpp")
  set(_Boost_CONTRACT_HEADERS            "boost/contract.hpp")
  if(Boost_VERSION VERSION_LESS 106100)
    set(_Boost_CONTEXT_HEADERS           "boost/context/all.hpp")
  else()
    set(_Boost_CONTEXT_HEADERS           "boost/context/detail/fcontext.hpp")
  endif()
  set(_Boost_COROUTINE_HEADERS           "boost/coroutine/all.hpp")
  set(_Boost_DATE_TIME_HEADERS           "boost/date_time/date.hpp")
  set(_Boost_EXCEPTION_HEADERS           "boost/exception/exception.hpp")
  set(_Boost_FIBER_HEADERS               "boost/fiber/all.hpp")
  set(_Boost_FILESYSTEM_HEADERS          "boost/filesystem/path.hpp")
  set(_Boost_GRAPH_HEADERS               "boost/graph/adjacency_list.hpp")
  set(_Boost_GRAPH_PARALLEL_HEADERS      "boost/graph/adjacency_list.hpp")
  set(_Boost_IOSTREAMS_HEADERS           "boost/iostreams/stream.hpp")
  set(_Boost_LOCALE_HEADERS              "boost/locale.hpp")
  set(_Boost_LOG_HEADERS                 "boost/log/core.hpp")
  set(_Boost_LOG_SETUP_HEADERS           "boost/log/detail/setup_config.hpp")
  set(_Boost_MATH_HEADERS                "boost/math_fwd.hpp")
  set(_Boost_MATH_C99_HEADERS            "boost/math/tr1.hpp")
  set(_Boost_MATH_C99F_HEADERS           "boost/math/tr1.hpp")
  set(_Boost_MATH_C99L_HEADERS           "boost/math/tr1.hpp")
  set(_Boost_MATH_TR1_HEADERS            "boost/math/tr1.hpp")
  set(_Boost_MATH_TR1F_HEADERS           "boost/math/tr1.hpp")
  set(_Boost_MATH_TR1L_HEADERS           "boost/math/tr1.hpp")
  set(_Boost_MPI_HEADERS                 "boost/mpi.hpp")
  set(_Boost_MPI_PYTHON_HEADERS          "boost/mpi/python/config.hpp")
  set(_Boost_NUMPY_HEADERS               "boost/python/numpy.hpp")
  set(_Boost_PRG_EXEC_MONITOR_HEADERS    "boost/test/prg_exec_monitor.hpp")
  set(_Boost_PROGRAM_OPTIONS_HEADERS     "boost/program_options.hpp")
  set(_Boost_PYTHON_HEADERS              "boost/python.hpp")
  set(_Boost_RANDOM_HEADERS              "boost/random.hpp")
  set(_Boost_REGEX_HEADERS               "boost/regex.hpp")
  set(_Boost_SERIALIZATION_HEADERS       "boost/serialization/serialization.hpp")
  set(_Boost_SIGNALS_HEADERS             "boost/signals.hpp")
  set(_Boost_STACKTRACE_ADDR2LINE_HEADERS "boost/stacktrace.hpp")
  set(_Boost_STACKTRACE_BACKTRACE_HEADERS "boost/stacktrace.hpp")
  set(_Boost_STACKTRACE_BASIC_HEADERS    "boost/stacktrace.hpp")
  set(_Boost_STACKTRACE_NOOP_HEADERS     "boost/stacktrace.hpp")
  set(_Boost_STACKTRACE_WINDBG_CACHED_HEADERS "boost/stacktrace.hpp")
  set(_Boost_STACKTRACE_WINDBG_HEADERS   "boost/stacktrace.hpp")
  set(_Boost_SYSTEM_HEADERS              "boost/system/config.hpp")
  set(_Boost_TEST_EXEC_MONITOR_HEADERS   "boost/test/test_exec_monitor.hpp")
  set(_Boost_THREAD_HEADERS              "boost/thread.hpp")
  set(_Boost_TIMER_HEADERS               "boost/timer.hpp")
  set(_Boost_TYPE_ERASURE_HEADERS        "boost/type_erasure/config.hpp")
  set(_Boost_UNIT_TEST_FRAMEWORK_HEADERS "boost/test/framework.hpp")
  set(_Boost_WAVE_HEADERS                "boost/wave.hpp")
  set(_Boost_WSERIALIZATION_HEADERS      "boost/archive/text_wiarchive.hpp")
  if(WIN32)
    set(_Boost_BZIP2_HEADERS             "boost/iostreams/filter/bzip2.hpp")
    set(_Boost_ZLIB_HEADERS              "boost/iostreams/filter/zlib.hpp")
  endif()

  string(TOUPPER ${component} uppercomponent)
  set(${_hdrs} ${_Boost_${uppercomponent}_HEADERS} PARENT_SCOPE)

  string(REGEX REPLACE ";" " " _boost_HDRS_STRING "${_Boost_${uppercomponent}_HEADERS}")
  if (NOT _boost_HDRS_STRING)
    set(_boost_HDRS_STRING "(none)")
  endif()
  # message(STATUS "Headers for Boost::${component}: ${_boost_HDRS_STRING}")
endfunction()

#
# Determine if any missing dependencies require adding to the component list.
#
# Sets _Boost_${COMPONENT}_DEPENDENCIES for each required component,
# plus _Boost_IMPORTED_TARGETS (TRUE if imported targets should be
# defined; FALSE if dependency information is unavailable).
#
# componentvar - the component list variable name
# extravar - the indirect dependency list variable name
#
#
function(_Boost_MISSING_DEPENDENCIES componentvar extravar)
  # _boost_unprocessed_components - list of components requiring processing
  # _boost_processed_components - components already processed (or currently being processed)
  # _boost_new_components - new components discovered for future processing
  #
  list(APPEND _boost_unprocessed_components ${${componentvar}})

  while(_boost_unprocessed_components)
    list(APPEND _boost_processed_components ${_boost_unprocessed_components})
    foreach(component ${_boost_unprocessed_components})
      string(TOUPPER ${component} uppercomponent)
      set(${_ret} ${_Boost_${uppercomponent}_DEPENDENCIES} PARENT_SCOPE)
      _Boost_COMPONENT_DEPENDENCIES("${component}" _Boost_${uppercomponent}_DEPENDENCIES)
      set(_Boost_${uppercomponent}_DEPENDENCIES ${_Boost_${uppercomponent}_DEPENDENCIES} PARENT_SCOPE)
      set(_Boost_IMPORTED_TARGETS ${_Boost_IMPORTED_TARGETS} PARENT_SCOPE)
      foreach(componentdep ${_Boost_${uppercomponent}_DEPENDENCIES})
        if (NOT ("${componentdep}" IN_LIST _boost_processed_components OR "${componentdep}" IN_LIST _boost_new_components))
          list(APPEND _boost_new_components ${componentdep})
        endif()
      endforeach()
    endforeach()
    set(_boost_unprocessed_components ${_boost_new_components})
    unset(_boost_new_components)
  endwhile()
  set(_boost_extra_components ${_boost_processed_components})
  if(_boost_extra_components AND ${componentvar})
    list(REMOVE_ITEM _boost_extra_components ${${componentvar}})
  endif()
  set(${componentvar} ${_boost_processed_components} PARENT_SCOPE)
  set(${extravar} ${_boost_extra_components} PARENT_SCOPE)
endfunction()

#
# Some boost libraries may require particular set of compler features.
# The very first one was `boost::fiber` introduced in Boost 1.62.
# One can check required compiler features of it in
# `${Boost_ROOT}/libs/fiber/build/Jamfile.v2`.
#
function(_Boost_COMPILER_FEATURES component _ret)
  # Boost >= 1.62 and < 1.67
  if(NOT Boost_VERSION VERSION_LESS 106200 AND Boost_VERSION VERSION_LESS 106700)
    set(_Boost_FIBER_COMPILER_FEATURES
        cxx_alias_templates
        cxx_auto_type
        cxx_constexpr
        cxx_defaulted_functions
        cxx_final
        cxx_lambdas
        cxx_noexcept
        cxx_nullptr
        cxx_rvalue_references
        cxx_thread_local
        cxx_variadic_templates
    )
  endif()
  string(TOUPPER ${component} uppercomponent)
  set(${_ret} ${_Boost_${uppercomponent}_COMPILER_FEATURES} PARENT_SCOPE)
endfunction()

#
# Update library search directory hint variable with paths used by prebuilt boost binaries.
#
# Prebuilt windows binaries (https://sourceforge.net/projects/boost/files/boost-binaries/)
# have library directories named using MSVC compiler version and architecture.
# This function would append corresponding directories if MSVC is a current compiler,
# so having `BOOST_ROOT` would be enough to specify to find everything.
#
function(_Boost_UPDATE_WINDOWS_LIBRARY_SEARCH_DIRS_WITH_PREBUILT_PATHS componentlibvar basedir)
  if("x${CMAKE_CXX_COMPILER_ID}" STREQUAL "xMSVC")
    if(CMAKE_SIZEOF_VOID_P EQUAL 8)
      set(_arch_suffix 64)
    else()
      set(_arch_suffix 32)
    endif()
    if(MSVC_TOOLSET_VERSION GREATER_EQUAL 141)
      list(APPEND ${componentlibvar} ${basedir}/lib${_arch_suffix}-msvc-14.1)
      list(APPEND ${componentlibvar} ${basedir}/lib${_arch_suffix}-msvc-14.0)
    elseif(MSVC_TOOLSET_VERSION GREATER_EQUAL 80)
      math(EXPR _toolset_major_version "${MSVC_TOOLSET_VERSION} / 10")
      list(APPEND ${componentlibvar} ${basedir}/lib${_arch_suffix}-msvc-${_toolset_major_version}.0)
    endif()
    set(${componentlibvar} ${${componentlibvar}} PARENT_SCOPE)
  endif()
endfunction()

#
# End functions/macros
#
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# main.
#-------------------------------------------------------------------------------


# If the user sets Boost_LIBRARY_DIR, use it as the default for both
# configurations.
if(NOT Boost_LIBRARY_DIR_RELEASE AND Boost_LIBRARY_DIR)
  set(Boost_LIBRARY_DIR_RELEASE "${Boost_LIBRARY_DIR}")
endif()
if(NOT Boost_LIBRARY_DIR_DEBUG AND Boost_LIBRARY_DIR)
  set(Boost_LIBRARY_DIR_DEBUG   "${Boost_LIBRARY_DIR}")
endif()

if(NOT DEFINED Boost_USE_DEBUG_LIBS)
  set(Boost_USE_DEBUG_LIBS TRUE)
endif()
if(NOT DEFINED Boost_USE_RELEASE_LIBS)
  set(Boost_USE_RELEASE_LIBS TRUE)
endif()
if(NOT DEFINED Boost_USE_MULTITHREADED)
  set(Boost_USE_MULTITHREADED TRUE)
endif()
if(NOT DEFINED Boost_USE_DEBUG_RUNTIME)
  set(Boost_USE_DEBUG_RUNTIME TRUE)
endif()

# Check the version of Boost against the requested version.
if(Boost_FIND_VERSION AND NOT Boost_FIND_VERSION_MINOR)
  message(SEND_ERROR "When requesting a specific version of Boost, you must provide at least the major and minor version numbers, e.g., 1.34")
endif()

if(Boost_FIND_VERSION_EXACT)
  # The version may appear in a directory with or without the patch
  # level, even when the patch level is non-zero.
  set(_boost_TEST_VERSIONS
    "${Boost_FIND_VERSION_MAJOR}.${Boost_FIND_VERSION_MINOR}.${Boost_FIND_VERSION_PATCH}"
    "${Boost_FIND_VERSION_MAJOR}.${Boost_FIND_VERSION_MINOR}")
else()
  # The user has not requested an exact version.  Among known
  # versions, find those that are acceptable to the user request.
  #
  # Note: When adding a new Boost release, also update the dependency
  # information in _Boost_COMPONENT_DEPENDENCIES and
  # _Boost_COMPONENT_HEADERS.  See the instructions at the top of
  # _Boost_COMPONENT_DEPENDENCIES.
  set(_Boost_KNOWN_VERSIONS ${Boost_ADDITIONAL_VERSIONS}
    "1.69.0" "1.69"
    "1.68.0" "1.68" "1.67.0" "1.67" "1.66.0" "1.66" "1.65.1" "1.65.0" "1.65"
    "1.64.0" "1.64" "1.63.0" "1.63" "1.62.0" "1.62" "1.61.0" "1.61" "1.60.0" "1.60"
    "1.59.0" "1.59" "1.58.0" "1.58" "1.57.0" "1.57" "1.56.0" "1.56" "1.55.0" "1.55"
    "1.54.0" "1.54" "1.53.0" "1.53" "1.52.0" "1.52" "1.51.0" "1.51"
    "1.50.0" "1.50" "1.49.0" "1.49" "1.48.0" "1.48" "1.47.0" "1.47" "1.46.1"
    "1.46.0" "1.46" "1.45.0" "1.45" "1.44.0" "1.44" "1.43.0" "1.43" "1.42.0" "1.42"
    "1.41.0" "1.41" "1.40.0" "1.40" "1.39.0" "1.39" "1.38.0" "1.38" "1.37.0" "1.37"
    "1.36.1" "1.36.0" "1.36" "1.35.1" "1.35.0" "1.35" "1.34.1" "1.34.0"
    "1.34" "1.33.1" "1.33.0" "1.33")

  set(_boost_TEST_VERSIONS)
  if(Boost_FIND_VERSION)
    set(_Boost_FIND_VERSION_SHORT "${Boost_FIND_VERSION_MAJOR}.${Boost_FIND_VERSION_MINOR}")
    # Select acceptable versions.
    foreach(version ${_Boost_KNOWN_VERSIONS})
      if(NOT "${version}" VERSION_LESS "${Boost_FIND_VERSION}")
        # This version is high enough.
        list(APPEND _boost_TEST_VERSIONS "${version}")
      elseif("${version}.99" VERSION_EQUAL "${_Boost_FIND_VERSION_SHORT}.99")
        # This version is a short-form for the requested version with
        # the patch level dropped.
        list(APPEND _boost_TEST_VERSIONS "${version}")
      endif()
    endforeach()
  else()
    # Any version is acceptable.
    set(_boost_TEST_VERSIONS "${_Boost_KNOWN_VERSIONS}")
  endif()
endif()

# The reason that we failed to find Boost. This will be set to a
# user-friendly message when we fail to find some necessary piece of
# Boost.
set(Boost_ERROR_REASON)

if(Boost_DEBUG)
  # Output some of their choices
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "_boost_TEST_VERSIONS = ${_boost_TEST_VERSIONS}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "Boost_USE_MULTITHREADED = ${Boost_USE_MULTITHREADED}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "Boost_USE_STATIC_LIBS = ${Boost_USE_STATIC_LIBS}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "Boost_USE_STATIC_RUNTIME = ${Boost_USE_STATIC_RUNTIME}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "Boost_ADDITIONAL_VERSIONS = ${Boost_ADDITIONAL_VERSIONS}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "Boost_NO_SYSTEM_PATHS = ${Boost_NO_SYSTEM_PATHS}")
endif()

# Supply Boost_LIB_DIAGNOSTIC_DEFINITIONS as a convenience target. It
# will only contain any interface definitions on WIN32, but is created
# on all platforms to keep end user code free from platform dependent
# code.  Also provide convenience targets to disable autolinking and
# enable dynamic linking.
if(NOT TARGET Boost::diagnostic_definitions)
  add_library(Boost::diagnostic_definitions INTERFACE IMPORTED)
  add_library(Boost::disable_autolinking INTERFACE IMPORTED)
  add_library(Boost::dynamic_linking INTERFACE IMPORTED)
endif()
if(WIN32)
  # In windows, automatic linking is performed, so you do not have
  # to specify the libraries.  If you are linking to a dynamic
  # runtime, then you can choose to link to either a static or a
  # dynamic Boost library, the default is to do a static link.  You
  # can alter this for a specific library "whatever" by defining
  # BOOST_WHATEVER_DYN_LINK to force Boost library "whatever" to be
  # linked dynamically.  Alternatively you can force all Boost
  # libraries to dynamic link by defining BOOST_ALL_DYN_LINK.

  # This feature can be disabled for Boost library "whatever" by
  # defining BOOST_WHATEVER_NO_LIB, or for all of Boost by defining
  # BOOST_ALL_NO_LIB.

  # If you want to observe which libraries are being linked against
  # then defining BOOST_LIB_DIAGNOSTIC will cause the auto-linking
  # code to emit a #pragma message each time a library is selected
  # for linking.
  set(Boost_LIB_DIAGNOSTIC_DEFINITIONS "-DBOOST_LIB_DIAGNOSTIC")
  set_target_properties(Boost::diagnostic_definitions PROPERTIES
    INTERFACE_COMPILE_DEFINITIONS "BOOST_LIB_DIAGNOSTIC")
  set_target_properties(Boost::disable_autolinking PROPERTIES
    INTERFACE_COMPILE_DEFINITIONS "BOOST_ALL_NO_LIB")
  set_target_properties(Boost::dynamic_linking PROPERTIES
    INTERFACE_COMPILE_DEFINITIONS "BOOST_ALL_DYN_LINK")
endif()

_Boost_CHECK_SPELLING(Boost_ROOT)
_Boost_CHECK_SPELLING(Boost_LIBRARYDIR)
_Boost_CHECK_SPELLING(Boost_INCLUDEDIR)

# Collect environment variable inputs as hints.  Do not consider changes.
foreach(v BOOSTROOT BOOST_ROOT BOOST_INCLUDEDIR BOOST_LIBRARYDIR)
  set(_env $ENV{${v}})
  if(_env)
    file(TO_CMAKE_PATH "${_env}" _ENV_${v})
  else()
    set(_ENV_${v} "")
  endif()
endforeach()
if(NOT _ENV_BOOST_ROOT AND _ENV_BOOSTROOT)
  set(_ENV_BOOST_ROOT "${_ENV_BOOSTROOT}")
endif()

# Collect inputs and cached results.  Detect changes since the last run.
if(NOT BOOST_ROOT AND BOOSTROOT)
  set(BOOST_ROOT "${BOOSTROOT}")
endif()
set(_Boost_VARS_DIR
  BOOST_ROOT
  Boost_NO_SYSTEM_PATHS
  )

if(Boost_DEBUG)
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "Declared as CMake or Environmental Variables:")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "  BOOST_ROOT = ${BOOST_ROOT}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "  BOOST_INCLUDEDIR = ${BOOST_INCLUDEDIR}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "  BOOST_LIBRARYDIR = ${BOOST_LIBRARYDIR}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "_boost_TEST_VERSIONS = ${_boost_TEST_VERSIONS}")
endif()

# ------------------------------------------------------------------------
#  Search for Boost include DIR
# ------------------------------------------------------------------------

set(_Boost_VARS_INC BOOST_INCLUDEDIR Boost_INCLUDE_DIR Boost_ADDITIONAL_VERSIONS)
_Boost_CHANGE_DETECT(_Boost_CHANGE_INCDIR ${_Boost_VARS_DIR} ${_Boost_VARS_INC})
# Clear Boost_INCLUDE_DIR if it did not change but other input affecting the
# location did.  We will find a new one based on the new inputs.
if(_Boost_CHANGE_INCDIR AND NOT _Boost_INCLUDE_DIR_CHANGED)
  unset(Boost_INCLUDE_DIR CACHE)
endif()

if(NOT Boost_INCLUDE_DIR)
  set(_boost_INCLUDE_SEARCH_DIRS "")
  if(BOOST_INCLUDEDIR)
    list(APPEND _boost_INCLUDE_SEARCH_DIRS ${BOOST_INCLUDEDIR})
  elseif(_ENV_BOOST_INCLUDEDIR)
    list(APPEND _boost_INCLUDE_SEARCH_DIRS ${_ENV_BOOST_INCLUDEDIR})
  endif()

  if( BOOST_ROOT )
    list(APPEND _boost_INCLUDE_SEARCH_DIRS ${BOOST_ROOT}/include ${BOOST_ROOT})
  elseif( _ENV_BOOST_ROOT )
    list(APPEND _boost_INCLUDE_SEARCH_DIRS ${_ENV_BOOST_ROOT}/include ${_ENV_BOOST_ROOT})
  endif()

  if( Boost_NO_SYSTEM_PATHS)
    list(APPEND _boost_INCLUDE_SEARCH_DIRS NO_CMAKE_SYSTEM_PATH NO_SYSTEM_ENVIRONMENT_PATH)
  else()
    if("x${CMAKE_CXX_COMPILER_ID}" STREQUAL "xMSVC")
      foreach(ver ${_Boost_KNOWN_VERSIONS})
        string(REPLACE "." "_" ver "${ver}")
        list(APPEND _boost_INCLUDE_SEARCH_DIRS PATHS "C:/local/boost_${ver}")
      endforeach()
    endif()
    list(APPEND _boost_INCLUDE_SEARCH_DIRS PATHS
      C:/boost/include
      C:/boost
      /sw/local/include
      )
  endif()

  # Try to find Boost by stepping backwards through the Boost versions
  # we know about.
  # Build a list of path suffixes for each version.
  set(_boost_PATH_SUFFIXES)
  foreach(_boost_VER ${_boost_TEST_VERSIONS})
    # Add in a path suffix, based on the required version, ideally
    # we could read this from version.hpp, but for that to work we'd
    # need to know the include dir already
    set(_boost_BOOSTIFIED_VERSION)

    # Transform 1.35 => 1_35 and 1.36.0 => 1_36_0
    if(_boost_VER MATCHES "([0-9]+)\\.([0-9]+)\\.([0-9]+)")
        set(_boost_BOOSTIFIED_VERSION
          "${CMAKE_MATCH_1}_${CMAKE_MATCH_2}_${CMAKE_MATCH_3}")
    elseif(_boost_VER MATCHES "([0-9]+)\\.([0-9]+)")
        set(_boost_BOOSTIFIED_VERSION
          "${CMAKE_MATCH_1}_${CMAKE_MATCH_2}")
    endif()

    list(APPEND _boost_PATH_SUFFIXES
      "boost-${_boost_BOOSTIFIED_VERSION}"
      "boost_${_boost_BOOSTIFIED_VERSION}"
      "boost/boost-${_boost_BOOSTIFIED_VERSION}"
      "boost/boost_${_boost_BOOSTIFIED_VERSION}"
      )

  endforeach()

  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "Include debugging info:")
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "  _boost_INCLUDE_SEARCH_DIRS = ${_boost_INCLUDE_SEARCH_DIRS}")
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "  _boost_PATH_SUFFIXES = ${_boost_PATH_SUFFIXES}")
  endif()

  # Look for a standard boost header file.
  find_path(Boost_INCLUDE_DIR
    NAMES         boost/config.hpp
    HINTS         ${_boost_INCLUDE_SEARCH_DIRS}
    PATH_SUFFIXES ${_boost_PATH_SUFFIXES}
    )
endif()

# ------------------------------------------------------------------------
#  Extract version information from version.hpp
# ------------------------------------------------------------------------

# Set Boost_FOUND based only on header location and version.
# It will be updated below for component libraries.
if(Boost_INCLUDE_DIR)
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "location of version.hpp: ${Boost_INCLUDE_DIR}/boost/version.hpp")
  endif()

  # Extract Boost_VERSION and Boost_LIB_VERSION from version.hpp
  set(Boost_VERSION 0)
  set(Boost_LIB_VERSION "")
  file(STRINGS "${Boost_INCLUDE_DIR}/boost/version.hpp" _boost_VERSION_HPP_CONTENTS REGEX "#define BOOST_(LIB_)?VERSION ")
  set(_Boost_VERSION_REGEX "([0-9]+)")
  set(_Boost_LIB_VERSION_REGEX "\"([0-9_]+)\"")
  foreach(v VERSION LIB_VERSION)
    if("${_boost_VERSION_HPP_CONTENTS}" MATCHES "#define BOOST_${v} ${_Boost_${v}_REGEX}")
      set(Boost_${v} "${CMAKE_MATCH_1}")
    endif()
  endforeach()
  unset(_boost_VERSION_HPP_CONTENTS)

  math(EXPR Boost_MAJOR_VERSION "${Boost_VERSION} / 100000")
  math(EXPR Boost_MINOR_VERSION "${Boost_VERSION} / 100 % 1000")
  math(EXPR Boost_SUBMINOR_VERSION "${Boost_VERSION} % 100")

  string(APPEND Boost_ERROR_REASON
    "Boost version: ${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}\nBoost include path: ${Boost_INCLUDE_DIR}")
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "version.hpp reveals boost "
                   "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")
  endif()

  if(Boost_FIND_VERSION)
    # Set Boost_FOUND based on requested version.
    set(_Boost_VERSION "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")
    if("${_Boost_VERSION}" VERSION_LESS "${Boost_FIND_VERSION}")
      set(Boost_FOUND 0)
      set(_Boost_VERSION_AGE "old")
    elseif(Boost_FIND_VERSION_EXACT AND
        NOT "${_Boost_VERSION}" VERSION_EQUAL "${Boost_FIND_VERSION}")
      set(Boost_FOUND 0)
      set(_Boost_VERSION_AGE "new")
    else()
      set(Boost_FOUND 1)
    endif()
    if(NOT Boost_FOUND)
      # State that we found a version of Boost that is too new or too old.
      string(APPEND Boost_ERROR_REASON
        "\nDetected version of Boost is too ${_Boost_VERSION_AGE}. Requested version was ${Boost_FIND_VERSION_MAJOR}.${Boost_FIND_VERSION_MINOR}")
      if (Boost_FIND_VERSION_PATCH)
        string(APPEND Boost_ERROR_REASON
          ".${Boost_FIND_VERSION_PATCH}")
      endif ()
      if (NOT Boost_FIND_VERSION_EXACT)
        string(APPEND Boost_ERROR_REASON " (or newer)")
      endif ()
      string(APPEND Boost_ERROR_REASON ".")
    endif ()
  else()
    # Caller will accept any Boost version.
    set(Boost_FOUND 1)
  endif()
else()
  set(Boost_FOUND 0)
  string(APPEND Boost_ERROR_REASON
    "Unable to find the Boost header files. Please set BOOST_ROOT to the root directory containing Boost or BOOST_INCLUDEDIR to the directory containing Boost's headers.")
endif()

# ------------------------------------------------------------------------
#  Prefix initialization
# ------------------------------------------------------------------------

set(Boost_LIB_PREFIX "")
if ( (GHSMULTI AND Boost_USE_STATIC_LIBS) OR
    (WIN32 AND Boost_USE_STATIC_LIBS AND NOT CYGWIN) )
  set(Boost_LIB_PREFIX "lib")
endif()

if ( NOT Boost_NAMESPACE )
  set(Boost_NAMESPACE "boost")
endif()

# ------------------------------------------------------------------------
#  Suffix initialization and compiler suffix detection.
# ------------------------------------------------------------------------

set(_Boost_VARS_NAME
  Boost_NAMESPACE
  Boost_COMPILER
  Boost_THREADAPI
  Boost_USE_DEBUG_PYTHON
  Boost_USE_MULTITHREADED
  Boost_USE_STATIC_LIBS
  Boost_USE_STATIC_RUNTIME
  Boost_USE_STLPORT
  Boost_USE_STLPORT_DEPRECATED_NATIVE_IOSTREAMS
  )
_Boost_CHANGE_DETECT(_Boost_CHANGE_LIBNAME ${_Boost_VARS_NAME})

# Setting some more suffixes for the library
if (Boost_COMPILER)
  set(_boost_COMPILER ${Boost_COMPILER})
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "using user-specified Boost_COMPILER = ${_boost_COMPILER}")
  endif()
else()
  # Attempt to guess the compiler suffix
  # NOTE: this is not perfect yet, if you experience any issues
  # please report them and use the Boost_COMPILER variable
  # to work around the problems.
  _Boost_GUESS_COMPILER_PREFIX(_boost_COMPILER)
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
      "guessed _boost_COMPILER = ${_boost_COMPILER}")
  endif()
endif()

set (_boost_MULTITHREADED "-mt")
if( NOT Boost_USE_MULTITHREADED )
  set (_boost_MULTITHREADED "")
endif()
if(Boost_DEBUG)
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
    "_boost_MULTITHREADED = ${_boost_MULTITHREADED}")
endif()

#======================
# Systematically build up the Boost ABI tag for the 'tagged' and 'versioned' layouts
# http://boost.org/doc/libs/1_66_0/more/getting_started/windows.html#library-naming
# http://boost.org/doc/libs/1_66_0/boost/config/auto_link.hpp
# http://boost.org/doc/libs/1_66_0/tools/build/src/tools/common.jam
# http://boost.org/doc/libs/1_66_0/boostcpp.jam
set( _boost_RELEASE_ABI_TAG "-")
set( _boost_DEBUG_ABI_TAG   "-")
# Key       Use this library when:
#  s        linking statically to the C++ standard library and
#           compiler runtime support libraries.
if(Boost_USE_STATIC_RUNTIME)
  set( _boost_RELEASE_ABI_TAG "${_boost_RELEASE_ABI_TAG}s")
  set( _boost_DEBUG_ABI_TAG   "${_boost_DEBUG_ABI_TAG}s")
endif()
#  g        using debug versions of the standard and runtime
#           support libraries
if(WIN32 AND Boost_USE_DEBUG_RUNTIME)
  if("x${CMAKE_CXX_COMPILER_ID}" STREQUAL "xMSVC"
          OR "x${CMAKE_CXX_COMPILER_ID}" STREQUAL "xClang"
          OR "x${CMAKE_CXX_COMPILER_ID}" STREQUAL "xIntel")
    string(APPEND _boost_DEBUG_ABI_TAG "g")
  endif()
endif()
#  y        using special debug build of python
if(Boost_USE_DEBUG_PYTHON)
  string(APPEND _boost_DEBUG_ABI_TAG "y")
endif()
#  d        using a debug version of your code
string(APPEND _boost_DEBUG_ABI_TAG "d")
#  p        using the STLport standard library rather than the
#           default one supplied with your compiler
if(Boost_USE_STLPORT)
  string(APPEND _boost_RELEASE_ABI_TAG "p")
  string(APPEND _boost_DEBUG_ABI_TAG "p")
endif()
#  n        using the STLport deprecated "native iostreams" feature
#           removed from the documentation in 1.43.0 but still present in
#           boost/config/auto_link.hpp
if(Boost_USE_STLPORT_DEPRECATED_NATIVE_IOSTREAMS)
  string(APPEND _boost_RELEASE_ABI_TAG "n")
  string(APPEND _boost_DEBUG_ABI_TAG "n")
endif()

#  -x86     Architecture and address model tag
#           First character is the architecture, then word-size, either 32 or 64
#           Only used in 'versioned' layout, added in Boost 1.66.0
if(DEFINED Boost_ARCHITECTURE)
  set(_boost_ARCHITECTURE_TAG "${Boost_ARCHITECTURE}")
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
      "using user-specified Boost_ARCHITECTURE = ${_boost_ARCHITECTURE_TAG}")
  endif()
else()
  set(_boost_ARCHITECTURE_TAG "")
  # {CMAKE_CXX_COMPILER_ARCHITECTURE_ID} is not currently set for all compilers
  if(NOT "x${CMAKE_CXX_COMPILER_ARCHITECTURE_ID}" STREQUAL "x" AND NOT Boost_VERSION VERSION_LESS 106600)
    string(APPEND _boost_ARCHITECTURE_TAG "-")
    # This needs to be kept in-sync with the section of CMakePlatformId.h.in
    # inside 'defined(_WIN32) && defined(_MSC_VER)'
    if(CMAKE_CXX_COMPILER_ARCHITECTURE_ID STREQUAL "IA64")
      string(APPEND _boost_ARCHITECTURE_TAG "i")
    elseif(CMAKE_CXX_COMPILER_ARCHITECTURE_ID STREQUAL "X86"
              OR CMAKE_CXX_COMPILER_ARCHITECTURE_ID STREQUAL "x64")
      string(APPEND _boost_ARCHITECTURE_TAG "x")
    elseif(CMAKE_CXX_COMPILER_ARCHITECTURE_ID MATCHES "^ARM")
      string(APPEND _boost_ARCHITECTURE_TAG "a")
    elseif(CMAKE_CXX_COMPILER_ARCHITECTURE_ID STREQUAL "MIPS")
      string(APPEND _boost_ARCHITECTURE_TAG "m")
    endif()

    if(CMAKE_SIZEOF_VOID_P EQUAL 8)
      string(APPEND _boost_ARCHITECTURE_TAG "64")
    else()
      string(APPEND _boost_ARCHITECTURE_TAG "32")
    endif()
  endif()
endif()

if(Boost_DEBUG)
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
    "_boost_RELEASE_ABI_TAG = ${_boost_RELEASE_ABI_TAG}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
    "_boost_DEBUG_ABI_TAG = ${_boost_DEBUG_ABI_TAG}")
endif()

# ------------------------------------------------------------------------
#  Begin finding boost libraries
# ------------------------------------------------------------------------

set(_Boost_VARS_LIB "")
foreach(c DEBUG RELEASE)
  set(_Boost_VARS_LIB_${c} BOOST_LIBRARYDIR Boost_LIBRARY_DIR_${c})
  list(APPEND _Boost_VARS_LIB ${_Boost_VARS_LIB_${c}})
  _Boost_CHANGE_DETECT(_Boost_CHANGE_LIBDIR_${c} ${_Boost_VARS_DIR} ${_Boost_VARS_LIB_${c}} Boost_INCLUDE_DIR)
  # Clear Boost_LIBRARY_DIR_${c} if it did not change but other input affecting the
  # location did.  We will find a new one based on the new inputs.
  if(_Boost_CHANGE_LIBDIR_${c} AND NOT _Boost_LIBRARY_DIR_${c}_CHANGED)
    unset(Boost_LIBRARY_DIR_${c} CACHE)
  endif()

  # If Boost_LIBRARY_DIR_[RELEASE,DEBUG] is set, prefer its value.
  if(Boost_LIBRARY_DIR_${c})
    set(_boost_LIBRARY_SEARCH_DIRS_${c} ${Boost_LIBRARY_DIR_${c}} NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
  else()
    set(_boost_LIBRARY_SEARCH_DIRS_${c} "")
    if(BOOST_LIBRARYDIR)
      list(APPEND _boost_LIBRARY_SEARCH_DIRS_${c} ${BOOST_LIBRARYDIR})
    elseif(_ENV_BOOST_LIBRARYDIR)
      list(APPEND _boost_LIBRARY_SEARCH_DIRS_${c} ${_ENV_BOOST_LIBRARYDIR})
    endif()

    if(BOOST_ROOT)
      list(APPEND _boost_LIBRARY_SEARCH_DIRS_${c} ${BOOST_ROOT}/lib ${BOOST_ROOT}/stage/lib)
      _Boost_UPDATE_WINDOWS_LIBRARY_SEARCH_DIRS_WITH_PREBUILT_PATHS(_boost_LIBRARY_SEARCH_DIRS_${c} "${BOOST_ROOT}")
    elseif(_ENV_BOOST_ROOT)
      list(APPEND _boost_LIBRARY_SEARCH_DIRS_${c} ${_ENV_BOOST_ROOT}/lib ${_ENV_BOOST_ROOT}/stage/lib)
      _Boost_UPDATE_WINDOWS_LIBRARY_SEARCH_DIRS_WITH_PREBUILT_PATHS(_boost_LIBRARY_SEARCH_DIRS_${c} "${_ENV_BOOST_ROOT}")
    endif()

    list(APPEND _boost_LIBRARY_SEARCH_DIRS_${c}
      ${Boost_INCLUDE_DIR}/lib
      ${Boost_INCLUDE_DIR}/../lib
      ${Boost_INCLUDE_DIR}/stage/lib
      )
    _Boost_UPDATE_WINDOWS_LIBRARY_SEARCH_DIRS_WITH_PREBUILT_PATHS(_boost_LIBRARY_SEARCH_DIRS_${c} "${Boost_INCLUDE_DIR}/..")
    _Boost_UPDATE_WINDOWS_LIBRARY_SEARCH_DIRS_WITH_PREBUILT_PATHS(_boost_LIBRARY_SEARCH_DIRS_${c} "${Boost_INCLUDE_DIR}")
    if( Boost_NO_SYSTEM_PATHS )
      list(APPEND _boost_LIBRARY_SEARCH_DIRS_${c} NO_CMAKE_SYSTEM_PATH NO_SYSTEM_ENVIRONMENT_PATH)
    else()
      foreach(ver ${_Boost_KNOWN_VERSIONS})
        string(REPLACE "." "_" ver "${ver}")
        _Boost_UPDATE_WINDOWS_LIBRARY_SEARCH_DIRS_WITH_PREBUILT_PATHS(_boost_LIBRARY_SEARCH_DIRS_${c} "C:/local/boost_${ver}")
      endforeach()
      _Boost_UPDATE_WINDOWS_LIBRARY_SEARCH_DIRS_WITH_PREBUILT_PATHS(_boost_LIBRARY_SEARCH_DIRS_${c} "C:/boost")
      list(APPEND _boost_LIBRARY_SEARCH_DIRS_${c} PATHS
        C:/boost/lib
        C:/boost
        /sw/local/lib
        )
    endif()
  endif()
endforeach()

if(Boost_DEBUG)
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
    "_boost_LIBRARY_SEARCH_DIRS_RELEASE = ${_boost_LIBRARY_SEARCH_DIRS_RELEASE}"
    "_boost_LIBRARY_SEARCH_DIRS_DEBUG   = ${_boost_LIBRARY_SEARCH_DIRS_DEBUG}")
endif()

# Support preference of static libs by adjusting CMAKE_FIND_LIBRARY_SUFFIXES
if( Boost_USE_STATIC_LIBS )
  set( _boost_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
  if(WIN32)
    list(INSERT CMAKE_FIND_LIBRARY_SUFFIXES 0 .lib .a)
  else()
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
  endif()
endif()

# We want to use the tag inline below without risking double dashes
if(_boost_RELEASE_ABI_TAG)
  if(${_boost_RELEASE_ABI_TAG} STREQUAL "-")
    set(_boost_RELEASE_ABI_TAG "")
  endif()
endif()
if(_boost_DEBUG_ABI_TAG)
  if(${_boost_DEBUG_ABI_TAG} STREQUAL "-")
    set(_boost_DEBUG_ABI_TAG "")
  endif()
endif()

# The previous behavior of FindBoost when Boost_USE_STATIC_LIBS was enabled
# on WIN32 was to:
#  1. Search for static libs compiled against a SHARED C++ standard runtime library (use if found)
#  2. Search for static libs compiled against a STATIC C++ standard runtime library (use if found)
# We maintain this behavior since changing it could break people's builds.
# To disable the ambiguous behavior, the user need only
# set Boost_USE_STATIC_RUNTIME either ON or OFF.
set(_boost_STATIC_RUNTIME_WORKAROUND false)
if(WIN32 AND Boost_USE_STATIC_LIBS)
  if(NOT DEFINED Boost_USE_STATIC_RUNTIME)
    set(_boost_STATIC_RUNTIME_WORKAROUND TRUE)
  endif()
endif()

# On versions < 1.35, remove the System library from the considered list
# since it wasn't added until 1.35.
if(Boost_VERSION AND Boost_FIND_COMPONENTS)
   if(Boost_VERSION LESS 103500)
     list(REMOVE_ITEM Boost_FIND_COMPONENTS system)
   endif()
endif()

# Additional components may be required via component dependencies.
# Add any missing components to the list.
_Boost_MISSING_DEPENDENCIES(Boost_FIND_COMPONENTS _Boost_EXTRA_FIND_COMPONENTS)

# If thread is required, get the thread libs as a dependency
if("thread" IN_LIST Boost_FIND_COMPONENTS)
  if(Boost_FIND_QUIETLY)
    set(_Boost_find_quiet QUIET)
  else()
    set(_Boost_find_quiet "")
  endif()
  find_package(Threads ${_Boost_find_quiet})
  unset(_Boost_find_quiet)
endif()

# If the user changed any of our control inputs flush previous results.
if(_Boost_CHANGE_LIBDIR_DEBUG OR _Boost_CHANGE_LIBDIR_RELEASE OR _Boost_CHANGE_LIBNAME)
  foreach(COMPONENT ${_Boost_COMPONENTS_SEARCHED})
    string(TOUPPER ${COMPONENT} UPPERCOMPONENT)
    foreach(c DEBUG RELEASE)
      set(_var Boost_${UPPERCOMPONENT}_LIBRARY_${c})
      unset(${_var} CACHE)
      set(${_var} "${_var}-NOTFOUND")
    endforeach()
  endforeach()
  set(_Boost_COMPONENTS_SEARCHED "")
endif()

foreach(COMPONENT ${Boost_FIND_COMPONENTS})
  string(TOUPPER ${COMPONENT} UPPERCOMPONENT)

  set( _boost_docstring_release "Boost ${COMPONENT} library (release)")
  set( _boost_docstring_debug   "Boost ${COMPONENT} library (debug)")

  # Compute component-specific hints.
  set(_Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT "")
  if(${COMPONENT} STREQUAL "mpi" OR ${COMPONENT} STREQUAL "mpi_python" OR
     ${COMPONENT} STREQUAL "graph_parallel")
    foreach(lib ${MPI_CXX_LIBRARIES} ${MPI_C_LIBRARIES})
      if(IS_ABSOLUTE "${lib}")
        get_filename_component(libdir "${lib}" PATH)
        string(REPLACE "\\" "/" libdir "${libdir}")
        list(APPEND _Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT ${libdir})
      endif()
    endforeach()
  endif()

  # Handle Python version suffixes
  unset(COMPONENT_PYTHON_VERSION_MAJOR)
  unset(COMPONENT_PYTHON_VERSION_MINOR)
  if(${COMPONENT} MATCHES "^(python|mpi_python|numpy)([0-9])\$")
    set(COMPONENT_UNVERSIONED "${CMAKE_MATCH_1}")
    set(COMPONENT_PYTHON_VERSION_MAJOR "${CMAKE_MATCH_2}")
  elseif(${COMPONENT} MATCHES "^(python|mpi_python|numpy)([0-9])\\.?([0-9])\$")
    set(COMPONENT_UNVERSIONED "${CMAKE_MATCH_1}")
    set(COMPONENT_PYTHON_VERSION_MAJOR "${CMAKE_MATCH_2}")
    set(COMPONENT_PYTHON_VERSION_MINOR "${CMAKE_MATCH_3}")
  endif()

  unset(_Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT_NAME)
  if (COMPONENT_PYTHON_VERSION_MINOR)
    # Boost >= 1.67
    list(APPEND _Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT_NAME "${COMPONENT_UNVERSIONED}${COMPONENT_PYTHON_VERSION_MAJOR}${COMPONENT_PYTHON_VERSION_MINOR}")
    # Debian/Ubuntu (Some versions omit the 2 and/or 3 from the suffix)
    list(APPEND _Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT_NAME "${COMPONENT_UNVERSIONED}${COMPONENT_PYTHON_VERSION_MAJOR}-py${COMPONENT_PYTHON_VERSION_MAJOR}${COMPONENT_PYTHON_VERSION_MINOR}")
    list(APPEND _Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT_NAME "${COMPONENT_UNVERSIONED}-py${COMPONENT_PYTHON_VERSION_MAJOR}${COMPONENT_PYTHON_VERSION_MINOR}")
    # Gentoo
    list(APPEND _Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT_NAME "${COMPONENT_UNVERSIONED}-${COMPONENT_PYTHON_VERSION_MAJOR}${COMPONENT_PYTHON_VERSION_MINOR}")
    # RPMs
    list(APPEND _Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT_NAME "${COMPONENT_UNVERSIONED}-${COMPONENT_PYTHON_VERSION_MAJOR}${COMPONENT_PYTHON_VERSION_MINOR}")
  endif()
  if (COMPONENT_PYTHON_VERSION_MAJOR AND NOT COMPONENT_PYTHON_VERSION_MINOR)
    # Boost < 1.67
    list(APPEND _Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT_NAME "${COMPONENT_UNVERSIONED}${COMPONENT_PYTHON_VERSION_MAJOR}")
  endif()

  # Consolidate and report component-specific hints.
  if(_Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT_NAME)
    list(REMOVE_DUPLICATES _Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT_NAME)
    if(Boost_DEBUG)
      message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
        "Component-specific library search names for ${COMPONENT_NAME}: "
        "${_Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT_NAME}")
    endif()
  endif()
  if(_Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT)
    list(REMOVE_DUPLICATES _Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT)
    if(Boost_DEBUG)
      message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
        "Component-specific library search paths for ${COMPONENT}: "
        "${_Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT}")
    endif()
  endif()

  #
  # Find headers
  #
  _Boost_COMPONENT_HEADERS("${COMPONENT}" Boost_${UPPERCOMPONENT}_HEADER_NAME)
  # Look for a standard boost header file.
  if(Boost_${UPPERCOMPONENT}_HEADER_NAME)
    if(EXISTS "${Boost_INCLUDE_DIR}/${Boost_${UPPERCOMPONENT}_HEADER_NAME}")
      set(Boost_${UPPERCOMPONENT}_HEADER ON)
    else()
      set(Boost_${UPPERCOMPONENT}_HEADER OFF)
    endif()
  else()
    set(Boost_${UPPERCOMPONENT}_HEADER ON)
    message(WARNING "No header defined for ${COMPONENT}; skipping header check")
  endif()

  #
  # Find RELEASE libraries
  #
  unset(_boost_RELEASE_NAMES)
  foreach(component IN LISTS _Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT_NAME COMPONENT)
    foreach(compiler IN LISTS _boost_COMPILER)
      list(APPEND _boost_RELEASE_NAMES
        ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${compiler}${_boost_MULTITHREADED}${_boost_RELEASE_ABI_TAG}${_boost_ARCHITECTURE_TAG}-${Boost_LIB_VERSION}
        ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${compiler}${_boost_MULTITHREADED}${_boost_RELEASE_ABI_TAG} )
    endforeach()
    list(APPEND _boost_RELEASE_NAMES
      ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${_boost_MULTITHREADED}${_boost_RELEASE_ABI_TAG}${_boost_ARCHITECTURE_TAG}-${Boost_LIB_VERSION}
      ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${_boost_MULTITHREADED}${_boost_RELEASE_ABI_TAG}
      ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${_boost_MULTITHREADED}
      ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component} )
    if(_boost_STATIC_RUNTIME_WORKAROUND)
      set(_boost_RELEASE_STATIC_ABI_TAG "-s${_boost_RELEASE_ABI_TAG}")
      foreach(compiler IN LISTS _boost_COMPILER)
        list(APPEND _boost_RELEASE_NAMES
          ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${compiler}${_boost_MULTITHREADED}${_boost_RELEASE_STATIC_ABI_TAG}${_boost_ARCHITECTURE_TAG}-${Boost_LIB_VERSION}
          ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${compiler}${_boost_MULTITHREADED}${_boost_RELEASE_STATIC_ABI_TAG} )
      endforeach()
      list(APPEND _boost_RELEASE_NAMES
        ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${_boost_MULTITHREADED}${_boost_RELEASE_STATIC_ABI_TAG}${_boost_ARCHITECTURE_TAG}-${Boost_LIB_VERSION}
        ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${_boost_MULTITHREADED}${_boost_RELEASE_STATIC_ABI_TAG} )
    endif()
  endforeach()
  if(Boost_THREADAPI AND ${COMPONENT} STREQUAL "thread")
    _Boost_PREPEND_LIST_WITH_THREADAPI(_boost_RELEASE_NAMES ${_boost_RELEASE_NAMES})
  endif()
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "Searching for ${UPPERCOMPONENT}_LIBRARY_RELEASE: ${_boost_RELEASE_NAMES}")
  endif()

  # if Boost_LIBRARY_DIR_RELEASE is not defined,
  # but Boost_LIBRARY_DIR_DEBUG is, look there first for RELEASE libs
  if(NOT Boost_LIBRARY_DIR_RELEASE AND Boost_LIBRARY_DIR_DEBUG)
    list(INSERT _boost_LIBRARY_SEARCH_DIRS_RELEASE 0 ${Boost_LIBRARY_DIR_DEBUG})
  endif()

  # Avoid passing backslashes to _Boost_FIND_LIBRARY due to macro re-parsing.
  string(REPLACE "\\" "/" _boost_LIBRARY_SEARCH_DIRS_tmp "${_boost_LIBRARY_SEARCH_DIRS_RELEASE}")

  if(Boost_USE_RELEASE_LIBS)
    _Boost_FIND_LIBRARY(Boost_${UPPERCOMPONENT}_LIBRARY_RELEASE RELEASE
      NAMES ${_boost_RELEASE_NAMES}
      HINTS ${_boost_LIBRARY_SEARCH_DIRS_tmp}
      NAMES_PER_DIR
      DOC "${_boost_docstring_release}"
      )
  endif()

  #
  # Find DEBUG libraries
  #
  unset(_boost_DEBUG_NAMES)
  foreach(component IN LISTS _Boost_FIND_LIBRARY_HINTS_FOR_COMPONENT_NAME COMPONENT)
    foreach(compiler IN LISTS _boost_COMPILER)
      list(APPEND _boost_DEBUG_NAMES
        ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${compiler}${_boost_MULTITHREADED}${_boost_DEBUG_ABI_TAG}${_boost_ARCHITECTURE_TAG}-${Boost_LIB_VERSION}
        ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${compiler}${_boost_MULTITHREADED}${_boost_DEBUG_ABI_TAG} )
    endforeach()
    list(APPEND _boost_DEBUG_NAMES
      ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${_boost_MULTITHREADED}${_boost_DEBUG_ABI_TAG}${_boost_ARCHITECTURE_TAG}-${Boost_LIB_VERSION}
      ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${_boost_MULTITHREADED}${_boost_DEBUG_ABI_TAG}
      ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${_boost_MULTITHREADED}
      ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component} )
    if(_boost_STATIC_RUNTIME_WORKAROUND)
      set(_boost_DEBUG_STATIC_ABI_TAG "-s${_boost_DEBUG_ABI_TAG}")
      foreach(compiler IN LISTS _boost_COMPILER)
        list(APPEND _boost_DEBUG_NAMES
          ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${compiler}${_boost_MULTITHREADED}${_boost_DEBUG_STATIC_ABI_TAG}${_boost_ARCHITECTURE_TAG}-${Boost_LIB_VERSION}
          ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${compiler}${_boost_MULTITHREADED}${_boost_DEBUG_STATIC_ABI_TAG} )
      endforeach()
      list(APPEND _boost_DEBUG_NAMES
        ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${_boost_MULTITHREADED}${_boost_DEBUG_STATIC_ABI_TAG}${_boost_ARCHITECTURE_TAG}-${Boost_LIB_VERSION}
        ${Boost_LIB_PREFIX}${Boost_NAMESPACE}_${component}${_boost_MULTITHREADED}${_boost_DEBUG_STATIC_ABI_TAG} )
    endif()
  endforeach()
  if(Boost_THREADAPI AND ${COMPONENT} STREQUAL "thread")
     _Boost_PREPEND_LIST_WITH_THREADAPI(_boost_DEBUG_NAMES ${_boost_DEBUG_NAMES})
  endif()
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "Searching for ${UPPERCOMPONENT}_LIBRARY_DEBUG: ${_boost_DEBUG_NAMES}")
  endif()

  # if Boost_LIBRARY_DIR_DEBUG is not defined,
  # but Boost_LIBRARY_DIR_RELEASE is, look there first for DEBUG libs
  if(NOT Boost_LIBRARY_DIR_DEBUG AND Boost_LIBRARY_DIR_RELEASE)
    list(INSERT _boost_LIBRARY_SEARCH_DIRS_DEBUG 0 ${Boost_LIBRARY_DIR_RELEASE})
  endif()

  # Avoid passing backslashes to _Boost_FIND_LIBRARY due to macro re-parsing.
  string(REPLACE "\\" "/" _boost_LIBRARY_SEARCH_DIRS_tmp "${_boost_LIBRARY_SEARCH_DIRS_DEBUG}")

  if(Boost_USE_DEBUG_LIBS)
    _Boost_FIND_LIBRARY(Boost_${UPPERCOMPONENT}_LIBRARY_DEBUG DEBUG
      NAMES ${_boost_DEBUG_NAMES}
      HINTS ${_boost_LIBRARY_SEARCH_DIRS_tmp}
      NAMES_PER_DIR
      DOC "${_boost_docstring_debug}"
      )
  endif ()

  if(Boost_REALPATH)
    _Boost_SWAP_WITH_REALPATH(Boost_${UPPERCOMPONENT}_LIBRARY_RELEASE "${_boost_docstring_release}")
    _Boost_SWAP_WITH_REALPATH(Boost_${UPPERCOMPONENT}_LIBRARY_DEBUG   "${_boost_docstring_debug}"  )
  endif()

  _Boost_ADJUST_LIB_VARS(${UPPERCOMPONENT})

  # Check if component requires some compiler features
  _Boost_COMPILER_FEATURES(${COMPONENT} _Boost_${UPPERCOMPONENT}_COMPILER_FEATURES)

endforeach()

# Restore the original find library ordering
if( Boost_USE_STATIC_LIBS )
  set(CMAKE_FIND_LIBRARY_SUFFIXES ${_boost_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
endif()

# ------------------------------------------------------------------------
#  End finding boost libraries
# ------------------------------------------------------------------------

set(Boost_INCLUDE_DIRS ${Boost_INCLUDE_DIR})
set(Boost_LIBRARY_DIRS)
if(Boost_LIBRARY_DIR_RELEASE)
  list(APPEND Boost_LIBRARY_DIRS ${Boost_LIBRARY_DIR_RELEASE})
endif()
if(Boost_LIBRARY_DIR_DEBUG)
  list(APPEND Boost_LIBRARY_DIRS ${Boost_LIBRARY_DIR_DEBUG})
endif()
if(Boost_LIBRARY_DIRS)
  list(REMOVE_DUPLICATES Boost_LIBRARY_DIRS)
endif()

# The above setting of Boost_FOUND was based only on the header files.
# Update it for the requested component libraries.
if(Boost_FOUND)
  # The headers were found.  Check for requested component libs.
  set(_boost_CHECKED_COMPONENT FALSE)
  set(_Boost_MISSING_COMPONENTS "")
  foreach(COMPONENT ${Boost_FIND_COMPONENTS})
    string(TOUPPER ${COMPONENT} UPPERCOMPONENT)
    set(_boost_CHECKED_COMPONENT TRUE)
    if(NOT Boost_${UPPERCOMPONENT}_FOUND AND Boost_FIND_REQUIRED_${COMPONENT})
      list(APPEND _Boost_MISSING_COMPONENTS ${COMPONENT})
    endif()
  endforeach()
  if(_Boost_MISSING_COMPONENTS AND _Boost_EXTRA_FIND_COMPONENTS)
    # Optional indirect dependencies are not counted as missing.
    list(REMOVE_ITEM _Boost_MISSING_COMPONENTS ${_Boost_EXTRA_FIND_COMPONENTS})
  endif()

  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] Boost_FOUND = ${Boost_FOUND}")
  endif()

  if (_Boost_MISSING_COMPONENTS)
    set(Boost_FOUND 0)
    # We were unable to find some libraries, so generate a sensible
    # error message that lists the libraries we were unable to find.
    string(APPEND Boost_ERROR_REASON
      "\nCould not find the following")
    if(Boost_USE_STATIC_LIBS)
      string(APPEND Boost_ERROR_REASON " static")
    endif()
    string(APPEND Boost_ERROR_REASON
      " Boost libraries:\n")
    foreach(COMPONENT ${_Boost_MISSING_COMPONENTS})
      string(TOUPPER ${COMPONENT} UPPERCOMPONENT)
      string(APPEND Boost_ERROR_REASON
        "        ${Boost_NAMESPACE}_${COMPONENT}${Boost_ERROR_REASON_${UPPERCOMPONENT}}\n")
    endforeach()

    list(LENGTH Boost_FIND_COMPONENTS Boost_NUM_COMPONENTS_WANTED)
    list(LENGTH _Boost_MISSING_COMPONENTS Boost_NUM_MISSING_COMPONENTS)
    if (${Boost_NUM_COMPONENTS_WANTED} EQUAL ${Boost_NUM_MISSING_COMPONENTS})
      string(APPEND Boost_ERROR_REASON
        "No Boost libraries were found. You may need to set BOOST_LIBRARYDIR to the directory containing Boost libraries or BOOST_ROOT to the location of Boost.")
    else ()
      string(APPEND Boost_ERROR_REASON
        "Some (but not all) of the required Boost libraries were found. You may need to install these additional Boost libraries. Alternatively, set BOOST_LIBRARYDIR to the directory containing Boost libraries or BOOST_ROOT to the location of Boost.")
    endif ()
  endif ()

  if( NOT Boost_LIBRARY_DIRS AND NOT _boost_CHECKED_COMPONENT )
    # Compatibility Code for backwards compatibility with CMake
    # 2.4's FindBoost module.

    # Look for the boost library path.
    # Note that the user may not have installed any libraries
    # so it is quite possible the Boost_LIBRARY_DIRS may not exist.
    set(_boost_LIB_DIR ${Boost_INCLUDE_DIR})

    if("${_boost_LIB_DIR}" MATCHES "boost-[0-9]+")
      get_filename_component(_boost_LIB_DIR ${_boost_LIB_DIR} PATH)
    endif()

    if("${_boost_LIB_DIR}" MATCHES "/include$")
      # Strip off the trailing "/include" in the path.
      get_filename_component(_boost_LIB_DIR ${_boost_LIB_DIR} PATH)
    endif()

    if(EXISTS "${_boost_LIB_DIR}/lib")
      string(APPEND _boost_LIB_DIR /lib)
    elseif(EXISTS "${_boost_LIB_DIR}/stage/lib")
      string(APPEND _boost_LIB_DIR "/stage/lib")
    else()
      set(_boost_LIB_DIR "")
    endif()

    if(_boost_LIB_DIR AND EXISTS "${_boost_LIB_DIR}")
      set(Boost_LIBRARY_DIRS ${_boost_LIB_DIR})
    endif()

  endif()
else()
  # Boost headers were not found so no components were found.
  foreach(COMPONENT ${Boost_FIND_COMPONENTS})
    string(TOUPPER ${COMPONENT} UPPERCOMPONENT)
    set(Boost_${UPPERCOMPONENT}_FOUND 0)
  endforeach()
endif()

# ------------------------------------------------------------------------
#  Add imported targets
# ------------------------------------------------------------------------

if(Boost_FOUND)
  # For header-only libraries
  if(NOT TARGET Boost::boost)
    add_library(Boost::boost INTERFACE IMPORTED)
    if(Boost_INCLUDE_DIRS)
      set_target_properties(Boost::boost PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${Boost_INCLUDE_DIRS}")
    endif()
  endif()

  foreach(COMPONENT ${Boost_FIND_COMPONENTS})
    if(_Boost_IMPORTED_TARGETS AND NOT TARGET Boost::${COMPONENT})
      string(TOUPPER ${COMPONENT} UPPERCOMPONENT)
      if(Boost_${UPPERCOMPONENT}_FOUND)
        if(Boost_USE_STATIC_LIBS)
          add_library(Boost::${COMPONENT} STATIC IMPORTED)
        else()
          # Even if Boost_USE_STATIC_LIBS is OFF, we might have static
          # libraries as a result.
          add_library(Boost::${COMPONENT} UNKNOWN IMPORTED)
        endif()
        if(Boost_INCLUDE_DIRS)
          set_target_properties(Boost::${COMPONENT} PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${Boost_INCLUDE_DIRS}")
        endif()
        if(EXISTS "${Boost_${UPPERCOMPONENT}_LIBRARY}")
          set_target_properties(Boost::${COMPONENT} PROPERTIES
            IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
            IMPORTED_LOCATION "${Boost_${UPPERCOMPONENT}_LIBRARY}")
        endif()
        if(EXISTS "${Boost_${UPPERCOMPONENT}_LIBRARY_RELEASE}")
          set_property(TARGET Boost::${COMPONENT} APPEND PROPERTY
            IMPORTED_CONFIGURATIONS RELEASE)
          set_target_properties(Boost::${COMPONENT} PROPERTIES
            IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
            IMPORTED_LOCATION_RELEASE "${Boost_${UPPERCOMPONENT}_LIBRARY_RELEASE}")
        endif()
        if(EXISTS "${Boost_${UPPERCOMPONENT}_LIBRARY_DEBUG}")
          set_property(TARGET Boost::${COMPONENT} APPEND PROPERTY
            IMPORTED_CONFIGURATIONS DEBUG)
          set_target_properties(Boost::${COMPONENT} PROPERTIES
            IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "CXX"
            IMPORTED_LOCATION_DEBUG "${Boost_${UPPERCOMPONENT}_LIBRARY_DEBUG}")
        endif()
        if(_Boost_${UPPERCOMPONENT}_DEPENDENCIES)
          unset(_Boost_${UPPERCOMPONENT}_TARGET_DEPENDENCIES)
          foreach(dep ${_Boost_${UPPERCOMPONENT}_DEPENDENCIES})
            list(APPEND _Boost_${UPPERCOMPONENT}_TARGET_DEPENDENCIES Boost::${dep})
          endforeach()
          if(COMPONENT STREQUAL "thread")
            list(APPEND _Boost_${UPPERCOMPONENT}_TARGET_DEPENDENCIES Threads::Threads)
          endif()
          set_target_properties(Boost::${COMPONENT} PROPERTIES
            INTERFACE_LINK_LIBRARIES "${_Boost_${UPPERCOMPONENT}_TARGET_DEPENDENCIES}")
        endif()
        if(_Boost_${UPPERCOMPONENT}_COMPILER_FEATURES)
          set_target_properties(Boost::${COMPONENT} PROPERTIES
            INTERFACE_COMPILE_FEATURES "${_Boost_${UPPERCOMPONENT}_COMPILER_FEATURES}")
        endif()
      endif()
    endif()
  endforeach()
endif()

# ------------------------------------------------------------------------
#  Notification to end user about what was found
# ------------------------------------------------------------------------

set(Boost_LIBRARIES "")
if(Boost_FOUND)
  if(NOT Boost_FIND_QUIETLY)
    message(STATUS "Boost version: ${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")
    if(Boost_FIND_COMPONENTS)
      message(STATUS "Found the following Boost libraries:")
    endif()
  endif()
  foreach( COMPONENT  ${Boost_FIND_COMPONENTS} )
    string( TOUPPER ${COMPONENT} UPPERCOMPONENT )
    if( Boost_${UPPERCOMPONENT}_FOUND )
      if(NOT Boost_FIND_QUIETLY)
        message (STATUS "  ${COMPONENT}")
      endif()
      list(APPEND Boost_LIBRARIES ${Boost_${UPPERCOMPONENT}_LIBRARY})
      if(COMPONENT STREQUAL "thread")
        list(APPEND Boost_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
      endif()
    endif()
  endforeach()
else()
  if(Boost_FIND_REQUIRED)
    message(SEND_ERROR "Unable to find the requested Boost libraries.\n${Boost_ERROR_REASON}")
  else()
    if(NOT Boost_FIND_QUIETLY)
      # we opt not to automatically output Boost_ERROR_REASON here as
      # it could be quite lengthy and somewhat imposing in its requests
      # Since Boost is not always a required dependency we'll leave this
      # up to the end-user.
      if(Boost_DEBUG OR Boost_DETAILED_FAILURE_MSG)
        message(STATUS "Could NOT find Boost\n${Boost_ERROR_REASON}")
      else()
        message(STATUS "Could NOT find Boost")
      endif()
    endif()
  endif()
endif()

# Configure display of cache entries in GUI.
foreach(v BOOSTROOT BOOST_ROOT ${_Boost_VARS_INC} ${_Boost_VARS_LIB})
  get_property(_type CACHE ${v} PROPERTY TYPE)
  if(_type)
    set_property(CACHE ${v} PROPERTY ADVANCED 1)
    if("x${_type}" STREQUAL "xUNINITIALIZED")
      if("x${v}" STREQUAL "xBoost_ADDITIONAL_VERSIONS")
        set_property(CACHE ${v} PROPERTY TYPE STRING)
      else()
        set_property(CACHE ${v} PROPERTY TYPE PATH)
      endif()
    endif()
  endif()
endforeach()

# Record last used values of input variables so we can
# detect on the next run if the user changed them.
foreach(v
    ${_Boost_VARS_INC} ${_Boost_VARS_LIB}
    ${_Boost_VARS_DIR} ${_Boost_VARS_NAME}
    )
  if(DEFINED ${v})
    set(_${v}_LAST "${${v}}" CACHE INTERNAL "Last used ${v} value.")
  else()
    unset(_${v}_LAST CACHE)
  endif()
endforeach()

# Maintain a persistent list of components requested anywhere since
# the last flush.
set(_Boost_COMPONENTS_SEARCHED "${_Boost_COMPONENTS_SEARCHED}")
list(APPEND _Boost_COMPONENTS_SEARCHED ${Boost_FIND_COMPONENTS})
list(REMOVE_DUPLICATES _Boost_COMPONENTS_SEARCHED)
list(SORT _Boost_COMPONENTS_SEARCHED)
set(_Boost_COMPONENTS_SEARCHED "${_Boost_COMPONENTS_SEARCHED}"
  CACHE INTERNAL "Components requested for this build tree.")

# Restore project's policies
cmake_policy(POP)
