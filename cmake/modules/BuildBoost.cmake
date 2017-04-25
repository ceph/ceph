# This module builds Boost
# executables are. It sets the following variables:
#
#  Boost_FOUND : boolean            - system has Boost
#  Boost_LIBRARIES : list(filepath) - the libraries needed to use Boost
#  Boost_INCLUDE_DIRS : list(path)  - the Boost include directories
#
# Following hints are respected
#
#  Boost_USE_STATIC_LIBS : boolean (default: OFF)
#  Boost_USE_MULTITHREADED : boolean (default: OFF)
#  BOOST_J: integer (defanult 1)

function(do_build_boost version)
  cmake_parse_arguments(Boost_BUILD "" "" COMPONENTS ${ARGN})
  set(boost_features "variant=release")
  if(Boost_USE_MULTITHREADED)
    list(APPEND boost_features "threading=multi")
  else()
    list(APPEND boost_features "threading=single")
  endif()
  if(Boost_USE_STATIC_LIBS)
    list(APPEND boost_features "link=static")
  else()
    list(APPEND boost_features "link=shared")
  endif()
  if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    list(APPEND boost_features "address-model=64")
  else()
    list(APPEND boost_features "address-model=32")
  endif()
  set(BOOST_CXXFLAGS "-fPIC -w") # check on arm, etc <---XXX
  list(APPEND boost_features "cxxflags=${BOOST_CXXFLAGS}")

  string(REPLACE ";" "," boost_with_libs "${Boost_BUILD_COMPONENTS}")
  # build b2 and prepare the project-config.jam for boost
  set(configure_command
    ./bootstrap.sh --prefix=<INSTALL_DIR>
    --with-libraries=${boost_with_libs})

  set(b2_cmd ./b2)
  if(BOOST_J)
    list(APPEND(b2_cmd -j${BOOST_J}))
  endif()
  if(CMAKE_VERBOSE_MAKEFILE)
    list(APPEND b2_cmd -d1)
  else()
    list(APPEND b2_cmd -d0)
  endif()

  set(build_command
    ${b2_cmd}
    #"--buildid=ceph" # changes lib names--can omit for static
    ${boost_features})

  set(install_command
    ${b2_cmd}
    headers install)
  set(boost_root_dir "${CMAKE_BINARY_DIR}/boost")
  if(EXISTS "${PROJECT_SOURCE_DIR}/src/boost/libs/config/include/boost/config.hpp")
    message(STATUS "boost already in src")
    set(source_dir
      SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/boost")
  elseif(version VERSION_GREATER 1.63)
    message(FATAL_ERROR "Unknown BOOST_REQUESTED_VERSION: ${version}")
  else()
    message(STATUS "boost will be downloaded from sf.net")
    set(boost_version 1.63.0)
    set(boost_md5 1c837ecd990bb022d07e7aab32b09847)
    string(REPLACE "." "_" boost_version_underscore ${boost_version} )
    set(boost_url http://downloads.sourceforge.net/project/boost/boost/${boost_version}/boost_${boost_version_underscore}.tar.bz2)
    set(source_dir
      URL ${boost_url}
      URL_MD5 ${boost_md5}
      DOWNLOAD_NO_PROGRESS 1)
  endif()
  # build all components in a single shot
  include(ExternalProject)
  ExternalProject_Add(Boost
    ${source_dir}
    CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${configure_command}
    BUILD_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${build_command}
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND ${install_command}
    PREFIX "${boost_root_dir}")
endfunction()

macro(build_boost version)
  do_build_boost(version ${ARGN})
  ExternalProject_Get_Property(Boost install_dir)
  set(Boost_INCLUDE_DIRS ${install_dir}/include)
  # create the directory so cmake won't complain when looking at the imported
  # target
  file(MAKE_DIRECTORY ${Boost_INCLUDE_DIRS})
  cmake_parse_arguments(Boost_BUILD "" "" COMPONENTS ${ARGN})
  foreach(c ${Boost_BUILD_COMPONENTS})
    string(TOUPPER ${c} upper_c)
    if(Boost_USE_STATIC_LIBS)
      add_library(Boost::${c} STATIC IMPORTED)
    else()
      add_library(Boost::${c} SHARED IMPORTED)
    endif()
    add_dependencies(Boost::${c} Boost)
    if(Boost_USE_STATIC_LIBS)
      set(Boost_${upper_c}_LIBRARY
        ${install_dir}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}boost_${c}${CMAKE_STATIC_LIBRARY_SUFFIX})
    else()
      set(Boost_${upper_c}_LIBRARY
        ${install_dir}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}boost_${c}${CMAKE_SHARED_LIBRARY_SUFFIX})
    endif()
    set_target_properties(Boost::${c} PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${Boost_INCLUDE_DIRS}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
      IMPORTED_LOCATION "${Boost_${upper_c}_LIBRARY}")
    list(APPEND Boost_LIBRARIES ${Boost_${upper_c}_LIBRARY})
  endforeach()

  find_package_handle_standard_args(Boost DEFAULT_MSG
    Boost_INCLUDE_DIRS Boost_LIBRARIES)
  mark_as_advanced(Boost_LIBRARIES BOOST_INCLUDE_DIRS)
endmacro()
