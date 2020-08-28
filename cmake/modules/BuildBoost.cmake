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

function(check_boost_version source_dir expected_version)
  set(version_hpp "${source_dir}/boost/version.hpp")
  if(NOT EXISTS ${version_hpp})
    message(FATAL_ERROR "${version_hpp} not found. Please either \"rm -rf ${source_dir}\" "
      "so I can download Boost v${expected_version} for you, or make sure ${source_dir} "
      "contains a full copy of Boost v${expected_version}.")
  endif()
  file(STRINGS "${version_hpp}" BOOST_VERSION_LINE
    REGEX "^#define[ \t]+BOOST_VERSION[ \t]+[0-9]+$")
  string(REGEX REPLACE "^#define[ \t]+BOOST_VERSION[ \t]+([0-9]+)$"
    "\\1" BOOST_VERSION "${BOOST_VERSION_LINE}")
  math(EXPR BOOST_VERSION_PATCH "${BOOST_VERSION} % 100")
  math(EXPR BOOST_VERSION_MINOR "${BOOST_VERSION} / 100 % 1000")
  math(EXPR BOOST_VERSION_MAJOR "${BOOST_VERSION} / 100000")
  set(version "${BOOST_VERSION_MAJOR}.${BOOST_VERSION_MINOR}.${BOOST_VERSION_PATCH}")
  if(version VERSION_LESS expected_version)
    message(FATAL_ERROR "Boost v${version} in ${source_dir} is not new enough. "
      "Please either \"rm -rf ${source_dir}\" so I can download Boost v${expected_version} "
      "for you, or make sure ${source_dir} contains a copy of Boost v${expected_version}.")
  else()
    message(STATUS "boost (${version} >= ${expected_version}) already in ${source_dir}")
  endif()
endfunction()

macro(list_replace list old new)
  list(FIND ${list} ${old} where)
  if(where GREATER -1)
    list(REMOVE_AT ${list} ${where})
    list(INSERT ${list} ${where} ${new})
  endif()
  unset(where)
endmacro()

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

  set(boost_with_libs)
  foreach(c ${Boost_BUILD_COMPONENTS})
    if(c MATCHES "^python([0-9])\$")
      set(with_python_version "${CMAKE_MATCH_1}")
      list(APPEND boost_with_libs "python")
    elseif(c MATCHES "^python([0-9])\\.?([0-9])\$")
      set(with_python_version "${CMAKE_MATCH_1}.${CMAKE_MATCH_2}")
      list(APPEND boost_with_libs "python")
    else()
      list(APPEND boost_with_libs ${c})
    endif()
  endforeach()
  list_replace(boost_with_libs "unit_test_framework" "test")
  string(REPLACE ";" "," boost_with_libs "${boost_with_libs}")
  # build b2 and prepare the project-config.jam for boost
  set(configure_command
    ./bootstrap.sh --prefix=<INSTALL_DIR>
    --with-libraries=${boost_with_libs})

  set(b2 ./b2)
  if(BOOST_J)
    message(STATUS "BUILDING Boost Libraries at j ${BOOST_J}")
    list(APPEND b2 -j${BOOST_J})
  endif()
  # suppress all debugging levels for b2
  list(APPEND b2 -d0)

  if(CMAKE_CXX_COMPILER_ID STREQUAL GNU)
    set(toolset gcc)
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL Clang)
    set(toolset clang)
  else()
    message(SEND_ERROR "unknown compiler: ${CMAKE_CXX_COMPILER_ID}")
  endif()

  set(user_config ${CMAKE_BINARY_DIR}/user-config.jam)
  # edit the user-config.jam so b2 will be able to use the specified
  # toolset and python
  file(WRITE ${user_config}
    "using ${toolset}"
    " : "
    " : ${CMAKE_CXX_COMPILER}"
    " ;\n")
  if(with_python_version)
    find_package(Python3 ${with_python_version} QUIET REQUIRED
      COMPONENTS Development)
    string(REPLACE ";" " " python3_includes "${Python3_INCLUDE_DIRS}")
    file(APPEND ${user_config}
      "using python"
      " : ${with_python_version}"
      " : ${Python3_EXECUTABLE}"
      " : ${python3_includes}"
      " : ${Python3_LIBRARIES}"
      " ;\n")
  endif()
  list(APPEND b2 --user-config=${user_config})

  list(APPEND b2 toolset=${toolset})
  if(with_python_version)
    list(APPEND b2 python=${with_python_version})
  endif()
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm|ARM")
    list(APPEND b2 abi=aapcs)
    list(APPEND b2 architecture=arm)
    list(APPEND b2 binary-format=elf)
  endif()
  if(WITH_BOOST_VALGRIND)
    list(APPEND b2 valgrind=on)
  endif()
  set(build_command
    ${b2} headers stage
    #"--buildid=ceph" # changes lib names--can omit for static
    ${boost_features})
  set(install_command
    ${b2} install)
  set(boost_root_dir "${CMAKE_BINARY_DIR}/boost")
  if(EXISTS "${PROJECT_SOURCE_DIR}/src/boost/bootstrap.sh")
    check_boost_version("${PROJECT_SOURCE_DIR}/src/boost" ${version})
    set(source_dir
      SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/boost")
  elseif(version VERSION_GREATER 1.73)
    message(FATAL_ERROR "Unknown BOOST_REQUESTED_VERSION: ${version}")
  else()
    message(STATUS "boost will be downloaded...")
    # NOTE: If you change this version number make sure the package is available
    # at the three URLs below (may involve uploading to download.ceph.com)
    set(boost_version 1.73.0)
    set(boost_sha256 4eb3b8d442b426dc35346235c8733b5ae35ba431690e38c6a8263dce9fcbb402)
    string(REPLACE "." "_" boost_version_underscore ${boost_version} )
    set(boost_url 
      https://dl.bintray.com/boostorg/release/${boost_version}/source/boost_${boost_version_underscore}.tar.bz2)
    if(CMAKE_VERSION VERSION_GREATER 3.7)
      set(boost_url
        "${boost_url} http://downloads.sourceforge.net/project/boost/boost/${boost_version}/boost_${boost_version_underscore}.tar.bz2")
      set(boost_url
        "${boost_url} https://download.ceph.com/qa/boost_${boost_version_underscore}.tar.bz2")
    endif()
    set(source_dir
      URL ${boost_url}
      URL_HASH SHA256=${boost_sha256}
      DOWNLOAD_NO_PROGRESS 1)
  endif()
  # build all components in a single shot
  include(ExternalProject)
  ExternalProject_Add(Boost
    ${source_dir}
    INSTALL_DIR "external"
    CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${configure_command}
    BUILD_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${build_command}
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND ${install_command}
    PREFIX "${boost_root_dir}")
endfunction()

set(Boost_context_DEPENDENCIES thread chrono system date_time)
set(Boost_coroutine_DEPENDENCIES context system)
set(Boost_filesystem_DEPENDENCIES system)
set(Boost_iostreams_DEPENDENCIES regex)
set(Boost_thread_DEPENDENCIES chrono system date_time atomic)

macro(build_boost version)
  do_build_boost(${version} ${ARGN})
  ExternalProject_Get_Property(Boost install_dir)
  set(Boost_INCLUDE_DIRS ${install_dir}/include)
  set(Boost_INCLUDE_DIR ${install_dir}/include)
  set(Boost_VERSION ${version})
  # create the directory so cmake won't complain when looking at the imported
  # target
  file(MAKE_DIRECTORY ${Boost_INCLUDE_DIRS})
  cmake_parse_arguments(Boost_BUILD "" "" COMPONENTS ${ARGN})
  foreach(c ${Boost_BUILD_COMPONENTS})
    list(APPEND components ${c})
    if(Boost_${c}_DEPENDENCIES)
      list(APPEND components ${Boost_${c}_DEPENDENCIES})
      list(REMOVE_DUPLICATES components)
    endif()
  endforeach()
  set(Boost_BUILD_COMPONENTS ${components})
  unset(components)

  foreach(c ${Boost_BUILD_COMPONENTS})
    string(TOUPPER ${c} upper_c)
    if(Boost_USE_STATIC_LIBS)
      add_library(Boost::${c} STATIC IMPORTED)
    else()
      add_library(Boost::${c} SHARED IMPORTED)
    endif()
    add_dependencies(Boost::${c} Boost)
    if(c MATCHES "^python")
      set(c "python${Python3_VERSION_MAJOR}${Python3_VERSION_MINOR}")
    endif()
    if(Boost_USE_STATIC_LIBS)
      set(Boost_${upper_c}_LIBRARY
        ${install_dir}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}boost_${c}${CMAKE_STATIC_LIBRARY_SUFFIX})
    else()
      set(Boost_${upper_c}_LIBRARY
        ${install_dir}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}boost_${c}${CMAKE_SHARED_LIBRARY_SUFFIX})
    endif()
    unset(buildid)
    set_target_properties(Boost::${c} PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${Boost_INCLUDE_DIRS}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
      IMPORTED_LOCATION "${Boost_${upper_c}_LIBRARY}")
    if((c MATCHES "coroutine|context") AND (WITH_BOOST_VALGRIND))
      set_target_properties(Boost::${c} PROPERTIES
	INTERFACE_COMPILE_DEFINITIONS "BOOST_USE_VALGRIND")
    endif()
    list(APPEND Boost_LIBRARIES ${Boost_${upper_c}_LIBRARY})
  endforeach()
  foreach(c ${Boost_BUILD_COMPONENTS})
    if(Boost_${c}_DEPENDENCIES)
      foreach(dep ${Boost_${c}_DEPENDENCIES})
        list(APPEND dependencies Boost::${dep})
      endforeach()
      set_target_properties(Boost::${c} PROPERTIES
        INTERFACE_LINK_LIBRARIES "${dependencies}")
      unset(dependencies)
    endif()
    set(Boost_${c}_FOUND "TRUE")
  endforeach()

  # for header-only libraries
  add_library(Boost::boost INTERFACE IMPORTED)
  set_target_properties(Boost::boost PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${Boost_INCLUDE_DIRS}")
  add_dependencies(Boost::boost Boost)
  find_package_handle_standard_args(Boost DEFAULT_MSG
    Boost_INCLUDE_DIRS Boost_LIBRARIES)
  mark_as_advanced(Boost_LIBRARIES BOOST_INCLUDE_DIRS)
endmacro()

function(maybe_add_boost_dep target)
  get_target_property(type ${target} TYPE)
  if(NOT type MATCHES "OBJECT_LIBRARY|STATIC_LIBRARY|SHARED_LIBRARY|EXECUTABLE")
    return()
  endif()
  get_target_property(sources ${target} SOURCES)
  string(GENEX_STRIP "${sources}" sources)
  foreach(src ${sources})
    get_filename_component(ext ${src} EXT)
    # assuming all cxx source files include boost header(s)
    if(ext MATCHES ".cc|.cpp|.cxx")
      add_dependencies(${target} Boost::boost)
      return()
    endif()
  endforeach()
endfunction()

# override add_library() to add Boost headers dependency
function(add_library target)
  _add_library(${target} ${ARGN})
  # can't add dependencies to aliases or imported libraries
  if (NOT ";${ARGN};" MATCHES ";(ALIAS|IMPORTED);")
    maybe_add_boost_dep(${target})
  endif()
endfunction()

function(add_executable target)
  _add_executable(${target} ${ARGN})
  maybe_add_boost_dep(${target})
endfunction()
