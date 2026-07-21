# This module builds Boost. It sets the following variables:
#
#  Boost_FOUND : boolean            - system has Boost
#  BOOST_ROOT : path
#  Boost_LIBRARIES : list(filepath) - the libraries needed to use Boost
#  Boost_LIBRARY_DIR_RELEASE : path - the library path
#  Boost_INCLUDE_DIRS : list(path)  - the Boost include directories
#
# Following hints are respected
#
#  Boost_USE_STATIC_LIBS : boolean (default: OFF)
#  Boost_USE_MULTITHREADED : boolean (default: OFF)
#  BOOST_J: integer (defanult 1)
#
# Note: Remove boost_redis submodule once upgraded to Boost version that includes it

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

# Populate a shared, version-partitioned Boost source cache exactly once, so
# multiple out-of-source build directories can reuse it read-only. Runs at
# configure time. The cache holds only pristine source plus the b2 engine; all
# variant-specific build artifacts are redirected into each build's own
# directory by do_build_boost(). Trailing arguments (${ARGN}) are the mirror
# URLs to try, in order.
function(prepare_shared_boost_source cache_dir source_dir version_underscore sha256 toolset)
  set(sentinel "${source_dir}/.ceph-boost-prepared")
  file(MAKE_DIRECTORY "${cache_dir}")
  # Serialize cold-start population across concurrent configures; GUARD FUNCTION
  # releases the (inter-process) lock when this function returns.
  file(LOCK "${cache_dir}/boost_${version_underscore}.lock"
    GUARD FUNCTION TIMEOUT 3600)
  if(EXISTS "${sentinel}")
    return()
  endif()
  message(STATUS "Boost cache: preparing shared source in ${source_dir}")

  set(downloads_dir "${cache_dir}/downloads")
  file(MAKE_DIRECTORY "${downloads_dir}")
  set(tarball "${downloads_dir}/boost_${version_underscore}.tar.bz2")
  if(NOT EXISTS "${tarball}")
    set(_downloaded OFF)
    # file(DOWNLOAD) takes a single URL, so fall through the mirrors manually.
    foreach(url ${ARGN})
      message(STATUS "Boost cache: downloading ${url}")
      file(DOWNLOAD "${url}" "${tarball}"
        EXPECTED_HASH SHA256=${sha256}
        STATUS _dl_status)
      list(GET _dl_status 0 _dl_code)
      if(_dl_code EQUAL 0)
        set(_downloaded ON)
        break()
      endif()
      list(GET _dl_status 1 _dl_msg)
      message(STATUS "Boost cache: mirror failed (${_dl_msg}); trying next")
      file(REMOVE "${tarball}")
    endforeach()
    if(NOT _downloaded)
      message(FATAL_ERROR "Boost cache: could not download Boost ${version_underscore} into ${tarball}")
    endif()
  endif()

  # The tarball's top-level directory is boost_<version_underscore>/, so
  # extracting into cache_dir yields ${source_dir}.
  if(NOT EXISTS "${source_dir}/boost/version.hpp")
    message(STATUS "Boost cache: extracting ${tarball}")
    file(ARCHIVE_EXTRACT INPUT "${tarball}" DESTINATION "${cache_dir}")
  endif()

  # Build the b2 engine once and place it OUTSIDE the source tree so the shared
  # source stays pristine (b2 finds the project via its working directory, not
  # via its own location).
  if(NOT EXISTS "${cache_dir}/b2")
    message(STATUS "Boost cache: building b2 engine")
    execute_process(
      COMMAND ./tools/build/src/engine/build.sh --cxx=${CMAKE_CXX_COMPILER} ${toolset}
      WORKING_DIRECTORY "${source_dir}"
      RESULT_VARIABLE _b2_result)
    if(NOT _b2_result EQUAL 0)
      message(FATAL_ERROR "Boost cache: failed to build b2 engine (exit ${_b2_result})")
    endif()
    file(COPY "${source_dir}/tools/build/src/engine/b2"
      DESTINATION "${cache_dir}"
      FILE_PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE)
  endif()

  # Mark the cache ready only after every step above succeeded.
  file(WRITE "${sentinel}" "boost ${version_underscore}\n")
endfunction()

function(do_build_boost root_dir version)
  cmake_parse_arguments(Boost_BUILD "" "" COMPONENTS ${ARGN})
  if(CMAKE_BUILD_TYPE STREQUAL Debug)
    set(boost_features "variant=debug")
  else()
    set(boost_features "variant=release")
  endif()
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

  set(boost_with_libs)
  foreach(c ${Boost_BUILD_COMPONENTS})
    if(c MATCHES "^python([0-9])\$")
      set(with_python_version "${CMAKE_MATCH_1}")
      list(APPEND boost_with_libs "python")
    elseif(c MATCHES "^python([0-9])\\.?([0-9]+)\$")
      set(with_python_version "${CMAKE_MATCH_1}.${CMAKE_MATCH_2}")
      list(APPEND boost_with_libs "python")
    else()
      list(APPEND boost_with_libs ${c})
    endif()
  endforeach()
  list_replace(boost_with_libs "unit_test_framework" "test")
  # keep the list form (used for per-library --with-<lib> flags) and derive the
  # comma-separated form bootstrap.sh's --with-libraries expects
  string(REPLACE ";" "," boost_with_libs_csv "${boost_with_libs}")

  if(CMAKE_CXX_COMPILER_ID STREQUAL GNU)
    set(toolset gcc)
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL Clang)
    set(toolset clang)
  else()
    message(SEND_ERROR "unknown compiler: ${CMAKE_CXX_COMPILER_ID}")
  endif()

  # Boost tarball coordinates, shared by every source-provisioning path below.
  # NOTE: If you change this version number make sure the package is available
  # at the three URLs below (may involve uploading to download.ceph.com)
  set(boost_version 1.87.0)
  set(boost_sha256 af57be25cb4c4f4b413ed692fe378affb4352ea50fbe294a11ef548f4d527d89)
  string(REPLACE "." "_" boost_version_underscore ${boost_version})
  set(boost_url
    https://download.ceph.com/qa/boost_${boost_version_underscore}.tar.bz2
    https://archives.boost.io//release/${boost_version}/source/boost_${boost_version_underscore}.tar.bz2
    https://boostorg.jfrog.io/artifactory/main/release/${boost_version}/source/boost_${boost_version_underscore}.tar.bz2)

  # Resolve where the Boost source lives and how b2 builds against it.
  #   boost_shared_source ON  -> the source tree is shared and kept pristine, so
  #                              b2 builds out of the source tree (WITH_BOOST_CACHE).
  set(boost_shared_source OFF)
  if(EXISTS "${PROJECT_SOURCE_DIR}/src/boost/bootstrap.sh")
    check_boost_version("${PROJECT_SOURCE_DIR}/src/boost" ${version})
    set(source_dir SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/boost")
  elseif(WITH_BOOST_CACHE)
    if(version VERSION_GREATER 1.87)
      message(FATAL_ERROR "Unknown BOOST_REQUESTED_VERSION: ${version}")
    endif()
    get_filename_component(boost_cache_dir "${WITH_BOOST_CACHE}" ABSOLUTE)
    set(boost_cache_source "${boost_cache_dir}/boost_${boost_version_underscore}")
    prepare_shared_boost_source("${boost_cache_dir}" "${boost_cache_source}"
      "${boost_version_underscore}" "${boost_sha256}" "${toolset}" ${boost_url})
    check_boost_version("${boost_cache_source}" ${version})
    set(source_dir SOURCE_DIR "${boost_cache_source}")
    set(boost_shared_source ON)
  elseif(version VERSION_GREATER 1.87)
    message(FATAL_ERROR "Unknown BOOST_REQUESTED_VERSION: ${version}")
  else()
    message(STATUS "boost will be downloaded...")
    set(source_dir
      URL ${boost_url}
      URL_HASH SHA256=${boost_sha256}
      DOWNLOAD_NO_PROGRESS 1)
  endif()

  # b2 driver: for the shared cache the engine was built during prep and lives
  # outside the pristine source; otherwise it is built in-tree by the build-bjam
  # step below.
  if(boost_shared_source)
    set(bjam "${boost_cache_dir}/b2")
  else()
    set(bjam <SOURCE_DIR>/b2)
  endif()

  set(user_config ${CMAKE_BINARY_DIR}/user-config.jam)
  # edit the user-config.jam so b2 will be able to use the specified
  # toolset and python
  file(WRITE ${user_config}
    "using ${toolset}"
    " : "
    " : ${CMAKE_CXX_COMPILER}"
    " : <compileflags>-fPIC <compileflags>-w <compileflags>-Wno-everything"
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

  set(b2 ${bjam})
  if(BOOST_J)
    message(STATUS "BUILDING Boost Libraries at j ${BOOST_J}")
    list(APPEND b2 -j${BOOST_J})
  endif()
  # suppress all debugging levels for b2
  list(APPEND b2 -d0)
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
  set(b2_targets headers stage)
  set(b2_install_targets install)
  if(WITH_ASAN)
    list(APPEND b2 context-impl=ucontext)
    # build the library with the BOOST_USE_ASAN consumers get from Boost::context,
    # so fiber_activation_record has one layout (else heap-buffer-overflow)
    list(APPEND b2 define=BOOST_USE_ASAN)
    # `context-impl` is declared in libs/context/build/Jamfile.v2; the headers/stage
    # and install targets never load it, so b2 aborts with `unknown feature
    # "<context-impl>"`. Name the context project as a target so its Jamfile loads
    # the feature first.
    list(PREPEND b2_targets libs/context/build)
    list(PREPEND b2_install_targets libs/context/build)
  endif()

  include(ExternalProject)
  if(boost_shared_source)
    # Redirect every variant-specific output out of the shared source tree and
    # into this build's own directory, so the cache stays pristine and safe to
    # share across concurrent builds.
    set(boost_artifacts "${CMAKE_BINARY_DIR}/boost-artifacts")
    set(boost_config_dir "${CMAKE_BINARY_DIR}/boost-config")
    file(MAKE_DIRECTORY "${boost_config_dir}")
    # Run the real bootstrap.sh at configure time rather than reimplementing the
    # platform detection it performs (ICU discovery and whatever future Boost
    # versions add). --with-bjam reuses the engine built during cache prep, so
    # nothing is compiled and the shared source is never written to; bootstrap
    # writes project-config.jam into this build's own boost-config/ (its cwd),
    # capturing ICU, the library selection and the install prefix exactly as a
    # normal build would. Removed first so repeated configures don't make
    # bootstrap accumulate project-config.jam.N backups.
    file(REMOVE "${boost_config_dir}/project-config.jam")
    execute_process(
      COMMAND "${boost_cache_source}/bootstrap.sh"
        --with-bjam=${bjam}
        --with-toolset=${toolset}
        --with-libraries=${boost_with_libs_csv}
        --prefix=${root_dir}
      WORKING_DIRECTORY "${boost_config_dir}"
      RESULT_VARIABLE _bootstrap_result
      OUTPUT_VARIABLE _bootstrap_output
      ERROR_VARIABLE _bootstrap_output)
    if(NOT _bootstrap_result EQUAL 0)
      message(FATAL_ERROR
        "Boost cache: bootstrap.sh failed (exit ${_bootstrap_result}):\n${_bootstrap_output}")
    endif()
    list(APPEND b2
      --project-config=${boost_config_dir}/project-config.jam
      --build-dir=${boost_artifacts}/build
      --stagedir=${boost_artifacts}/stage)
    # `b2 stage` builds the libraries listed in project-config.jam into a stable,
    # flat directory. No `headers` target: the release tarball already ships the
    # monolithic boost/ header tree, so it is a no-op that would only risk
    # touching the source.
    set(build_command ${b2} stage ${boost_features})
    # Deliberately skip `b2 install`: it would re-copy the entire (~180 MB)
    # header tree - already present, pristine, in the shared cache - plus the
    # libraries into this build dir. Instead expose the expected
    # ${CMAKE_BINARY_DIR}/boost/{include,lib} layout (which downstream BOOST_ROOT
    # consumers such as Arrow and OpenTelemetry, and the imported targets set by
    # build_boost(), rely on) as symlinks: headers point back at the cache, libs
    # at this build's staged libs. Nothing is duplicated on disk. (Symlinks are
    # a Unix-only feature, matching the bash-based cache prep above.)
    ExternalProject_Add(Boost
      ${source_dir}
      CONFIGURE_COMMAND ""
      BUILD_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${build_command}
      BUILD_IN_SOURCE 1
      BUILD_BYPRODUCTS ${Boost_LIBRARIES}
      INSTALL_COMMAND ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/include <INSTALL_DIR>/lib
              COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/include
              COMMAND ${CMAKE_COMMAND} -E create_symlink "${boost_cache_source}/boost" <INSTALL_DIR>/include/boost
              COMMAND ${CMAKE_COMMAND} -E create_symlink "${boost_artifacts}/stage/lib" <INSTALL_DIR>/lib
      PREFIX "${root_dir}")
  else()
    set(configure_command
      ./bootstrap.sh --prefix=<INSTALL_DIR>
      --with-libraries=${boost_with_libs_csv}
      --with-toolset=${toolset}
      --with-bjam=${bjam})
    set(build_command
      ${b2} headers stage
      #"--buildid=ceph" # changes lib names--can omit for static
      ${boost_features})
    set(install_command
      ${b2} install)
    # build all components in a single shot
    ExternalProject_Add(Boost
      ${source_dir}
      CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${configure_command}
      BUILD_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${build_command}
      BUILD_IN_SOURCE 1
      BUILD_BYPRODUCTS ${Boost_LIBRARIES}
      INSTALL_COMMAND ${install_command}
      PREFIX "${root_dir}")
    ExternalProject_Add_Step(Boost build-bjam
      COMMAND ./tools/build/src/engine/build.sh --cxx=${CMAKE_CXX_COMPILER} ${toolset}
      COMMAND ${CMAKE_COMMAND} -E copy ./tools/build/src/engine/b2 ${bjam}
      DEPENDEES download
      DEPENDERS configure
      COMMENT "Building B2 engine.."
      WORKING_DIRECTORY <SOURCE_DIR>)
  endif()
endfunction()

set(Boost_context_DEPENDENCIES thread chrono system date_time)
set(Boost_coroutine_DEPENDENCIES context system)
set(Boost_filesystem_DEPENDENCIES system)
set(Boost_iostreams_DEPENDENCIES regex)
set(Boost_thread_DEPENDENCIES chrono system date_time atomic)

# define a macro, so the Boost_* variables are visible by its caller
macro(build_boost version)
  # add the Boost::${component} libraries, do this before adding the "Boost"
  # target, so we can collect "Boost_LIBRARIES" which is then used by
  # ExternalProject_Add(Boost ...)
  set(install_dir "${CMAKE_BINARY_DIR}/boost")
  set(BOOST_ROOT ${install_dir})
  set(Boost_INCLUDE_DIRS ${install_dir}/include)
  set(Boost_INCLUDE_DIR ${install_dir}/include)
  set(Boost_LIBRARY_DIR_RELEASE ${install_dir}/lib)
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
  # Remove the `headers` from the list of components to build as
  # `headers` is an interface only target we add later.
  list(REMOVE_ITEM Boost_BUILD_COMPONENTS headers)
  unset(components)

  foreach(c ${Boost_BUILD_COMPONENTS})
    string(TOUPPER ${c} upper_c)
    if(Boost_USE_STATIC_LIBS)
      add_library(Boost::${c} STATIC IMPORTED)
    else()
      add_library(Boost::${c} SHARED IMPORTED)
    endif()
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
    # ASan's BOOST_USE_ASAN/BOOST_USE_UCONTEXT are defined tree-wide in the
    # top-level CMakeLists.txt, not per-target.
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

  # download, bootstrap and build Boost
  do_build_boost(${install_dir} ${version} ${ARGN})

  # add dependencies from Boost::${component} to Boost
  foreach(c ${Boost_BUILD_COMPONENTS})
    add_dependencies(Boost::${c} Boost)
  endforeach()

  # for header-only libraries
  add_library(Boost::headers INTERFACE IMPORTED)
  set_target_properties(Boost::headers PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${Boost_INCLUDE_DIRS}")
  add_dependencies(Boost::headers Boost)
  find_package_handle_standard_args(Boost DEFAULT_MSG
    Boost_INCLUDE_DIRS Boost_LIBRARIES)
  mark_as_advanced(Boost_LIBRARIES BOOST_INCLUDE_DIRS)

  add_library(Boost::boost INTERFACE IMPORTED)
  set_property(TARGET Boost::boost APPEND PROPERTY INTERFACE_LINK_LIBRARIES
    Boost::headers)
 
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
      add_dependencies(${target} Boost::headers)
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
  # can't add dependencies to aliases
  if (NOT ";${ARGN};" MATCHES ";(ALIAS);")
    maybe_add_boost_dep(${target})
  endif()
endfunction()
