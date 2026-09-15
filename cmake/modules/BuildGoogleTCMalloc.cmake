function(build_google_tcmalloc)
  if(NOT LINUX OR CMAKE_CROSSCOMPILING OR
      NOT CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|aarch64|arm64)$")
    message(FATAL_ERROR "GOOGLE_TCMALLOC requires native Linux x86_64 or aarch64")
  endif()
  set(tcmalloc_BINARY_DIR "${CMAKE_BINARY_DIR}/google-tcmalloc")
  file(MAKE_DIRECTORY "${tcmalloc_BINARY_DIR}")
  foreach(file MODULE.bazel BUILD.bazel)
    configure_file("${CMAKE_CURRENT_FUNCTION_LIST_DIR}/GoogleTCMalloc/${file}"
      "${tcmalloc_BINARY_DIR}/${file}" COPYONLY)
  endforeach()
  foreach(file google_tcmalloc.cc google_tcmalloc.h)
    configure_file("${CMAKE_SOURCE_DIR}/src/perfglue/${file}"
      "${tcmalloc_BINARY_DIR}/perfglue/${file}" COPYONLY)
  endforeach()
  set(bazel_version 8.5.1)
  find_program(GOOGLE_TCMALLOC_BAZEL NAMES bazel-8.5.1 bazelisk bazel)
  if(NOT GOOGLE_TCMALLOC_BAZEL)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)$")
      set(bazel_arch x86_64)
      set(bazel_sha256 61d89402f0368e64b6c827be5de79d8e65382e8124c3cbb97325611a1851392e)
    else()
      set(bazel_arch arm64)
      set(bazel_sha256 b7f2a85595e8a87d54843bc656c1e379fd9b8c1b5f783dc41717c1d7eb7cc49f)
    endif()
    set(GOOGLE_TCMALLOC_BAZEL "${tcmalloc_BINARY_DIR}/bazel")
    file(DOWNLOAD
      "https://github.com/bazelbuild/bazel/releases/download/${bazel_version}/bazel-${bazel_version}-linux-${bazel_arch}"
      "${GOOGLE_TCMALLOC_BAZEL}"
      EXPECTED_HASH "SHA256=${bazel_sha256}" TLS_VERIFY ON)
    file(CHMOD "${GOOGLE_TCMALLOC_BAZEL}"
      PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
      GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
  endif()
  set(bazel_command ${CMAKE_COMMAND} -E env
    "USE_BAZEL_VERSION=${bazel_version}"
    "BAZELISK_HOME=${tcmalloc_BINARY_DIR}/bazelisk"
    "${GOOGLE_TCMALLOC_BAZEL}")
  execute_process(COMMAND ${bazel_command} --version
    WORKING_DIRECTORY "${tcmalloc_BINARY_DIR}"
    OUTPUT_VARIABLE bazel_output OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE bazel_result)
  if(NOT bazel_result EQUAL 0 OR NOT bazel_output STREQUAL "bazel ${bazel_version}")
    message(FATAL_ERROR
      "GOOGLE_TCMALLOC_BAZEL must point to Bazel ${bazel_version} or Bazelisk")
  endif()
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(bazel_mode dbg)
  else()
    set(bazel_mode opt)
  endif()
  if(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    list(APPEND bazel_options --copt=-g)
  elseif(CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
    list(APPEND bazel_options --copt=-Os)
  endif()
  if(WITH_STATIC_LIBSTDCXX)
    list(APPEND bazel_options --linkopt=-static-libstdc++ --linkopt=-static-libgcc)
  endif()
  set(tcmalloc_LIBRARY "${tcmalloc_BINARY_DIR}/libceph_tcmalloc.so.0")
  include(ExternalProject)
  ExternalProject_Add(google_tcmalloc_ext
    SOURCE_DIR "${tcmalloc_BINARY_DIR}"
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ${bazel_command}
      --batch --ignore_all_rc_files
      "--output_user_root=${tcmalloc_BINARY_DIR}/bazel-root"
      build -c ${bazel_mode} --cxxopt=-std=c++20 --strip=never
      "--repo_env=CC=${CMAKE_CXX_COMPILER}"
      ${bazel_options} --symlink_prefix=ceph-bazel- //:libceph_tcmalloc.so.0
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
      "${tcmalloc_BINARY_DIR}/ceph-bazel-bin/libceph_tcmalloc.so.0"
      "${tcmalloc_LIBRARY}"
    BUILD_IN_SOURCE TRUE
    BUILD_ALWAYS TRUE
    BUILD_BYPRODUCTS "${tcmalloc_LIBRARY}"
    INSTALL_COMMAND ""
    LOG_BUILD ON
    LOG_OUTPUT_ON_FAILURE ON)
  add_library(google_tcmalloc SHARED IMPORTED)
  add_dependencies(google_tcmalloc google_tcmalloc_ext)
  set_target_properties(google_tcmalloc PROPERTIES
    IMPORTED_LOCATION "${tcmalloc_LIBRARY}"
    IMPORTED_SONAME libceph_tcmalloc.so.0)
  install(FILES "${tcmalloc_LIBRARY}" DESTINATION ${CMAKE_INSTALL_LIBDIR}/ceph)
endfunction()
