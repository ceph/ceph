function(build_uring)
  include(FindMake)
  find_make("MAKE_EXECUTABLE" "make_cmd")

  if(EXISTS "${PROJECT_SOURCE_DIR}/src/liburing/configure")
    set(source_dir_args
      SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/liburing")
  else()
    set(source_dir_args
      SOURCE_DIR ${CMAKE_BINARY_DIR}/src/liburing
      GIT_REPOSITORY https://github.com/axboe/liburing.git
      GIT_TAG "liburing-0.7"
      GIT_SHALLOW TRUE
      GIT_CONFIG advice.detachedHead=false)
  endif()

  include(ExternalProject)
  ExternalProject_Add(liburing_ext
    ${source_dir_args}
    CONFIGURE_COMMAND env CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} <SOURCE_DIR>/configure
    BUILD_COMMAND ${make_cmd} "CFLAGS=${CMAKE_C_FLAGS} -fPIC" -C src -s
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS "<SOURCE_DIR>/src/liburing.a"
    INSTALL_COMMAND ""
    UPDATE_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)
  unset(make_cmd)

  ExternalProject_Get_Property(liburing_ext source_dir)
  set(URING_INCLUDE_DIR "${source_dir}/src/include")
  set(URING_LIBRARY_DIR "${source_dir}/src")
  set(URING_INCLUDE_DIR ${URING_INCLUDE_DIR} PARENT_SCOPE)
  set(URING_LIBRARY_DIR ${URING_LIBRARY_DIR} PARENT_SCOPE)

  add_library(uring::uring STATIC IMPORTED GLOBAL)
  add_dependencies(uring::uring liburing_ext)
  file(MAKE_DIRECTORY ${URING_INCLUDE_DIR})
  set_target_properties(uring::uring PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${URING_INCLUDE_DIR}
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${URING_LIBRARY_DIR}/liburing.a")
endfunction()
