function(build_pmdk enable_ndctl)
  include(FindMake)
  find_make("MAKE_EXECUTABLE" "make_cmd")

  if(EXISTS "${PROJECT_SOURCE_DIR}/src/pmdk/Makefile")
    set(source_dir_args
      SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/pmdk")
  else()
    # ceph/pmdk 1.10 predates RISC-V support (its Makefile.inc aborts with
    # "unsupported architecture: riscv64"). daos-stack/pmdk carries that
    # support, so use it on riscv64.
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "^riscv64$")
      set(pmdk_git_repository https://github.com/daos-stack/pmdk.git)
      set(pmdk_git_tag "2.1.3")
    else()
      set(pmdk_git_repository https://github.com/ceph/pmdk.git)
      set(pmdk_git_tag "1.10")
    endif()
    set(source_dir_args
      SOURCE_DIR ${CMAKE_BINARY_DIR}/src/pmdk
      GIT_REPOSITORY ${pmdk_git_repository}
      GIT_TAG ${pmdk_git_tag}
      GIT_SHALLOW TRUE
      GIT_CONFIG advice.detachedHead=false)
  endif()

  set(LIBPMEM_INTERFACE_LINK_LIBRARIES Threads::Threads)
  if(${enable_ndctl})
    set(ndctl "y")
    list(APPEND LIBPMEM_INTERFACE_LINK_LIBRARIES ndctl::ndctl daxctl::daxctl)
  else()
    set(ndctl "n")
  endif()

  # Use debug PMDK libs in debug lib/rbd builds
  if(CMAKE_BUILD_TYPE STREQUAL Debug)
    set(PMDK_LIB_DIR "debug")
  else()
    set(PMDK_LIB_DIR "nondebug")
  endif()

  set(pmdk_cflags "-Wno-error -fno-lto")
  # daos-stack pmdk turns the "no NDCTL -> can't detect dirty shutdowns / bad
  # blocks" warnings into hard build errors; acknowledge them so libpmemobj
  # builds without NDCTL (matches the long-standing ceph/pmdk 1.10 behavior).
  include(ExternalProject)
  ExternalProject_Add(pmdk_ext
      ${source_dir_args}
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ${make_cmd} CC=${CMAKE_C_COMPILER} "EXTRA_CFLAGS=${pmdk_cflags}" NDCTL_ENABLE=${ndctl} BUILD_EXAMPLES=n BUILD_BENCHMARKS=n DOC=n PMEMOBJ_IGNORE_DIRTY_SHUTDOWN=y PMEMOBJ_IGNORE_BAD_BLOCKS=y
      BUILD_IN_SOURCE 1
      BUILD_BYPRODUCTS "<SOURCE_DIR>/src/${PMDK_LIB_DIR}/libpmem.a" "<SOURCE_DIR>/src/${PMDK_LIB_DIR}/libpmemobj.a"
      INSTALL_COMMAND "")
  unset(make_cmd)

  ExternalProject_Get_Property(pmdk_ext source_dir)
  set(PMDK_SRC "${source_dir}/src")
  set(PMDK_INCLUDE "${source_dir}/src/include")
  set(PMDK_LIB "${source_dir}/src/${PMDK_LIB_DIR}")

  # libpmem
  add_library(pmdk::pmem STATIC IMPORTED GLOBAL)
  add_dependencies(pmdk::pmem pmdk_ext)
  file(MAKE_DIRECTORY ${PMDK_INCLUDE})
  find_package(Threads)
  set_target_properties(pmdk::pmem PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${PMDK_INCLUDE}
    IMPORTED_LOCATION "${PMDK_LIB}/libpmem.a"
    INTERFACE_LINK_LIBRARIES "${LIBPMEM_INTERFACE_LINK_LIBRARIES}")

  # libpmemobj
  add_library(pmdk::pmemobj STATIC IMPORTED GLOBAL)
  add_dependencies(pmdk::pmemobj pmdk_ext)
  set_target_properties(pmdk::pmemobj PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${PMDK_INCLUDE}
    IMPORTED_LOCATION "${PMDK_LIB}/libpmemobj.a"
    INTERFACE_LINK_LIBRARIES "pmdk::pmem;${CMAKE_THREAD_LIBS_INIT}")
endfunction()
