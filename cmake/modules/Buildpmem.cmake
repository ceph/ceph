function(build_pmem)
  include(ExternalProject)
  set(PMDK_SRC "${CMAKE_BINARY_DIR}/src/nvml/src")
  set(PMDK_INCLUDE "${PMDK_SRC}/include")

  # Use debug PMDK libs in debug lib/rbd builds
  if(CMAKE_BUILD_TYPE STREQUAL Debug)
    set(PMDK_LIB_DIR "debug")
  else()
    set(PMDK_LIB_DIR "nondebug")
  endif()
  set(PMDK_LIB "${PMDK_SRC}/${PMDK_LIB_DIR}")

  ExternalProject_Add(nvml_ext
      GIT_REPOSITORY "https://github.com/ceph/nvml.git"
      GIT_TAG "dd622819dd4ee97d3920f913c70be"
      SOURCE_DIR ${CMAKE_BINARY_DIR}/src/nvml
      CONFIGURE_COMMAND ""
      # Explicitly built w/o NDCTL, otherwise if ndtcl is present on the
      # build system tests statically linking to librbd (which uses
      # libpmemobj) will not link (because we don't build the ndctl
      # static library here).
      BUILD_COMMAND $(MAKE) NDCTL_ENABLE=n
      BUILD_IN_SOURCE 1
      BUILD_BYPRODUCTS "${PMDK_LIB}/libpmem.a" "${PMDK_LIB}/libpmemobj.a"
      INSTALL_COMMAND "true")

    # libpmem
    add_library(pmem::pmem STATIC IMPORTED)
    add_dependencies(pmem::pmem nvml_ext)
    file(MAKE_DIRECTORY ${PMDK_INCLUDE})
    set_target_properties(pmem::pmem PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES ${PMDK_INCLUDE}
      IMPORTED_LOCATION "${PMDK_LIB}/libpmem.a"
      INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})

    # libpmemobj
    add_library(pmem::pmemobj STATIC IMPORTED)
    add_dependencies(pmem::pmemobj nvml_ext)
    set_target_properties(pmem::pmemobj PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES ${PMDK_INCLUDE}
      IMPORTED_LOCATION "${PMDK_LIB}/libpmemobj.a"
      INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})

endfunction()
