function(build_pmem)
  include(ExternalProject)
  set(PMDK_SRC "${CMAKE_BINARY_DIR}/src/pmdk/src")
  set(PMDK_INCLUDE "${PMDK_SRC}/include")

  set(PMDK_LIB "${PMDK_SRC}/lib")

  include(FindMake)
  find_make("MAKE_EXECUTABLE" "make_cmd")

  ExternalProject_Add(pmdk_ext
      SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/pmdk
      CONFIGURE_COMMAND ""
      # Explicitly built w/o NDCTL, otherwise if ndtcl is present on the
      # build system tests statically linking to librbd (which uses
      # libpmemobj) will not link (because we don't build the ndctl
      # static library here).
      BUILD_COMMAND ${make_cmd} CC=${CMAKE_C_COMPILER} NDCTL_ENABLE=n
      BUILD_IN_SOURCE 1
      BUILD_BYPRODUCTS "${PMDK_LIB}/libpmem.a" "${PMDK_LIB}/libpmemobj.a"
      INSTALL_COMMAND ${make_cmd} install prefix= LIB_PREFIX=lib DESTDIR=${PMDK_SRC} NDCTL_ENABLE=n)

  # libpmem
  add_library(pmem::pmem STATIC IMPORTED)
  add_dependencies(pmem::pmem pmdk_ext)
  file(MAKE_DIRECTORY ${PMDK_INCLUDE})
  set_target_properties(pmem::pmem PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${PMDK_INCLUDE}
    IMPORTED_LOCATION "${PMDK_LIB}/libpmem.a"
    INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})

  # libpmemobj
  add_library(pmem::pmemobj STATIC IMPORTED)
  add_dependencies(pmem::pmemobj pmdk_ext)
  set_target_properties(pmem::pmemobj PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${PMDK_INCLUDE}
    IMPORTED_LOCATION "${PMDK_LIB}/libpmemobj.a"
    INTERFACE_LINK_LIBRARIES "pmem::pmem;${CMAKE_THREAD_LIBS_INIT}")
endfunction()
