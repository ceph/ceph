macro(build_spdk)
  set(DPDK_DIR ${CMAKE_BINARY_DIR}/src/dpdk)
  if(NOT TARGET dpdk-ext)
    include(BuildDPDK)
    build_dpdk(${DPDK_DIR})
  endif()
  find_package(CUnit REQUIRED)
  if(LINUX)
    find_package(aio REQUIRED)
    find_package(uuid REQUIRED)
  endif()

  find_program (MAKE_EXECUTABLE NAMES make gmake)
  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    # try to inherit command line arguments passed by parent "make" job
    set(make_cmd "$(MAKE)")
  else()
    set(make_cmd "${MAKE_EXECUTABLE}")
  endif()

  include(ExternalProject)
  ExternalProject_Add(spdk-ext
    DEPENDS dpdk-ext
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/spdk
    CONFIGURE_COMMAND ./configure --with-dpdk=${DPDK_DIR}
    # unset $CFLAGS, otherwise it will interfere with how SPDK sets
    # its include directory.
    # unset $LDFLAGS, otherwise SPDK will fail to mock some functions.
    BUILD_COMMAND env -i PATH=$ENV{PATH} CC=${CMAKE_C_COMPILER} ${make_cmd} EXTRA_CFLAGS="-fPIC"
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND "true")
  unset(make_cmd)
  ExternalProject_Get_Property(spdk-ext source_dir)
  foreach(c nvme log lvol env_dpdk util)
    add_library(spdk::${c} STATIC IMPORTED)
    add_dependencies(spdk::${c} spdk-ext)
    set_target_properties(spdk::${c} PROPERTIES
      IMPORTED_LOCATION "${source_dir}/build/lib/${CMAKE_STATIC_LIBRARY_PREFIX}spdk_${c}${CMAKE_STATIC_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${source_dir}/include")
    list(APPEND SPDK_LIBRARIES spdk::${c})
  endforeach()
  set_target_properties(spdk::env_dpdk PROPERTIES
    INTERFACE_LINK_LIBRARIES "dpdk::dpdk;rt")
  set_target_properties(spdk::lvol PROPERTIES
    INTERFACE_LINK_LIBRARIES spdk::util)
  set_target_properties(spdk::util PROPERTIES
    INTERFACE_LINK_LIBRARIES ${UUID_LIBRARIES})
  set(SPDK_INCLUDE_DIR "${source_dir}/include")
  unset(source_dir)
endmacro()
