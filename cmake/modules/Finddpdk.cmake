# Try to find dpdk
#
# Once done, this will define
#
# dpdk_FOUND
# dpdk_INCLUDE_DIR
# dpdk_LIBRARIES

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
  pkg_check_modules(dpdk_pc QUIET libdpdk)
endif()

find_path(dpdk_INCLUDE_DIR rte_config.h
  HINTS
    ENV DPDK_DIR
    ${dpdk_pc_INCLUDE_DIRS}
  PATH_SUFFIXES dpdk include)

set(components
  bus_pci
  cmdline
  eal
  ethdev
  hash
  kvargs
  mbuf
  mempool
  mempool_ring
  mempool_stack
  pci
  pmd_af_packet
  pmd_bond
  pmd_i40e
  pmd_ixgbe
  pmd_mlx5
  pmd_ring
  pmd_vmxnet3_uio
  ring)

set(dpdk_LIBRARIES)

foreach(c ${components})
  find_library(DPDK_rte_${c}_LIBRARY rte_${c}
    HINTS
      ENV DPDK_DIR
      ${dpdk_pc_LIBRARY_DIRS}
    PATH_SUFFIXES lib)
  if(DPDK_rte_${c}_LIBRARY)
    set(dpdk_lib dpdk::${c})
    if (NOT TARGET ${dpdk_lib})
      add_library(${dpdk_lib} UNKNOWN IMPORTED)
      set_target_properties(${dpdk_lib} PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${dpdk_INCLUDE_DIR}"
        IMPORTED_LOCATION "${DPDK_rte_${c}_LIBRARY}")
      if(c STREQUAL pmd_mlx5)
        find_package(verbs QUIET)
        if(verbs_FOUND)
          target_link_libraries(${dpdk_lib} INTERFACE IBVerbs::verbs)
        endif()
      endif()
    endif()
    list(APPEND dpdk_LIBRARIES ${dpdk_lib})
  endif()
endforeach()

mark_as_advanced(dpdk_INCLUDE_DIR ${dpdk_LIBRARIES})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(dpdk DEFAULT_MSG
  dpdk_INCLUDE_DIR
  dpdk_LIBRARIES)

if(dpdk_FOUND)
  if(NOT TARGET dpdk::dpdk)
    add_library(dpdk::dpdk INTERFACE IMPORTED)
    find_package(Threads QUIET)
    set_target_properties(dpdk::dpdk PROPERTIES
      INTERFACE_LINK_LIBRARIES ${dpdk_LIBRARIES})
  endif()
endif()
