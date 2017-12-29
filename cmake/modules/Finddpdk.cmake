# Try to find dpdk
#
# Once done, this will define
#
# DPDK_FOUND
# DPDK_INCLUDE_DIR
# DPDK_LIBRARIES

find_path(DPDK_INCLUDE_DIR rte_config.h
  PATH_SUFFIXES dpdk
  HINTS $ENV{DPDK_DIR}/include)

set(components
  pci bus_pci cmdline eal ethdev hash kvargs mbuf
  mempool mempool_ring mempool_stack
  pmd_bond pmd_vmxnet3_uio pmd_ixgbe pmd_i40e pmd_ring pmd_af_packet
  ring)

foreach(c ${components})
  find_library(DPDK_rte_${c}_LIBRARY rte_${c}
    HINTS $ENV{DPDK_DIR}/lib)
endforeach()

foreach(c ${components})
  list(APPEND check_LIBRARIES "${DPDK_rte_${c}_LIBRARY}")
endforeach()

mark_as_advanced(DPDK_INCLUDE_DIR ${check_LIBRARIES})

if (EXISTS ${WITH_DPDK_MLX5})
  find_library(DPDK_rte_pmd_mlx5_LIBRARY rte_pmd_mlx5)
  list(APPEND check_LIBRARIES ${DPDK_rte_pmd_mlx5_LIBRARY})
  mark_as_advanced(DPDK_rte_pmd_mlx5_LIBRARY)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(dpdk DEFAULT_MSG
  DPDK_INCLUDE_DIR
  check_LIBRARIES)

if(DPDK_FOUND)
  if(EXISTS ${WITH_DPDK_MLX5})
    list(APPEND check_LIBRARIES -libverbs)
  endif()
  set(DPDK_LIBRARIES
    -Wl,--whole-archive ${check_LIBRARIES} -Wl,--no-whole-archive)
endif(DPDK_FOUND)
