# Try to find spdk
#
# Once done, this will define
#
# DPDK_FOUND
# DPDK_INCLUDE_DIR
# DPDK_LIBRARIES

find_path(DPDK_INCLUDE_DIR rte_config.h
  PATH_SUFFIXES dpdk)
find_library(DPDK_rte_eal_LIBRARY rte_eal)
find_library(DPDK_rte_mempool_LIBRARY rte_mempool)
find_library(DPDK_rte_ring_LIBRARY rte_ring)

mark_as_advanced(DPDK_INCLUDE_DIR
  DPDK_rte_eal_LIBRARY
  DPDK_rte_mempool_LIBRARY
  DPDK_rte_ring_LIBRARY)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(dpdk DEFAULT_MSG
  DPDK_INCLUDE_DIR
  DPDK_rte_eal_LIBRARY
  DPDK_rte_mempool_LIBRARY
  DPDK_rte_ring_LIBRARY)

if(DPDK_FOUND)
  set(DPDK_LIBRARIES
    ${DPDK_rte_eal_LIBRARY}
    ${DPDK_rte_mempool_LIBRARY}
    ${DPDK_rte_ring_LIBRARY})
endif(DPDK_FOUND)

