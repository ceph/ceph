# Try to find dpdk
#
# Once done, this will define
#
# DPDK_FOUND
# DPDK_INCLUDE_DIR
# DPDK_LIBRARIES

find_path(DPDK_INCLUDE_DIR rte_config.h
  PATH_SUFFIXES dpdk)
find_library(DPDK_rte_hash_LIBRARY rte_hash)
find_library(DPDK_rte_kvargs_LIBRARY rte_kvargs)
find_library(DPDK_rte_mbuf_LIBRARY rte_mbuf)
find_library(DPDK_ethdev_LIBRARY ethdev)
find_library(DPDK_rte_mempool_LIBRARY rte_mempool)
find_library(DPDK_rte_ring_LIBRARY rte_ring)
find_library(DPDK_rte_eal_LIBRARY rte_eal)
find_library(DPDK_rte_cmdline_LIBRARY rte_cmdline)
find_library(DPDK_rte_pmd_bond_LIBRARY rte_pmd_bond)
find_library(DPDK_rte_pmd_vmxnet3_uio_LIBRARY rte_pmd_vmxnet3_uio)
find_library(DPDK_rte_pmd_ixgbe_LIBRARY rte_pmd_ixgbe)
find_library(DPDK_rte_pmd_e1000_LIBRARY rte_pmd_e1000)
find_library(DPDK_rte_pmd_ring_LIBRARY rte_pmd_ring)
find_library(DPDK_rte_pmd_af_packet_LIBRARY rte_pmd_af_packet)

set(check_LIBRARIES
  ${DPDK_rte_hash_LIBRARY}
  ${DPDK_rte_kvargs_LIBRARY}
  ${DPDK_rte_mbuf_LIBRARY}
  ${DPDK_ethdev_LIBRARY}
  ${DPDK_rte_mempool_LIBRARY}
  ${DPDK_rte_ring_LIBRARY}
  ${DPDK_rte_eal_LIBRARY}
  ${DPDK_rte_cmdline_LIBRARY}
  ${DPDK_rte_pmd_bond_LIBRARY}
  ${DPDK_rte_pmd_vmxnet3_uio_LIBRARY}
  ${DPDK_rte_pmd_ixgbe_LIBRARY}
  ${DPDK_rte_pmd_e1000_LIBRARY}
  ${DPDK_rte_pmd_af_packet_LIBRARY})

mark_as_advanced(DPDK_INCLUDE_DIR
  DPDK_rte_hash_LIBRARY
  DPDK_rte_kvargs_LIBRARY
  DPDK_rte_mbuf_LIBRARY
  DPDK_ethdev_LIBRARY
  DPDK_rte_mempool_LIBRARY
  DPDK_rte_ring_LIBRARY
  DPDK_rte_eal_LIBRARY
  DPDK_rte_cmdline_LIBRARY
  DPDK_rte_pmd_bond_LIBRARY
  DPDK_rte_pmd_ixgbe_LIBRARY
  DPDK_rte_pmd_e1000_LIBRARY
  DPDK_rte_pmd_vmxnet3_uio_LIBRARY
  DPDK_rte_pmd_af_packet_LIBRARY)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(dpdk DEFAULT_MSG
  DPDK_INCLUDE_DIR
  check_LIBRARIES)

if(DPDK_FOUND)
  set(DPDK_LIBRARIES
    -Wl,--whole-archive ${check_LIBRARIES} -Wl,--no-whole-archive)
endif(DPDK_FOUND)
