# Try to find dpdk
#
# Once done, this will define
#
# DPDK_FOUND
# DPDK_INCLUDE_DIR
# DPDK_LIBRARIES

find_path(DPDK_INCLUDE_DIR rte_config.h
  PATH_SUFFIXES dpdk)

if(DPDK_INCLUDE_DIR AND EXISTS "${DPDK_INCLUDE_DIR}/rte_version.h")
  file(STRINGS "${DPDK_INCLUDE_DIR}/rte_version.h" rte_version_h REGEX
    "^#define RTE_VER_.*")
  string(REGEX
    REPLACE "^.*RTE_VER_YEAR[ \t]+([0-9]+).*$" "\\1"
    DPDK_VERSION_MAJOR "${rte_version_h}")
  string(REGEX
    REPLACE "^.*RTE_VER_MONTH[ \t]+([0-9]+).*$" "\\1"
    DPDK_VERSION_MINOR "${rte_version_h}")
  string(REGEX
    REPLACE "^.*RTE_VER_MINOR[ \t]+([0-9]+).*$" "\\1"
    DPDK_VERSION_PATCH "${rte_version_h}")
  string(REGEX
    REPLACE "^.*RTE_VER_RELEASE[ \t]+([0-9]+).*$" "\\1"
    DPDK_VERSION_TWEAK "${rte_version_h}")
  set(DPDK_VERSION
    "${DPDK_VERSION_MAJOR}.${DPDK_VERSION_MINOR}.${DPDK_VERSION_PATCH}.${DPDK_VERSION_TWEAK}")
endif()

set(components
  cmdline hash kvargs eal mbuf mempool ring
  pmd_bond pmd_vmxnet3_uio pmd_ixgbe pmd_i40e pmd_ring pmd_af_packet)

if(NOT DPDK_VERSION VERSION_LESS "16.11")
  list(APPEND components "ethdev")
endif()

foreach(c ${components})
  find_library(DPDK_rte_${c}_LIBRARY rte_${c})
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
find_package_handle_standard_args(dpdk
  REQUIRED_VARS DPDK_INCLUDE_DIR check_LIBRARIES
  VERSION_VAR DPDK_VERSION)

if(DPDK_FOUND)
if (EXISTS ${WITH_DPDK_MLX5})
  list(APPEND check_LIBRARIES -libverbs)
endif()
  set(DPDK_LIBRARIES
    -Wl,--whole-archive ${check_LIBRARIES} -lpthread -Wl,--no-whole-archive)
endif(DPDK_FOUND)
