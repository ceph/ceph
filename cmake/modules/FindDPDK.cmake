# Try to find DPDK
#
# Once done, this will define
#
# DPDK::dpdk
# DPDK_FOUND
# DPDK_INCLUDE_DIRS
# DPDK_LIBRARIES

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
  pkg_check_modules(DPDK QUIET libdpdk)
endif()

if(DPDK_INCLUDE_DIRS)
  # good
elseif(TARGET DPDK::dpdk)
  get_target_property(DPDK_INCLUDE_DIRS
     DPDK::dpdk INTERFACE_INCLUDE_DIRECTORIES)
else()
  find_path(DPDK_config_INCLUDE_DIR rte_config.h
    HINTS
      ENV DPDK_DIR
    PATH_SUFFIXES
      dpdk
      include)
  find_path(DPDK_common_INCLUDE_DIR rte_common.h
    HINTS
      ENV DPDK_DIR
    PATH_SUFFIXES
      dpdk
      include)
  set(DPDK_INCLUDE_DIRS "${DPDK_config_INCLUDE_DIR}")
  if(DPDK_common_INCLUDE_DIR AND NOT DPDK_config_INCLUDE_DIR STREQUAL DPDK_common_INCLUDE_DIR)
    list(APPEND DPDK_INCLUDE_DIRS "${DPDK_common_INCLUDE_DIR}")
  endif()
endif()

set(components
  bus_pci
  bus_vdev
  cfgfile
  cmdline
  eal
  ethdev
  hash
  kvargs
  mbuf
  mempool
  mempool_ring
  net
  pci
  pmd_af_packet
  pmd_bnxt
  pmd_bond
  pmd_cxgbe
  pmd_e1000
  pmd_ena
  pmd_enic
  pmd_i40e
  pmd_ixgbe
  pmd_mlx5
  pmd_nfp
  pmd_qede
  pmd_ring
  pmd_sfc_efx
  pmd_vmxnet3_uio
  pmd_hns3
  pmd_hinic
  ring
  timer)

# for collecting DPDK library targets, it will be used when defining DPDK::dpdk
set(_DPDK_libs)
# for list of DPDK library archive paths
set(DPDK_LIBRARIES "")
foreach(c ${components})
  set(DPDK_lib DPDK::${c})
  if(TARGET ${DPDK_lib})
    get_target_property(DPDK_rte_${c}_LIBRARY
      ${DPDK_lib} IMPORTED_LOCATION)
  else()
    find_library(DPDK_rte_${c}_LIBRARY rte_${c}
      HINTS
        ENV DPDK_DIR
        ${DPDK_LIBRARY_DIRS}
        PATH_SUFFIXES lib)
  endif()
  if(DPDK_rte_${c}_LIBRARY)
    if (NOT TARGET ${DPDK_lib})
      add_library(${DPDK_lib} UNKNOWN IMPORTED)
      set_target_properties(${DPDK_lib} PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${DPDK_INCLUDE_DIRS}"
        IMPORTED_LOCATION "${DPDK_rte_${c}_LIBRARY}")
      if(c STREQUAL pmd_mlx5)
        find_package(verbs QUIET)
        if(verbs_FOUND)
          target_link_libraries(${DPDK_lib} INTERFACE IBVerbs::verbs)
        endif()
      endif()
    endif()
    list(APPEND _DPDK_libs ${DPDK_lib})
    list(APPEND DPDK_LIBRARIES ${DPDK_rte_${c}_LIBRARY})
  endif()
endforeach()

mark_as_advanced(DPDK_INCLUDE_DIRS ${DPDK_LIBRARIES})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(DPDK DEFAULT_MSG
  DPDK_INCLUDE_DIRS
  DPDK_LIBRARIES)

if(DPDK_FOUND)
  if(NOT TARGET DPDK::cflags)
     if(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64|AMD64")
      set(rte_cflags "-march=core2")
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm|ARM")
      set(rte_cflags "-march=armv7-a")
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64")
      set(rte_cflags "-march=armv8-a+crc")
    endif()
    add_library(DPDK::cflags INTERFACE IMPORTED)
    if (rte_cflags)
      set_target_properties(DPDK::cflags PROPERTIES
        INTERFACE_COMPILE_OPTIONS "${rte_cflags}")
    endif()
  endif()

  if(NOT TARGET DPDK::dpdk)
    add_library(DPDK::dpdk INTERFACE IMPORTED)
    find_package(Threads QUIET)
    list(APPEND _DPDK_libs
      Threads::Threads
      DPDK::cflags
      numa)
    set_target_properties(DPDK::dpdk PROPERTIES
      INTERFACE_LINK_LIBRARIES "${_DPDK_libs}"
      INTERFACE_INCLUDE_DIRECTORIES "${DPDK_INCLUDE_DIRS}")
  endif()
endif()

unset(_DPDK_libs)
