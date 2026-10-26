macro(build_spdk)
  if(LINUX)
    find_package(aio REQUIRED)
    find_package(uuid REQUIRED)
  endif()
  find_package(NUMA REQUIRED)
  include(FindMake)
  find_make("MAKE_EXECUTABLE" "make_cmd")

  set(spdk_CFLAGS "-fPIC")
  include(CheckCCompilerFlag)
  check_c_compiler_flag("-Wno-address-of-packed-member" HAVE_WARNING_ADDRESS_OF_PACKED_MEMBER)
  if(HAVE_WARNING_ADDRESS_OF_PACKED_MEMBER)
    string(APPEND spdk_CFLAGS " -Wno-address-of-packed-member")
  endif()
  check_c_compiler_flag("-Wno-unused-but-set-variable"
    HAVE_UNUSED_BUT_SET_VARIABLE)
  if(HAVE_UNUSED_BUT_SET_VARIABLE)
    string(APPEND spdk_CFLAGS " -Wno-unused-but-set-variable")
  endif()

  include(ExternalProject)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64|AMD64")
    # a safer option than relying on the build host's arch
    set(target_arch core2)
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm64")
    # likewise, rather than SPDK's default of tuning for the build host
    set(target_arch generic)
  else()
    # default arch used by SPDK
    set(target_arch native)
  endif()

  set(source_dir "${CMAKE_SOURCE_DIR}/src/spdk")
  set(dpdk_lib_dir "${source_dir}/dpdk/build/lib")
  # static link closure of spdk_nvme, spdk_env_dpdk and spdk_sock_posix
  foreach(c nvme keyring sock sock_posix dma env_dpdk trace rpc jsonrpc json util log)
    set(lib_path "${source_dir}/build/lib/${CMAKE_STATIC_LIBRARY_PREFIX}spdk_${c}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    list(APPEND spdk_libs "${lib_path}")
  endforeach()
  foreach(c eal kvargs log telemetry argparse mempool_ring mempool ring
      bus_pci pci power timer ethdev net mbuf meter power_acpi
      power_amd_pstate power_cppc power_intel_pstate power_intel_uncore
      power_kvm_vm)
    set(lib_path "${dpdk_lib_dir}/${CMAKE_STATIC_LIBRARY_PREFIX}rte_${c}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    list(APPEND spdk_libs "${lib_path}")
  endforeach()
  set(isal_lib "${source_dir}/isa-l/.libs/libisal${CMAKE_STATIC_LIBRARY_SUFFIX}")

  ExternalProject_Add(spdk-ext
    SOURCE_DIR ${source_dir}
    # only the NVMe initiator is used
    CONFIGURE_COMMAND env CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ./configure
      --disable-tests
      --disable-unit-tests
      --disable-examples
      --disable-apps
      --without-vhost
      --without-virtio
      --without-nvme-cuse
      --without-fsdev
      --target-arch=${target_arch}
    # unset $CFLAGS, otherwise it will interfere with how SPDK sets
    # its include directory.
    # unset $LDFLAGS, otherwise SPDK will fail to mock some functions.
    # LOG_BUILD runs this from a script, where $(MAKE) is not expanded.
    BUILD_COMMAND env -i PATH=$ENV{PATH} CC=${CMAKE_C_COMPILER} ${MAKE_EXECUTABLE} EXTRA_CFLAGS=${spdk_CFLAGS}
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS ${spdk_libs} ${isal_lib}
    INSTALL_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)
  unset(make_cmd)

  set(SPDK_INCLUDE_DIR "${source_dir}/include")
  add_library(spdk::spdk INTERFACE IMPORTED)
  add_dependencies(spdk::spdk spdk-ext)
  # workaround for https://review.spdk.io/gerrit/c/spdk/spdk/+/6798
  set_target_properties(spdk::spdk PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${SPDK_INCLUDE_DIR}
    INTERFACE_LINK_LIBRARIES
    "-Wl,--whole-archive $<JOIN:${spdk_libs}, > -Wl,--no-whole-archive;${isal_lib};OpenSSL::SSL;OpenSSL::Crypto;${NUMA_LIBRARIES};${UUID_LIBRARIES};rt;dl")
  unset(source_dir)
  unset(dpdk_lib_dir)
endmacro()
